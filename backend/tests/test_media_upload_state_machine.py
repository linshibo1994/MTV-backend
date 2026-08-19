"""媒体上传持久化状态机与崩溃恢复测试。"""

from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import CheckConstraint, create_engine, delete, event, insert, literal, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session
from starlette.datastructures import UploadFile

from app.core.errors import ConflictError
from app.models import storage_reference_guard
from app.models.asset_entry import AssetEntry
from app.models.audit_event import AuditEvent
from app.models.base import Base
from app.models.generation_job import GenerationJob
from app.models.materials import Materials
from app.models.media_storage_job import MediaStorageJob
from app.models.media_upload import MediaUpload
from app.models.script import Script
from app.models.storage_reference_guard import acquire_storage_key_locks
from app.models.subject import Subject
from app.models.user import User
from app.providers.storage import StorageObjectConflictError, StorageObjectNotFoundError
from app.providers.storage_local import LocalStorageProvider
from app.services import media_storage_jobs
from app.workers import media_storage as media_storage_worker


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


class _AsyncSessionAdapter:
    def __init__(self, session: Session) -> None:
        self.session = session

    async def execute(self, statement):
        return self.session.execute(statement)

    async def get(self, *args, **kwargs):
        return self.session.get(*args, **kwargs)

    async def flush(self) -> None:
        self.session.flush()

    async def commit(self) -> None:
        self.session.commit()

    async def rollback(self) -> None:
        self.session.rollback()

    async def delete(self, row) -> None:
        self.session.delete(row)

    def add(self, row) -> None:
        self.session.add(row)

def _tables(engine) -> None:
    Base.metadata.create_all(engine)


def _seed(raw: Session, *, review_status: str = "not_designed") -> None:
    raw.add_all(
        [
            Script(
                id="script-1",
                title="剧本",
                scope="personal",
                owner_user_id="user-1",
                created_by_user_id="user-1",
            ),
            Materials(id="material-1", name="剧本", script_id="script-1"),
            Subject(
                id="subject-1",
                script_id="script-1",
                type="character",
                name="主角",
                description="描述",
                review_status=review_status,
            ),
        ]
    )
    raw.commit()


class _Storage:
    def __init__(self, raw: Session) -> None:
        self.raw = raw
        self.objects: dict[str, bytes] = {}
        self.events: list[str] = []

    async def put_file(self, key: str, path: str) -> str:
        upload = self.raw.scalar(select(MediaUpload))
        cleanup = self.raw.scalar(
            select(MediaStorageJob).where(MediaStorageJob.job_type == "delete")
        )
        assert upload is not None and upload.status == "staging"
        assert cleanup is not None and cleanup.upload_id == upload.id
        assert self.raw.scalar(select(AssetEntry)) is None
        self.events.append("durable_staging_before_put")
        self.objects[key] = Path(path).read_bytes()
        return key

    async def promote(self, source_key: str, final_key: str) -> str:
        assert self.raw.scalar(select(AssetEntry)) is None
        self.events.append("promote_before_asset")
        self.objects[final_key] = self.objects.pop(source_key)
        return final_key

    async def promote_if_absent(self, source_key: str, final_key: str) -> bool:
        if final_key in self.objects:
            return False
        await self.promote(source_key, final_key)
        return True

    async def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    async def exists(self, key: str) -> bool:
        return key in self.objects

    async def get_size(self, key: str) -> int:
        return len(self.objects[key])

    async def iter_chunks(self, key: str, **_kwargs):
        yield self.objects[key]


def test_media_upload_model_tracks_durable_ownership_and_lifecycle() -> None:
    columns = MediaUpload.__table__.c
    for name in (
        "script_id",
        "material_id",
        "subject_id",
        "asset_id",
        "temp_storage_key",
        "final_storage_key",
        "mime_type",
        "size_bytes",
        "sha256",
        "status",
        "completed_at",
    ):
        assert name in columns
    assert columns.status.default.arg == "staging"
    check_names = {
        constraint.name
        for constraint in MediaUpload.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert check_names == {"ck_media_uploads_status"}
    max_attempts = MediaStorageJob.__table__.c.max_attempts
    assert max_attempts.default is not None and max_attempts.default.arg == 8
    assert max_attempts.server_default is None


def test_media_upload_tombstone_survives_business_parent_deletion_schema() -> None:
    columns = MediaUpload.__table__.c
    assert "owner_script_id" in columns
    assert columns.owner_script_id.nullable is False
    assert columns.script_id.nullable is True
    assert columns.material_id.nullable is True
    foreign_keys = {foreign_key.parent.name: foreign_key for foreign_key in MediaUpload.__table__.foreign_keys}
    assert foreign_keys["script_id"].ondelete == "SET NULL"
    assert foreign_keys["material_id"].ondelete == "SET NULL"


@pytest.mark.parametrize(
    ("subject_id", "mime", "filename"),
    [
        (None, "image/png", "reference.png"),
        (None, "video/mp4", "clip.mp4"),
        ("subject-1", "video/mp4", "clip.mp4"),
    ],
)
def test_upload_persists_staging_before_io_and_all_media_become_authorizable(
    monkeypatch: pytest.MonkeyPatch,
    subject_id: str | None,
    mime: str,
    filename: str,
) -> None:
    async def scenario() -> None:
        from app.api.v1 import media
        from app.providers import storage as storage_module

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            storage = _Storage(raw)
            db = _AsyncSessionAdapter(raw)
            monkeypatch.setattr(
                storage_module,
                "settings",
                SimpleNamespace(
                    storage_driver="oss",
                    aliyun_oss_public_url="https://cdn-ai-public.commind.com",
                ),
            )
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: mime)

            result = await media.upload_image(
                "material-1",
                filename,
                UploadFile(BytesIO(b"validated-media"), filename=filename),
                subject_id,
                db,
                SimpleNamespace(),
            )

            upload = raw.scalar(select(MediaUpload))
            assert upload is not None and upload.status == "ready"
            assert upload.final_storage_key == result["storage_key"].lstrip("/")
            assert result["public_url"] == (
                f"https://cdn-ai-public.commind.com/{upload.final_storage_key}"
            )
            assert storage.events[:1] == ["durable_staging_before_put"]
            await media._authorize_upload_media_key(
                db,
                upload.final_storage_key,
                SimpleNamespace(),
                False,
            )

    asyncio.run(scenario())


def test_subject_asset_is_created_only_after_verified_promote(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        from app.api.v1 import media

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            storage = _Storage(raw)
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "image/png")

            result = await media.upload_image(
                "material-1",
                "reference.png",
                UploadFile(BytesIO(b"validated-image"), filename="reference.png"),
                "subject-1",
                _AsyncSessionAdapter(raw),
                SimpleNamespace(),
            )

            upload = raw.scalar(select(MediaUpload))
            entry = raw.scalar(select(AssetEntry))
            assert storage.events == ["durable_staging_before_put", "promote_before_asset"]
            assert upload is not None and upload.status == "ready"
            assert entry is not None and upload.asset_id == entry.id == result["asset_id"]
            assert raw.get(Subject, "subject-1").review_status == "pending"

    asyncio.run(scenario())


def test_permanent_promote_failure_leaves_no_candidate_or_subject_state_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        from app.api.v1 import media
        from app.providers.storage import StorageObjectNotFoundError

        class _MissingStorage(_Storage):
            async def promote(self, source_key: str, final_key: str) -> str:
                self.objects.pop(source_key, None)
                raise StorageObjectNotFoundError(source_key)

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            storage = _MissingStorage(raw)
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "image/png")

            with pytest.raises(StorageObjectNotFoundError):
                await media.upload_image(
                    "material-1",
                    "reference.png",
                    UploadFile(BytesIO(b"validated-image"), filename="reference.png"),
                    "subject-1",
                    _AsyncSessionAdapter(raw),
                    SimpleNamespace(),
                )

            upload = raw.scalar(select(MediaUpload))
            assert upload is not None and upload.status == "failed"
            assert raw.scalar(select(AssetEntry)) is None
            assert raw.get(Subject, "subject-1").review_status == "not_designed"

    asyncio.run(scenario())


def test_candidate_delete_marks_upload_deleted_and_queues_both_object_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        from app.api.v1 import media
        from app.services import review

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            db = _AsyncSessionAdapter(raw)
            storage = _Storage(raw)
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "image/png")

            result = await media.upload_image(
                "material-1",
                "reference.png",
                UploadFile(BytesIO(b"validated-image"), filename="reference.png"),
                "subject-1",
                db,
                SimpleNamespace(),
            )
            upload = raw.scalar(select(MediaUpload))
            assert upload is not None and upload.status == "ready"

            await review.remove_candidate_with_recalc(db, "subject-1", result["asset_id"])
            raw.refresh(upload)
            delete_keys = set(
                raw.scalars(
                    select(MediaStorageJob.source_key).where(
                        MediaStorageJob.job_type == "delete",
                        MediaStorageJob.status.in_(["pending", "retry"]),
                    )
                ).all()
            )
            assert upload.status == "deleted"
            assert delete_keys == {upload.temp_storage_key, upload.final_storage_key}
            with pytest.raises(Exception) as denied:
                await media._authorize_upload_media_key(
                    db,
                    upload.final_storage_key,
                    SimpleNamespace(),
                    False,
                )
            assert denied.value.status_code == 404

    asyncio.run(scenario())


def test_promote_replay_accepts_exact_final_and_removes_temp() -> None:
    async def scenario() -> None:
        payload = b"same-media"
        source_key = "upload_images/_tmp/material-1/replay.png"
        final_key = "upload_images/material-1/replay.png"
        storage = _Storage.__new__(_Storage)
        storage.raw = None
        storage.objects = {source_key: payload, final_key: payload}
        storage.events = []
        claim = media_storage_jobs.MediaStorageJobClaim(
            id="job-replay",
            idempotency_key="promote:replay",
            job_type="promote",
            source_key=source_key,
            final_key=final_key,
            asset_id=None,
            status="running",
            attempt=2,
            lease_token="lease-replay",
            expected_sha256=hashlib.sha256(payload).hexdigest(),
            expected_size_bytes=len(payload),
        )

        assert await media_storage_jobs.promote_with_integrity(storage, claim) == final_key
        assert source_key not in storage.objects
        assert storage.objects[final_key] == payload

    asyncio.run(scenario())


def test_promote_replay_rejects_unrelated_final_without_overwrite() -> None:
    async def scenario() -> None:
        payload = b"expected-media"
        unrelated = b"other-media!!"
        source_key = "upload_images/_tmp/material-1/conflict.png"
        final_key = "upload_images/material-1/conflict.png"
        storage = _Storage.__new__(_Storage)
        storage.raw = None
        storage.objects = {source_key: payload, final_key: unrelated}
        storage.events = []
        claim = media_storage_jobs.MediaStorageJobClaim(
            id="job-conflict",
            idempotency_key="promote:conflict",
            job_type="promote",
            source_key=source_key,
            final_key=final_key,
            asset_id=None,
            status="running",
            attempt=1,
            lease_token="lease-conflict",
            expected_sha256=hashlib.sha256(payload).hexdigest(),
            expected_size_bytes=len(payload),
        )

        with pytest.raises(StorageObjectConflictError):
            await media_storage_jobs.promote_with_integrity(storage, claim)
        assert storage.objects[source_key] == payload
        assert storage.objects[final_key] == unrelated

    asyncio.run(scenario())


def test_local_promote_race_never_clobbers_unrelated_final(tmp_path: Path) -> None:
    async def scenario() -> None:
        class _RacingStorage(LocalStorageProvider):
            def __init__(self, root: str) -> None:
                super().__init__(root)
                self.injected = False

            async def exists(self, key: str) -> bool:
                exists = await super().exists(key)
                if key == final_key and not exists and not self.injected:
                    self.injected = True
                    await self.put(final_key, unrelated)
                    return False
                return exists

        payload = b"expected-media"
        unrelated = b"unrelated-final"
        source_key = "upload_images/_tmp/material-1/race.png"
        final_key = "upload_images/material-1/race.png"
        storage = _RacingStorage(str(tmp_path / "storage"))
        await storage.put(source_key, payload)
        claim = media_storage_jobs.MediaStorageJobClaim(
            id="job-local-race",
            idempotency_key="promote:local-race",
            job_type="promote",
            source_key=source_key,
            final_key=final_key,
            asset_id=None,
            status="running",
            attempt=1,
            lease_token="lease-local-race",
            expected_sha256=hashlib.sha256(payload).hexdigest(),
            expected_size_bytes=len(payload),
        )

        with pytest.raises(StorageObjectConflictError):
            await media_storage_jobs.promote_with_integrity(storage, claim)
        assert await storage.get(final_key) == unrelated
        assert await storage.get(source_key) == payload

    asyncio.run(scenario())


@pytest.mark.parametrize("status", ["staging", "failed", "deleted"])
def test_non_ready_upload_tombstone_blocks_legacy_asset_fallback(
    monkeypatch: pytest.MonkeyPatch,
    status: str,
) -> None:
    async def scenario() -> None:
        from app.api.v1 import media

        authorized: list[str] = []

        async def _authorize(_db, script_id: str, _user, _require_write: bool) -> None:
            authorized.append(script_id)

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            final_key = f"upload_images/material-1/{status}.png"
            asset = AssetEntry(
                id=f"asset-{status}",
                script_id="script-1",
                subject_id="subject-1",
                kind="image",
                storage_key=f"/{final_key}",
                mime_type="image/png",
                size_bytes=4,
                source="upload_images",
                status="candidate",
            )
            raw.add(asset)
            raw.flush()
            upload_kwargs = {
                "id": f"upload-{status}",
                "script_id": "script-1",
                "material_id": "material-1",
                "subject_id": "subject-1",
                "asset_id": asset.id,
                "temp_storage_key": f"upload_images/_tmp/material-1/{status}.png",
                "final_storage_key": final_key,
                "original_filename": f"{status}.png",
                "mime_type": "image/png",
                "size_bytes": 4,
                "sha256": "d" * 64,
                "status": status,
            }
            if "owner_script_id" in MediaUpload.__table__.c:
                upload_kwargs["owner_script_id"] = "script-1"
            raw.add(MediaUpload(**upload_kwargs))
            raw.commit()
            monkeypatch.setattr(media, "_authorize_script", _authorize)

            with pytest.raises(Exception) as denied:
                await media._authorize_upload_media_key(
                    _AsyncSessionAdapter(raw), final_key, SimpleNamespace(), False
                )
            assert denied.value.status_code == 404
            assert authorized == []

    asyncio.run(scenario())


def test_ready_upload_authorizes_without_legacy_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import media

        authorized: list[str] = []

        async def _authorize(_db, script_id: str, _user, _require_write: bool) -> None:
            authorized.append(script_id)

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            upload_kwargs = {
                "id": "upload-ready-auth",
                "script_id": "script-1",
                "material_id": "material-1",
                "subject_id": None,
                "temp_storage_key": "upload_images/_tmp/material-1/ready-auth.mp4",
                "final_storage_key": "upload_images/material-1/ready-auth.mp4",
                "original_filename": "ready-auth.mp4",
                "mime_type": "video/mp4",
                "size_bytes": 4,
                "sha256": "e" * 64,
                "status": "ready",
            }
            if "owner_script_id" in MediaUpload.__table__.c:
                upload_kwargs["owner_script_id"] = "script-1"
            raw.add(MediaUpload(**upload_kwargs))
            raw.commit()
            monkeypatch.setattr(media, "_authorize_script", _authorize)

            await media._authorize_upload_media_key(
                _AsyncSessionAdapter(raw), upload_kwargs["final_storage_key"], SimpleNamespace(), False
            )
            assert authorized == ["script-1"]

    asyncio.run(scenario())


@pytest.mark.parametrize("status", ["ready", "failed", "deleted"])
def test_make_upload_cleanup_due_preserves_terminal_state_and_reactivates_intent(
    status: str,
) -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            completed_at = datetime.now(UTC) - timedelta(minutes=5)
            upload = MediaUpload(
                id=f"upload-terminal-{status}",
                script_id="script-1",
                material_id="material-1",
                subject_id=None,
                temp_storage_key=f"upload_images/_tmp/material-1/{status}.mp4",
                final_storage_key=f"upload_images/material-1/{status}.mp4",
                original_filename=f"{status}.mp4",
                mime_type="video/mp4",
                size_bytes=4,
                sha256="a" * 64,
                status=status,
                last_error_type="OriginalTerminalError",
                completed_at=completed_at,
            )
            raw.add(upload)
            raw.flush()
            cleanup = MediaStorageJob(
                id=f"cleanup-terminal-{status}",
                idempotency_key=f"upload-cleanup:{upload.id}",
                job_type="delete",
                source_key=upload.temp_storage_key,
                upload_id=upload.id,
                status="canceled",
                available_at=datetime.now(UTC) + timedelta(hours=1),
                completed_at=completed_at,
            )
            raw.add(cleanup)
            raw.commit()

            await media_storage_jobs.make_upload_cleanup_due(
                _AsyncSessionAdapter(raw),
                upload.id,
                RuntimeError("late upload failure"),
            )

            assert upload.status == status
            assert upload.last_error_type == "OriginalTerminalError"
            assert upload.completed_at == completed_at
            assert cleanup.status == "pending"
            assert cleanup.available_at <= datetime.now(UTC).replace(tzinfo=None)
            assert cleanup.completed_at is None

    asyncio.run(scenario())


def test_retry_limit_reaches_failed_terminal_state() -> None:
    async def scenario() -> None:
        class _FailingStorage:
            async def promote_if_absent(
                self, _source_key: str, _final_key: str
            ) -> bool:
                raise TimeoutError("transient failure")

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            db = _AsyncSessionAdapter(raw)
            job = await media_storage_jobs.enqueue_promote(
                db,
                idempotency_key="promote:max-attempts",
                source_key="upload_images/_tmp/material-1/max.png",
                final_key="upload_images/material-1/max.png",
                max_attempts=1,
            )
            raw.commit()

            with pytest.raises(TimeoutError):
                await media_storage_worker.process_one(db, _FailingStorage(), owner="worker-a")
            raw.refresh(job)
            assert job.status == "failed"
            assert job.attempt == job.max_attempts == 1
            assert job.completed_at is not None

    asyncio.run(scenario())


def test_terminal_upload_promote_cleans_both_keys_but_delete_does_not_respawn() -> None:
    async def scenario() -> None:
        class _FailingStorage:
            async def promote_if_absent(
                self, _source_key: str, _final_key: str
            ) -> bool:
                raise TimeoutError("promote timeout")

            async def delete(self, _key: str) -> None:
                raise TimeoutError("delete timeout")

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            upload = MediaUpload(
                id="upload-terminal-cleanup",
                script_id="script-1",
                material_id="material-1",
                subject_id=None,
                temp_storage_key="upload_images/_tmp/material-1/terminal.mp4",
                final_storage_key="upload_images/material-1/terminal.mp4",
                original_filename="terminal.mp4",
                mime_type="video/mp4",
                size_bytes=4,
                sha256="c" * 64,
                status="staging",
            )
            raw.add(upload)
            raw.flush()
            db = _AsyncSessionAdapter(raw)
            promote = await media_storage_jobs.enqueue_promote(
                db,
                idempotency_key="promote:terminal-cleanup",
                source_key=upload.temp_storage_key,
                final_key=upload.final_storage_key,
                upload_id=upload.id,
                max_attempts=1,
            )
            raw.commit()

            with pytest.raises(TimeoutError, match="promote timeout"):
                await media_storage_worker.process_one(db, _FailingStorage(), owner="worker-a")
            raw.refresh(upload)
            raw.refresh(promote)
            cleanup_jobs = list(
                raw.scalars(
                    select(MediaStorageJob).where(MediaStorageJob.job_type == "delete")
                ).all()
            )
            assert upload.status == "failed"
            assert promote.status == "failed"
            assert {job.source_key for job in cleanup_jobs} == {
                upload.temp_storage_key,
                upload.final_storage_key,
            }

            for cleanup in cleanup_jobs:
                cleanup.max_attempts = 1
            raw.commit()
            with pytest.raises(TimeoutError, match="delete timeout"):
                await media_storage_worker.process_one(db, _FailingStorage(), owner="worker-b")
            assert len(
                raw.scalars(
                    select(MediaStorageJob).where(MediaStorageJob.job_type == "delete")
                ).all()
            ) == 2

    asyncio.run(scenario())


def test_poison_job_does_not_block_next_healthy_job() -> None:
    async def scenario() -> None:
        class _StorageWithMissingPromote:
            def __init__(self) -> None:
                self.deleted: list[str] = []

            async def promote_if_absent(
                self, source_key: str, _final_key: str
            ) -> bool:
                raise StorageObjectNotFoundError(source_key)

            async def delete(self, key: str) -> None:
                self.deleted.append(key)

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            db = _AsyncSessionAdapter(raw)
            now = datetime.now(UTC).replace(tzinfo=None)
            poison = await media_storage_jobs.enqueue_job(
                db,
                idempotency_key="promote:poison",
                job_type="promote",
                source_key="upload_images/_tmp/material-1/missing.png",
                final_key="upload_images/material-1/missing.png",
                available_at=now - timedelta(seconds=2),
            )
            healthy = await media_storage_jobs.enqueue_job(
                db,
                idempotency_key="delete:healthy",
                job_type="delete",
                source_key="upload_images/material-1/old.png",
                available_at=now - timedelta(seconds=1),
            )
            raw.commit()
            storage = _StorageWithMissingPromote()

            result = await media_storage_worker.process_due_jobs(
                db, storage, owner="worker-a", limit=2
            )
            raw.refresh(poison)
            raw.refresh(healthy)
            assert result == {
                "succeeded": [healthy.id],
                "retried": [],
                "failed": [poison.id],
                "canceled": [],
            }
            assert poison.status == "failed"
            assert healthy.status == "succeeded"
            assert storage.deleted == [healthy.source_key]

    asyncio.run(scenario())


def test_reference_writer_cancels_pending_delete_and_fresh_intent_converges() -> None:
    async def scenario() -> None:
        class _RecordingStorage:
            def __init__(self) -> None:
                self.deleted: list[str] = []

            async def delete(self, key: str) -> None:
                self.deleted.append(key)

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            storage_key = "generate_images/material-1/referenced.png"
            asset = AssetEntry(
                id="asset-delete-effect-reference",
                script_id="script-1",
                subject_id="subject-1",
                kind="image",
                storage_key=storage_key,
                mime_type="image/png",
                size_bytes=1,
                source="generate_images",
            )
            job = MediaStorageJob(
                id="delete-effect-reference",
                idempotency_key="delete-effect-reference",
                job_type="delete",
                source_key=storage_key,
                asset_id=asset.id,
                status="pending",
                available_at=datetime.now(UTC) - timedelta(seconds=1),
            )
            raw.add_all([asset, job])
            raw.commit()
            db = _AsyncSessionAdapter(raw)
            storage = _RecordingStorage()
            raw.refresh(job)
            assert job.status == "canceled"

            deferred = await media_storage_worker.process_due_jobs(
                db,
                storage,
                owner="delete-effect-reference-worker",
                limit=1,
            )
            raw.refresh(job)
            assert deferred == {
                "succeeded": [],
                "retried": [],
                "failed": [],
                "canceled": [],
            }
            assert job.status == "canceled"
            assert job.attempt == 0
            assert storage.deleted == []

            asset.set_status("deleted")
            raw.commit()
            reactivated = await media_storage_jobs.enqueue_delete(
                db,
                idempotency_key=job.idempotency_key,
                source_key=storage_key,
                asset_id=asset.id,
                available_at=datetime.now(UTC) - timedelta(seconds=1),
                reactivate_canceled=True,
            )
            raw.commit()
            assert reactivated.id == job.id

            converged = await media_storage_worker.process_due_jobs(
                db,
                storage,
                owner="delete-effect-converge-worker",
                limit=1,
            )
            raw.refresh(job)
            assert converged == {
                "succeeded": [job.id],
                "retried": [],
                "failed": [],
                "canceled": [],
            }
            assert job.status == "succeeded"
            assert job.attempt == 1
            assert storage.deleted == [storage_key]

    asyncio.run(scenario())


def test_delete_worker_rolls_back_effect_failure_and_remains_retryable() -> None:
    async def scenario() -> None:
        class _FailOnceStorage:
            def __init__(self) -> None:
                self.calls = 0

            async def delete(self, _key: str) -> None:
                self.calls += 1
                if self.calls == 1:
                    raise TimeoutError("delete timeout")

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            job = MediaStorageJob(
                id="delete-effect-timeout",
                idempotency_key="delete-effect-timeout",
                job_type="delete",
                source_key="generate_images/material-1/delete-timeout.png",
                status="pending",
                available_at=datetime.now(UTC) - timedelta(seconds=1),
                max_attempts=2,
            )
            raw.add(job)
            raw.commit()
            db = _AsyncSessionAdapter(raw)
            storage = _FailOnceStorage()

            with pytest.raises(TimeoutError, match="delete timeout"):
                await media_storage_worker.process_one(
                    db,
                    storage,
                    owner="delete-effect-timeout-worker",
                )
            raw.refresh(job)
            assert job.status == "deleting"
            assert job.attempt == 1
            assert job.lease_token is None

            job.available_at = datetime.now(UTC) - timedelta(seconds=1)
            raw.commit()
            assert (
                await media_storage_worker.process_one(
                    db,
                    storage,
                    owner="delete-effect-retry-worker",
                )
                == job.id
            )
            raw.refresh(job)
            assert job.status == "succeeded"
            assert job.attempt == 2
            assert storage.calls == 2

    asyncio.run(scenario())


def test_stale_deleting_reclaim_backs_off_then_exhausts_as_tombstone() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            storage_key = "generate_images/material-1/delete-effect-exhausted.png"
            job = MediaStorageJob(
                id="delete-effect-exhausted",
                idempotency_key="delete-effect-exhausted",
                job_type="delete",
                source_key=storage_key,
                status="deleting",
                attempt=1,
                max_attempts=3,
                available_at=datetime.now(UTC) - timedelta(seconds=1),
                lease_owner="stale-delete-worker",
                lease_token="stale-delete-lease",
                lease_until=datetime.now(UTC) - timedelta(seconds=1),
            )
            raw.add(job)
            raw.commit()
            db = _AsyncSessionAdapter(raw)

            first_reclaim = await media_storage_jobs.claim_next_job(
                db,
                owner="delete-reclaim-two",
            )
            assert first_reclaim is not None
            assert first_reclaim.status == "deleting"
            assert first_reclaim.attempt == 2
            assert first_reclaim.lease_token != "stale-delete-lease"
            await db.commit()

            before_backoff = datetime.now(UTC).replace(tzinfo=None)
            assert await media_storage_jobs.mark_execution_error(
                db,
                first_reclaim,
                TimeoutError("delete outcome unknown"),
            )
            await db.commit()
            raw.refresh(job)
            assert job.status == "deleting"
            assert job.lease_token is None
            assert job.available_at >= before_backoff + timedelta(seconds=1)

            job.available_at = datetime.now(UTC) - timedelta(seconds=1)
            raw.commit()
            final_reclaim = await media_storage_jobs.claim_next_job(
                db,
                owner="delete-reclaim-three",
            )
            assert final_reclaim is not None
            assert final_reclaim.status == "deleting"
            assert final_reclaim.attempt == 3
            await db.commit()
            assert await media_storage_jobs.mark_execution_error(
                db,
                final_reclaim,
                TimeoutError("delete outcome still unknown"),
            )
            await db.commit()
            raw.refresh(job)
            assert job.status == "effect_failed"
            assert job.attempt == job.max_attempts == 3
            assert job.lease_token is None
            assert job.completed_at is not None

            raw.add(
                AssetEntry(
                    id="asset-after-effect-exhaustion",
                    script_id="script-1",
                    subject_id="subject-1",
                    kind="image",
                    storage_key=storage_key,
                    mime_type="image/png",
                    size_bytes=1,
                    source="generate_images",
                )
            )
            with pytest.raises(ConflictError, match="正在删除|已删除"):
                raw.commit()
            raw.rollback()

    asyncio.run(scenario())


def test_delete_not_found_is_idempotent_success() -> None:
    async def scenario() -> None:
        class _MissingStorage:
            async def delete(self, key: str) -> None:
                raise StorageObjectNotFoundError(key)

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            job = MediaStorageJob(
                id="delete-object-already-missing",
                idempotency_key="delete-object-already-missing",
                job_type="delete",
                source_key="generate_images/material-1/already-missing.png",
                status="pending",
                available_at=datetime.now(UTC) - timedelta(seconds=1),
            )
            raw.add(job)
            raw.commit()

            assert (
                await media_storage_worker.process_one(
                    _AsyncSessionAdapter(raw),
                    _MissingStorage(),
                    owner="delete-not-found-worker",
                )
                == job.id
            )
            raw.refresh(job)
            assert job.status == "succeeded"
            assert job.attempt == 1
            assert job.completed_at is not None

    asyncio.run(scenario())


def test_delete_commit_failure_keeps_tombstone_blocks_new_reference_and_reclaims() -> None:
    async def scenario() -> None:
        storage_key = "generate_images/material-1/delete-commit-uncertain.png"

        class _DeletingStorage:
            def __init__(self) -> None:
                self.objects = {storage_key}
                self.calls = 0

            async def delete(self, key: str) -> None:
                self.calls += 1
                self.objects.discard(key)

        class _FailCommitAfterDelete(_AsyncSessionAdapter):
            def __init__(self, session: Session, storage: _DeletingStorage) -> None:
                super().__init__(session)
                self.storage = storage
                self.failed = False

            async def commit(self) -> None:
                if not self.storage.objects and not self.failed:
                    self.failed = True
                    raise RuntimeError("simulated final commit failure")
                await super().commit()

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            job = MediaStorageJob(
                id="delete-commit-uncertain",
                idempotency_key="delete-commit-uncertain",
                job_type="delete",
                source_key=storage_key,
                status="pending",
                available_at=datetime.now(UTC) - timedelta(seconds=1),
                max_attempts=3,
            )
            raw.add(job)
            raw.commit()
            storage = _DeletingStorage()
            db = _FailCommitAfterDelete(raw, storage)

            with pytest.raises(RuntimeError, match="simulated final commit failure"):
                await media_storage_worker.process_one(
                    db,
                    storage,
                    owner="delete-commit-uncertain-worker",
                )
            raw.refresh(job)
            assert storage.objects == set()
            assert job.status == "deleting"
            assert job.lease_token is None

            raw.add(
                AssetEntry(
                    id="asset-after-delete-commit-failure",
                    script_id="script-1",
                    subject_id="subject-1",
                    kind="image",
                    storage_key=storage_key,
                    mime_type="image/png",
                    size_bytes=1,
                    source="generate_images",
                )
            )
            with pytest.raises(ConflictError, match="正在删除|已删除"):
                raw.commit()
            raw.rollback()

            job = raw.get(MediaStorageJob, job.id)
            assert job is not None
            job.available_at = datetime.now(UTC) - timedelta(seconds=1)
            raw.commit()
            assert (
                await media_storage_worker.process_one(
                    db,
                    storage,
                    owner="delete-commit-reclaim-worker",
                )
                == job.id
            )
            raw.refresh(job)
            assert job.status == "succeeded"
            assert job.attempt == 2
            assert storage.calls == 2

    asyncio.run(scenario())


def test_reference_writer_cancels_pre_effect_delete_and_rejects_tombstone() -> None:
    storage_key = "generate_images/material-1/reference-write-guard.png"
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        job = MediaStorageJob(
            id="reference-write-guard-delete",
            idempotency_key="reference-write-guard-delete",
            job_type="delete",
            source_key=storage_key,
            status="pending",
        )
        raw.add(job)
        raw.commit()

        first = AssetEntry(
            id="reference-write-before-effect",
            script_id="script-1",
            subject_id="subject-1",
            kind="image",
            storage_key=storage_key,
            mime_type="image/png",
            size_bytes=1,
            source="generate_images",
        )
        raw.add(first)
        raw.commit()
        raw.refresh(job)
        assert job.status == "canceled"

        raw.delete(first)
        raw.commit()
        job.status = "succeeded"
        job.completed_at = datetime.now(UTC)
        raw.commit()

        raw.add(
            AssetEntry(
                id="reference-write-after-effect",
                script_id="script-1",
                subject_id="subject-1",
                kind="image",
                storage_key=storage_key,
                mime_type="image/png",
                size_bytes=1,
                source="generate_images",
            )
        )
        with pytest.raises(ConflictError, match="正在删除|已删除"):
            raw.commit()
        raw.rollback()


def test_reference_guard_rejects_insert_from_select_after_tombstone() -> None:
    storage_key = "generate_images/material-1/from-select.png"
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        raw.add(
            MediaStorageJob(
                id="from-select-tombstone",
                idempotency_key="from-select-tombstone",
                job_type="delete",
                source_key=storage_key,
                status="succeeded",
                completed_at=datetime.now(UTC),
            )
        )
        raw.commit()
        statement = insert(AssetEntry).from_select(
            [
                "id",
                "script_id",
                "subject_id",
                "kind",
                "storage_key",
                "mime_type",
                "size_bytes",
                "source",
                "status",
                "is_approved",
                "superseded",
                "created_at",
                "updated_at",
            ],
            select(
                literal("asset-from-select"),
                literal("script-1"),
                literal("subject-1"),
                literal("image"),
                literal(storage_key),
                literal("image/png"),
                literal(1),
                literal("generate_images"),
                literal("candidate"),
                literal(False),
                literal(False),
                literal(datetime.now(UTC)),
                literal(datetime.now(UTC)),
            ),
            include_defaults=False,
        )

        with pytest.raises(ConflictError, match="无法安全判定|正在删除|已删除"):
            raw.execute(statement)
            raw.commit()
        raw.rollback()
        assert raw.get(AssetEntry, "asset-from-select") is None


@pytest.mark.parametrize(
    "method_name",
    ["bulk_save_objects", "bulk_insert_mappings", "bulk_update_mappings"],
)
def test_production_session_explicitly_rejects_legacy_reference_bulk_api(
    method_name: str,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _tables(engine)
    guarded_session_type = storage_reference_guard.StorageReferenceGuardedSession
    with guarded_session_type(engine, expire_on_commit=False) as raw:
        _seed(raw)
        values = {
            "id": f"asset-{method_name}",
            "script_id": "script-1",
            "subject_id": "subject-1",
            "kind": "image",
            "storage_key": f"generate_images/material-1/{method_name}.png",
            "mime_type": "image/png",
            "size_bytes": 1,
            "source": "generate_images",
        }
        with pytest.raises(ConflictError, match="legacy bulk"):
            if method_name == "bulk_save_objects":
                raw.bulk_save_objects([AssetEntry(**values)])
            elif method_name == "bulk_insert_mappings":
                raw.bulk_insert_mappings(AssetEntry, [values])
            else:
                raw.bulk_update_mappings(AssetEntry, [{"id": values["id"], **values}])


def test_reference_guard_rejects_direct_connection_write_after_tombstone() -> None:
    storage_key = "generate_images/material-1/direct-connection.png"
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        raw.add(
            MediaStorageJob(
                id="direct-connection-tombstone",
                idempotency_key="direct-connection-tombstone",
                job_type="delete",
                source_key=storage_key,
                status="succeeded",
                completed_at=datetime.now(UTC),
            )
        )
        raw.commit()

    with engine.begin() as connection:
        with pytest.raises(ConflictError, match="正在删除|已删除"):
            connection.execute(
                insert(AssetEntry).values(
                    id="asset-direct-connection",
                    script_id="script-1",
                    subject_id="subject-1",
                    kind="image",
                    storage_key=storage_key,
                    mime_type="image/png",
                    size_bytes=1,
                    source="generate_images",
                )
            )


def test_reference_guard_rejects_driver_sql_and_limits_bypass_to_explicit_connection() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _tables(engine)
    statement = "UPDATE pipeline_jobs SET output_json = '{}' WHERE 1 = 0"

    with engine.begin() as connection:
        with pytest.raises(ConflictError, match="原始 SQL"):
            connection.exec_driver_sql(statement)

        bypassed = connection.execution_options(
            **{
                storage_reference_guard.STORAGE_REFERENCE_GUARD_BYPASS: True,
            }
        )
        bypassed.exec_driver_sql(statement)


def test_reference_writes_canonicalize_scalar_and_nested_json_keys() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        asset = AssetEntry(
            id="asset-canonical-reference",
            script_id="script-1",
            subject_id="subject-1",
            kind="image",
            storage_key="////generate_images/material-1/canonical.png",
            mime_type="image/png",
            size_bytes=1,
            source="generate_images",
        )
        generation = GenerationJob(
            job_id="generation-canonical-reference",
            material_id="material-1",
            script_id="script-1",
            subject_id="subject-1",
            owner_user_id="owner-canonical-reference",
            idempotency_key="generation-canonical-reference",
            request_hash="a" * 64,
            input_version=1,
            attempt=1,
            job_type="generate_images",
            status="succeeded",
            progress=100,
            output_json={
                "nested": [
                    {"storage_key": "////generate_images/material-1/nested-canonical.png"}
                ]
            },
        )
        raw.add_all([asset, generation])
        raw.commit()
        raw.refresh(asset)
        raw.refresh(generation)

        assert asset.storage_key == "generate_images/material-1/canonical.png"
        assert generation.output_json == {
            "nested": [
                {"storage_key": "generate_images/material-1/nested-canonical.png"}
            ]
        }


def test_media_storage_empty_claim_ends_transaction() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            db = _AsyncSessionAdapter(raw)

            assert (
                await media_storage_worker.process_one(
                    db,
                    _Storage(raw),
                    owner="empty-claim-worker",
                )
                is None
            )
            assert not raw.in_transaction()

    asyncio.run(scenario())


def test_media_storage_claim_lease_covers_celery_hard_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        observed: list[int] = []

        async def _claim(_db, *, owner: str, lease_seconds: int):
            assert owner == "lease-duration-worker"
            observed.append(lease_seconds)
            return None

        class _DB:
            async def rollback(self) -> None:
                return None

        monkeypatch.setattr(media_storage_jobs, "claim_next_job", _claim)
        assert (
            await media_storage_worker.process_one(
                _DB(),
                SimpleNamespace(),
                owner="lease-duration-worker",
            )
            is None
        )
        assert observed
        assert observed[0] >= int(media_storage_worker.app.conf.task_time_limit) + 60

    asyncio.run(scenario())


@pytest.mark.parametrize("claim_mode", ["next", "by_id"])
def test_stale_running_promote_at_attempt_limit_never_reclaims_max_plus_one(
    claim_mode: str,
) -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            job = MediaStorageJob(
                id=f"stale-promote-{claim_mode}",
                idempotency_key=f"stale-promote-{claim_mode}",
                job_type="promote",
                source_key=f"upload_images/_tmp/material-1/stale-{claim_mode}.png",
                final_key=f"upload_images/material-1/stale-{claim_mode}.png",
                status="running",
                attempt=1,
                max_attempts=1,
                available_at=datetime.now(UTC) - timedelta(seconds=5),
                lease_owner="expired-worker",
                lease_token="expired-token",
                lease_until=datetime.now(UTC) - timedelta(seconds=1),
            )
            raw.add(job)
            raw.commit()
            db = _AsyncSessionAdapter(raw)

            if claim_mode == "next":
                claim = await media_storage_jobs.claim_next_job(db, owner="replacement-worker")
            else:
                claim = await media_storage_jobs.claim_job(
                    db,
                    job.id,
                    owner="replacement-worker",
                )

            assert claim is not None
            assert claim.status == "failed"
            assert claim.attempt == claim.max_attempts == 1
            raw.refresh(job)
            assert job.status == "failed"
            assert job.attempt == 1
            assert job.lease_owner is None
            assert job.lease_token is None
            assert job.lease_until is None

    asyncio.run(scenario())


@pytest.mark.parametrize("timeout_type", [SoftTimeLimitExceeded, TimeLimitExceeded])
def test_process_due_jobs_never_swallows_worker_time_limits(
    monkeypatch: pytest.MonkeyPatch,
    timeout_type: type[Exception],
) -> None:
    async def scenario() -> None:
        timeout = timeout_type(900)
        setattr(timeout, "media_storage_job_id", "timed-out-media-job")

        async def _raise_timeout(*_args, **_kwargs):
            raise timeout

        class _DB:
            async def get(self, *_args, **_kwargs):
                return SimpleNamespace(status="retry")

        monkeypatch.setattr(media_storage_worker, "process_one", _raise_timeout)
        with pytest.raises(timeout_type):
            await media_storage_worker.process_due_jobs(
                _DB(),
                SimpleNamespace(),
                owner="timeout-worker",
                limit=2,
            )

    asyncio.run(scenario())


def test_process_due_jobs_converges_after_final_commit_ack_is_lost() -> None:
    async def scenario() -> None:
        source_key = "upload_images/_tmp/material-1/ack-lost.png"
        final_key = "upload_images/material-1/ack-lost.png"

        class _Storage:
            def __init__(self) -> None:
                self.objects = {source_key: b"payload"}
                self.promote_calls = 0

            async def promote_if_absent(self, source: str, final: str) -> bool:
                self.promote_calls += 1
                self.objects[final] = self.objects.pop(source)
                return True

        class _CommitAppliedButAckLost(_AsyncSessionAdapter):
            def __init__(self, session: Session, job_id: str) -> None:
                super().__init__(session)
                self.job_id = job_id
                self.raised = False

            async def commit(self) -> None:
                self.session.commit()
                job = self.session.get(MediaStorageJob, self.job_id)
                if job is not None and job.status == "succeeded" and not self.raised:
                    self.raised = True
                    raise ConnectionError("final commit acknowledgement lost")

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            upload = MediaUpload(
                id="upload-ack-lost",
                script_id="script-1",
                material_id="material-1",
                subject_id=None,
                temp_storage_key=source_key,
                final_storage_key=final_key,
                original_filename="ack-lost.png",
                mime_type="image/png",
                size_bytes=7,
                sha256=hashlib.sha256(b"payload").hexdigest(),
                status="staging",
            )
            raw.add(upload)
            raw.flush()
            job = await media_storage_jobs.enqueue_promote(
                _AsyncSessionAdapter(raw),
                idempotency_key="promote:ack-lost",
                source_key=source_key,
                final_key=final_key,
                upload_id=upload.id,
            )
            raw.commit()
            storage = _Storage()
            db = _CommitAppliedButAckLost(raw, job.id)

            result = await media_storage_worker.process_due_jobs(
                db,
                storage,
                owner="ack-lost-worker",
                limit=1,
            )

            raw.refresh(job)
            raw.refresh(upload)
            assert result == {
                "succeeded": [job.id],
                "retried": [],
                "failed": [],
                "canceled": [],
            }
            assert job.status == "succeeded"
            assert upload.status == "ready"
            assert storage.promote_calls == 1
            assert storage.objects == {final_key: b"payload"}

    asyncio.run(scenario())


def test_process_due_jobs_reports_canceled_claim_separately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        job_id = "media-job-canceled"
        exc = ConflictError("媒体存储任务 lease 已失效")
        setattr(exc, "media_storage_job_id", job_id)

        async def _raise_canceled(*_args, **_kwargs) -> None:
            raise exc

        class _CanceledJobDB:
            async def get(self, model, requested_job_id: str, *, populate_existing: bool):
                assert model is MediaStorageJob
                assert requested_job_id == job_id
                assert populate_existing is True
                return SimpleNamespace(status="canceled")

        monkeypatch.setattr(media_storage_worker, "process_one", _raise_canceled)

        result = await media_storage_worker.process_due_jobs(
            _CanceledJobDB(),
            SimpleNamespace(),
            owner="worker-canceled",
            limit=1,
        )

        assert result == {
            "succeeded": [],
            "retried": [],
            "failed": [],
            "canceled": [job_id],
        }

    asyncio.run(scenario())


def test_recover_media_storage_jobs_returns_canceled_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processed = {
        "succeeded": ["job-succeeded"],
        "retried": ["job-retry"],
        "failed": ["job-failed"],
        "canceled": ["job-canceled"],
    }

    def _run(coroutine):
        coroutine.close()
        return processed

    monkeypatch.setattr(media_storage_worker, "run_worker_async", _run)

    assert media_storage_worker.recover_media_storage_jobs.run(limit=4) == {
        "processed": ["job-succeeded"],
        "count": 1,
        "retried": ["job-retry"],
        "failed": ["job-failed"],
        "canceled": ["job-canceled"],
    }


@pytest.mark.parametrize("entrypoint", ["api", "worker"])
def test_execution_db_error_rolls_back_before_persisting_claim_error(
    monkeypatch: pytest.MonkeyPatch,
    entrypoint: str,
) -> None:
    """结果事务失败后必须先清理 Session，再按已提交 lease 落 retry。"""

    async def scenario() -> None:
        class _Storage:
            def __init__(self) -> None:
                self.objects = {
                    "upload_images/_tmp/material-1/db-error.png": b"payload"
                }

            async def promote(self, source_key: str, final_key: str) -> str:
                self.objects[final_key] = self.objects.pop(source_key)
                return final_key

            async def promote_if_absent(self, source_key: str, final_key: str) -> bool:
                if final_key in self.objects:
                    return False
                await self.promote(source_key, final_key)
                return True

        async def _poison_result_transaction(db, claim) -> None:
            db.add(
                MediaStorageJob(
                    id=f"duplicate-{claim.id}",
                    idempotency_key=claim.idempotency_key,
                    job_type="delete",
                    source_key="upload_images/material-1/duplicate.png",
                    status="pending",
                )
            )
            await db.flush()

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            db = _AsyncSessionAdapter(raw)
            job = await media_storage_jobs.enqueue_promote(
                db,
                idempotency_key=f"promote:db-error:{entrypoint}",
                source_key="upload_images/_tmp/material-1/db-error.png",
                final_key="upload_images/material-1/db-error.png",
            )
            raw.commit()
            monkeypatch.setattr(
                media_storage_jobs,
                "finalize_upload_ready",
                _poison_result_transaction,
            )

            with pytest.raises(IntegrityError):
                if entrypoint == "api":
                    from app.api.v1 import media

                    await media._run_promote_job(db, _Storage(), job.id)
                else:
                    await media_storage_worker.process_one(
                        db,
                        _Storage(),
                        owner="db-error-worker",
                    )

            raw.expire_all()
            refreshed = raw.get(MediaStorageJob, job.id)
            assert refreshed is not None
            assert refreshed.status == "retry"
            assert refreshed.attempt == 1
            assert refreshed.last_error_type == "IntegrityError"

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("job_status", "upload_status"),
    [
        ("running", "staging"),
        ("failed", "failed"),
        ("canceled", "deleted"),
    ],
)
def test_upload_request_claim_miss_never_reports_non_ready_as_success(
    monkeypatch: pytest.MonkeyPatch,
    job_status: str,
    upload_status: str,
) -> None:
    async def scenario() -> None:
        from app.api.v1 import media

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            upload = MediaUpload(
                id=f"upload-claim-miss-{job_status}",
                script_id="script-1",
                material_id="material-1",
                subject_id=None,
                temp_storage_key=f"upload_images/_tmp/material-1/{job_status}.mp4",
                final_storage_key=f"upload_images/material-1/{job_status}.mp4",
                original_filename=f"{job_status}.mp4",
                mime_type="video/mp4",
                size_bytes=4,
                sha256="f" * 64,
                status=upload_status,
            )
            raw.add(upload)
            raw.flush()
            job = MediaStorageJob(
                id=f"promote-claim-miss-{job_status}",
                idempotency_key=f"promote:claim-miss:{job_status}",
                job_type="promote",
                source_key=upload.temp_storage_key,
                final_key=upload.final_storage_key,
                upload_id=upload.id,
                expected_sha256=upload.sha256,
                expected_size_bytes=upload.size_bytes,
                status=job_status,
                attempt=1,
                lease_owner="competing-worker" if job_status == "running" else None,
                lease_token="competing-lease" if job_status == "running" else None,
                lease_until=(
                    datetime.now(UTC) + timedelta(minutes=5)
                    if job_status == "running"
                    else None
                ),
            )
            raw.add(job)
            raw.commit()
            monkeypatch.setattr(
                media,
                "_PROMOTE_OUTCOME_WAIT_SECONDS",
                0.0,
                raising=False,
            )

            with pytest.raises(ConflictError):
                await media._run_promote_job(
                    _AsyncSessionAdapter(raw),
                    object(),
                    job.id,
                )

    asyncio.run(scenario())


def test_upload_request_claim_miss_returns_fresh_ready_asset_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        from app.api.v1 import media

        engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'claim-miss.db'}")
        _tables(engine)
        with Session(engine, expire_on_commit=False) as request_raw:
            _seed(request_raw)
            upload = MediaUpload(
                id="upload-claim-miss-ready",
                script_id="script-1",
                material_id="material-1",
                subject_id=None,
                temp_storage_key="upload_images/_tmp/material-1/ready.mp4",
                final_storage_key="upload_images/material-1/ready.mp4",
                original_filename="ready.mp4",
                mime_type="video/mp4",
                size_bytes=4,
                sha256="a" * 64,
                status="staging",
            )
            request_raw.add(upload)
            request_raw.flush()
            job = MediaStorageJob(
                id="promote-claim-miss-ready",
                idempotency_key="promote:claim-miss:ready",
                job_type="promote",
                source_key=upload.temp_storage_key,
                final_key=upload.final_storage_key,
                upload_id=upload.id,
                expected_sha256=upload.sha256,
                expected_size_bytes=upload.size_bytes,
                status="pending",
            )
            request_raw.add(job)
            request_raw.commit()
            assert request_raw.get(MediaUpload, upload.id).status == "staging"

            with Session(engine, expire_on_commit=False) as competing_raw:
                competing_upload = competing_raw.get(MediaUpload, upload.id)
                competing_job = competing_raw.get(MediaStorageJob, job.id)
                assert competing_upload is not None and competing_job is not None
                competing_upload.status = "ready"
                competing_upload.asset_id = "asset-from-competing-worker"
                competing_upload.completed_at = datetime.now(UTC)
                competing_job.status = "succeeded"
                competing_job.asset_id = competing_upload.asset_id
                competing_job.completed_at = datetime.now(UTC)
                competing_raw.commit()

            asset_id = await media._run_promote_job(
                _AsyncSessionAdapter(request_raw),
                object(),
                job.id,
            )

            assert asset_id == "asset-from-competing-worker"
            refreshed = request_raw.get(MediaUpload, upload.id)
            assert refreshed is not None
            assert refreshed.status == "ready"
            assert refreshed.asset_id == asset_id

    asyncio.run(scenario())


def test_idempotency_key_rejects_different_payload() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _tables(engine)
        with Session(engine) as raw:
            db = _AsyncSessionAdapter(raw)
            first = await media_storage_jobs.enqueue_delete(
                db,
                idempotency_key="delete:stable-key",
                source_key="upload_images/material-1/first.png",
            )
            duplicate = await media_storage_jobs.enqueue_delete(
                db,
                idempotency_key="delete:stable-key",
                source_key="upload_images/material-1/first.png",
            )
            assert duplicate.id == first.id
            with pytest.raises(ConflictError):
                await media_storage_jobs.enqueue_delete(
                    db,
                    idempotency_key="delete:stable-key",
                    source_key="upload_images/material-1/other.png",
                )

    asyncio.run(scenario())


def test_local_put_file_never_exposes_partial_target(monkeypatch, tmp_path) -> None:
    async def scenario() -> None:
        source = tmp_path / "source.bin"
        source.write_bytes(b"complete-object")
        root = tmp_path / "storage"
        storage = LocalStorageProvider(str(root))
        key = "upload_images/material-1/atomic.bin"
        target = root / key
        original_replace = os.replace

        def _fail_before_publish(source_path: str, target_path: str) -> None:
            assert Path(target_path) == target
            assert not target.exists()
            assert Path(source_path).name.startswith(".atomic.bin.part-")
            raise OSError("simulated publish failure")

        monkeypatch.setattr(os, "replace", _fail_before_publish)
        with pytest.raises(OSError, match="publish failure"):
            await storage.put_file(key, str(source))
        assert not await storage.exists(key)
        assert list(target.parent.glob(".atomic.bin.part-*")) == []

        monkeypatch.setattr(os, "replace", original_replace)
        orphan_part = target.parent / ".atomic.bin.part-interrupted"
        orphan_part.write_bytes(b"partial")
        await storage.delete(key)
        assert not orphan_part.exists()

    asyncio.run(scenario())


def test_subject_delete_cleans_all_upload_keys_and_cancels_promote() -> None:
    async def scenario() -> None:
        from app.crud import asset as asset_crud

        engine = create_engine("sqlite+pysqlite:///:memory:")

        @event.listens_for(engine, "connect")
        def _enable_foreign_keys(connection, _record) -> None:
            connection.execute("PRAGMA foreign_keys=ON")

        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            asset = AssetEntry(
                id="asset-upload",
                script_id="script-1",
                subject_id="subject-1",
                kind="image",
                storage_key="/upload_images/material-1/subject.png",
                mime_type="image/png",
                size_bytes=4,
                source="upload_images",
            )
            raw.add(asset)
            raw.flush()
            upload = MediaUpload(
                id="upload-subject-delete",
                script_id="script-1",
                material_id="material-1",
                subject_id="subject-1",
                asset_id=asset.id,
                temp_storage_key="upload_images/_tmp/material-1/subject.png",
                final_storage_key="upload_images/material-1/subject.png",
                original_filename="subject.png",
                mime_type="image/png",
                size_bytes=4,
                sha256="a" * 64,
                status="staging",
            )
            raw.add(upload)
            raw.flush()
            promote = MediaStorageJob(
                id="promote-subject-delete",
                idempotency_key="upload-promote:upload-subject-delete",
                job_type="promote",
                source_key=upload.temp_storage_key,
                final_key=upload.final_storage_key,
                upload_id=upload.id,
                status="pending",
            )
            raw.add(promote)
            raw.commit()

            await asset_crud.delete_subject(
                _AsyncSessionAdapter(raw), raw.get(Subject, "subject-1")
            )
            raw.commit()
            raw.refresh(upload)
            raw.refresh(promote)
            delete_keys = set(
                raw.scalars(
                    select(MediaStorageJob.source_key).where(
                        MediaStorageJob.job_type == "delete"
                    )
                ).all()
            )
            assert upload.status == "deleted"
            assert promote.status == "canceled"
            assert delete_keys == {upload.temp_storage_key, upload.final_storage_key}
            assert raw.get(Subject, "subject-1") is None

    asyncio.run(scenario())


def test_script_delete_collects_subjectless_upload_temp_and_final_keys() -> None:
    async def scenario() -> None:
        from app.crud import script as script_crud

        engine = create_engine("sqlite+pysqlite:///:memory:")

        @event.listens_for(engine, "connect")
        def _enable_foreign_keys(connection, _record) -> None:
            connection.execute("PRAGMA foreign_keys=ON")

        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            upload = MediaUpload(
                id="upload-script-delete",
                script_id="script-1",
                material_id="material-1",
                subject_id=None,
                temp_storage_key="upload_images/_tmp/material-1/subjectless.mp4",
                final_storage_key="upload_images/material-1/subjectless.mp4",
                original_filename="subjectless.mp4",
                mime_type="video/mp4",
                size_bytes=4,
                sha256="b" * 64,
                status="ready",
            )
            raw.add(upload)
            raw.commit()

            await script_crud.delete_script(
                _AsyncSessionAdapter(raw), raw.get(Script, "script-1")
            )
            delete_keys = set(
                raw.scalars(
                    select(MediaStorageJob.source_key).where(
                        MediaStorageJob.job_type == "delete"
                    )
                ).all()
            )
            assert delete_keys == {upload.temp_storage_key, upload.final_storage_key}
            assert raw.get(Script, "script-1") is None

    asyncio.run(scenario())


def test_script_delete_retains_deleted_upload_tombstone_and_job_links() -> None:
    async def scenario() -> None:
        from app.crud import script as script_crud

        engine = create_engine("sqlite+pysqlite:///:memory:")

        @event.listens_for(engine, "connect")
        def _enable_foreign_keys(connection, _record) -> None:
            connection.execute("PRAGMA foreign_keys=ON")

        _tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            _seed(raw)
            upload_kwargs = {
                "id": "upload-script-tombstone",
                "script_id": "script-1",
                "material_id": "material-1",
                "subject_id": None,
                "temp_storage_key": "upload_images/_tmp/material-1/tombstone.mp4",
                "final_storage_key": "upload_images/material-1/tombstone.mp4",
                "original_filename": "tombstone.mp4",
                "mime_type": "video/mp4",
                "size_bytes": 4,
                "sha256": "f" * 64,
                "status": "staging",
            }
            if "owner_script_id" in MediaUpload.__table__.c:
                upload_kwargs["owner_script_id"] = "script-1"
            upload = MediaUpload(**upload_kwargs)
            raw.add(upload)
            raw.flush()
            promote = MediaStorageJob(
                id="promote-script-tombstone",
                idempotency_key="upload-promote:upload-script-tombstone",
                job_type="promote",
                source_key=upload.temp_storage_key,
                final_key=upload.final_storage_key,
                upload_id=upload.id,
                status="pending",
            )
            raw.add(promote)
            raw.commit()

            await script_crud.delete_script(
                _AsyncSessionAdapter(raw), raw.get(Script, "script-1")
            )

            raw.expire_all()
            retained = raw.get(MediaUpload, upload.id)
            raw.refresh(promote)
            delete_jobs = list(
                raw.scalars(
                    select(MediaStorageJob).where(MediaStorageJob.job_type == "delete")
                ).all()
            )
            assert retained is not None
            assert retained.status == "deleted"
            assert retained.owner_script_id == "script-1"
            assert retained.script_id is None
            assert retained.material_id is None
            assert promote.status == "canceled"
            assert promote.upload_id == retained.id
            assert {job.source_key for job in delete_jobs} == {
                retained.temp_storage_key,
                retained.final_storage_key,
            }
            assert {job.upload_id for job in delete_jobs} == {retained.id}

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 结果提交/剧本删除锁序测试",
)
@pytest.mark.parametrize("result_kind", ["success", "failure"])
def test_postgres_upload_result_and_script_delete_use_consistent_lock_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    result_kind: str,
) -> None:
    async def scenario() -> None:
        from app.crud import script as script_crud

        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = hashlib.sha256(os.urandom(16)).hexdigest()[:12]
        script_id = f"s-result-delete-{result_kind}-{suffix}"
        material_id = f"m-result-delete-{result_kind}-{suffix}"
        upload_id = f"u-result-delete-{result_kind}-{suffix}"
        temp_key = f"upload_images/_tmp/{material_id}/result.mp4"
        final_key = f"upload_images/{material_id}/result.mp4"
        payload = f"result-delete-{result_kind}".encode()
        storage = LocalStorageProvider(str(tmp_path / f"storage-{result_kind}"))
        upload_locked = asyncio.Event()
        release_result = asyncio.Event()
        delete_locked_job = asyncio.Event()
        release_delete = asyncio.Event()
        result_task: asyncio.Task[None] | None = None
        delete_task: asyncio.Task[None] | None = None
        job_lock_probe: asyncio.Task[bool] | None = None
        upload_wait_probe: asyncio.Task[None] | None = None
        promote_id: str | None = None
        result_backend_pid: int | None = None
        delete_backend_pid: int | None = None
        result_pid_ready = asyncio.Event()
        delete_pid_ready = asyncio.Event()
        original_cancel = media_storage_jobs.cancel_promotes_for_delete

        async def _pause_delete_after_job_lock(*args, **kwargs) -> set[str]:
            keys = await original_cancel(*args, **kwargs)
            delete_locked_job.set()
            await release_delete.wait()
            return keys

        monkeypatch.setattr(
            media_storage_jobs,
            "cancel_promotes_for_delete",
            _pause_delete_after_job_lock,
        )

        async def _commit_result(claim: media_storage_jobs.MediaStorageJobClaim) -> None:
            nonlocal result_backend_pid
            async with session_factory() as db:
                result_backend_pid = int(
                    (await db.execute(text("SELECT pg_backend_pid()"))).scalar_one()
                )
                result_pid_ready.set()
                if result_kind == "success":
                    asset_id = await media_storage_jobs.finalize_upload_ready(db, claim)
                else:
                    upload = await db.get(MediaUpload, upload_id, with_for_update=True)
                    assert upload is not None and upload.status == "staging"
                    asset_id = None
                upload_locked.set()
                await release_result.wait()
                if result_kind == "success":
                    assert await media_storage_jobs.mark_succeeded(
                        db,
                        claim.id,
                        claim.lease_token,
                        asset_id=asset_id,
                    )
                else:
                    assert await media_storage_jobs.mark_execution_error(
                        db,
                        claim,
                        ValueError("模拟永久晋升失败"),
                    )
                await db.commit()

        async def _delete_script() -> None:
            nonlocal delete_backend_pid
            async with session_factory() as db:
                delete_backend_pid = int(
                    (await db.execute(text("SELECT pg_backend_pid()"))).scalar_one()
                )
                delete_pid_ready.set()
                script = await db.get(Script, script_id)
                assert script is not None
                await script_crud.delete_script(db, script)

        async def _wait_until_delete_is_blocked_by_result() -> None:
            await asyncio.gather(result_pid_ready.wait(), delete_pid_ready.wait())
            assert result_backend_pid is not None
            assert delete_backend_pid is not None
            async with session_factory() as db:
                while True:
                    blockers = (
                        await db.execute(
                            text("SELECT pg_blocking_pids(:pid)"),
                            {"pid": delete_backend_pid},
                        )
                    ).scalar_one()
                    if result_backend_pid in blockers:
                        return
                    await asyncio.sleep(0.01)

        try:
            async with session_factory() as db:
                db.add(
                    Script(
                        id=script_id,
                        title="结果提交与剧本删除锁序",
                        scope="personal",
                        owner_user_id=f"owner-{suffix}",
                        created_by_user_id=f"owner-{suffix}",
                    )
                )
                await db.flush()
                db.add(Materials(id=material_id, name="结果锁序画布", script_id=script_id))
                await db.commit()

            async with session_factory() as db:
                await media_storage_jobs.create_upload_staging(
                    db,
                    upload_id=upload_id,
                    script_id=script_id,
                    material_id=material_id,
                    subject_id=None,
                    temp_storage_key=temp_key,
                    final_storage_key=final_key,
                    original_filename="result.mp4",
                    mime_type="video/mp4",
                    size_bytes=len(payload),
                    sha256=hashlib.sha256(payload).hexdigest(),
                )
                await db.commit()
            await storage.put(temp_key, payload)
            await storage.put(final_key, payload)

            async with session_factory() as db:
                promote = await media_storage_jobs.activate_upload_promote(db, upload_id)
                promote_id = promote.id
                await db.commit()
            async with session_factory() as db:
                claim = await media_storage_jobs.claim_job(
                    db,
                    promote_id,
                    owner=f"result-delete-worker-{suffix}",
                )
                assert claim is not None
                await db.commit()

            result_task = asyncio.create_task(_commit_result(claim))
            await asyncio.wait_for(upload_locked.wait(), timeout=5)
            delete_task = asyncio.create_task(_delete_script())

            # 旧实现先取得 Job 锁，修复后则先等待 Upload；两条路径最终都必须由
            # pg_blocking_pids 证明删除连接正等待结果事务，不能依赖固定 sleep 猜时序。
            job_lock_probe = asyncio.create_task(delete_locked_job.wait())
            upload_wait_probe = asyncio.create_task(_wait_until_delete_is_blocked_by_result())
            done, _ = await asyncio.wait(
                {job_lock_probe, upload_wait_probe},
                timeout=5,
                return_when=asyncio.FIRST_COMPLETED,
            )
            assert done
            release_delete.set()
            await asyncio.wait_for(upload_wait_probe, timeout=5)
            if not job_lock_probe.done():
                job_lock_probe.cancel()
                await asyncio.gather(job_lock_probe, return_exceptions=True)
            release_result.set()

            outcomes = await asyncio.wait_for(
                asyncio.gather(result_task, delete_task, return_exceptions=True),
                timeout=10,
            )
            assert outcomes == [None, None]

            async with session_factory() as db:
                upload = await db.get(MediaUpload, upload_id)
                promote = await db.get(MediaStorageJob, promote_id)
                assert upload is not None
                assert upload.status == "deleted"
                assert upload.script_id is None
                assert upload.material_id is None
                assert upload.owner_script_id == script_id
                assert promote is not None
                assert promote.status == ("succeeded" if result_kind == "success" else "failed")
                assert await db.get(Script, script_id) is None

            async with session_factory() as db:
                processed = await media_storage_worker.process_due_jobs(
                    db,
                    storage,
                    owner=f"result-delete-cleanup-{suffix}",
                    limit=10,
                )
                assert processed["retried"] == []
                assert processed["failed"] == []
                assert processed["canceled"] == []
            assert not await storage.exists(temp_key)
            assert not await storage.exists(final_key)
        finally:
            release_delete.set()
            release_result.set()
            for task in (result_task, delete_task, job_lock_probe, upload_wait_probe):
                if task is not None and not task.done():
                    task.cancel()
                if task is not None:
                    await asyncio.gather(task, return_exceptions=True)
            async with session_factory() as db:
                await db.execute(
                    delete(MediaStorageJob).where(MediaStorageJob.upload_id == upload_id)
                )
                await db.execute(
                    delete(MediaStorageJob).where(
                        MediaStorageJob.source_key.in_([temp_key, final_key])
                    )
                )
                await db.execute(delete(MediaUpload).where(MediaUpload.id == upload_id))
                await db.execute(delete(Materials).where(Materials.script_id == script_id))
                await db.execute(delete(Script).where(Script.id == script_id))
                await db.commit()
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 并发幂等测试",
)
def test_postgres_concurrent_enqueue_returns_single_job() -> None:
    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = hashlib.sha256(os.urandom(16)).hexdigest()[:16]
        idempotency_key = f"concurrent-delete:{suffix}"
        source_key = f"upload_images/material-1/{suffix}.png"

        async def _enqueue() -> str:
            async with session_factory() as db:
                row = await media_storage_jobs.enqueue_delete(
                    db,
                    idempotency_key=idempotency_key,
                    source_key=source_key,
                )
                await db.commit()
                return row.id

        try:
            first_id, second_id = await asyncio.gather(_enqueue(), _enqueue())
            assert first_id == second_id
            async with session_factory() as db:
                rows = list(
                    (
                        await db.execute(
                            select(MediaStorageJob).where(
                                MediaStorageJob.idempotency_key == idempotency_key
                            )
                        )
                    ).scalars().all()
                )
                assert len(rows) == 1
                await db.delete(rows[0])
                await db.commit()
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过 durable delete commit uncertainty 测试",
)
def test_postgres_delete_commit_failure_keeps_tombstone_and_reclaims() -> None:
    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = hashlib.sha256(os.urandom(16)).hexdigest()[:12]
        script_id = f"s-delete-commit-{suffix}"
        subject_id = f"sub-delete-commit-{suffix}"
        asset_id = f"asset-delete-commit-{suffix}"
        source_key = f"generate_images/delete-commit/{suffix}.png"
        idempotency_key = f"delete-commit:{suffix}"

        class _DeletingStorage:
            def __init__(self) -> None:
                self.objects = {source_key}
                self.calls = 0

            async def delete(self, key: str) -> None:
                self.calls += 1
                self.objects.discard(key)

        class _FailFinalCommit:
            def __init__(self, db, storage: _DeletingStorage) -> None:
                self.db = db
                self.storage = storage
                self.failed = False

            def __getattr__(self, name: str):
                return getattr(self.db, name)

            async def commit(self) -> None:
                if not self.storage.objects and not self.failed:
                    self.failed = True
                    raise RuntimeError("simulated postgres final commit failure")
                await self.db.commit()

        storage = _DeletingStorage()
        job_id: str | None = None
        try:
            async with session_factory() as db:
                script = Script(
                    id=script_id,
                    title="删除提交不确定性",
                    scope="personal",
                    owner_user_id=f"owner-{suffix}",
                    created_by_user_id=f"owner-{suffix}",
                )
                subject = Subject(
                    id=subject_id,
                    script_id=script_id,
                    type="character",
                    name="提交不确定性主体",
                )
                db.add_all([script, subject])
                job = await media_storage_jobs.enqueue_delete(
                    db,
                    idempotency_key=idempotency_key,
                    source_key=source_key,
                    available_at=datetime.now(UTC) - timedelta(seconds=1),
                    max_attempts=3,
                )
                job_id = job.id
                await db.commit()

            async with session_factory() as db:
                with pytest.raises(
                    RuntimeError,
                    match="simulated postgres final commit failure",
                ):
                    await media_storage_worker.process_one(
                        _FailFinalCommit(db, storage),
                        storage,
                        owner=f"delete-commit-worker-{suffix}",
                    )

            assert storage.objects == set()
            async with session_factory() as db:
                job = await db.get(MediaStorageJob, job_id)
                assert job is not None
                assert job.status == "deleting"
                assert job.attempt == 1
                assert job.lease_token is None

            async with session_factory() as db:
                db.add(
                    AssetEntry(
                        id=asset_id,
                        script_id=script_id,
                        subject_id=subject_id,
                        kind="image",
                        storage_key=source_key,
                        mime_type="image/png",
                        size_bytes=1,
                        source="generate_images",
                    )
                )
                with pytest.raises(ConflictError, match="正在删除|已删除"):
                    await db.commit()
                await db.rollback()

            async with session_factory() as db:
                job = await db.get(MediaStorageJob, job_id, with_for_update=True)
                assert job is not None
                job.available_at = datetime.now(UTC) - timedelta(seconds=1)
                await db.commit()
            async with session_factory() as db:
                assert (
                    await media_storage_worker.process_one(
                        db,
                        storage,
                        owner=f"delete-reclaim-worker-{suffix}",
                    )
                    == job_id
                )
            async with session_factory() as db:
                job = await db.get(MediaStorageJob, job_id)
                assert job is not None
                assert job.status == "succeeded"
                assert job.attempt == 2
            assert storage.calls == 2
        finally:
            async with session_factory() as db:
                await db.execute(delete(AssetEntry).where(AssetEntry.id == asset_id))
                await db.execute(
                    delete(MediaStorageJob).where(
                        MediaStorageJob.idempotency_key == idempotency_key
                    )
                )
                await db.execute(delete(Subject).where(Subject.id == subject_id))
                await db.execute(delete(Script).where(Script.id == script_id))
                await db.commit()
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过 writer/delete advisory lock 串行测试",
)
@pytest.mark.parametrize("winner", ["writer", "worker"])
def test_postgres_reference_writer_and_delete_effect_share_key_lock(winner: str) -> None:
    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = hashlib.sha256(os.urandom(16)).hexdigest()[:10]
        script_id = f"s-key-lock-{winner}-{suffix}"
        subject_id = f"sub-key-lock-{winner}-{suffix}"
        asset_id = f"asset-key-lock-{winner}-{suffix}"
        source_key = f"generate_images/key-lock/{winner}-{suffix}.png"
        idempotency_key = f"key-lock:{winner}:{suffix}"
        job_id: str | None = None

        async def _wait_for_blocker(
            observer,
            *,
            blocked_pid: int,
            blocker_pid: int,
        ) -> None:
            for _ in range(500):
                blockers = (
                    await observer.execute(
                        text("SELECT pg_blocking_pids(:pid)"),
                        {"pid": blocked_pid},
                    )
                ).scalar_one()
                if blocker_pid in blockers:
                    return
                await asyncio.sleep(0.01)
            raise AssertionError("未观察到预期 advisory lock 阻塞关系")

        try:
            async with session_factory() as db:
                script = Script(
                    id=script_id,
                    title="对象键串行",
                    scope="personal",
                    owner_user_id=f"owner-{suffix}",
                    created_by_user_id=f"owner-{suffix}",
                )
                subject = Subject(
                    id=subject_id,
                    script_id=script_id,
                    type="character",
                    name="对象键串行主体",
                )
                db.add_all([script, subject])
                job = await media_storage_jobs.enqueue_delete(
                    db,
                    idempotency_key=idempotency_key,
                    source_key=source_key,
                    available_at=datetime.now(UTC) - timedelta(seconds=1),
                )
                job_id = job.id
                await db.commit()
            async with session_factory() as db:
                claim = await media_storage_jobs.claim_next_job(
                    db,
                    owner=f"key-lock-worker-{suffix}",
                )
                assert claim is not None and claim.status == "running"
                await db.commit()

            async with (
                session_factory() as writer_db,
                session_factory() as worker_db,
                session_factory() as observer,
            ):
                writer_pid = int(
                    (await writer_db.execute(text("SELECT pg_backend_pid()"))).scalar_one()
                )
                worker_pid = int(
                    (await worker_db.execute(text("SELECT pg_backend_pid()"))).scalar_one()
                )
                writer_task: asyncio.Task[None] | None = None
                worker_task: asyncio.Task[bool] | None = None
                if winner == "writer":
                    await acquire_storage_key_locks(writer_db, {source_key})
                    writer_db.add(
                        AssetEntry(
                            id=asset_id,
                            script_id=script_id,
                            subject_id=subject_id,
                            kind="image",
                            storage_key=source_key,
                            mime_type="image/png",
                            size_bytes=1,
                            source="generate_images",
                        )
                    )
                    await writer_db.flush()
                    worker_task = asyncio.create_task(
                        media_storage_jobs.prepare_delete_effect(worker_db, claim)
                    )
                    await asyncio.wait_for(
                        _wait_for_blocker(
                            observer,
                            blocked_pid=worker_pid,
                            blocker_pid=writer_pid,
                        ),
                        timeout=10,
                    )
                    await writer_db.commit()
                    outcome = await asyncio.gather(worker_task, return_exceptions=True)
                    assert len(outcome) == 1
                    assert isinstance(outcome[0], ConflictError)
                    await worker_db.rollback()
                else:
                    assert await media_storage_jobs.prepare_delete_effect(worker_db, claim)
                    writer_db.add(
                        AssetEntry(
                            id=asset_id,
                            script_id=script_id,
                            subject_id=subject_id,
                            kind="image",
                            storage_key=source_key,
                            mime_type="image/png",
                            size_bytes=1,
                            source="generate_images",
                        )
                    )
                    writer_task = asyncio.create_task(writer_db.commit())
                    await asyncio.wait_for(
                        _wait_for_blocker(
                            observer,
                            blocked_pid=writer_pid,
                            blocker_pid=worker_pid,
                        ),
                        timeout=10,
                    )
                    await worker_db.commit()
                    outcome = await asyncio.gather(writer_task, return_exceptions=True)
                    assert len(outcome) == 1
                    assert isinstance(outcome[0], ConflictError)
                    await writer_db.rollback()

            async with session_factory() as db:
                job = await db.get(MediaStorageJob, job_id)
                asset = await db.get(AssetEntry, asset_id)
                assert job is not None
                if winner == "writer":
                    assert job.status == "canceled"
                    assert asset is not None
                else:
                    assert job.status == "deleting"
                    assert asset is None
        finally:
            async with session_factory() as db:
                await db.execute(delete(AssetEntry).where(AssetEntry.id == asset_id))
                await db.execute(
                    delete(MediaStorageJob).where(
                        MediaStorageJob.idempotency_key == idempotency_key
                    )
                )
                await db.execute(delete(Subject).where(Subject.id == subject_id))
                await db.execute(delete(Script).where(Script.id == script_id))
                await db.commit()
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过物理删除 effect-time 引用复核测试",
)
def test_postgres_delete_effect_rechecks_reference_added_after_intent(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = hashlib.sha256(os.urandom(16)).hexdigest()[:12]
        script_id = f"s-effect-reference-{suffix}"
        subject_id = f"sub-effect-reference-{suffix}"
        asset_id = f"asset-effect-reference-{suffix}"
        source_key = f"generate_images/effect-reference/{suffix}.png"
        idempotency_key = f"effect-reference-delete:{suffix}"
        storage = LocalStorageProvider(str(tmp_path / "storage"))
        job_id: str | None = None

        try:
            await storage.put(source_key, b"effect-time-reference")
            async with session_factory() as db:
                db.add(
                    Script(
                        id=script_id,
                        title="物理删除引用复核",
                        scope="personal",
                        owner_user_id=f"owner-{suffix}",
                        created_by_user_id=f"owner-{suffix}",
                    )
                )
                await db.flush()
                db.add(
                    Subject(
                        id=subject_id,
                        script_id=script_id,
                        type="character",
                        name="引用主体",
                    )
                )
                job = await media_storage_jobs.enqueue_delete(
                    db,
                    idempotency_key=idempotency_key,
                    source_key=source_key,
                )
                job_id = job.id
                await db.commit()

            # delete intent 已提交后，另一事务才新增业务引用。
            async with session_factory() as db:
                db.add(
                    AssetEntry(
                        id=asset_id,
                        script_id=script_id,
                        subject_id=subject_id,
                        kind="image",
                        storage_key=source_key,
                        mime_type="image/png",
                        size_bytes=1,
                        source="generate_images",
                    )
                )
                await db.commit()

            async with session_factory() as db:
                deferred = await media_storage_worker.process_due_jobs(
                    db,
                    storage,
                    owner=f"effect-reference-worker-{suffix}",
                    limit=1,
                )
                job = await db.get(MediaStorageJob, job_id)
                assert deferred["retried"] == []
                assert deferred["succeeded"] == []
                assert deferred["canceled"] == []
                assert job is not None and job.status == "canceled"
                assert job.attempt == 0
            assert await storage.exists(source_key)

            async with session_factory() as db:
                asset = await db.get(AssetEntry, asset_id)
                assert asset is not None
                await db.delete(asset)
                replacement = await media_storage_jobs.enqueue_delete(
                    db,
                    idempotency_key=f"{idempotency_key}:replacement",
                    source_key=source_key,
                    available_at=datetime.now(UTC) - timedelta(seconds=1),
                )
                replacement_id = replacement.id
                await db.commit()

            async with session_factory() as db:
                converged = await media_storage_worker.process_due_jobs(
                    db,
                    storage,
                    owner=f"effect-converge-worker-{suffix}",
                    limit=1,
                )
                original = await db.get(MediaStorageJob, job_id)
                replacement = await db.get(MediaStorageJob, replacement_id)
                assert converged["succeeded"] == [replacement_id]
                assert converged["retried"] == []
                assert original is not None and original.status == "canceled"
                assert replacement is not None and replacement.status == "succeeded"
                assert replacement.attempt == 1
            assert not await storage.exists(source_key)
        finally:
            async with session_factory() as db:
                await db.execute(delete(AssetEntry).where(AssetEntry.id == asset_id))
                await db.execute(
                    delete(MediaStorageJob).where(
                        MediaStorageJob.idempotency_key.in_(
                            [idempotency_key, f"{idempotency_key}:replacement"]
                        )
                    )
                )
                await db.execute(delete(Subject).where(Subject.id == subject_id))
                await db.execute(delete(Script).where(Script.id == script_id))
                await db.commit()
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过迟到上传清理状态单调性测试",
)
def test_postgres_late_temp_write_after_script_delete_keeps_deleted_tombstone(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        from app.crud import script as script_crud

        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = hashlib.sha256(os.urandom(16)).hexdigest()[:12]
        script_id = f"s-late-upload-{suffix}"
        material_id = f"m-late-upload-{suffix}"
        upload_id = f"u-late-upload-{suffix}"
        temp_key = f"upload_images/_tmp/{material_id}/late.mp4"
        final_key = f"upload_images/{material_id}/late.mp4"
        payload = b"late-upload-payload"
        storage = LocalStorageProvider(str(tmp_path / "storage"))
        deleted_completed_at: datetime | None = None

        try:
            async with session_factory() as db:
                db.add(
                    Script(
                        id=script_id,
                        title="迟到上传清理竞态",
                        scope="personal",
                        owner_user_id=f"owner-{suffix}",
                        created_by_user_id=f"owner-{suffix}",
                    )
                )
                await db.flush()
                db.add(Materials(id=material_id, name="迟到上传画布", script_id=script_id))
                await db.commit()

            async with session_factory() as db:
                await media_storage_jobs.create_upload_staging(
                    db,
                    upload_id=upload_id,
                    script_id=script_id,
                    material_id=material_id,
                    subject_id=None,
                    temp_storage_key=temp_key,
                    final_storage_key=final_key,
                    original_filename="late.mp4",
                    mime_type="video/mp4",
                    size_bytes=len(payload),
                    sha256=hashlib.sha256(payload).hexdigest(),
                )
                await db.commit()

            async with session_factory() as db:
                script = await db.get(Script, script_id)
                assert script is not None
                await script_crud.delete_script(db, script)

            # 删除事务先清过当时尚不存在的两端对象，随后才发生迟到 temp 写入。
            async with session_factory() as db:
                before_late_write = await media_storage_worker.process_due_jobs(
                    db,
                    storage,
                    owner=f"late-upload-pre-cleanup-{suffix}",
                    limit=10,
                )
                assert len(before_late_write["succeeded"]) == 2
                upload = await db.get(MediaUpload, upload_id)
                assert upload is not None and upload.status == "deleted"
                deleted_completed_at = upload.completed_at

            await storage.put(temp_key, payload)
            assert await storage.exists(temp_key)

            async with session_factory() as db:
                with pytest.raises(ConflictError) as activation_error:
                    await media_storage_jobs.activate_upload_promote(db, upload_id)
                await db.rollback()
                await media_storage_jobs.make_upload_cleanup_due(
                    db,
                    upload_id,
                    activation_error.value,
                )
                await db.commit()

            async with session_factory() as db:
                upload = await db.get(MediaUpload, upload_id)
                cleanup = (
                    await db.execute(
                        select(MediaStorageJob).where(
                            MediaStorageJob.idempotency_key == f"upload-cleanup:{upload_id}"
                        )
                    )
                ).scalar_one()
                assert upload is not None
                assert upload.status == "deleted"
                assert upload.last_error_type is None
                assert upload.completed_at == deleted_completed_at
                assert cleanup.status == "pending"
                assert cleanup.available_at <= datetime.now(UTC)
                assert cleanup.completed_at is None

            async with session_factory() as db:
                after_late_write = await media_storage_worker.process_due_jobs(
                    db,
                    storage,
                    owner=f"late-upload-final-cleanup-{suffix}",
                    limit=10,
                )
                assert len(after_late_write["succeeded"]) == 1
                upload = await db.get(MediaUpload, upload_id)
                assert upload is not None and upload.status == "deleted"
            assert not await storage.exists(temp_key)
        finally:
            async with session_factory() as db:
                await db.execute(
                    delete(MediaStorageJob).where(MediaStorageJob.upload_id == upload_id)
                )
                await db.execute(delete(MediaUpload).where(MediaUpload.id == upload_id))
                await db.execute(delete(Materials).where(Materials.script_id == script_id))
                await db.execute(delete(Script).where(Script.id == script_id))
                await db.commit()
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 主体删除锁序测试",
)
def test_postgres_subject_delete_does_not_wait_for_promote_storage_io(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        from app.crud import asset as asset_crud

        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = hashlib.sha256(os.urandom(16)).hexdigest()[:12]
        script_id = f"s-subject-lock-{suffix}"
        material_id = f"m-subject-lock-{suffix}"
        subject_id = f"sub-subject-lock-{suffix}"
        upload_id = f"u-subject-lock-{suffix}"
        temp_key = f"upload_images/_tmp/{material_id}/subject.png"
        final_key = f"upload_images/{material_id}/subject.png"
        payload = b"subject-delete-lock-order"
        storage = LocalStorageProvider(str(tmp_path / "storage"))
        promote_entered = asyncio.Event()
        release_promote = asyncio.Event()
        delete_committed = asyncio.Event()
        worker_task: asyncio.Task[str | None] | None = None
        delete_task: asyncio.Task[None] | None = None
        promote_id: str | None = None

        class _BlockingStorage:
            def __getattr__(self, name: str):
                return getattr(storage, name)

            async def promote_if_absent(self, source_key: str, target_key: str) -> bool:
                promote_entered.set()
                await release_promote.wait()
                return await storage.promote_if_absent(source_key, target_key)

        async def _run_worker() -> str | None:
            async with session_factory() as db:
                return await media_storage_worker.process_one(
                    db,
                    _BlockingStorage(),
                    owner=f"subject-lock-worker-{suffix}",
                )

        async def _delete_subject() -> None:
            async with session_factory() as db:
                subject = await asset_crud.get_subject_for_identity_update(
                    db,
                    script_id,
                    subject_id,
                )
                assert subject is not None
                await asset_crud.delete_subject(db, subject)
                await db.commit()
                delete_committed.set()

        try:
            async with session_factory() as db:
                db.add(
                    Script(
                        id=script_id,
                        title="媒体锁序测试",
                        scope="personal",
                        owner_user_id=f"owner-{suffix}",
                        created_by_user_id=f"owner-{suffix}",
                    )
                )
                await db.flush()
                db.add_all(
                    [
                        Materials(id=material_id, name="锁序画布", script_id=script_id),
                        Subject(
                            id=subject_id,
                            script_id=script_id,
                            type="character",
                            name="锁序主体",
                            review_status="not_designed",
                        ),
                    ]
                )
                await db.commit()

            async with session_factory() as db:
                await media_storage_jobs.create_upload_staging(
                    db,
                    upload_id=upload_id,
                    script_id=script_id,
                    material_id=material_id,
                    subject_id=subject_id,
                    temp_storage_key=temp_key,
                    final_storage_key=final_key,
                    original_filename="subject.png",
                    mime_type="image/png",
                    size_bytes=len(payload),
                    sha256=hashlib.sha256(payload).hexdigest(),
                )
                await db.commit()
            await storage.put(temp_key, payload)
            async with session_factory() as db:
                promote = await media_storage_jobs.activate_upload_promote(db, upload_id)
                promote_id = promote.id
                await db.commit()

            worker_task = asyncio.create_task(_run_worker())
            await asyncio.wait_for(promote_entered.wait(), timeout=5)
            delete_task = asyncio.create_task(_delete_subject())

            # 领取事务应已提交，删除只等待业务行锁，不能等待远端存储 I/O。
            await asyncio.wait_for(delete_committed.wait(), timeout=2)
            await asyncio.wait_for(delete_task, timeout=2)

            release_promote.set()
            worker_result = await asyncio.gather(worker_task, return_exceptions=True)
            assert len(worker_result) == 1
            assert isinstance(worker_result[0], ConflictError)

            async with session_factory() as db:
                upload = await db.get(MediaUpload, upload_id)
                job = await db.get(MediaStorageJob, promote_id)
                assert upload is not None and upload.status == "deleted"
                assert upload.subject_id is None
                assert job is not None and job.status == "canceled"
                assert await db.get(Subject, subject_id) is None

            async with session_factory() as db:
                cleaned = await media_storage_worker.process_due_jobs(
                    db,
                    storage,
                    owner=f"subject-lock-cleanup-{suffix}",
                    limit=10,
                )
                assert len(cleaned["succeeded"]) == 2
                assert cleaned["retried"] == []
                assert cleaned["failed"] == []
            assert not await storage.exists(temp_key)
            assert not await storage.exists(final_key)
        finally:
            release_promote.set()
            for task in (worker_task, delete_task):
                if task is not None and not task.done():
                    task.cancel()
                if task is not None:
                    await asyncio.gather(task, return_exceptions=True)
            async with session_factory() as db:
                await db.execute(
                    delete(MediaStorageJob).where(MediaStorageJob.upload_id == upload_id)
                )
                await db.execute(delete(MediaUpload).where(MediaUpload.id == upload_id))
                await db.execute(delete(AssetEntry).where(AssetEntry.script_id == script_id))
                await db.execute(delete(Subject).where(Subject.script_id == script_id))
                await db.execute(delete(Materials).where(Materials.script_id == script_id))
                await db.execute(delete(Script).where(Script.id == script_id))
                await db.commit()
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 晋升/删除竞态测试",
)
def test_postgres_running_promote_and_script_delete_preserve_tombstones_and_cleanup(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        from app.api.v1 import media
        from app.core.errors import NotFoundError
        from app.crud import script as script_crud

        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = hashlib.sha256(os.urandom(16)).hexdigest()[:12]
        script_id = f"s-promote-delete-{suffix}"
        material_id = f"m-promote-delete-{suffix}"
        running_upload_id = f"u-running-{suffix}"
        pending_upload_id = f"u-pending-{suffix}"
        running_payload = b"running-promote-payload"
        pending_payload = b"pending-promote-payload"
        storage = LocalStorageProvider(str(tmp_path / "storage"))
        promote_entered = asyncio.Event()
        release_promote = asyncio.Event()
        worker_task: asyncio.Task[str | None] | None = None
        delete_task: asyncio.Task[None] | None = None

        class _BlockingStorage:
            def __getattr__(self, name: str):
                return getattr(storage, name)

            async def promote_if_absent(self, source_key: str, final_key: str) -> bool:
                promote_entered.set()
                await release_promote.wait()
                return await storage.promote_if_absent(source_key, final_key)

        async def _create_upload(upload_id: str, filename: str, payload: bytes) -> MediaStorageJob:
            temp_key = f"upload_images/_tmp/{material_id}/{filename}"
            final_key = f"upload_images/{material_id}/{filename}"
            async with session_factory() as db:
                await media_storage_jobs.create_upload_staging(
                    db,
                    upload_id=upload_id,
                    script_id=script_id,
                    material_id=material_id,
                    subject_id=None,
                    temp_storage_key=temp_key,
                    final_storage_key=final_key,
                    original_filename=filename,
                    mime_type="video/mp4",
                    size_bytes=len(payload),
                    sha256=hashlib.sha256(payload).hexdigest(),
                )
                await db.commit()
            await storage.put(temp_key, payload)
            async with session_factory() as db:
                promote = await media_storage_jobs.activate_upload_promote(db, upload_id)
                await db.commit()
                return promote

        async def _run_worker() -> str | None:
            async with session_factory() as db:
                return await media_storage_worker.process_one(
                    db,
                    _BlockingStorage(),
                    owner=f"promote-delete-worker-{suffix}",
                )

        async def _delete_script() -> None:
            async with session_factory() as db:
                script = await db.get(Script, script_id)
                assert script is not None
                await script_crud.delete_script(db, script)

        upload_ids = {running_upload_id, pending_upload_id}
        source_prefix = f"upload_images/%/{material_id}/%"
        try:
            async with session_factory() as db:
                db.add(
                    Script(
                        id=script_id,
                        title="晋升与剧本删除竞态",
                        scope="personal",
                        owner_user_id=f"owner-{suffix}",
                        created_by_user_id=f"owner-{suffix}",
                    )
                )
                await db.flush()
                db.add(Materials(id=material_id, name="竞态测试画布", script_id=script_id))
                await db.commit()

            running_promote = await _create_upload(
                running_upload_id,
                "running.mp4",
                running_payload,
            )
            worker_task = asyncio.create_task(_run_worker())
            await asyncio.wait_for(promote_entered.wait(), timeout=5)

            pending_promote = await _create_upload(
                pending_upload_id,
                "pending.mp4",
                pending_payload,
            )
            delete_task = asyncio.create_task(_delete_script())
            await asyncio.wait_for(delete_task, timeout=2)

            release_promote.set()
            worker_result = await asyncio.gather(worker_task, return_exceptions=True)
            assert len(worker_result) == 1
            assert isinstance(worker_result[0], ConflictError)

            async with session_factory() as db:
                uploads = list(
                    (
                        await db.execute(
                            select(MediaUpload)
                            .where(MediaUpload.id.in_(upload_ids))
                            .order_by(MediaUpload.id.asc())
                        )
                    ).scalars().all()
                )
                assert len(uploads) == 2
                assert {upload.status for upload in uploads} == {"deleted"}
                assert {upload.owner_script_id for upload in uploads} == {script_id}
                assert {upload.script_id for upload in uploads} == {None}
                assert {upload.material_id for upload in uploads} == {None}
                assert await db.get(Script, script_id) is None
                assert await db.get(Materials, material_id) is None

                jobs = list(
                    (
                        await db.execute(
                            select(MediaStorageJob).where(MediaStorageJob.upload_id.in_(upload_ids))
                        )
                    ).scalars().all()
                )
                promote_by_id = {
                    job.id: job for job in jobs if job.job_type == "promote"
                }
                assert promote_by_id[running_promote.id].status == "canceled"
                assert promote_by_id[pending_promote.id].status == "canceled"
                delete_jobs = [job for job in jobs if job.job_type == "delete"]
                assert {job.upload_id for job in delete_jobs} == upload_ids
                assert {job.source_key for job in delete_jobs} == {
                    upload.temp_storage_key for upload in uploads
                } | {upload.final_storage_key for upload in uploads}

                with pytest.raises(NotFoundError):
                    await media._authorize_upload_media_key(
                        db,
                        running_promote.final_key,
                        SimpleNamespace(),
                        False,
                    )

            async with session_factory() as db:
                processed = await media_storage_worker.process_due_jobs(
                    db,
                    storage,
                    owner=f"promote-delete-cleanup-{suffix}",
                    limit=10,
                )
                assert len(processed["succeeded"]) == 4
                assert processed["retried"] == []
                assert processed["failed"] == []

            async with session_factory() as db:
                uploads = list(
                    (
                        await db.execute(select(MediaUpload).where(MediaUpload.id.in_(upload_ids)))
                    ).scalars().all()
                )
                assert {upload.status for upload in uploads} == {"deleted"}
                for upload in uploads:
                    assert not await storage.exists(upload.temp_storage_key)
                    assert not await storage.exists(upload.final_storage_key)
        finally:
            release_promote.set()
            for task in (worker_task, delete_task):
                if task is not None and not task.done():
                    try:
                        await asyncio.wait_for(task, timeout=5)
                    except Exception:
                        task.cancel()
            async with session_factory() as db:
                await db.execute(
                    delete(MediaStorageJob).where(
                        (MediaStorageJob.upload_id.in_(upload_ids))
                        | (MediaStorageJob.source_key.like(source_prefix))
                    )
                )
                await db.execute(delete(MediaUpload).where(MediaUpload.id.in_(upload_ids)))
                await db.execute(delete(Materials).where(Materials.script_id == script_id))
                await db.execute(delete(Script).where(Script.id == script_id))
                await db.commit()
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL HTTP 媒体流程",
)
def test_postgres_http_upload_range_delete_and_physical_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        from PIL import Image

        from app.api.v1 import asset_library, media

        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = hashlib.sha256(os.urandom(16)).hexdigest()[:12]
        user_id = f"u-{suffix}"
        script_id = f"s-{suffix}"
        material_id = f"m-{suffix}"
        subject_id = f"sub-{suffix}"
        user = User(
            user_id=user_id,
            username=f"media-http-{suffix}",
            password_hash="test-only",
            enabled=True,
            account_type="personal",
        )
        storage = LocalStorageProvider(str(tmp_path / "storage"))

        image_buffer = BytesIO()
        Image.new("RGB", (8, 8), (12, 34, 56)).save(image_buffer, format="PNG")
        image_bytes = image_buffer.getvalue()

        app = FastAPI()
        app.include_router(media.router, prefix="/media")
        app.include_router(asset_library.router, prefix="/v3api")

        async def _db_override():
            async with session_factory() as db:
                yield db

        app.dependency_overrides[media.get_db] = _db_override
        app.dependency_overrides[media.get_current_user] = lambda: user
        app.dependency_overrides[asset_library.get_db] = _db_override
        app.dependency_overrides[asset_library.get_current_user] = lambda: user
        monkeypatch.setattr(media, "get_storage", lambda: storage)

        try:
            async with session_factory() as db:
                db.add_all(
                    [
                        user,
                        Script(
                            id=script_id,
                            title="媒体 HTTP 状态机测试",
                            scope="personal",
                            owner_user_id=user_id,
                            created_by_user_id=user_id,
                        ),
                    ]
                )
                await db.flush()
                db.add_all(
                    [
                        Materials(id=material_id, name="测试画布", script_id=script_id),
                        Subject(
                            id=subject_id,
                            script_id=script_id,
                            type="character",
                            name="测试主体",
                            review_status="not_designed",
                        ),
                    ]
                )
                await db.commit()

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                uploaded = await client.post(
                    f"/media/upload_images/{material_id}/portrait.png",
                    params={"subject_id": subject_id},
                    files={"file": ("portrait.png", image_bytes, "image/png")},
                )
                assert uploaded.status_code == 200, uploaded.text
                result = uploaded.json()
                storage_key = result["storage_key"]
                asset_id = result["asset_id"]
                assert asset_id is not None

                ranged = await client.get(
                    f"/media{storage_key}", headers={"Range": "bytes=0-15"}
                )
                assert ranged.status_code == 206
                assert ranged.content == image_bytes[:16]
                assert ranged.headers["content-range"] == f"bytes 0-15/{len(image_bytes)}"

                removed = await client.delete(
                    f"/v3api/scripts/{script_id}/asset-library/subjects/"
                    f"{subject_id}/candidates/{asset_id}"
                )
                assert removed.status_code == 200, removed.text

                denied = await client.get(f"/media{storage_key}")
                assert denied.status_code == 404

            async with session_factory() as db:
                jobs = await media_storage_worker.process_due_jobs(
                    db, storage, owner=f"http-test-{suffix}", limit=10
                )
                upload = (
                    await db.execute(
                        select(MediaUpload).where(MediaUpload.id == result["upload_id"])
                    )
                ).scalar_one()
                assert upload.status == "deleted"
                assert len(jobs["succeeded"]) == 2
                assert not await storage.exists(upload.temp_storage_key)
                assert not await storage.exists(upload.final_storage_key)
        finally:
            async with session_factory() as db:
                await db.execute(
                    delete(MediaStorageJob).where(
                        MediaStorageJob.source_key.like(f"%/{material_id}/%")
                    )
                )
                await db.execute(
                    delete(MediaUpload).where(MediaUpload.owner_script_id == script_id)
                )
                await db.execute(delete(AssetEntry).where(AssetEntry.script_id == script_id))
                await db.execute(delete(Subject).where(Subject.script_id == script_id))
                await db.execute(delete(Materials).where(Materials.script_id == script_id))
                await db.execute(delete(Script).where(Script.id == script_id))
                await db.execute(delete(AuditEvent).where(AuditEvent.actor_user_id == user_id))
                await db.execute(delete(User).where(User.user_id == user_id))
                await db.commit()
            await engine.dispose()

    asyncio.run(scenario())
