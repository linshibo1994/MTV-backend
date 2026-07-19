"""T4 资产库审核的真实服务与事务行为测试。"""

import asyncio
import sys
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, event, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session
from starlette.datastructures import UploadFile

from app.core.errors import ConflictError
from app.crud import asset as asset_crud
from app.models.asset_entry import AssetEntry
from app.models.audit_event import AuditEvent
from app.models.base import Base
from app.models.canvas_edge import CanvasEdge
from app.models.canvas_node import CanvasNode
from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.generation_job import GenerationJob
from app.models.materials import Materials
from app.models.media_storage_job import MediaStorageJob
from app.models.media_upload import MediaUpload
from app.models.parent_subject import ParentSubject
from app.models.scene import Scene
from app.models.script import Script
from app.models.shot import Shot
from app.models.shot_group import ShotGroup
from app.models.subject import Subject
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.timeline_render_job import TimelineRenderJob
from app.models.user import User
from app.models.video_generation_job import VideoGenerationJob
from app.schemas.materials import build_subject_prompt
from app.services import reference_aggregator
from app.services import review as review_service


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


class _AsyncSessionAdapter:
    """以真实 SQLite transaction 承载 async CRUD/service 调用。"""

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


class _MemoryStorage:
    """覆盖上传完整性校验契约的内存存储替身。"""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.put_keys: list[str] = []
        self.promotions: list[tuple[str, str]] = []
        self.delete_keys: list[str] = []

    async def put(self, key: str, data: bytes) -> str:
        self.put_keys.append(key)
        self.objects[key] = data
        return key

    async def put_file(self, key: str, path: str) -> str:
        return await self.put(key, Path(path).read_bytes())

    async def promote(self, source_key: str, final_key: str) -> str:
        self.promotions.append((source_key, final_key))
        self.objects[final_key] = self.objects.pop(source_key)
        return final_key

    async def promote_if_absent(self, source_key: str, final_key: str) -> bool:
        if final_key in self.objects:
            return False
        await self.promote(source_key, final_key)
        return True

    async def delete(self, key: str) -> None:
        self.delete_keys.append(key)
        self.objects.pop(key, None)

    async def exists(self, key: str) -> bool:
        return key in self.objects

    async def get_size(self, key: str) -> int:
        return len(self.objects[key])

    async def iter_chunks(self, key: str, **_kwargs):
        yield self.objects[key]


def _asset_tables(engine) -> None:
    Base.metadata.create_all(
        engine,
        tables=[
            Script.__table__,
            User.__table__,
            Episode.__table__,
            Scene.__table__,
            ParentSubject.__table__,
            Subject.__table__,
            AssetEntry.__table__,
            GenerationJob.__table__,
            MediaStorageJob.__table__,
            MediaUpload.__table__,
            Materials.__table__,
            CanvasNode.__table__,
            CanvasEdge.__table__,
            ShotGroup.__table__,
            Shot.__table__,
            VideoGenerationJob.__table__,
            TimelineRenderJob.__table__,
        ],
    )


def _script() -> Script:
    return Script(
        id="script-1",
        title="剧本",
        scope="personal",
        owner_user_id="user-1",
        created_by_user_id="user-1",
    )


def _subject(*, review_status: str = "pending") -> Subject:
    return Subject(
        id="subject-1",
        script_id="script-1",
        type="character",
        name="主角",
        description="主角描述",
        review_status=review_status,
    )


def _seed_subject_delete_generation_graph(
    raw: Session,
    *,
    status: str = "succeeded",
    error_type: str | None = None,
    retryable: bool = False,
) -> tuple[User, GenerationJob]:
    user = User(
        user_id="user-1",
        username="subject-delete-owner",
        password_hash="unused",
    )
    account = CreditAccount(
        id="account-subject-delete",
        owner_type="user",
        owner_id=user.user_id,
        balance_points=99,
        frozen_points=0,
        status="active",
    )
    raw.add_all(
        [
            user,
            _script(),
            Materials(id="material-1", name="剧本", script_id="script-1"),
            _subject(review_status="approved"),
            account,
        ]
    )
    raw.flush()
    job = GenerationJob(
        job_id="job-subject-delete",
        material_id="material-1",
        script_id="script-1",
        subject_id="subject-1",
        owner_user_id=user.user_id,
        idempotency_key="subject-delete-generation",
        request_hash="a" * 64,
        input_version=1,
        attempt=1,
        job_type="generate_images",
        status=status,
        progress=100 if status == "succeeded" else 0,
        error_type=error_type,
        retryable=retryable,
        input_json={},
    )
    reservation = CreditReservation(
        id="reservation-subject-delete",
        account_id=account.id,
        source_type="generation_job",
        source_id=job.job_id,
        reserved_points=1,
        actual_points=1,
        status="captured",
    )
    log = CreditLog(
        id="log-subject-delete",
        account_id=account.id,
        reservation_id=reservation.id,
        source_type="generation_job",
        source_id=job.job_id,
        log_type="settle",
        points_delta=-1,
        frozen_delta=-1,
        balance_after=99,
        frozen_after=0,
    )
    dispatch = TaskDispatchOutbox(
        id="dispatch-subject-delete",
        task_type="generation.generate_images",
        job_id=job.job_id,
        attempt=job.attempt,
        payload={"job_id": job.job_id},
        status="pending",
    )
    raw.add_all([job, reservation, log, dispatch])
    raw.commit()
    return user, job


class _Result:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class _NestedConflict:
    def __init__(self, db) -> None:
        self.db = db

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        if exc_type is not None:
            self.db.nested_rollback_count += 1
        return False


class _ConcurrentEnsureSession:
    """模拟唯一约束先失败、随后重查到胜者的会话。"""

    def __init__(self, existing):
        self.existing = existing
        self.execute_count = 0
        self.outer_rollback_count = 0
        self.nested_rollback_count = 0

    async def execute(self, _statement):
        self.execute_count += 1
        return _Result(None if self.execute_count == 1 else self.existing)

    def add(self, _value):
        return None

    async def flush(self):
        raise IntegrityError("insert", {}, RuntimeError("unique"))

    def begin_nested(self):
        return _NestedConflict(self)

    async def rollback(self):
        self.outer_rollback_count += 1


def _entry(entry_id: str, *, kind: str = "image") -> AssetEntry:
    return AssetEntry(
        id=entry_id,
        script_id="script-1",
        subject_id="subject-1",
        kind=kind,
        storage_key=f"generated/{entry_id}",
        mime_type="image/png" if kind == "image" else "audio/mpeg",
        size_bytes=1,
        source="generate_images",
    )


def test_asset_entry_exposes_full_lifecycle_and_legacy_approval_columns():
    columns = AssetEntry.__table__.c

    assert "status" in columns
    assert "superseded_by_id" in columns
    assert "is_approved" in columns
    assert "superseded" in columns
    assert columns.status.default.arg == "candidate"


def test_media_storage_job_declares_durable_recovery_contract():
    columns = MediaStorageJob.__table__.c

    for name in (
        "idempotency_key",
        "job_type",
        "source_key",
        "final_key",
        "asset_id",
        "status",
        "attempt",
        "available_at",
        "lease_owner",
        "lease_token",
        "lease_until",
        "last_error_type",
        "last_error_message",
        "completed_at",
    ):
        assert name in columns

    assert columns.status.default.arg == "pending"
    assert columns.attempt.default.arg == 0


def test_media_storage_job_claim_retry_and_sanitized_failure_are_durable():
    async def scenario() -> None:
        from app.services import media_storage_jobs

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            db = _AsyncSessionAdapter(raw)
            first = await media_storage_jobs.enqueue_promote(
                db,
                idempotency_key="promote:asset-1",
                source_key="upload_images/_tmp/material-1/tmp.bin",
                final_key="upload_images/material-1/final.png",
                asset_id="asset-1",
            )
            duplicate = await media_storage_jobs.enqueue_promote(
                db,
                idempotency_key="promote:asset-1",
                source_key="upload_images/_tmp/material-1/tmp.bin",
                final_key="upload_images/material-1/final.png",
                asset_id="asset-1",
            )
            raw.commit()

            assert first.id == duplicate.id
            assert raw.query(MediaStorageJob).count() == 1

            claim = await media_storage_jobs.claim_next_job(db, owner="worker-a", lease_seconds=60)
            assert claim is not None
            assert claim.status == "running"
            assert claim.attempt == 1
            assert claim.lease_token

            second = await media_storage_jobs.claim_next_job(db, owner="worker-b", lease_seconds=60)
            assert second is None

            assert not await media_storage_jobs.mark_succeeded(db, claim.id, "wrong-token")
            await media_storage_jobs.mark_retry(
                db,
                claim.id,
                claim.lease_token,
                RuntimeError("credential marker leaked from local path"),
            )
            raw.commit()

            row = raw.get(MediaStorageJob, claim.id)
            assert row.status == "retry"
            assert row.lease_token is None
            assert row.last_error_type == "RuntimeError"
            assert row.last_error_message == "媒体存储任务执行失败"
            assert "credential" not in row.last_error_message
            assert row.available_at > row.created_at

    asyncio.run(scenario())


def test_asset_lifecycle_keeps_legacy_approval_fields_in_sync():
    entry = _entry("lifecycle-image")

    entry.set_status("approved")
    assert (entry.status, entry.is_approved, entry.superseded) == (
        "approved",
        True,
        False,
    )
    entry.set_status("superseded", superseded_by_id="replacement-image")
    assert (entry.status, entry.is_approved, entry.superseded, entry.superseded_by_id) == (
        "superseded",
        False,
        True,
        "replacement-image",
    )
    entry.set_status("deleted")
    assert (entry.status, entry.is_approved, entry.superseded) == (
        "deleted",
        False,
        False,
    )


@pytest.mark.asyncio
async def test_first_candidate_moves_not_designed_subject_to_pending():
    subject = SimpleNamespace(id="subject-1", script_id="script-1", review_status="not_designed")

    class _Db:
        def add(self, _value):
            return None

        async def flush(self):
            return None

    await asset_crud.add_candidate(
        _Db(),
        "script-1",
        subject.id,
        "image",
        "generated/image-1",
        "image/png",
        1,
        "generate_images",
        subject=subject,
    )

    assert subject.review_status == "pending"


@pytest.mark.asyncio
async def test_first_audio_candidate_moves_sound_subject_to_pending():
    subject = SimpleNamespace(
        id="sound-1",
        script_id="script-1",
        type="sound",
        review_status="not_designed",
    )

    class _Db:
        def add(self, _value):
            return None

        async def flush(self):
            return None

    await asset_crud.add_candidate(
        _Db(),
        "script-1",
        subject.id,
        "audio",
        "upload_images/material-1/voice.mp3",
        "audio/mpeg",
        1,
        "upload_images",
        subject=subject,
    )

    assert subject.review_status == "pending"


def test_serialize_subject_uses_parent_subject_relationship():
    subject = SimpleNamespace(
        id="subject-1",
        name="子形态",
        type="character",
        description=None,
        parent_id="parent-1",
        parent_subject=SimpleNamespace(name="父主体"),
        is_parent=False,
        review_status="not_designed",
        image_entry_id=None,
        image_entry=None,
        asset_entries=[],
        episode_nos=[],
        whitelist_status=None,
        created_at=None,
        updated_at=None,
    )

    assert asset_crud.serialize_subject(subject).parent_name == "父主体"


@pytest.mark.asyncio
async def test_ensure_materials_rechecks_after_unique_conflict():
    winner = SimpleNamespace(id="winner", name="剧本", script_id="script-1")
    db = _ConcurrentEnsureSession(winner)
    script = SimpleNamespace(id="script-1", title="剧本")

    materials, created = await asset_crud.ensure_materials(db, script)

    assert materials is winner
    assert created is False
    assert db.outer_rollback_count == 0
    assert db.nested_rollback_count == 1
    assert db.execute_count == 2


@pytest.mark.asyncio
async def test_second_approve_with_stale_expected_asset_returns_409(monkeypatch):
    old_id = "old-image"
    first_id = "first-image"
    second_id = "second-image"
    subject = SimpleNamespace(
        id="subject-1",
        script_id="script-1",
        name="主角",
        review_status="pending",
        image_entry_id=old_id,
        image_entry=None,
        asset_entries=[],
        parent=None,
    )
    entries = {first_id: _entry(first_id), second_id: _entry(second_id)}

    class _Db:
        async def flush(self):
            return None

        async def commit(self):
            return None

        async def rollback(self):
            return None

    db = _Db()
    monkeypatch.setattr(asset_crud, "get_subject_for_update", lambda *_: _async_value(subject))
    monkeypatch.setattr(asset_crud, "get_candidate", lambda _db, entry_id: _async_value(entries.get(entry_id)))
    monkeypatch.setattr(asset_crud, "get_subject", lambda *_: _async_value(subject))
    monkeypatch.setattr(asset_crud, "serialize_subject", lambda subject, _map: subject)
    monkeypatch.setattr(asset_crud, "get_episode_id_map", lambda *_: _async_value({}))
    monkeypatch.setattr(review_service, "search_shot_groups_referencing_subject", _empty_impact)

    await review_service.approve_image_with_lock(
        db, subject.id, first_id, expected_image_entry_id=old_id, confirm=True
    )
    assert entries[first_id].status == "approved"
    assert entries[first_id].is_approved is True

    with pytest.raises(ConflictError):
        await review_service.approve_image_with_lock(
            db, subject.id, second_id, expected_image_entry_id=old_id, confirm=True
        )


def test_first_concurrent_approval_without_expected_token_has_one_winner(monkeypatch):
    """两次真实 service/CRUD 事务竞争首次正式图，第二次必须被服务端拒绝。"""
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), _subject()])
            raw.add_all([_entry("candidate-a"), _entry("candidate-b")])
            raw.commit()
            db = _AsyncSessionAdapter(raw)

            async def _empty_impact(*_args, **_kwargs):
                return SimpleNamespace(referenced=False, shot_group_count=0, details=[])

            monkeypatch.setattr(review_service, "search_shot_groups_referencing_subject", _empty_impact)
            await review_service.approve_image_with_lock(db, "subject-1", "candidate-a", confirm=True)
            with pytest.raises(ConflictError):
                await review_service.approve_image_with_lock(
                    db, "subject-1", "candidate-b", confirm=True
                )

            with Session(engine) as verify:
                subject = verify.get(Subject, "subject-1")
                assert subject is not None
                assert subject.image_entry_id == "candidate-a"
                assert verify.get(AssetEntry, "candidate-b").status == "candidate"

    asyncio.run(scenario())


def test_first_approval_api_returns_200_then_409_without_expected_token(monkeypatch):
    """API 不依赖可选 expected_image_entry_id 也必须拒绝第二次首次批准。"""
    async def scenario() -> None:
        from app.api.v1 import asset_library

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), _subject(), _entry("candidate-a"), _entry("candidate-b")])
            raw.commit()
            db = _AsyncSessionAdapter(raw)
            app = FastAPI()
            app.include_router(asset_library.router)

            async def _db_override():
                yield db

            app.dependency_overrides[asset_library.get_db] = _db_override
            app.dependency_overrides[asset_library.get_current_user] = lambda: SimpleNamespace()
            monkeypatch.setattr(asset_library, "check_script_access", _allow)
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                first = await client.post(
                    "/scripts/script-1/asset-library/subjects/subject-1/approve-image",
                    json={"candidate_id": "candidate-a", "confirm": True},
                )
                second = await client.post(
                    "/scripts/script-1/asset-library/subjects/subject-1/approve-image",
                    json={"candidate_id": "candidate-b", "confirm": True},
                )
            assert sorted([first.status_code, second.status_code]) == [200, 409]

    asyncio.run(scenario())


@pytest.mark.parametrize("status", ["deleted", "superseded", "approved"])
def test_non_candidate_image_cannot_be_reapproved(monkeypatch, status: str):
    """已删除、已替换、历史已批准条目均不能进入批准链路。"""
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), _subject()])
            candidate = _entry("candidate-1")
            candidate.set_status(
                status,
                superseded_by_id="replacement" if status == "superseded" else None,
            )
            raw.add(candidate)
            raw.commit()
            db = _AsyncSessionAdapter(raw)
            with pytest.raises(ConflictError):
                await review_service.approve_image_with_lock(
                    db, "subject-1", "candidate-1", confirm=True
                )

    asyncio.run(scenario())


def test_upload_image_uses_candidate_crud_and_moves_media_subject_pending(monkeypatch):
    """真实上传入口创建候选，并按图片/声音候选推进对应主体审核状态。"""
    async def scenario() -> None:
        from app.api.v1 import media

        class _Storage:
            def __init__(self) -> None:
                self.objects: dict[str, bytes] = {}

            async def put(self, key: str, data: bytes) -> None:
                self.objects[key] = data

            async def promote(self, source_key: str, final_key: str) -> str:
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

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                _script(),
                Materials(id="material-1", name="剧本", script_id="script-1"),
                _subject(review_status="not_designed"),
                Subject(
                    id="sound-1", script_id="script-1", type="sound", name="配音",
                    review_status="not_designed",
                ),
            ])
            raw.commit()
            db = _AsyncSessionAdapter(raw)
            monkeypatch.setattr(media, "get_storage", lambda: _Storage())
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "image/png")

            image = UploadFile(BytesIO(b"png"), filename="reference.png")
            result = await media.upload_image(
                "material-1", "reference.png", image, "subject-1", db, SimpleNamespace()
            )
            assert result["asset_id"]
            assert raw.get(Subject, "subject-1").review_status == "pending"

            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "audio/mpeg")
            audio = UploadFile(BytesIO(b"mp3"), filename="voice.mp3")
            await media.upload_image(
                "material-1", "voice.mp3", audio, "sound-1", db, SimpleNamespace()
            )
            assert raw.get(Subject, "sound-1").review_status == "pending"

    asyncio.run(scenario())


def test_missing_subject_and_candidate_are_not_forbidden():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add(_script())
            raw.commit()
            db = _AsyncSessionAdapter(raw)
            with pytest.raises(Exception) as missing_subject:
                await review_service.approve_image_with_lock(db, "absent", "also-absent")
            with pytest.raises(Exception) as missing_candidate:
                raw.add(_subject())
                raw.commit()
                await review_service.approve_image_with_lock(db, "subject-1", "also-absent")
            assert missing_subject.value.status_code == 404
            assert missing_candidate.value.status_code == 404

    asyncio.run(scenario())


def test_existing_subject_from_another_script_is_forbidden_at_asset_api(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import asset_library

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                _script(),
                Script(
                    id="script-2", title="他人剧本", scope="personal", owner_user_id="user-2",
                    created_by_user_id="user-2",
                ),
                Subject(
                    id="other-subject", script_id="script-2", type="character", name="他人主体"
                ),
            ])
            raw.commit()
            monkeypatch.setattr(asset_library, "check_script_access", _allow)
            with pytest.raises(Exception) as exc_info:
                await asset_library.list_candidates(
                    "script-1", "other-subject", _AsyncSessionAdapter(raw), SimpleNamespace()
                )
            assert exc_info.value.status_code == 403

    asyncio.run(scenario())


def test_upload_and_media_missing_top_resources_return_404(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        class _Storage:
            async def put(self, _key: str, _data: bytes) -> None:
                return None

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), Materials(id="material-1", name="剧本", script_id="script-1")])
            raw.commit()
            db = _AsyncSessionAdapter(raw)
            monkeypatch.setattr(media, "get_storage", lambda: _Storage())
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "image/png")
            with pytest.raises(Exception) as missing_subject:
                await media.upload_image(
                    "material-1", "reference.png", UploadFile(BytesIO(b"png"), filename="reference.png"),
                    "absent", db, SimpleNamespace(),
                )
            with pytest.raises(Exception) as missing_script:
                await media._authorize_by_storage_key(
                    db, "render_outputs/absent/output.mp4", SimpleNamespace()
                )
            assert missing_subject.value.status_code == 404
            assert missing_script.value.status_code == 404

    asyncio.run(scenario())


async def _async_value(value):
    return value


async def _empty_impact(*_args, **_kwargs):
    return SimpleNamespace(referenced=False, shot_group_count=0, details=[])


def test_stable_subject_and_asset_ids_drive_reference_matching_after_rename():
    materials = {
        "character": [
            {"subject_id": "subject-1", "asset_entry_id": "image-1", "name": "旧名"}
        ]
    }

    assert review_service.material_references_subject(
        materials,
        subject_id="subject-1",
        asset_entry_id="image-1",
        subject_name="新名",
    ) == (True, "character")
    assert review_service.material_references_subject(
        materials,
        subject_id="subject-2",
        asset_entry_id="image-1",
        subject_name="旧名",
    ) == (False, None)


def test_storyboard_name_arrays_remain_compatible_with_stable_identifiers():
    materials = {
        "character": ["旧名"],
        "background": [],
        "prop": [],
        "other": [],
    }

    assert review_service.material_references_subject(
        materials,
        subject_id="subject-1",
        asset_entry_id="image-1",
        subject_name="旧名",
    ) == (True, "character")


def test_storyboard_materials_persist_stable_subject_ids_for_rename_safe_impact():
    from app.services.storyboard import _classify_materials

    subject = Subject(
        id="subject-1",
        script_id="script-1",
        type="character",
        name="旧名",
        image_entry_id="image-1",
    )

    materials = _classify_materials(["旧名"], [subject])

    assert materials["character"] == [
        {"subject_id": "subject-1", "asset_entry_id": "image-1", "name": "旧名"}
    ]
    assert review_service.material_references_subject(
        materials,
        subject_id="subject-1",
        asset_entry_id="image-1",
        subject_name="改名后",
    ) == (True, "character")


def test_prompt_helper_is_available_through_review_service():
    expected = build_subject_prompt("小明", "character", "蓝色外套")

    assert review_service.build_subject_prompt("小明", "character", "蓝色外套") == expected


@pytest.mark.parametrize("status", ["approved", "superseded", "deleted"])
def test_remove_candidate_rejects_every_non_candidate_lifecycle_state(status: str):
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), _subject()])
            entry = _entry("entry-1")
            entry.set_status(status, superseded_by_id="replacement" if status == "superseded" else None)
            raw.add(entry)
            raw.commit()

            with pytest.raises(ConflictError):
                await review_service.remove_candidate_with_recalc(
                    _AsyncSessionAdapter(raw), "subject-1", "entry-1"
                )
            assert entry.status == status
            assert entry.superseded_by_id == (
                "replacement" if status == "superseded" else None
            )

    asyncio.run(scenario())


def test_asset_entry_database_lifecycle_check_rejects_inconsistent_columns():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _asset_tables(engine)
    with Session(engine) as session:
        session.add_all([_script(), _subject()])
        session.commit()
        invalid = _entry("invalid-entry")
        invalid.status = "approved"
        invalid.is_approved = False
        invalid.superseded = False
        session.add(invalid)
        with pytest.raises(IntegrityError):
            session.commit()


def test_delete_approved_subject_respects_foreign_key_order_and_parent_count():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")

        @event.listens_for(engine, "connect")
        def _enable_foreign_keys(connection, _record) -> None:
            connection.execute("PRAGMA foreign_keys=ON")

        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            parent = ParentSubject(
                id="parent-1", script_id="script-1", name="父主体", type="character", child_count=1
            )
            subject = _subject(review_status="approved")
            subject.parent_id = parent.id
            raw.add_all([_script(), parent, subject])
            raw.commit()
            image = _entry("approved-image")
            image.set_status("approved")
            raw.add(image)
            raw.commit()
            subject.image_entry_id = image.id
            raw.commit()

            await asset_crud.delete_subject(_AsyncSessionAdapter(raw), subject)
            raw.commit()

        with Session(engine) as verify:
            assert verify.scalar(select(Subject.id).where(Subject.id == "subject-1")) is None
            assert verify.scalar(select(AssetEntry.id).where(AssetEntry.id == "approved-image")) is None
            assert verify.get(ParentSubject, "parent-1").child_count == 0

    asyncio.run(scenario())


def test_delete_subject_api_removes_terminal_generation_and_preserves_financial_history(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        from app.api.v1 import asset_library

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")

        @event.listens_for(engine, "connect")
        def _enable_foreign_keys(connection, _record) -> None:
            connection.execute("PRAGMA foreign_keys=ON")

        Base.metadata.create_all(engine)
        with Session(engine, expire_on_commit=False) as raw:
            user, job = _seed_subject_delete_generation_graph(raw)
            db = _AsyncSessionAdapter(raw)
            app = FastAPI()
            app.include_router(asset_library.router)

            async def _db_override():
                yield db

            app.dependency_overrides[asset_library.get_db] = _db_override
            app.dependency_overrides[asset_library.get_current_user] = lambda: user
            monkeypatch.setattr(asset_library, "check_script_access", _allow)

            async with AsyncClient(
                transport=ASGITransport(app=app, raise_app_exceptions=False),
                base_url="http://test",
            ) as client:
                response = await client.delete(
                    "/scripts/script-1/asset-library/subjects/subject-1"
                )

            assert response.status_code == 200, response.text
            assert raw.get(Subject, "subject-1") is None
            assert raw.get(GenerationJob, job.job_id) is None
            dispatch = raw.get(TaskDispatchOutbox, "dispatch-subject-delete")
            assert dispatch is not None and dispatch.status == "canceled"
            assert raw.get(CreditReservation, "reservation-subject-delete") is not None
            assert raw.get(CreditLog, "log-subject-delete") is not None
            audit = raw.scalar(
                select(AuditEvent).where(
                    AuditEvent.action == "subject_delete",
                    AuditEvent.resource_id == "subject-1",
                )
            )
            assert audit is not None

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status", "error_type", "retryable"),
    [
        pytest.param("starting", None, False, id="active"),
        pytest.param("failed", "financial_cleanup", True, id="financial-debt"),
    ],
)
def test_delete_subject_api_rejects_blocking_generation_without_partial_changes(
    monkeypatch,
    status: str,
    error_type: str | None,
    retryable: bool,
) -> None:
    async def scenario() -> None:
        from app.api.v1 import asset_library

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")

        @event.listens_for(engine, "connect")
        def _enable_foreign_keys(connection, _record) -> None:
            connection.execute("PRAGMA foreign_keys=ON")

        Base.metadata.create_all(engine)
        with Session(engine, expire_on_commit=False) as raw:
            user, job = _seed_subject_delete_generation_graph(
                raw,
                status=status,
                error_type=error_type,
                retryable=retryable,
            )
            db = _AsyncSessionAdapter(raw)
            app = FastAPI()
            app.include_router(asset_library.router)

            async def _db_override():
                yield db

            app.dependency_overrides[asset_library.get_db] = _db_override
            app.dependency_overrides[asset_library.get_current_user] = lambda: user
            monkeypatch.setattr(asset_library, "check_script_access", _allow)

            async with AsyncClient(
                transport=ASGITransport(app=app, raise_app_exceptions=False),
                base_url="http://test",
            ) as client:
                response = await client.delete(
                    "/scripts/script-1/asset-library/subjects/subject-1"
                )

            assert response.status_code == 409, response.text
            assert raw.get(Subject, "subject-1") is not None
            assert raw.get(GenerationJob, job.job_id) is not None
            dispatch = raw.get(TaskDispatchOutbox, "dispatch-subject-delete")
            assert dispatch is not None and dispatch.status == "pending"
            assert raw.scalar(
                select(AuditEvent.id).where(AuditEvent.action == "subject_delete")
            ) is None

    asyncio.run(scenario())


def test_delete_subject_api_rolls_back_when_strict_audit_fails(monkeypatch) -> None:
    async def scenario() -> None:
        from app.api.v1 import asset_library

        async def _allow(*_args, **_kwargs) -> None:
            return None

        async def _fail_audit(*_args, **_kwargs) -> None:
            raise RuntimeError("audit unavailable")

        engine = create_engine("sqlite+pysqlite:///:memory:")

        @event.listens_for(engine, "connect")
        def _enable_foreign_keys(connection, _record) -> None:
            connection.execute("PRAGMA foreign_keys=ON")

        Base.metadata.create_all(engine)
        with Session(engine, expire_on_commit=False) as raw:
            user, job = _seed_subject_delete_generation_graph(raw)
            db = _AsyncSessionAdapter(raw)
            app = FastAPI()
            app.include_router(asset_library.router)

            async def _db_override():
                yield db

            app.dependency_overrides[asset_library.get_db] = _db_override
            app.dependency_overrides[asset_library.get_current_user] = lambda: user
            monkeypatch.setattr(asset_library, "check_script_access", _allow)
            audit = AsyncMock(side_effect=_fail_audit)
            monkeypatch.setattr(asset_library, "strict_audit_log", audit)

            async with AsyncClient(
                transport=ASGITransport(app=app, raise_app_exceptions=False),
                base_url="http://test",
            ) as client:
                response = await client.delete(
                    "/scripts/script-1/asset-library/subjects/subject-1"
                )

            assert response.status_code == 500
            audit.assert_awaited_once()
            assert raw.get(Subject, "subject-1") is not None
            assert raw.get(GenerationJob, job.job_id) is not None
            dispatch = raw.get(TaskDispatchOutbox, "dispatch-subject-delete")
            assert dispatch is not None and dispatch.status == "pending"
            assert raw.get(CreditReservation, "reservation-subject-delete") is not None
            assert raw.get(CreditLog, "log-subject-delete") is not None

    asyncio.run(scenario())


def test_delete_subject_api_preserves_generation_referenced_upload_and_cleans_orphan(
    monkeypatch,
):
    async def scenario() -> None:
        from app.api.v1 import asset_library
        from app.services import media_storage_jobs

        async def _allow(*_args, **_kwargs) -> None:
            return None

        async def _ignore_audit(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")

        @event.listens_for(engine, "connect")
        def _enable_foreign_keys(connection, _record) -> None:
            connection.execute("PRAGMA foreign_keys=ON")

        Base.metadata.create_all(engine)
        with Session(engine, expire_on_commit=False) as raw:
            user = User(
                user_id="user-1",
                username="subject-delete-owner",
                password_hash="unused",
            )
            retained_subject = Subject(
                id="subject-retained",
                script_id="script-retained",
                type="character",
                name="保留主体",
                review_status="approved",
            )
            retained_script = Script(
                id="script-retained",
                title="保留剧本",
                scope="personal",
                owner_user_id=user.user_id,
                created_by_user_id=user.user_id,
            )
            retained_episode = Episode(
                episode_id="episode-retained",
                script_id=retained_script.id,
                episode_no=1,
                title="保留分集",
            )
            retained_material = Materials(
                id="material-retained",
                name="保留画布",
                script_id=retained_script.id,
            )
            raw.add_all(
                [
                    user,
                    _script(),
                    retained_script,
                    Materials(id="material-1", name="剧本", script_id="script-1"),
                    retained_material,
                    retained_episode,
                    _subject(review_status="approved"),
                    retained_subject,
                ]
            )
            raw.commit()

            shared_final = "upload_images/material-1/shared.png"
            shared_temp = "upload_images/_tmp/material-1/shared.png"
            ready_final = "upload_images/material-1/ready-shared.png"
            ready_temp = "upload_images/_tmp/material-1/ready-shared.png"
            orphan_final = "upload_images/material-1/orphan.png"
            orphan_temp = "upload_images/_tmp/material-1/orphan.png"
            shared_asset = AssetEntry(
                id="asset-shared",
                script_id="script-1",
                subject_id="subject-1",
                kind="image",
                storage_key=f"/{shared_final}",
                mime_type="image/png",
                size_bytes=6,
                source="upload_images",
            )
            orphan_asset = AssetEntry(
                id="asset-orphan",
                script_id="script-1",
                subject_id="subject-1",
                kind="image",
                storage_key=f"/{orphan_final}",
                mime_type="image/png",
                size_bytes=6,
                source="upload_images",
            )
            ready_asset = AssetEntry(
                id="asset-ready-shared",
                script_id="script-1",
                subject_id="subject-1",
                kind="image",
                storage_key=f"/{ready_final}",
                mime_type="image/png",
                size_bytes=6,
                source="upload_images",
            )
            raw.add_all([shared_asset, ready_asset, orphan_asset])
            raw.flush()

            shared_upload = MediaUpload(
                id="upload-shared",
                script_id="script-1",
                material_id="material-1",
                subject_id="subject-1",
                asset_id=shared_asset.id,
                temp_storage_key=shared_temp,
                final_storage_key=shared_final,
                original_filename="shared.png",
                mime_type="image/png",
                size_bytes=6,
                sha256="a" * 64,
                status="staging",
            )
            orphan_upload = MediaUpload(
                id="upload-orphan",
                script_id="script-1",
                material_id="material-1",
                subject_id="subject-1",
                asset_id=orphan_asset.id,
                temp_storage_key=orphan_temp,
                final_storage_key=orphan_final,
                original_filename="orphan.png",
                mime_type="image/png",
                size_bytes=6,
                sha256="b" * 64,
                status="staging",
            )
            ready_upload = MediaUpload(
                id="upload-ready-shared",
                script_id="script-1",
                material_id="material-1",
                subject_id="subject-1",
                asset_id=ready_asset.id,
                temp_storage_key=ready_temp,
                final_storage_key=ready_final,
                original_filename="ready-shared.png",
                mime_type="image/png",
                size_bytes=6,
                sha256="d" * 64,
                status="ready",
            )
            shared_promote = MediaStorageJob(
                id="promote-shared",
                idempotency_key="upload-promote:upload-shared",
                job_type="promote",
                source_key=shared_temp,
                final_key=shared_final,
                upload_id=shared_upload.id,
                status="pending",
            )
            orphan_promote = MediaStorageJob(
                id="promote-orphan",
                idempotency_key="upload-promote:upload-orphan",
                job_type="promote",
                source_key=orphan_temp,
                final_key=orphan_final,
                upload_id=orphan_upload.id,
                status="pending",
            )
            ready_promote = MediaStorageJob(
                id="promote-ready-shared",
                idempotency_key="upload-promote:upload-ready-shared",
                job_type="promote",
                source_key=ready_temp,
                final_key=ready_final,
                upload_id=ready_upload.id,
                status="succeeded",
                completed_at=datetime.now(UTC),
            )
            retained_job = GenerationJob(
                job_id="generation-retained",
                material_id=retained_material.id,
                script_id=retained_script.id,
                subject_id=retained_subject.id,
                owner_user_id=user.user_id,
                idempotency_key="generation-retained",
                request_hash="c" * 64,
                input_version=1,
                attempt=1,
                job_type="generate_images",
                status="succeeded",
                progress=100,
                episode_id=retained_episode.episode_id,
                output_json=None,
                metrics={"retained_storage": {"storage_key": ready_final}},
            )
            retained_timeline = TimelineRenderJob(
                job_id="timeline-retained",
                script_id=retained_script.id,
                episode_id=retained_episode.episode_id,
                owner_user_id=user.user_id,
                idempotency_key="timeline-retained",
                request_hash="e" * 64,
                input_json={"source": {"storage_key": f"/{shared_final}"}},
                output_storage_key="render_outputs/script-retained/output.mp4",
                status="succeeded",
            )
            raw.add_all(
                [
                    shared_upload,
                    ready_upload,
                    orphan_upload,
                    retained_job,
                    retained_timeline,
                ]
            )
            raw.flush()
            raw.add_all([shared_promote, ready_promote, orphan_promote])
            raw.commit()

            db = _AsyncSessionAdapter(raw)
            app = FastAPI()
            app.include_router(asset_library.router)

            async def _db_override():
                yield db

            app.dependency_overrides[asset_library.get_db] = _db_override
            app.dependency_overrides[asset_library.get_current_user] = lambda: user
            monkeypatch.setattr(asset_library, "check_script_access", _allow)
            monkeypatch.setattr(asset_library, "audit_log", _ignore_audit)

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.delete(
                    "/scripts/script-1/asset-library/subjects/subject-1"
                )

            assert response.status_code == 200
            raw.refresh(shared_upload)
            raw.refresh(ready_upload)
            raw.refresh(orphan_upload)
            raw.refresh(shared_promote)
            raw.refresh(ready_promote)
            raw.refresh(orphan_promote)
            assert raw.get(Subject, "subject-1") is None
            assert raw.get(GenerationJob, retained_job.job_id) is not None
            assert raw.get(TimelineRenderJob, retained_timeline.job_id) is not None
            assert shared_upload.status == "staging"
            assert shared_upload.subject_id is None
            assert shared_upload.asset_id is None
            assert shared_promote.status == "pending"
            assert ready_upload.status == "ready"
            assert ready_upload.subject_id is None
            assert ready_upload.asset_id is None
            assert ready_promote.status == "succeeded"
            assert orphan_upload.status == "deleted"
            assert orphan_promote.status == "canceled"

            delete_keys = set(
                raw.scalars(
                    select(MediaStorageJob.source_key).where(
                        MediaStorageJob.job_type == "delete"
                    )
                ).all()
            )
            assert delete_keys == {ready_temp, orphan_temp, orphan_final}
            assert {shared_temp, shared_final}.isdisjoint(delete_keys)
            assert ready_final not in delete_keys

            claim = await media_storage_jobs.claim_job(
                db,
                shared_promote.id,
                owner="late-shared-promote",
            )
            assert claim is not None
            await db.commit()
            asset_id = await media_storage_jobs.finalize_upload_ready(db, claim)
            assert asset_id is None
            assert await media_storage_jobs.mark_succeeded(
                db,
                claim.id,
                claim.lease_token,
                asset_id=asset_id,
            )
            await db.commit()
            raw.refresh(shared_upload)
            raw.refresh(shared_promote)
            assert shared_upload.status == "ready"
            assert shared_promote.status == "succeeded"

    asyncio.run(scenario())


def test_get_subject_for_update_refreshes_stale_identity_map(tmp_path):
    """锁定读取必须强制拿数据库新值，不能复用当前 Session 的旧 identity map。"""

    async def scenario() -> None:
        engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'fresh.db'}")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as seed:
            seed.add_all([_script(), _subject(review_status="approved")])
            old_image = _entry("old-image")
            new_image = _entry("new-image")
            old_image.set_status("approved")
            seed.add_all([old_image, new_image])
            seed.commit()
            subject = seed.get(Subject, "subject-1")
            subject.image_entry_id = old_image.id
            seed.commit()

        with Session(engine, expire_on_commit=False) as stale_session:
            stale_session.get(Subject, "subject-1")
            with Session(engine, expire_on_commit=False) as writer:
                subject = writer.get(Subject, "subject-1")
                subject.image_entry_id = "new-image"
                writer.commit()

            fresh = await asset_crud.get_subject_for_update(
                _AsyncSessionAdapter(stale_session), "subject-1"
            )
            assert fresh.image_entry_id == "new-image"

    asyncio.run(scenario())


def test_delete_subject_clears_external_superseded_references_before_asset_delete():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")

        @event.listens_for(engine, "connect")
        def _enable_foreign_keys(connection, _record) -> None:
            connection.execute("PRAGMA foreign_keys=ON")

        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            survivor = Subject(
                id="subject-2",
                script_id="script-1",
                type="character",
                name="配角",
                review_status="approved",
            )
            raw.add_all([_script(), _subject(review_status="approved"), survivor])
            raw.commit()

            deleted_asset = _entry("deleted-subject-image")
            history = AssetEntry(
                id="history-image",
                script_id="script-1",
                subject_id="subject-2",
                kind="image",
                storage_key="generated/history",
                mime_type="image/png",
                size_bytes=1,
                source="generate_images",
            )
            deleted_asset.set_status("approved")
            history.set_status("superseded", superseded_by_id=deleted_asset.id)
            raw.add_all([deleted_asset, history])
            raw.commit()
            subject = raw.get(Subject, "subject-1")
            subject.image_entry_id = deleted_asset.id
            raw.commit()

            await asset_crud.delete_subject(_AsyncSessionAdapter(raw), subject)
            raw.commit()

        with Session(engine) as verify:
            history = verify.get(AssetEntry, "history-image")
            assert history.superseded_by_id is None
            assert history.status == "deleted"

    asyncio.run(scenario())


def test_upload_missing_subject_does_not_write_an_orphan_object(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        class _Storage:
            def __init__(self) -> None:
                self.put_keys: list[str] = []

            async def put(self, key: str, _data: bytes) -> None:
                self.put_keys.append(key)

            async def delete(self, _key: str) -> None:
                return None

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        storage = _Storage()
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), Materials(id="material-1", name="剧本", script_id="script-1")])
            raw.commit()
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "image/png")
            with pytest.raises(Exception) as exc_info:
                await media.upload_image(
                    "material-1", "reference.png", UploadFile(BytesIO(b"png"), filename="reference.png"),
                    "missing", _AsyncSessionAdapter(raw), SimpleNamespace(),
                )
            assert exc_info.value.status_code == 404
            assert storage.put_keys == []

    asyncio.run(scenario())


def test_video_upload_does_not_create_an_image_candidate(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        class _Storage:
            def __init__(self) -> None:
                self.objects: dict[str, bytes] = {}

            async def put(self, key: str, data: bytes) -> None:
                self.objects[key] = data

            async def promote(self, source_key: str, final_key: str) -> str:
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

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), Materials(id="material-1", name="剧本", script_id="script-1"), _subject()])
            raw.commit()
            monkeypatch.setattr(media, "get_storage", lambda: _Storage())
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "video/mp4")
            result = await media.upload_image(
                "material-1", "clip.mp4", UploadFile(BytesIO(b"mp4"), filename="clip.mp4"),
                "subject-1", _AsyncSessionAdapter(raw), SimpleNamespace(),
            )
            assert result["asset_id"] is None
            assert raw.scalar(select(AssetEntry.id)) is None

    asyncio.run(scenario())


def test_upload_writes_temp_key_and_persists_promote_job_for_subjectless_upload(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        storage = _MemoryStorage()
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), Materials(id="material-1", name="剧本", script_id="script-1")])
            raw.commit()
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "video/mp4")
            result = await media.upload_image(
                "material-1", "clip.mp4", UploadFile(BytesIO(b"mp4"), filename="clip.mp4"),
                None, _AsyncSessionAdapter(raw), SimpleNamespace(),
            )

            cleanup = raw.scalar(
                select(MediaStorageJob).where(MediaStorageJob.job_type == "delete")
            )
            promote = raw.scalar(
                select(MediaStorageJob).where(MediaStorageJob.job_type == "promote")
            )
            assert cleanup is not None and cleanup.status == "canceled"
            assert promote is not None and promote.status == "succeeded"
            assert promote.asset_id is None
            assert storage.put_keys == [promote.source_key]
            assert storage.put_keys[0].startswith("upload_images/_tmp/material-1/")
            assert storage.promotions == [(promote.source_key, promote.final_key)]
            assert result["storage_key"] == f"/{promote.final_key}"
            assert not (Path(str(media.settings.storage_root)) / "upload-cleanup-intents.jsonl").exists()

    from pathlib import Path

    asyncio.run(scenario())


def test_upload_streams_temp_before_persisting_promote_intent(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        class _Storage(_MemoryStorage):
            def __init__(self) -> None:
                super().__init__()
                self.events: list[tuple[str, int]] = []

            async def put_file(self, key: str, path: str) -> str:
                self.events.append((key, raw.query(MediaStorageJob).count()))
                assert Path(path).exists()
                return await _MemoryStorage.put(self, key, Path(path).read_bytes())

            async def put(self, _key: str, _data: bytes) -> None:
                raise AssertionError("上传不得通过 bytes put 复制整对象")

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        storage = _Storage()
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), Materials(id="material-1", name="剧本", script_id="script-1")])
            raw.commit()
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "video/mp4")

            result = await media.upload_image(
                "material-1", "clip.mp4", UploadFile(BytesIO(b"mp4"), filename="clip.mp4"),
                None, _AsyncSessionAdapter(raw), SimpleNamespace(),
            )

            job = raw.scalar(
                select(MediaStorageJob).where(MediaStorageJob.job_type == "promote")
            )
            assert job is not None
            assert storage.events == [(job.source_key, 1)]
            assert result["storage_key"] == f"/{job.final_key}"

    asyncio.run(scenario())


def test_upload_temp_write_failure_activates_durable_cleanup(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        class _Storage(_MemoryStorage):
            async def put(self, key: str, _data: bytes) -> str:
                self.put_keys.append(key)
                raise RuntimeError("secret put failure")

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        storage = _Storage()
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                _script(),
                Materials(id="material-1", name="剧本", script_id="script-1"),
                _subject(review_status="not_designed"),
            ])
            raw.commit()
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "image/png")
            with pytest.raises(RuntimeError, match="secret put failure"):
                await media.upload_image(
                    "material-1", "reference.png", UploadFile(BytesIO(b"png"), filename="reference.png"),
                    "subject-1", _AsyncSessionAdapter(raw), SimpleNamespace(),
                )

            upload = raw.scalar(select(MediaUpload))
            cleanup = raw.scalar(
                select(MediaStorageJob).where(MediaStorageJob.job_type == "delete")
            )
            assert storage.delete_keys == []
            assert upload is not None and upload.status == "failed"
            assert upload.last_error_type == "RuntimeError"
            assert cleanup is not None and cleanup.status == "pending"
            assert abs((cleanup.available_at - upload.completed_at).total_seconds()) < 1
            assert raw.scalar(select(AssetEntry)) is None
            assert raw.get(Subject, "subject-1").review_status == "not_designed"

    asyncio.run(scenario())


def test_upload_promote_failure_leaves_retry_job_for_worker_recovery(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        class _Storage(_MemoryStorage):
            async def promote(self, source_key: str, final_key: str) -> str:
                self.promotions.append((source_key, final_key))
                raise RuntimeError("secret promote failure")

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        storage = _Storage()
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), Materials(id="material-1", name="剧本", script_id="script-1")])
            raw.commit()
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "video/mp4")
            with pytest.raises(RuntimeError, match="secret promote failure"):
                await media.upload_image(
                    "material-1", "clip.mp4", UploadFile(BytesIO(b"mp4"), filename="clip.mp4"),
                    None, _AsyncSessionAdapter(raw), SimpleNamespace(),
                )

            job = raw.scalar(
                select(MediaStorageJob).where(MediaStorageJob.job_type == "promote")
            )
            assert job is not None
            assert job.status == "retry"
            assert job.job_type == "promote"
            assert job.attempt == 1
            assert job.last_error_type == "RuntimeError"
            assert job.last_error_message == "媒体存储任务执行失败"
            assert "secret" not in job.last_error_message

    asyncio.run(scenario())


def test_upload_promote_missing_source_marks_intent_failed(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media
        from app.providers.storage import StorageObjectNotFoundError

        class _Storage(_MemoryStorage):
            async def promote(self, source_key: str, _final_key: str) -> str:
                raise StorageObjectNotFoundError(source_key)

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), Materials(id="material-1", name="剧本", script_id="script-1")])
            raw.commit()
            monkeypatch.setattr(media, "get_storage", _Storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "video/mp4")

            with pytest.raises(StorageObjectNotFoundError):
                await media.upload_image(
                    "material-1",
                    "clip.mp4",
                    UploadFile(BytesIO(b"mp4"), filename="clip.mp4"),
                    None,
                    _AsyncSessionAdapter(raw),
                    SimpleNamespace(),
                )

            job = raw.scalar(
                select(MediaStorageJob).where(MediaStorageJob.job_type == "promote")
            )
            assert job is not None
            assert job.status == "failed"
            assert job.last_error_type == "StorageObjectNotFoundError"

    asyncio.run(scenario())


def test_upload_commit_then_interruption_is_recovered_by_worker(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media
        from app.workers import media_storage as media_worker

        async def _allow(*_args, **_kwargs) -> None:
            return None

        async def _interrupt_after_commit(
            _db,
            _storage,
            job_id: str,
            *,
            claim=None,
            **_kwargs,
        ) -> None:
            assert claim is not None
            assert claim.id == job_id
            assert claim.status == "running"
            persisted = raw.get(MediaStorageJob, job_id)
            assert persisted is not None
            assert persisted.status == "running"
            assert persisted.attempt == 1
            assert persisted.lease_token == claim.lease_token
            raise RuntimeError("simulated process interruption")

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        storage = _MemoryStorage()
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), Materials(id="material-1", name="剧本", script_id="script-1")])
            raw.commit()
            db = _AsyncSessionAdapter(raw)
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "video/mp4")
            monkeypatch.setattr(media, "_run_promote_job", _interrupt_after_commit)

            with pytest.raises(RuntimeError, match="simulated process interruption"):
                await media.upload_image(
                    "material-1",
                    "clip.mp4",
                    UploadFile(BytesIO(b"mp4"), filename="clip.mp4"),
                    None,
                    db,
                    SimpleNamespace(),
                )

            job = raw.scalar(
                select(MediaStorageJob).where(MediaStorageJob.job_type == "promote")
            )
            assert job is not None
            assert job.status == "running"
            assert job.attempt == 1
            assert job.source_key in storage.objects
            assert job.final_key not in storage.objects

            job.lease_until = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=1)
            raw.commit()
            assert await media_worker.process_one(db, storage, owner="worker-a") == job.id
            raw.refresh(job)
            assert job.status == "succeeded"
            assert job.attempt == 2
            assert job.source_key not in storage.objects
            assert storage.objects[job.final_key] == b"mp4"

    asyncio.run(scenario())


def test_media_storage_worker_consumes_promote_and_delete_jobs_idempotently():
    async def scenario() -> None:
        from app.services import media_storage_jobs
        from app.workers import media_storage as media_worker

        class _Storage:
            def __init__(self) -> None:
                self.promotions: list[tuple[str, str]] = []
                self.deletes: list[str] = []

            async def promote_if_absent(
                self, source_key: str, final_key: str
            ) -> bool:
                self.promotions.append((source_key, final_key))
                return True

            async def delete(self, key: str) -> None:
                self.deletes.append(key)

        engine = create_engine("sqlite+pysqlite:///:memory:")
        Base.metadata.create_all(engine)
        storage = _Storage()
        with Session(engine, expire_on_commit=False) as raw:
            db = _AsyncSessionAdapter(raw)
            promote = await media_storage_jobs.enqueue_promote(
                db,
                idempotency_key="promote:asset-1",
                source_key="upload_images/_tmp/material-1/tmp.png",
                final_key="upload_images/material-1/final.png",
                asset_id="asset-1",
            )
            delete = await media_storage_jobs.enqueue_delete(
                db,
                idempotency_key="delete:tmp",
                source_key="upload_images/_tmp/material-1/orphan.png",
            )
            raw.commit()

            assert await media_worker.process_one(db, storage, owner="worker-a") == promote.id
            assert await media_worker.process_one(db, storage, owner="worker-a") == delete.id
            assert await media_worker.process_one(db, storage, owner="worker-a") is None

            assert storage.promotions == [("upload_images/_tmp/material-1/tmp.png", "upload_images/material-1/final.png")]
            assert storage.deletes == ["upload_images/_tmp/material-1/orphan.png"]
            assert {row.status for row in raw.scalars(select(MediaStorageJob)).all()} == {"succeeded"}

    asyncio.run(scenario())


def test_media_storage_worker_marks_missing_promote_source_failed():
    async def scenario() -> None:
        from app.providers.storage import StorageObjectNotFoundError
        from app.services import media_storage_jobs
        from app.workers import media_storage as media_worker

        class _Storage:
            async def promote_if_absent(
                self, source_key: str, _final_key: str
            ) -> bool:
                raise StorageObjectNotFoundError(source_key)

            async def delete(self, _key: str) -> None:
                return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            db = _AsyncSessionAdapter(raw)
            job = await media_storage_jobs.enqueue_promote(
                db,
                idempotency_key="promote:missing",
                source_key="upload_images/_tmp/material-1/missing.png",
                final_key="upload_images/material-1/final.png",
            )
            raw.commit()

            with pytest.raises(StorageObjectNotFoundError):
                await media_worker.process_one(db, _Storage(), owner="worker-a")

            raw.refresh(job)
            assert job.status == "failed"
            assert job.attempt == 1
            assert job.last_error_type == "StorageObjectNotFoundError"
            assert await media_worker.process_one(db, _Storage(), owner="worker-a") is None

    asyncio.run(scenario())


def test_media_storage_worker_retries_transient_promote_with_backoff():
    async def scenario() -> None:
        from app.services import media_storage_jobs
        from app.workers import media_storage as media_worker

        class _Storage:
            async def promote_if_absent(
                self, _source_key: str, _final_key: str
            ) -> bool:
                raise TimeoutError("temporary storage timeout")

            async def delete(self, _key: str) -> None:
                return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            db = _AsyncSessionAdapter(raw)
            job = await media_storage_jobs.enqueue_promote(
                db,
                idempotency_key="promote:timeout",
                source_key="upload_images/_tmp/material-1/timeout.png",
                final_key="upload_images/material-1/final.png",
            )
            raw.commit()

            with pytest.raises(TimeoutError, match="temporary storage timeout"):
                await media_worker.process_one(db, _Storage(), owner="worker-a")

            raw.refresh(job)
            assert job.status == "retry"
            assert job.attempt == 1
            assert job.available_at > job.created_at
            assert job.last_error_type == "TimeoutError"
            assert await media_worker.process_one(db, _Storage(), owner="worker-a") is None

    asyncio.run(scenario())


def test_material_image_http_distinguishes_missing_from_cross_script_asset(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import materials as materials_api

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                _script(),
                Script(
                    id="script-2", title="剧本二", scope="personal", owner_user_id="user-2",
                    created_by_user_id="user-2",
                ),
                Materials(id="material-1", name="剧本", script_id="script-1"),
                Materials(id="material-2", name="剧本二", script_id="script-2"),
                _subject(),
                Subject(id="subject-2", script_id="script-2", type="character", name="他人主体"),
                AssetEntry(
                    id="other-image", script_id="script-2", subject_id="subject-2", kind="image",
                    storage_key="generated/other", mime_type="image/png", size_bytes=1,
                    source="generate_images",
                ),
            ])
            raw.commit()
            app = FastAPI()
            app.include_router(materials_api.router)

            async def _db_override():
                yield _AsyncSessionAdapter(raw)

            app.dependency_overrides[materials_api.get_db] = _db_override
            app.dependency_overrides[materials_api.get_current_user] = lambda: SimpleNamespace()
            monkeypatch.setattr(materials_api, "check_script_access", _allow)
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                cross_script = await client.get("/materials/material-1/image/other-image")
                missing = await client.get("/materials/material-1/image/missing")
            assert cross_script.status_code == 403
            assert missing.status_code == 404

    asyncio.run(scenario())


def test_upload_asset_database_failure_keeps_retryable_promote(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media
        from app.crud import asset as asset_crud_module

        async def _allow(*_args, **_kwargs) -> None:
            return None

        async def _db_failure(*_args, **_kwargs):
            raise RuntimeError("数据库写入失败")

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        storage = _MemoryStorage()
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), Materials(id="material-1", name="剧本", script_id="script-1"), _subject()])
            raw.commit()
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "image/png")
            monkeypatch.setattr(asset_crud_module, "add_candidate", _db_failure)
            with pytest.raises(RuntimeError, match="数据库写入失败"):
                await media.upload_image(
                    "material-1", "reference.png", UploadFile(BytesIO(b"png"), filename="reference.png"),
                    "subject-1", _AsyncSessionAdapter(raw), SimpleNamespace(),
                )
            upload = raw.scalar(select(MediaUpload))
            promote = raw.scalar(
                select(MediaStorageJob).where(MediaStorageJob.job_type == "promote")
            )
            assert upload is not None and upload.status == "staging"
            assert promote is not None and promote.status == "retry"
            assert promote.final_key in storage.objects
            assert storage.delete_keys == []
            assert raw.scalar(select(AssetEntry.id)) is None

    asyncio.run(scenario())


def test_fake_png_upload_is_rejected_by_real_mime_probe(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        class _Storage:
            def __init__(self) -> None:
                self.put_keys: list[str] = []

            async def put(self, key: str, _data: bytes) -> None:
                self.put_keys.append(key)

            async def delete(self, _key: str) -> None:
                return None

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        storage = _Storage()
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), Materials(id="material-1", name="剧本", script_id="script-1")])
            raw.commit()
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            app = FastAPI()
            app.include_router(media.router)

            async def _db_override():
                yield _AsyncSessionAdapter(raw)

            app.dependency_overrides[media.get_db] = _db_override
            app.dependency_overrides[media.get_current_user] = lambda: SimpleNamespace()
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.post(
                    "/upload_images/material-1/fake.png",
                    files={"file": ("fake.png", b"not a real png", "image/png")},
                )

            assert response.status_code == 400
            assert storage.put_keys == []

    asyncio.run(scenario())


def test_audio_mime_uses_real_container_when_wav_is_disguised_as_mp3(monkeypatch, tmp_path):
    from app.api.v1 import media

    class _AudioSegment:
        @staticmethod
        def from_file(_path):
            return object()

    monkeypatch.setitem(sys.modules, "pydub", SimpleNamespace(AudioSegment=_AudioSegment))
    monkeypatch.setattr(media, "_probe_av_mime_with_ffprobe", lambda *_args: "audio/wav", raising=False)
    sample = tmp_path / "voice.mp3"
    sample.write_bytes(b"RIFF....WAVEfmt ")

    assert media._detect_mime(str(sample), "voice.mp3") == "audio/wav"


def test_video_mime_uses_real_container_when_quicktime_is_disguised_as_mp4(monkeypatch, tmp_path):
    from app.api.v1 import media

    monkeypatch.setattr(media, "_probe_av_mime_with_ffprobe", lambda *_args: "video/quicktime", raising=False)
    sample = tmp_path / "clip.mp4"
    sample.write_bytes(b"not-used")

    assert media._detect_mime(str(sample), "clip.mp4") == "video/quicktime"


def test_audio_video_probe_tool_missing_fails_closed(monkeypatch, tmp_path):
    from app.api.v1 import media

    monkeypatch.setattr(media, "_probe_av_mime_with_ffprobe", lambda *_args: None, raising=False)
    monkeypatch.setitem(sys.modules, "pydub", None)
    sample = tmp_path / "voice.mp3"
    sample.write_bytes(b"not-used")

    assert media._detect_mime(str(sample), "voice.mp3") == "application/octet-stream"


def test_upload_asset_database_failure_keeps_durable_intent(monkeypatch, tmp_path):
    async def scenario() -> None:
        from app.api.v1 import media
        from app.crud import asset as asset_crud_module

        async def _allow(*_args, **_kwargs) -> None:
            return None

        async def _db_failure(*_args, **_kwargs):
            raise RuntimeError("数据库写入失败")

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        storage = _MemoryStorage()
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), Materials(id="material-1", name="剧本", script_id="script-1"), _subject()])
            raw.commit()
            monkeypatch.setattr(media.settings, "storage_root", str(tmp_path))
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "image/png")
            monkeypatch.setattr(asset_crud_module, "add_candidate", _db_failure)
            with pytest.raises(RuntimeError, match="数据库写入失败"):
                await media.upload_image(
                    "material-1", "reference.png", UploadFile(BytesIO(b"png"), filename="reference.png"),
                    "subject-1", _AsyncSessionAdapter(raw), SimpleNamespace(),
                )

            assert len(storage.put_keys) == 1
            assert storage.delete_keys == []
            cleanup = raw.scalar(
                select(MediaStorageJob).where(MediaStorageJob.job_type == "delete")
            )
            promote = raw.scalar(
                select(MediaStorageJob).where(MediaStorageJob.job_type == "promote")
            )
            assert cleanup is not None and cleanup.status == "canceled"
            assert promote is not None and promote.status == "retry"
            assert raw.scalar(select(AssetEntry)) is None

    asyncio.run(scenario())


def test_upload_initial_commit_failure_performs_no_storage_io(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        class _CommitFailingSession(_AsyncSessionAdapter):
            def __init__(self, session: Session) -> None:
                super().__init__(session)
                self.commit_calls = 0

            async def commit(self) -> None:
                self.commit_calls += 1
                raise RuntimeError("数据库提交失败")

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        storage = _MemoryStorage()
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                _script(),
                Materials(id="material-1", name="剧本", script_id="script-1"),
                _subject(review_status="not_designed"),
            ])
            raw.commit()
            db = _CommitFailingSession(raw)
            monkeypatch.setattr(media, "get_storage", lambda: storage)
            monkeypatch.setattr(media, "check_script_access", _allow)
            monkeypatch.setattr(media, "_detect_mime", lambda *_args: "image/png")

            with pytest.raises(RuntimeError, match="数据库提交失败"):
                await media.upload_image(
                    "material-1",
                    "reference.png",
                    UploadFile(BytesIO(b"png"), filename="reference.png"),
                    "subject-1",
                    db,
                    SimpleNamespace(),
                )

            assert db.commit_calls == 1
            assert storage.put_keys == []
            assert storage.delete_keys == []
            assert storage.objects == {}
            assert raw.scalar(select(MediaStorageJob)) is None
            assert raw.scalar(select(AssetEntry)) is None
            assert raw.get(Subject, "subject-1").review_status == "not_designed"

    asyncio.run(scenario())


def test_media_proxy_uses_asset_entry_mime_type_without_row_attribute_error(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        class _Storage:
            async def exists(self, _key: str) -> bool:
                return True

            async def get_size(self, _key: str) -> int:
                return 1

            async def iter_chunks(self, *_args, **_kwargs):
                yield b"x"

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                _script(),
                Materials(id="material-1", name="剧本", script_id="script-1"),
                _subject(),
                AssetEntry(
                    id="asset-1", script_id="script-1", subject_id="subject-1", kind="image",
                    storage_key="/upload_images/material-1/reference.png", mime_type="image/webp",
                    size_bytes=1, source="upload_images",
                ),
            ])
            raw.commit()
            monkeypatch.setattr(media, "get_storage", lambda: _Storage())
            monkeypatch.setattr(media, "check_script_access", _allow)
            response = await media.get_media(
                "upload_images/material-1/reference.png",
                None,
                _AsyncSessionAdapter(raw),
                SimpleNamespace(),
            )
            assert response.media_type == "image/webp"

    asyncio.run(scenario())


def test_reference_aggregator_prefers_stable_subject_ids_after_subject_rename():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                _script(),
                Episode(
                    episode_id="episode-1",
                    script_id="script-1",
                    episode_no=1,
                    title="第一集",
                ),
                Subject(
                    id="subject-1",
                    script_id="script-1",
                    type="character",
                    name="新名",
                    review_status="approved",
                    image_entry_id="image-1",
                ),
                AssetEntry(
                    id="image-1",
                    script_id="script-1",
                    subject_id="subject-1",
                    kind="image",
                    storage_key="/generated/image-1.png",
                    mime_type="image/png",
                    size_bytes=1,
                    source="generate_images",
                    sha256="a" * 64,
                    is_approved=True,
                ),
                ShotGroup(
                    shot_group_id="group-1",
                    script_id="script-1",
                    episode_id="episode-1",
                    scene_no=1,
                    group_no="G1",
                    materials={
                        "character": [
                            {"subject_id": "subject-1", "asset_entry_id": "image-1", "name": "旧名"}
                        ]
                    },
                ),
            ])
            raw.commit()

            result = await reference_aggregator.aggregate_references(
                _AsyncSessionAdapter(raw), "group-1"
            )
            assert result.missing_subjects == []
            assert result.reference_images[0]["storage_key"] == "/generated/image-1.png"

    asyncio.run(scenario())


def test_reference_aggregator_falls_back_from_superseded_asset_to_current_subject_image():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            old_image = AssetEntry(
                id="old-image",
                script_id="script-1",
                subject_id="subject-1",
                kind="image",
                storage_key="/generated/old.png",
                mime_type="image/png",
                size_bytes=1,
                source="generate_images",
                sha256="b" * 64,
            )
            old_image.set_status("superseded", superseded_by_id="new-image")
            new_image = AssetEntry(
                id="new-image",
                script_id="script-1",
                subject_id="subject-1",
                kind="image",
                storage_key="/generated/new.png",
                mime_type="image/png",
                size_bytes=1,
                source="generate_images",
                sha256="c" * 64,
            )
            new_image.set_status("approved")
            raw.add_all([
                _script(),
                Episode(episode_id="episode-1", script_id="script-1", episode_no=1, title="第一集"),
                Subject(
                    id="subject-1",
                    script_id="script-1",
                    type="character",
                    name="主角",
                    review_status="approved",
                    image_entry_id="new-image",
                ),
                old_image,
                new_image,
                ShotGroup(
                    shot_group_id="group-1",
                    script_id="script-1",
                    episode_id="episode-1",
                    scene_no=1,
                    group_no="G1",
                    materials={
                        "character": [
                            {"subject_id": "subject-1", "asset_entry_id": "old-image", "name": "主角"}
                        ]
                    },
                ),
            ])
            raw.commit()

            result = await reference_aggregator.aggregate_references(
                _AsyncSessionAdapter(raw), "group-1"
            )
            assert result.missing_subjects == []
            assert result.reference_images[0]["storage_key"] == "/generated/new.png"

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("asset_id", "expected_missing"),
    [("other-subject-image", ["主角"]), ("other-script-image", ["主角"])],
)
def test_reference_aggregator_rejects_explicit_asset_outside_subject_or_script(
    asset_id: str, expected_missing: list[str]
):
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            good_image = AssetEntry(
                id="good-image",
                script_id="script-1",
                subject_id="subject-1",
                kind="image",
                storage_key="/generated/good.png",
                mime_type="image/png",
                size_bytes=1,
                source="generate_images",
            )
            good_image.set_status("approved")
            other_subject_image = AssetEntry(
                id="other-subject-image",
                script_id="script-1",
                subject_id="subject-2",
                kind="image",
                storage_key="/generated/other-subject.png",
                mime_type="image/png",
                size_bytes=1,
                source="generate_images",
            )
            other_subject_image.set_status("approved")
            other_script_image = AssetEntry(
                id="other-script-image",
                script_id="script-2",
                subject_id="subject-3",
                kind="image",
                storage_key="/generated/other-script.png",
                mime_type="image/png",
                size_bytes=1,
                source="generate_images",
            )
            other_script_image.set_status("approved")
            raw.add_all([
                _script(),
                Script(
                    id="script-2", title="剧本二", scope="personal", owner_user_id="user-2",
                    created_by_user_id="user-2",
                ),
                Episode(episode_id="episode-1", script_id="script-1", episode_no=1, title="第一集"),
                Subject(
                    id="subject-1", script_id="script-1", type="character", name="主角",
                    review_status="approved", image_entry_id="good-image",
                ),
                Subject(
                    id="subject-2", script_id="script-1", type="character", name="配角",
                    review_status="approved", image_entry_id="other-subject-image",
                ),
                Subject(
                    id="subject-3", script_id="script-2", type="character", name="他剧本",
                    review_status="approved", image_entry_id="other-script-image",
                ),
                good_image,
                other_subject_image,
                other_script_image,
                ShotGroup(
                    shot_group_id="group-1",
                    script_id="script-1",
                    episode_id="episode-1",
                    scene_no=1,
                    group_no="G1",
                    materials={
                        "character": [
                            {"subject_id": "subject-1", "asset_entry_id": asset_id, "name": "主角"}
                        ]
                    },
                ),
            ])
            raw.commit()

            result = await reference_aggregator.aggregate_references(
                _AsyncSessionAdapter(raw), "group-1"
            )
            assert result.reference_images == []
            assert result.missing_subjects == expected_missing

    asyncio.run(scenario())


def test_reference_audio_approval_is_independent_from_image_review_status():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            audio = AssetEntry(
                id="voice-1",
                script_id="script-1",
                subject_id="sound-1",
                kind="audio",
                storage_key="/upload/voice.mp3",
                mime_type="audio/mpeg",
                size_bytes=1,
                source="upload_images",
                sha256="d" * 64,
            )
            audio.set_status("approved")
            raw.add_all([
                _script(),
                Episode(
                    episode_id="episode-1",
                    script_id="script-1",
                    episode_no=1,
                    title="第一集",
                ),
                Subject(
                    id="sound-1",
                    script_id="script-1",
                    type="sound",
                    name="旁白",
                    review_status="not_designed",
                ),
                audio,
                ShotGroup(
                    shot_group_id="group-1",
                    script_id="script-1",
                    episode_id="episode-1",
                    scene_no=1,
                    group_no="G1",
                    audio_elements=["旁白"],
                ),
            ])
            raw.commit()

            result = await reference_aggregator.aggregate_references(
                _AsyncSessionAdapter(raw), "group-1"
            )
            assert result.missing_audio == []
            assert result.reference_audios[0]["storage_key"] == "/upload/voice.mp3"

    asyncio.run(scenario())


def test_approve_image_rejects_candidate_from_another_script_even_with_same_subject_id():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                _script(),
                Script(
                    id="script-2",
                    title="他人剧本",
                    scope="personal",
                    owner_user_id="user-2",
                    created_by_user_id="user-2",
                ),
                _subject(),
                AssetEntry(
                    id="cross-script-candidate",
                    script_id="script-2",
                    subject_id="subject-1",
                    kind="image",
                    storage_key="/generate_images/material-2/cross.png",
                    mime_type="image/png",
                    size_bytes=1,
                    source="generate_images",
                ),
            ])
            raw.commit()

            with pytest.raises(Exception) as exc_info:
                await review_service.approve_image_with_lock(
                    _AsyncSessionAdapter(raw),
                    "subject-1",
                    "cross-script-candidate",
                    confirm=True,
                )

            assert exc_info.value.status_code == 403
            assert raw.get(Subject, "subject-1").image_entry_id is None
            assert raw.get(AssetEntry, "cross-script-candidate").status == "candidate"

    asyncio.run(scenario())


def test_approve_audio_rejects_candidate_from_another_script_even_with_same_subject_id():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                _script(),
                Script(
                    id="script-2",
                    title="他人剧本",
                    scope="personal",
                    owner_user_id="user-2",
                    created_by_user_id="user-2",
                ),
                Subject(
                    id="sound-1",
                    script_id="script-1",
                    type="sound",
                    name="旁白",
                    review_status="pending",
                ),
                AssetEntry(
                    id="cross-script-audio",
                    script_id="script-2",
                    subject_id="sound-1",
                    kind="audio",
                    storage_key="/upload_images/material-2/cross.mp3",
                    mime_type="audio/mpeg",
                    size_bytes=1,
                    source="upload_images",
                ),
            ])
            raw.commit()

            with pytest.raises(Exception) as exc_info:
                await review_service.approve_audio_with_lock(
                    _AsyncSessionAdapter(raw),
                    "sound-1",
                    "cross-script-audio",
                )

            assert exc_info.value.status_code == 403
            assert raw.get(AssetEntry, "cross-script-audio").status == "candidate"

    asyncio.run(scenario())


def test_list_candidates_filters_out_entries_with_foreign_script_id_for_same_subject():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                _script(),
                Script(
                    id="script-2",
                    title="他人剧本",
                    scope="personal",
                    owner_user_id="user-2",
                    created_by_user_id="user-2",
                ),
                _subject(),
                _entry("local-image"),
                AssetEntry(
                    id="foreign-image",
                    script_id="script-2",
                    subject_id="subject-1",
                    kind="image",
                    storage_key="/generate_images/material-2/foreign.png",
                    mime_type="image/png",
                    size_bytes=1,
                    source="generate_images",
                ),
            ])
            raw.commit()

            images, audios = await asset_crud.list_candidates(
                _AsyncSessionAdapter(raw), "subject-1"
            )

            assert [item.id for item in images] == ["local-image"]
            assert audios == []

    asyncio.run(scenario())


def test_add_candidate_rejects_subject_from_another_script_before_flush():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            subject = Subject(
                id="subject-2",
                script_id="script-2",
                type="character",
                name="他剧本主体",
                review_status="not_designed",
            )
            raw.add_all([
                _script(),
                Script(
                    id="script-2",
                    title="他人剧本",
                    scope="personal",
                    owner_user_id="user-2",
                    created_by_user_id="user-2",
                ),
                subject,
            ])
            raw.commit()

            with pytest.raises(Exception) as exc_info:
                await asset_crud.add_candidate(
                    _AsyncSessionAdapter(raw),
                    "script-1",
                    subject.id,
                    "image",
                    "/generate_images/material-1/a.png",
                    "image/png",
                    1,
                    "generate_images",
                    subject=subject,
                )

            assert exc_info.value.status_code == 403
            assert raw.scalar(select(AssetEntry.id)) is None

    asyncio.run(scenario())


def test_remove_audio_candidate_recalculates_pending_sound_subject_from_audio_candidates():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            subject = Subject(
                id="sound-1",
                script_id="script-1",
                type="sound",
                name="旁白",
                review_status="pending",
            )
            audio_a = AssetEntry(
                id="audio-a",
                script_id="script-1",
                subject_id="sound-1",
                kind="audio",
                storage_key="/upload_images/material-1/a.mp3",
                mime_type="audio/mpeg",
                size_bytes=1,
                source="upload_images",
            )
            audio_b = AssetEntry(
                id="audio-b",
                script_id="script-1",
                subject_id="sound-1",
                kind="audio",
                storage_key="/upload_images/material-1/b.mp3",
                mime_type="audio/mpeg",
                size_bytes=1,
                source="upload_images",
            )
            raw.add_all([_script(), subject, audio_a, audio_b])
            raw.commit()

            await review_service.remove_candidate_with_recalc(
                _AsyncSessionAdapter(raw), "sound-1", "audio-a"
            )
            assert raw.get(Subject, "sound-1").review_status == "pending"

            await review_service.remove_candidate_with_recalc(
                _AsyncSessionAdapter(raw), "sound-1", "audio-b"
            )
            assert raw.get(Subject, "sound-1").review_status == "not_designed"

    asyncio.run(scenario())


def test_replace_impact_reports_audio_references_when_subject_has_no_image_entry():
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            audio = AssetEntry(
                id="voice-1",
                script_id="script-1",
                subject_id="sound-1",
                kind="audio",
                storage_key="/upload_images/material-1/voice.mp3",
                mime_type="audio/mpeg",
                size_bytes=1,
                source="upload_images",
            )
            audio.set_status("approved")
            raw.add_all([
                _script(),
                Episode(
                    episode_id="episode-1",
                    script_id="script-1",
                    episode_no=1,
                    title="第一集",
                ),
                Subject(
                    id="sound-1",
                    script_id="script-1",
                    type="sound",
                    name="旁白",
                    review_status="not_designed",
                ),
                audio,
                ShotGroup(
                    shot_group_id="group-1",
                    script_id="script-1",
                    episode_id="episode-1",
                    scene_no=1,
                    group_no="G1",
                    audio_elements=["旁白"],
                ),
            ])
            raw.commit()

            impact = await review_service.get_replace_impact(
                _AsyncSessionAdapter(raw), "sound-1"
            )

            assert impact.referenced is True
            assert impact.shot_group_count == 1
            assert impact.details[0].materials_category == "audio"

    asyncio.run(scenario())


def test_storage_provider_exposes_signed_url_and_local_uses_proxy_contract(monkeypatch, tmp_path):
    from app.providers.storage import StorageProvider
    from app.providers.storage_local import LocalStorageProvider

    provider = LocalStorageProvider(root=str(tmp_path))

    assert isinstance(provider, StorageProvider)
    assert provider.signed_url("/upload_images/material-1/a.png").endswith(
        "/api/x/cctv/zhihui/flow-app/upload_images/material-1/a.png"
    )


def test_s3_driver_without_dependency_fails_closed(monkeypatch):
    from app.providers import storage

    monkeypatch.setattr(storage.settings, "storage_driver", "s3")
    monkeypatch.setattr(storage, "_storage_singleton", None)

    with pytest.raises(RuntimeError, match="S3 存储不可用"):
        storage.get_storage()


def test_local_storage_path_resolution_rejects_same_prefix_escape(tmp_path):
    from app.providers.storage_local import LocalStorageProvider

    root = tmp_path / "root"
    outside = tmp_path / "root_evil"
    outside.mkdir()
    provider = LocalStorageProvider(root=str(root))

    with pytest.raises(ValueError):
        provider._full_path(Path("../root_evil/upload_images/material-1/a.png").as_posix())


def test_media_proxy_invalid_range_returns_416_with_content_range(monkeypatch):
    async def scenario() -> None:
        from fastapi import HTTPException

        from app.api.v1 import media

        class _Storage:
            async def exists(self, _key: str) -> bool:
                return True

            async def get_size(self, _key: str) -> int:
                return 10

            async def iter_chunks(self, *_args, **_kwargs):
                yield b"0123456789"

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                _script(),
                Materials(id="material-1", name="剧本", script_id="script-1"),
                _subject(),
                AssetEntry(
                    id="asset-1",
                    script_id="script-1",
                    subject_id="subject-1",
                    kind="image",
                    storage_key="/upload_images/material-1/a.png",
                    mime_type="image/png",
                    size_bytes=10,
                    source="upload_images",
                ),
            ])
            raw.commit()
            monkeypatch.setattr(media, "get_storage", lambda: _Storage())
            monkeypatch.setattr(media, "check_script_access", _allow)

            with pytest.raises(HTTPException) as exc_info:
                await media.get_media(
                    "upload_images/material-1/a.png",
                    "bytes=9-1",
                    _AsyncSessionAdapter(raw),
                    SimpleNamespace(),
                )

            assert exc_info.value.status_code == 416
            assert exc_info.value.headers == {"Content-Range": "bytes */10"}

    asyncio.run(scenario())


def test_media_authorizes_video_job_output_and_frame_by_exact_stored_key(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                _script(),
                Episode(
                    episode_id="episode-1",
                    script_id="script-1",
                    episode_no=1,
                    title="第一集",
                ),
                ShotGroup(
                    shot_group_id="group-1",
                    script_id="script-1",
                    episode_id="episode-1",
                    scene_no=1,
                    group_no="G1",
                    unit_id="group:group-1",
                    reference_frames={
                        "first_frame": "generate_images/video/frames/group-1_first.png",
                        "last_frame": "/generate_images/video/frames/group-1_last.png",
                        "reference_frame": "generate_images/video/frames/group-1_reference.png",
                    },
                ),
                VideoGenerationJob(
                    job_id="video-job-1",
                    script_id="script-1",
                    episode_id="episode-1",
                    shot_group_id="group-1",
                    unit_id="group:group-1",
                    unit_runtime_key="group-1:1",
                    provider_key="jianyu",
                    job_type="all_reference",
                    status="succeeded",
                    progress=100,
                    scene_no=1,
                    output_json={
                        "video_storage_key": "/generate_images/video/video-job-1.mp4"
                    },
                ),
            ])
            raw.commit()
            monkeypatch.setattr(media, "check_script_access", _allow)

            await media._authorize_by_storage_key(
                _AsyncSessionAdapter(raw),
                "generate_images/video/video-job-1.mp4",
                SimpleNamespace(),
            )
            for frame_key in (
                "generate_images/video/frames/group-1_first.png",
                "generate_images/video/frames/group-1_last.png",
                "generate_images/video/frames/group-1_reference.png",
            ):
                await media._authorize_by_storage_key(
                    _AsyncSessionAdapter(raw),
                    frame_key,
                    SimpleNamespace(),
                )

    asyncio.run(scenario())


def test_media_requires_timeline_render_job_for_generate_outputs_key(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([
                _script(),
                User(user_id="user-1", username="u", password_hash="x"),
                Episode(
                    episode_id="episode-1",
                    script_id="script-1",
                    episode_no=1,
                    title="第一集",
                ),
                TimelineRenderJob(
                    job_id="render-1",
                    script_id="script-1",
                    episode_id="episode-1",
                    owner_user_id="user-1",
                    idempotency_key="render:1",
                    request_hash="hash",
                    status="succeeded",
                    progress=100,
                    input_json={},
                    output_storage_key="generate_outputs/script-1/episode-1/render-1.mp4",
                ),
            ])
            raw.commit()
            monkeypatch.setattr(media, "check_script_access", _allow)

            await media._authorize_by_storage_key(
                _AsyncSessionAdapter(raw),
                "generate_outputs/script-1/episode-1/render-1.mp4",
                SimpleNamespace(),
            )
            with pytest.raises(Exception) as exc_info:
                await media._authorize_by_storage_key(
                    _AsyncSessionAdapter(raw),
                    "generate_outputs/script-1/episode-1/missing.mp4",
                    SimpleNamespace(),
                )
            assert exc_info.value.status_code == 404

    asyncio.run(scenario())


def test_media_proxy_requires_generated_image_key_to_match_asset_or_generation_output(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        class _Storage:
            async def exists(self, _key: str) -> bool:
                return True

            async def get_size(self, _key: str) -> int:
                return 1

            async def iter_chunks(self, *_args, **_kwargs):
                yield b"x"

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), Materials(id="material-1", name="剧本", script_id="script-1"), _subject()])
            raw.commit()
            monkeypatch.setattr(media, "get_storage", lambda: _Storage())
            monkeypatch.setattr(media, "check_script_access", _allow)

            with pytest.raises(Exception) as orphan:
                await media.get_media(
                    "generate_images/material-1/orphan.png",
                    None,
                    _AsyncSessionAdapter(raw),
                    SimpleNamespace(),
                )
            assert orphan.value.status_code == 404

            raw.add(
                AssetEntry(
                    id="asset-1",
                    script_id="script-1",
                    subject_id="subject-1",
                    kind="image",
                    storage_key="/generate_images/material-1/asset.png",
                    mime_type="image/png",
                    size_bytes=1,
                    source="generate_images",
                )
            )
            raw.commit()
            response = await media.get_media(
                "generate_images/material-1/asset.png",
                None,
                _AsyncSessionAdapter(raw),
                SimpleNamespace(),
            )
            assert response.status_code == 200

    asyncio.run(scenario())


def test_media_proxy_requires_upload_key_to_match_exact_asset_entry(monkeypatch):
    async def scenario() -> None:
        from app.api.v1 import media

        class _Storage:
            async def exists(self, _key: str) -> bool:
                return True

            async def get_size(self, _key: str) -> int:
                return 1

            async def iter_chunks(self, *_args, **_kwargs):
                yield b"x"

        async def _allow(*_args, **_kwargs) -> None:
            return None

        engine = create_engine("sqlite+pysqlite:///:memory:")
        _asset_tables(engine)
        with Session(engine, expire_on_commit=False) as raw:
            raw.add_all([_script(), Materials(id="material-1", name="剧本", script_id="script-1"), _subject()])
            raw.commit()
            monkeypatch.setattr(media, "get_storage", lambda: _Storage())
            monkeypatch.setattr(media, "check_script_access", _allow)

            with pytest.raises(Exception) as orphan:
                await media.get_media(
                    "upload_images/material-1/orphan.png",
                    None,
                    _AsyncSessionAdapter(raw),
                    SimpleNamespace(),
                )
            assert orphan.value.status_code == 404

            raw.add(
                AssetEntry(
                    id="upload-asset-1",
                    script_id="script-1",
                    subject_id="subject-1",
                    kind="image",
                    storage_key="/upload_images/material-1/asset.png",
                    mime_type="image/png",
                    size_bytes=1,
                    source="upload_images",
                )
            )
            raw.commit()
            response = await media.get_media(
                "upload_images/material-1/asset.png",
                None,
                _AsyncSessionAdapter(raw),
                SimpleNamespace(),
            )
            assert response.status_code == 200

    asyncio.run(scenario())
