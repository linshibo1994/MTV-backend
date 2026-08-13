"""F10 参考图画风提取可靠异步链路专项测试。"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.core.errors import ConflictError
from app.models.art_style import ArtStyle
from app.models.art_style_extraction_job import ArtStyleExtractionJob
from app.models.asset_entry import AssetEntry
from app.models.script import Script
from app.models.subject import Subject
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.user import User
from app.services.art_style_extraction_jobs import (
    claim_art_style_extraction,
    execute_claimed_art_style_extraction,
    fail_expired_art_style_extractions,
    queue_art_style_extraction,
)
from app.workers.outbox import publish_outbox_payload


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    del type_, compiler, kwargs
    return "JSON"


class _AsyncNested:
    def __init__(self, transaction) -> None:
        self.transaction = transaction

    async def __aenter__(self):
        return self.transaction.__enter__()

    async def __aexit__(self, exc_type, exc, tb):
        return self.transaction.__exit__(exc_type, exc, tb)


class _AsyncSessionAdapter:
    def __init__(self, session: Session) -> None:
        self.session = session

    async def execute(self, statement):
        return self.session.execute(statement)

    async def flush(self) -> None:
        self.session.flush()

    async def commit(self) -> None:
        self.session.commit()

    async def refresh(self, row) -> None:
        self.session.refresh(row)

    async def get(self, model, key):
        return self.session.get(model, key)

    def add(self, row) -> None:
        self.session.add(row)

    def begin_nested(self):
        return _AsyncNested(self.session.begin_nested())


def _db() -> tuple[_AsyncSessionAdapter, Session, SimpleNamespace]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for table in (
        User.__table__,
        Script.__table__,
        Subject.__table__,
        AssetEntry.__table__,
        ArtStyle.__table__,
        ArtStyleExtractionJob.__table__,
        TaskDispatchOutbox.__table__,
    ):
        table.create(engine)
    raw = Session(engine, expire_on_commit=False)
    user = User(
        user_id="art-style-user",
        username="art-style-user",
        password_hash="hash",
        enabled=True,
        account_type="personal",
    )
    script = Script(
        id="art-style-script",
        title="画风异步专项",
        scope="personal",
        owner_user_id=user.user_id,
        created_by_user_id=user.user_id,
    )
    subject = Subject(
        id="art-style-subject",
        script_id=script.id,
        type="character",
        name="角色",
        pipeline_name="角色",
        episode_nos=[1],
        review_status="not_designed",
    )
    data = b"valid-image-content"
    asset = AssetEntry(
        id="art-style-asset",
        script_id=script.id,
        subject_id=subject.id,
        kind="image",
        storage_key="upload_images/material/style.png",
        mime_type="image/png",
        size_bytes=len(data),
        source="upload_images",
        sha256=hashlib.sha256(data).hexdigest(),
        status="approved",
    )
    raw.add_all([user, script, subject, asset])
    raw.commit()
    return _AsyncSessionAdapter(raw), raw, SimpleNamespace(
        user=user, script=script, asset=asset, data=data
    )


def test_queue_is_idempotent_and_provider_is_only_called_by_claimed_worker() -> None:
    """POST 阶段只持久化任务；重复消息/请求不会重复调用外部 Provider。"""

    async def scenario() -> None:
        db, raw, seeded = _db()
        storage = SimpleNamespace(get=AsyncMock(return_value=seeded.data))
        provider = SimpleNamespace(
            provider_name="mock-vision",
            model_name="mock-v1",
            extract_style=AsyncMock(
                return_value={
                    "prompt_zh": "冷调电影光影",
                    "prompt_en": "cold cinematic lighting",
                    "negative_prompt": "flat light",
                    "visual_style": "live_action",
                }
            ),
        )
        first = await queue_art_style_extraction(
            db,
            script_id=seeded.script.id,
            name="冷调电影",
            asset_ids=[seeded.asset.id],
            actor_user_id=seeded.user.user_id,
            idempotency_key="extract-once",
            storage=storage,
        )
        replay = await queue_art_style_extraction(
            db,
            script_id=seeded.script.id,
            name="冷调电影",
            asset_ids=[seeded.asset.id],
            actor_user_id=seeded.user.user_id,
            idempotency_key="extract-once",
            storage=storage,
        )
        assert replay.id == first.id
        assert provider.extract_style.await_count == 0
        assert raw.scalar(select(func.count()).select_from(TaskDispatchOutbox)) == 1
        assert first.billing_mode == "unmetered"

        claim = await claim_art_style_extraction(
            db,
            job_id=first.id,
            expected_attempt=1,
            owner="worker-a",
        )
        assert claim is not None
        await db.commit()
        await execute_claimed_art_style_extraction(
            db,
            claim=claim,
            provider=provider,
            storage=storage,
        )
        await db.commit()
        assert provider.extract_style.await_count == 1
        saved = raw.get(ArtStyleExtractionJob, first.id)
        assert saved is not None and saved.status == "succeeded"
        assert saved.art_style_id is not None
        style = raw.get(ArtStyle, saved.art_style_id)
        assert style is not None and style.prompt_en == "cold cinematic lighting"

        # 重复 Celery 消息在终态无法再次领取，自然不会再次调用 Provider。
        duplicate_claim = await claim_art_style_extraction(
            db,
            job_id=first.id,
            expected_attempt=1,
            owner="worker-b",
        )
        assert duplicate_claim is None
        assert provider.extract_style.await_count == 1

    asyncio.run(scenario())


def test_idempotency_payload_conflict_and_provider_error_are_sanitized() -> None:
    async def scenario() -> None:
        db, raw, seeded = _db()
        storage = SimpleNamespace(get=AsyncMock(return_value=seeded.data))
        row = await queue_art_style_extraction(
            db,
            script_id=seeded.script.id,
            name="安全错误",
            asset_ids=[seeded.asset.id],
            actor_user_id=seeded.user.user_id,
            idempotency_key="stable-key",
            storage=storage,
        )
        with pytest.raises(ConflictError, match="不同"):
            await queue_art_style_extraction(
                db,
                script_id=seeded.script.id,
                name="另一个名字",
                asset_ids=[seeded.asset.id],
                actor_user_id=seeded.user.user_id,
                idempotency_key="stable-key",
                storage=storage,
            )
        claim = await claim_art_style_extraction(
            db, job_id=row.id, expected_attempt=1, owner="worker-a"
        )
        assert claim is not None
        await db.commit()
        provider = SimpleNamespace(
            provider_name="mock-vision",
            model_name="mock-v1",
            extract_style=AsyncMock(side_effect=RuntimeError("SECRET provider response body")),
        )
        await execute_claimed_art_style_extraction(
            db, claim=claim, provider=provider, storage=storage
        )
        await db.commit()
        saved = raw.get(ArtStyleExtractionJob, row.id)
        assert saved is not None and saved.status == "failed"
        assert saved.error_code == "provider_failed"
        assert "SECRET" not in (saved.error_message or "")

    asyncio.run(scenario())


def test_expired_running_job_fails_without_provider_replay() -> None:
    async def scenario() -> None:
        db, raw, seeded = _db()
        storage = SimpleNamespace(get=AsyncMock(return_value=seeded.data))
        row = await queue_art_style_extraction(
            db,
            script_id=seeded.script.id,
            name="超时画风",
            asset_ids=[seeded.asset.id],
            actor_user_id=seeded.user.user_id,
            idempotency_key="timeout-key",
            storage=storage,
        )
        claim = await claim_art_style_extraction(
            db, job_id=row.id, expected_attempt=1, owner="lost-worker"
        )
        assert claim is not None
        row.leased_until = datetime.now(UTC) - timedelta(seconds=1)
        await db.commit()
        assert await fail_expired_art_style_extractions(db) == 1
        await db.commit()
        saved = raw.get(ArtStyleExtractionJob, row.id)
        assert saved is not None and saved.status == "failed"
        assert saved.error_code == "worker_timeout"

    asyncio.run(scenario())


def test_openapi_requires_idempotency_and_outbox_publishes_only_async_worker(monkeypatch) -> None:
    """HTTP 契约为 202 Job，Outbox 载荷只构造后台 Worker 签名。"""

    from app.main import create_app
    from app.workers import art_style_extraction as worker

    spec = create_app().openapi()
    post = spec["paths"]["/v3api/scripts/{script_id}/art-styles/extract"]["post"]
    assert "202" in post["responses"]
    idempotency = next(item for item in post["parameters"] if item["name"] == "Idempotency-Key")
    assert idempotency["in"] == "header" and idempotency["required"] is True
    assert "/v3api/scripts/{script_id}/art-styles/extractions/{job_id}" in spec["paths"]

    applied = SimpleNamespace(apply_async=MagicMock(return_value=None))
    signature = MagicMock(return_value=applied)
    monkeypatch.setattr(worker.extract_art_style_task, "s", signature)

    publish_outbox_payload(
        "art_style.extract",
        {"job_id": "style-job", "attempt": 2},
    )
    signature.assert_called_once_with("style-job", 2)
    applied.apply_async.assert_called_once_with()
