"""F10 画风异步任务在真实 PostgreSQL 下的并发契约。"""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.errors import ConflictError
from app.models.art_style import ArtStyle
from app.models.art_style_extraction_job import ArtStyleExtractionJob
from app.models.asset_entry import AssetEntry
from app.models.script import Script
from app.models.subject import Subject
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.user import User
from app.services.art_style_extraction_jobs import queue_art_style_extraction

_DATABASE_URL_ENV = "MTV_TEST_POSTGRES_URL"
_SCRIPT_ID = "script-art-style-pg"
_USER_ID = "user-art-style-pg"


def _database_url() -> str:
    value = os.environ.get(_DATABASE_URL_ENV)
    if not value:
        pytest.skip(f"未配置 {_DATABASE_URL_ENV}，跳过真实 PostgreSQL 测试")
    if make_url(value).drivername != "postgresql+asyncpg":
        pytest.fail(f"{_DATABASE_URL_ENV} 必须使用 postgresql+asyncpg 驱动")
    return value


@asynccontextmanager
async def _isolated_db() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(_database_url(), pool_size=4, max_overflow=0)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db:
        await db.execute(delete(TaskDispatchOutbox).where(TaskDispatchOutbox.task_type == "art_style.extract"))
        await db.execute(delete(ArtStyleExtractionJob).where(ArtStyleExtractionJob.script_id == _SCRIPT_ID))
        await db.execute(delete(ArtStyle).where(ArtStyle.script_id == _SCRIPT_ID))
        await db.execute(delete(AssetEntry).where(AssetEntry.script_id == _SCRIPT_ID))
        await db.execute(delete(Subject).where(Subject.script_id == _SCRIPT_ID))
        await db.execute(delete(Script).where(Script.id == _SCRIPT_ID))
        await db.execute(delete(User).where(User.user_id == _USER_ID))
        db.add(User(user_id=_USER_ID, username=_USER_ID, password_hash="hash", enabled=True, account_type="personal"))
        db.add(Script(id=_SCRIPT_ID, title="F10 PG", scope="personal", owner_user_id=_USER_ID, created_by_user_id=_USER_ID))
        db.add(Subject(id="subject-art-style-pg", script_id=_SCRIPT_ID, type="character", name="角色", pipeline_name="角色", episode_nos=[1], review_status="not_designed"))
        data = b"postgres-style-image"
        db.add(
            AssetEntry(
                id="asset-art-style-pg",
                script_id=_SCRIPT_ID,
                subject_id="subject-art-style-pg",
                kind="image",
                storage_key="upload_images/pg/style.png",
                mime_type="image/png",
                size_bytes=len(data),
                source="upload_images",
                sha256=hashlib.sha256(data).hexdigest(),
                status="approved",
            )
        )
        await db.commit()
    try:
        yield sessions
    finally:
        async with sessions() as db:
            await db.execute(delete(TaskDispatchOutbox).where(TaskDispatchOutbox.task_type == "art_style.extract"))
            await db.execute(delete(ArtStyleExtractionJob).where(ArtStyleExtractionJob.script_id == _SCRIPT_ID))
            await db.execute(delete(ArtStyle).where(ArtStyle.script_id == _SCRIPT_ID))
            await db.execute(delete(AssetEntry).where(AssetEntry.script_id == _SCRIPT_ID))
            await db.execute(delete(Subject).where(Subject.script_id == _SCRIPT_ID))
            await db.execute(delete(Script).where(Script.id == _SCRIPT_ID))
            await db.execute(delete(User).where(User.user_id == _USER_ID))
            await db.commit()
        await engine.dispose()


def test_postgres_concurrent_same_name_is_safe_and_idempotent() -> None:
    async def scenario() -> None:
        async with _isolated_db() as sessions:
            data = b"postgres-style-image"
            storage = type("Storage", (), {"get": lambda self, key: asyncio.sleep(0, result=data)})()

            async def enqueue(key: str) -> tuple[str, str]:
                async with sessions() as db:
                    try:
                        row = await queue_art_style_extraction(
                            db,
                            script_id=_SCRIPT_ID,
                            name="PG 并发画风",
                            asset_ids=["asset-art-style-pg"],
                            actor_user_id=_USER_ID,
                            idempotency_key=key,
                            storage=storage,
                        )
                        await db.commit()
                        return "ok", row.id
                    except ConflictError:
                        await db.rollback()
                        return "conflict", key

            outcomes = await asyncio.gather(enqueue("concurrent-a"), enqueue("concurrent-b"))
            assert sorted(result[0] for result in outcomes) == ["conflict", "ok"]
            async with sessions() as db:
                assert int(
                    (await db.execute(select(func.count()).select_from(ArtStyleExtractionJob).where(ArtStyleExtractionJob.script_id == _SCRIPT_ID))).scalar_one()
                ) == 1
                assert int(
                    (await db.execute(select(func.count()).select_from(TaskDispatchOutbox).where(TaskDispatchOutbox.task_type == "art_style.extract"))).scalar_one()
                ) == 1

    asyncio.run(scenario())
