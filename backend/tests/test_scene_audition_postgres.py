"""Coverage Audition 在真实 PostgreSQL 下的事务与并发契约。"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import delete, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.errors import BadRequestError
from app.models.episode import Episode
from app.models.scene_audition import SceneAudition
from app.models.script import Script
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.user import User
from app.services.scene_auditions import queue_audition

_DATABASE_URL_ENV = "MTV_TEST_POSTGRES_URL"


def _database_url() -> str:
    value = os.environ.get(_DATABASE_URL_ENV)
    if not value:
        pytest.skip(f"未配置 {_DATABASE_URL_ENV}，跳过真实 PostgreSQL 测试")
    url = make_url(value)
    if url.drivername != "postgresql+asyncpg":
        pytest.fail(f"{_DATABASE_URL_ENV} 必须使用 postgresql+asyncpg 驱动")
    return value


@asynccontextmanager
async def _isolated_audition_db() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(_database_url(), pool_size=4, max_overflow=0)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db:
        await db.execute(delete(TaskDispatchOutbox).where(TaskDispatchOutbox.task_type == "scene_audition.generate"))
        await db.execute(delete(SceneAudition))
        await db.execute(delete(Episode).where(Episode.episode_id == "episode-audition-pg"))
        await db.execute(delete(Script).where(Script.id == "script-audition-pg"))
        await db.execute(delete(User).where(User.user_id == "user-audition-pg"))
        db.add(
            User(
                user_id="user-audition-pg",
                username="user-audition-pg",
                password_hash="hash",
                enabled=True,
                account_type="personal",
            )
        )
        db.add(
            Script(
                id="script-audition-pg",
                title="导演方案 PostgreSQL 专项",
                scope="personal",
                owner_user_id="user-audition-pg",
                created_by_user_id="user-audition-pg",
            )
        )
        db.add(
            Episode(
                episode_id="episode-audition-pg",
                script_id="script-audition-pg",
                episode_no=987654,
                title="导演方案专项",
                script_text="身份揭示场景。",
                episode_version=3,
            )
        )
        await db.commit()
    try:
        yield sessions
    finally:
        async with sessions() as db:
            await db.execute(
                delete(TaskDispatchOutbox).where(
                    TaskDispatchOutbox.task_type == "scene_audition.generate"
                )
            )
            await db.execute(delete(SceneAudition).where(SceneAudition.script_id == "script-audition-pg"))
            await db.execute(delete(Episode).where(Episode.episode_id == "episode-audition-pg"))
            await db.execute(delete(Script).where(Script.id == "script-audition-pg"))
            await db.execute(delete(User).where(User.user_id == "user-audition-pg"))
            await db.commit()
        await engine.dispose()


def test_scene_audition_postgres_flush_and_concurrent_idempotency() -> None:
    """合法关键类型可落库；并发重复点击只保留一条记录和一个 attempt。"""

    async def scenario() -> None:
        async with _isolated_audition_db() as sessions:

            async def enqueue() -> str:
                async with sessions() as db:
                    row = await queue_audition(
                        db,
                        script_id="script-audition-pg",
                        episode_id="episode-audition-pg",
                        scene_no=1,
                        expected_episode_version=3,
                        scene_type="identity_reveal",
                    )
                    await db.commit()
                    return row.id

            first_id, replay_id = await asyncio.gather(enqueue(), enqueue())
            assert first_id == replay_id
            async with sessions() as db:
                auditions = list(
                    (
                        await db.execute(
                            select(SceneAudition).where(
                                SceneAudition.script_id == "script-audition-pg"
                            )
                        )
                    ).scalars()
                )
                outboxes = list(
                    (
                        await db.execute(
                            select(TaskDispatchOutbox).where(
                                TaskDispatchOutbox.task_type == "scene_audition.generate",
                                TaskDispatchOutbox.job_id == first_id,
                            )
                        )
                    ).scalars()
                )
                assert len(auditions) == 1
                assert auditions[0].scene_type == "identity_reveal"
                assert auditions[0].attempt_count == 1
                assert len(outboxes) == 1
                assert outboxes[0].attempt == 1

                auditions[0].status = "failed"
                auditions[0].error_code = "model_generation_failed"
                auditions[0].error_message = "导演方案生成失败，请重试"
                auditions[0].finished_at = auditions[0].updated_at
                await db.commit()

            # 两个失败重试请求竞争同一记录行锁，只允许第一个创建 attempt=2。
            retry_id, retry_replay_id = await asyncio.gather(enqueue(), enqueue())
            assert retry_id == retry_replay_id == first_id
            async with sessions() as db:
                retried = await db.get(SceneAudition, first_id)
                assert retried is not None
                assert retried.status == "draft"
                assert retried.attempt_count == 2
                attempts = list(
                    (
                        await db.execute(
                            select(TaskDispatchOutbox.attempt)
                            .where(
                                TaskDispatchOutbox.task_type == "scene_audition.generate",
                                TaskDispatchOutbox.job_id == first_id,
                            )
                            .order_by(TaskDispatchOutbox.attempt)
                        )
                    ).scalars()
                )
                assert attempts == [1, 2]

                with pytest.raises(BadRequestError, match="仅人物亮相"):
                    await queue_audition(
                        db,
                        script_id="script-audition-pg",
                        episode_id="episode-audition-pg",
                        scene_no=2,
                        expected_episode_version=3,
                        scene_type="ordinary",
                    )

                changed_type = await queue_audition(
                    db,
                    script_id="script-audition-pg",
                    episode_id="episode-audition-pg",
                    scene_no=1,
                    expected_episode_version=3,
                    scene_type="relationship_reversal",
                )
                assert changed_type.id == first_id
                assert changed_type.status == "stale"
                assert changed_type.scene_type == "identity_reveal"
                assert len(
                    list(
                        (
                            await db.execute(
                                select(TaskDispatchOutbox).where(
                                    TaskDispatchOutbox.task_type
                                    == "scene_audition.generate",
                                    TaskDispatchOutbox.job_id == first_id,
                                )
                            )
                        ).scalars()
                    )
                ) == 2

    asyncio.run(scenario())
