"""T7 分镜 API 邻接行为测试。"""

from __future__ import annotations

import os
import secrets
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.core.errors import ForbiddenError


async def test_storyboard_cleanup_blocks_by_shot_group_foreign_key() -> None:
    """重生成必须按真实外键阻断，不能依赖可漂移的 unit_id 或任务状态。"""
    from sqlalchemy.dialects import postgresql

    from app.core.errors import ConflictError
    from app.services import storyboard as storyboard_service

    class _GroupRows:
        def scalars(self):
            return self

        def all(self):
            return ["group-old"]

    class _BlockingRow:
        def scalar_one_or_none(self):
            return "video-old"

    class CaptureDB:
        def __init__(self) -> None:
            self.statements: list[object] = []
            self.results = [_GroupRows(), _BlockingRow()]

        async def execute(self, statement):
            self.statements.append(statement)
            return self.results.pop(0)

    db = CaptureDB()
    with pytest.raises(ConflictError) as exc_info:
        await storyboard_service._cleanup_existing_storyboard(db, "episode-1")

    assert exc_info.value.detail == (
        "该分集存在进行中或已生成视频的镜头组，"
        "请先删除或等待相关视频任务结束后再重新生成分镜"
    )
    assert len(db.statements) == 2
    blocking_sql = " ".join(
        str(db.statements[1].compile(dialect=postgresql.dialect())).split()
    )
    assert "video_generation_jobs.shot_group_id IN" in blocking_sql
    assert "video_generation_jobs.unit_id" not in blocking_sql
    assert "video_generation_jobs.status" not in blocking_sql


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 分镜外键阻断测试",
)
@pytest.mark.parametrize("status", ["failed", "canceled"])
def test_postgres_storyboard_cleanup_returns_conflict_for_terminal_video_reference(
    status: str,
) -> None:
    import asyncio

    from sqlalchemy import delete
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.errors import ConflictError
    from app.models.episode import Episode
    from app.models.script import Script
    from app.models.shot_group import ShotGroup
    from app.models.video_generation_job import VideoGenerationJob
    from app.services import storyboard as storyboard_service

    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = secrets.token_hex(6)
        script_id = f"script-story-{suffix}"
        episode_id = f"episode-story-{suffix}"
        group_id = f"group-story-{suffix}"
        job_id = f"video-story-{suffix}"

        try:
            async with session_factory() as db:
                try:
                    db.add(
                        Script(
                            id=script_id,
                            title="分镜外键阻断测试",
                            scope="personal",
                            owner_user_id="user-story",
                            created_by_user_id="user-story",
                        )
                    )
                    await db.flush()
                    db.add(
                        Episode(
                            episode_id=episode_id,
                            script_id=script_id,
                            episode_no=1,
                            title="第一集",
                        )
                    )
                    await db.flush()
                    db.add(
                        ShotGroup(
                            shot_group_id=group_id,
                            script_id=script_id,
                            episode_id=episode_id,
                            scene_no=1,
                            group_no="G1",
                        )
                    )
                    await db.flush()
                    db.add(
                        VideoGenerationJob(
                            job_id=job_id,
                            script_id=script_id,
                            episode_id=episode_id,
                            shot_group_id=group_id,
                            scene_no=1,
                            unit_id=f"legacy:{group_id}",
                            unit_runtime_key=f"{episode_id}:1:legacy:{group_id}",
                            provider_key="modelsai",
                            job_type="all_reference",
                            status=status,
                        )
                    )
                    await db.commit()

                    with pytest.raises(ConflictError) as exc_info:
                        await storyboard_service._cleanup_existing_storyboard(
                            db,
                            episode_id,
                        )
                    assert exc_info.value.detail == (
                        "该分集存在进行中或已生成视频的镜头组，"
                        "请先删除或等待相关视频任务结束后再重新生成分镜"
                    )
                    await db.rollback()

                    assert await db.get(ShotGroup, group_id) is not None
                    assert await db.get(VideoGenerationJob, job_id) is not None
                finally:
                    await db.rollback()
                    await db.execute(
                        delete(VideoGenerationJob).where(
                            VideoGenerationJob.job_id == job_id
                        )
                    )
                    await db.execute(
                        delete(ShotGroup).where(ShotGroup.shot_group_id == group_id)
                    )
                    await db.execute(
                        delete(Episode).where(Episode.episode_id == episode_id)
                    )
                    await db.execute(delete(Script).where(Script.id == script_id))
                    await db.commit()
        finally:
            await engine.dispose()

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 分镜版本栅栏测试",
)
def test_postgres_storyboard_refreshes_episode_after_llm_before_cleanup(
    monkeypatch,
) -> None:
    import asyncio

    from sqlalchemy import delete, update
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.models.episode import Episode
    from app.models.script import Script
    from app.services import storyboard as storyboard_service

    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = secrets.token_hex(6)
        script_id = f"script-fence-{suffix}"
        episode_id = f"episode-fence-{suffix}"
        cleanup = AsyncMock()

        async def update_episode_after_llm(*_args, **_kwargs) -> dict:
            async with session_factory() as writer:
                await writer.execute(
                    update(Episode)
                    .where(Episode.episode_id == episode_id)
                    .values(
                        script_text="并发保存后的新正文",
                        episode_version=2,
                        client_save_seq=1,
                    )
                )
                await writer.commit()
            return {"ignored": True}

        monkeypatch.setattr(
            storyboard_service,
            "_call_llm",
            update_episode_after_llm,
        )
        monkeypatch.setattr(
            storyboard_service,
            "validate_storyboard_contract",
            lambda *_args, **_kwargs: SimpleNamespace(
                model_dump=lambda: {"scenes": []}
            ),
        )
        monkeypatch.setattr(
            storyboard_service,
            "_cleanup_existing_storyboard",
            cleanup,
        )

        try:
            async with session_factory() as db:
                try:
                    db.add(
                        Script(
                            id=script_id,
                            title="分镜版本栅栏测试",
                            scope="personal",
                            owner_user_id="user-fence",
                            created_by_user_id="user-fence",
                        )
                    )
                    await db.flush()
                    db.add(
                        Episode(
                            episode_id=episode_id,
                            script_id=script_id,
                            episode_no=1,
                            title="第一集",
                            script_text="LLM 开始前的旧正文",
                            episode_version=1,
                            client_save_seq=0,
                        )
                    )
                    await db.commit()

                    with pytest.raises(
                        RuntimeError,
                        match="storyboard input changed before commit",
                    ):
                        await storyboard_service.generate_storyboard(
                            db,
                            script_id,
                            episode_id,
                            expected_episode_version=1,
                            expected_client_save_seq=0,
                        )
                    cleanup.assert_not_awaited()
                    await db.rollback()
                finally:
                    await db.rollback()
                    await db.execute(
                        delete(Episode).where(Episode.episode_id == episode_id)
                    )
                    await db.execute(delete(Script).where(Script.id == script_id))
                    await db.commit()
        finally:
            await engine.dispose()

    asyncio.run(scenario())


async def test_storyboard_http_unauthenticated_returns_401(client) -> None:
    response = await client.get("/v3api/scripts/script-1/episodes/episode-1/shot-hierarchy")

    assert response.status_code == 401
    assert response.json() == {"detail": "未登录"}


async def test_storyboard_http_permission_denied_returns_403(client, monkeypatch) -> None:
    from app.api.v1 import shots
    from app.core.auth import get_current_user
    from app.core.database import get_db

    script = SimpleNamespace(id="script-1", scope="personal", owner_user_id="owner")
    user = SimpleNamespace(user_id="user-2", enabled=True)

    class _Result:
        def scalar_one_or_none(self):
            return script

    db = SimpleNamespace(execute=AsyncMock(return_value=_Result()))

    async def override_db():
        yield db

    async def override_user():
        return user

    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_current_user] = override_user
    monkeypatch.setattr(shots, "check_script_access", AsyncMock(side_effect=ForbiddenError("无权限")))

    response = await client.get("/v3api/shots/script-1/cover")

    assert response.status_code == 403


async def test_storyboard_idempotency_key_http_max_length(client, monkeypatch) -> None:
    from app.api.v1 import shots
    from app.core.auth import get_current_user

    async def override_user():
        return SimpleNamespace(user_id="user-1", enabled=True)

    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_current_user] = override_user
    validate = AsyncMock(side_effect=AssertionError("超长 header 不应进入分镜业务逻辑"))
    monkeypatch.setattr(shots, "_validate_script_episode", validate)

    response = await client.post(
        "/v3api/scripts/script-1/episodes/episode-1/generate-storyboard",
        params={"episode_version": 1},
        headers={"Idempotency-Key": "x" * 129},
    )

    assert response.status_code == 422
    assert response.json() == {"detail": "字段校验失败"}
    validate.assert_not_awaited()


async def test_storyboard_http_denies_cross_resource_job_idor(client, monkeypatch) -> None:
    from app.api.v1 import shots
    from app.core.auth import get_current_user
    from app.core.database import get_db

    user = SimpleNamespace(user_id="user-1", enabled=True)
    cross_job = SimpleNamespace(
        job_id="job-cross",
        task_id=None,
        episode_id="episode-2",
        script_id="script-2",
        status="pending",
        progress=0,
        progress_stage=None,
        progress_message=None,
        error_message=None,
        detail=None,
    )

    async def override_db():
        yield SimpleNamespace()

    async def override_user():
        return user

    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_current_user] = override_user
    monkeypatch.setattr(shots, "_validate_script_episode", AsyncMock())
    monkeypatch.setattr(shots, "get_job", AsyncMock(return_value=cross_job))

    response = await client.get(
        "/v3api/scripts/script-1/episodes/episode-1/storyboard/job-cross"
    )

    assert response.status_code == 403


async def test_storyboard_generate_rolls_back_when_reserve_fails(client, monkeypatch) -> None:
    from app.api.v1 import shots
    from app.core.auth import get_current_user
    from app.core.database import get_db

    user = SimpleNamespace(user_id="user-1", enabled=True)
    script = SimpleNamespace(id="script-1", scope="personal", owner_user_id="user-1")
    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_version=3,
        client_save_seq=4,
        script_text="剧情",
    )
    account = SimpleNamespace(id="account-1", status="active")
    job = SimpleNamespace(
        job_id="job-1",
        attempt=1,
        status="pending",
        reservation_id=None,
        detail=None,
        task_id=None,
    )

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

        def scalar_one(self):
            return self.value

    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(script),
                _Result(episode),
                _Result(None),
                _Result(None),
                _Result(account),
                _Result(0),
            ]
        ),
        rollback=AsyncMock(),
        commit=AsyncMock(),
    )

    async def override_db():
        yield db

    async def override_user():
        return user

    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides[get_db] = override_db
    app.dependency_overrides[get_current_user] = override_user
    monkeypatch.setattr(shots, "check_script_access", AsyncMock())
    monkeypatch.setattr(shots, "create_job", AsyncMock(return_value=job))
    monkeypatch.setattr(shots, "reserve", AsyncMock(side_effect=RuntimeError("reserve failed")))
    monkeypatch.setattr(shots, "enqueue_task", AsyncMock())

    with pytest.raises(RuntimeError, match="reserve failed"):
        await client.post(
            "/v3api/scripts/script-1/episodes/episode-1/generate-storyboard",
            params={"episode_version": 3},
            headers={"Idempotency-Key": "idem-rollback"},
        )

    db.rollback.assert_awaited()
    db.commit.assert_not_awaited()
    shots.enqueue_task.assert_not_awaited()


async def test_storyboard_cancel_locks_job_before_credit_release(monkeypatch) -> None:
    from app.api.v1 import shots

    user = SimpleNamespace(user_id="user-1", enabled=True)
    job = SimpleNamespace(
        job_id="job-1",
        task_id=None,
        episode_id="episode-1",
        script_id="script-1",
        status="running",
        progress=50,
        progress_stage="generating",
        progress_message=None,
        error_message=None,
        detail=None,
        reservation_id="reservation-1",
        attempt=1,
    )

    class _OutboxResult:
        def scalars(self):
            return self

        def all(self):
            return []

    events: list[str] = []

    async def lock_job(*args, **kwargs):
        events.append("job_lock")
        assert kwargs == {"with_for_update": True, "populate_existing": True}
        return job

    async def release_credit(*args, **kwargs):
        events.append("release")

    async def cancel_job(*args, **kwargs):
        events.append("terminal_cas")
        job.status = "canceled"
        return True

    db = SimpleNamespace(
        get=AsyncMock(side_effect=lock_job),
        execute=AsyncMock(return_value=_OutboxResult()),
        rollback=AsyncMock(),
        commit=AsyncMock(),
    )
    monkeypatch.setattr(
        shots,
        "_get_authorized_storyboard_job",
        AsyncMock(return_value=job),
    )
    monkeypatch.setattr(shots, "release", release_credit)
    monkeypatch.setattr(shots, "cancel_job", cancel_job)

    result = await shots.cancel_storyboard(
        "script-1", "episode-1", "job-1", db=db, user=user
    )

    assert result.status == "canceled"
    assert events == ["job_lock", "release", "terminal_cas"]
    db.commit.assert_awaited_once()
    db.rollback.assert_not_awaited()


async def test_storyboard_cancel_rolls_back_when_terminal_cas_fails(monkeypatch) -> None:
    from app.api.v1 import shots

    user = SimpleNamespace(user_id="user-1", enabled=True)
    job = SimpleNamespace(
        job_id="job-1",
        task_id=None,
        episode_id="episode-1",
        script_id="script-1",
        status="running",
        progress=50,
        progress_stage="generating",
        progress_message=None,
        error_message=None,
        detail=None,
        reservation_id="reservation-1",
        attempt=1,
    )

    class _OutboxResult:
        def scalars(self):
            return self

        def all(self):
            return []

    db = SimpleNamespace(
        get=AsyncMock(return_value=job),
        execute=AsyncMock(return_value=_OutboxResult()),
        rollback=AsyncMock(),
        commit=AsyncMock(),
    )
    monkeypatch.setattr(
        shots,
        "_get_authorized_storyboard_job",
        AsyncMock(return_value=job),
    )
    monkeypatch.setattr(shots, "release", AsyncMock())
    monkeypatch.setattr(shots, "cancel_job", AsyncMock(return_value=False))

    with pytest.raises(shots.ConflictError, match="状态已变化"):
        await shots.cancel_storyboard(
            "script-1", "episode-1", "job-1", db=db, user=user
        )

    db.rollback.assert_awaited_once()
    db.commit.assert_not_awaited()
