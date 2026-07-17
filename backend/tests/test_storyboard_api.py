"""T7 分镜 API 邻接行为测试。"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.core.errors import ForbiddenError


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
