"""T9 时间线 ASGI 行为测试。

只使用 dependency override + fake DB，不启动真实服务。
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from app.api.v1 import timeline as timeline_api
from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.errors import ConflictError, ForbiddenError, NotFoundError
from app.main import app


class _FakeDB:
    async def commit(self) -> None:
        return None


@pytest.fixture(autouse=True)
def _clear_overrides() -> Iterator[None]:
    app.dependency_overrides.clear()
    yield
    app.dependency_overrides.clear()


async def _override_db() -> AsyncIterator[_FakeDB]:
    yield _FakeDB()


def _override_user(user_id: str = "user-1"):
    async def _inner():
        yield SimpleNamespace(user_id=user_id, enabled=True)

    return _inner


async def test_timeline_api_without_cookie_returns_401() -> None:
    app.dependency_overrides[get_db] = _override_db
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(
            "/v3api/video-timeline-arrangements",
            params={"script_id": "script-1", "episode_id": "episode-1"},
        )
    assert response.status_code == 401
    assert response.json()["detail"] == "未登录"


async def test_timeline_api_role_forbidden_returns_403(monkeypatch) -> None:
    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = _override_user("viewer")

    async def deny(*args, **kwargs):
        raise ForbiddenError("只读成员无权运行 AI 任务")

    monkeypatch.setattr(timeline_api, "check_timeline_script_access", deny)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/v3api/video-timeline-arrangements/render",
            json={"script_id": "script-1", "episode_id": "episode-1"},
            headers={"Idempotency-Key": "idem-1"},
        )
    assert response.status_code == 403


async def test_timeline_api_cross_tenant_idor_returns_403(monkeypatch) -> None:
    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = _override_user("user-2")

    async def deny(*args, **kwargs):
        raise ForbiddenError("时间线片段不属于该剧本分集")

    monkeypatch.setattr(timeline_api, "check_timeline_access", deny)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.patch(
            "/v3api/video-timeline-arrangements/arrangement-other",
            json={"is_selected": False, "expected_version": 0},
        )
    assert response.status_code == 403


async def test_timeline_api_missing_job_returns_404(monkeypatch) -> None:
    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = _override_user()

    async def missing(*args, **kwargs):
        raise NotFoundError("渲染任务不存在")

    monkeypatch.setattr(timeline_api.timeline_render, "get_render_status", missing)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/v3api/video-timeline-arrangements/render/missing")
    assert response.status_code == 404


async def test_timeline_api_idempotency_conflict_returns_409(monkeypatch) -> None:
    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = _override_user()

    async def allow(*args, **kwargs):
        return SimpleNamespace(id="script-1")

    async def conflict(*args, **kwargs):
        raise ConflictError("Idempotency-Key 已用于不同时间线快照")

    monkeypatch.setattr(timeline_api, "check_timeline_script_access", allow)
    monkeypatch.setattr(timeline_api.timeline_render, "render", conflict)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/v3api/video-timeline-arrangements/render",
            json={"script_id": "script-1", "episode_id": "episode-1"},
            headers={"Idempotency-Key": "idem-1"},
        )
    assert response.status_code == 409


async def test_timeline_idempotency_key_http_max_length(monkeypatch) -> None:
    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = _override_user()
    access = AsyncMock(side_effect=AssertionError("超长 header 不应进入时间线业务逻辑"))
    render = AsyncMock(side_effect=AssertionError("超长 header 不应创建合成任务"))
    monkeypatch.setattr(timeline_api, "check_timeline_script_access", access)
    monkeypatch.setattr(timeline_api.timeline_render, "render", render)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/v3api/video-timeline-arrangements/render",
            json={"script_id": "script-1", "episode_id": "episode-1"},
            headers={"Idempotency-Key": "x" * 257},
        )

    assert response.status_code == 422
    assert response.json() == {"detail": "字段校验失败"}
    access.assert_not_awaited()
    render.assert_not_awaited()


async def test_timeline_api_patch_without_version_returns_422() -> None:
    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = _override_user()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.patch(
            "/v3api/video-timeline-arrangements/arrangement-1",
            json={"is_selected": False},
        )
    assert response.status_code == 422


async def test_timeline_api_retry_endpoint_returns_new_job(monkeypatch) -> None:
    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = _override_user()

    async def retry(*args, **kwargs):
        return {"job_id": "render-retry", "status": "starting", "output_storage_key": "generate_outputs/s/e/render-retry.mp4"}

    monkeypatch.setattr(timeline_api.timeline_render, "retry_render", retry)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v3api/video-timeline-arrangements/render/render-failed/retry")
    assert response.status_code == 202
    assert response.json()["job_id"] == "render-retry"
