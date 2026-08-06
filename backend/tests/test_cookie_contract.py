"""会话与 TGW 粘性 Cookie 的端到端响应契约。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from fastapi import Response

from app.api.v1 import auth
from app.core.config import settings
from app.models.user import User
from app.schemas.auth import LoginIn


class _Result:
    def __init__(self, *, scalar=None, rows=None) -> None:
        self._scalar = scalar
        self._rows = rows or []

    def scalar_one_or_none(self):
        return self._scalar

    def all(self):
        return self._rows


async def test_login_sets_non_identifying_tgw_cookie_with_required_attributes(
    monkeypatch,
) -> None:
    user = User(
        user_id="user-sensitive-id",
        username="alice",
        password_hash="unused",
        enabled=True,
        is_admin=False,
        account_type="personal",
        can_manage_accounts=False,
        management_scope="none",
    )
    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(scalar=user),
                _Result(scalar=None),
                _Result(rows=[]),
            ]
        )
    )
    request = MagicMock()
    request.session = {}
    response = Response()
    monkeypatch.setattr(auth, "verify_password", lambda *_args: True)
    monkeypatch.setattr(auth, "audit_log", AsyncMock(return_value=None))

    await auth.login(
        LoginIn(username="alice", password="correct-password"),
        request,
        response,
        db,
    )

    cookie = next(
        value
        for value in response.headers.getlist("set-cookie")
        if value.startswith(f"{settings.tgw_l7_route_cookie}=")
    )
    assert "user-sensitive-id" not in cookie
    assert "Path=/" in cookie
    assert f"Max-Age={settings.session_max_age}" in cookie
    assert "SameSite=lax" in cookie
    assert "HttpOnly" not in cookie
    assert ("Secure" in cookie) is settings.session_cookie_secure_enabled


async def test_logout_deletes_session_and_tgw_cookies() -> None:
    request = MagicMock()
    request.session = {"user_id": "user-1", "active_org_id": "org-1"}
    response = Response()

    result = await auth.logout(request, response)

    assert result == {"detail": "已退出"}
    assert request.session == {}
    deleted = {
        value.split("=", 1)[0]: value
        for value in response.headers.getlist("set-cookie")
    }
    assert settings.session_cookie_name in deleted
    assert settings.tgw_l7_route_cookie in deleted
    assert "Max-Age=0" in deleted[settings.session_cookie_name]
    assert "Max-Age=0" in deleted[settings.tgw_l7_route_cookie]
