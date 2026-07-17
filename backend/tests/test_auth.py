"""鉴权与多租户模块测试 — T1 验收。

覆盖:
- 权限矩阵 (08§1.3 九动作矩阵的内容轴 6 动作)
- check_script_access 各角色×动作矩阵 + personal/team scope + 资源级拒绝
- require_write 兼容 kwarg -> Action 映射
- /auth/login 成功/失败、未登录 401
- /auth/me 返回 organizations[] + active_org_id
- /auth/switch-org 越权 403

不连真实 DB：权限逻辑用 mock db，端点测试用 dependency_overrides 注入 mock。
"""

from contextlib import AbstractAsyncContextManager
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport  # noqa: F401  (保持与 conftest 一致)
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from app.api.v1.auth import login
from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.errors import ForbiddenError, NotAuthenticatedError
from app.core.permissions import (
    _ROLE_PERMISSIONS,
    Action,
    _role_allows,
    check_script_access,
)
from app.models.audit_event import AuditEvent
from app.schemas.auth import LoginIn
from app.services.audit import best_effort_log

# ---------------------------------------------------------------------------
# 测试辅助
# ---------------------------------------------------------------------------


def make_user(
    user_id: str = "u1",
    enabled: bool = True,
    is_admin: bool = False,
    account_type: str = "personal",
    org_id: str | None = None,
    department_id: str | None = None,
    can_manage_accounts: bool = False,
    management_scope: str = "none",
    username: str = "tester",
    active_org_id: str | None = None,
):
    from app.models.user import User

    u = User(
        user_id=user_id,
        username=username,
        password_hash="x",
        enabled=enabled,
        is_admin=is_admin,
        account_type=account_type,
        org_id=org_id,
        department_id=department_id,
        can_manage_accounts=can_manage_accounts,
        management_scope=management_scope,
    )
    if active_org_id is not None:
        setattr(u, "active_org_id", active_org_id)
    return u


def make_script(
    script_id: str = "s1",
    scope: str = "personal",
    owner_user_id: str = "u1",
    created_by_user_id: str = "u1",
    org_id: str | None = None,
):
    from app.models.script import Script

    return Script(
        id=script_id,
        title="t",
        scope=scope,
        owner_user_id=owner_user_id,
        created_by_user_id=created_by_user_id,
        org_id=org_id,
    )


def _member(role: str):
    from app.models.project_member import ProjectMember

    return ProjectMember(script_id="s1", user_id="u2", project_role=role)


class _Result:
    """模拟 SQLAlchemy Result 的最小子集。"""

    def __init__(self, scalar=None, all_list=None):
        self._scalar = scalar
        self._all = all_list or []

    def scalar_one_or_none(self):
        return self._scalar

    def all(self):
        return self._all


class _NestedTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


def make_mock_db(execute_results=None):
    """构造 mock AsyncSession，可配置 execute 的顺序返回值。"""
    db = AsyncMock()
    db.add = MagicMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()
    db.rollback = AsyncMock()
    db.close = AsyncMock()
    db.begin_nested = MagicMock(return_value=_NestedTransaction())
    if execute_results is not None:
        db.execute = AsyncMock(side_effect=list(execute_results))
    else:
        db.execute = AsyncMock(return_value=_Result(scalar=None))
    return db


class _AuditSessionContext(AbstractAsyncContextManager):
    """阻止权限拒绝审计回退到默认数据库引擎。"""

    def __init__(self, db):
        self.db = db

    async def __aenter__(self):
        return self.db

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _AuditSessionFactory:
    def __init__(self, maker):
        self.maker = maker
        self.sessions = []

    def __call__(self):
        session = self.maker()
        self.sessions.append(session)
        return session


@pytest.fixture(autouse=True)
def isolate_audit_database(monkeypatch):
    """所有审计均使用 mock 会话，测试绝不连接默认 PostgreSQL。"""
    audit_db = make_mock_db()
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        lambda: _AuditSessionContext(audit_db),
    )
    return audit_db


class _SyncSessionAsyncAdapter:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._session.close()
        return False

    async def execute(self, *args, **kwargs):
        return self._session.execute(*args, **kwargs)

    async def scalar(self, *args, **kwargs):
        return self._session.scalar(*args, **kwargs)

    def add(self, *args, **kwargs):
        return self._session.add(*args, **kwargs)

    async def flush(self):
        return self._session.flush()

    async def commit(self):
        return self._session.commit()

    async def rollback(self):
        return self._session.rollback()

    def begin_nested(self):
        return _SyncNestedAsyncAdapter(self._session.begin_nested())

    def in_transaction(self):
        return self._session.in_transaction()


class _SyncNestedAsyncAdapter:
    def __init__(self, transaction):
        self._transaction = transaction

    async def __aenter__(self):
        self._transaction.__enter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return self._transaction.__exit__(exc_type, exc, tb)


def _make_audit_sqlite_session(tmp_path):
    """创建只含审计表的真实 AsyncSession，避免连接真实 PostgreSQL。"""
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'audit.sqlite3'}")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE audit_events (
                    id VARCHAR(36) PRIMARY KEY,
                    actor_user_id VARCHAR(36),
                    actor_username VARCHAR(128),
                    action VARCHAR(64) NOT NULL,
                    resource_type VARCHAR(64) NOT NULL,
                    resource_id VARCHAR(36),
                    "before" JSON,
                    "after" JSON,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
                )
                """
            )
        )
        conn.execute(
            text("CREATE TABLE markers (id INTEGER PRIMARY KEY, value VARCHAR(32))")
        )
    sync_maker = sessionmaker(engine, expire_on_commit=False)
    return engine, lambda: _SyncSessionAsyncAdapter(sync_maker())


# ---------------------------------------------------------------------------
# 权限矩阵
# ---------------------------------------------------------------------------

# 08§1.3 内容轴 6 动作期望矩阵
MATRIX = {
    "owner": {
        Action.READ, Action.EDIT, Action.RUN_AI, Action.REVIEW,
        Action.MANAGE_MEMBERS, Action.DELETE,
    },
    "viewer": {Action.READ},
    "editor": {Action.READ, Action.EDIT, Action.RUN_AI},
    "reviewer": {Action.READ, Action.REVIEW},
    "manager": {
        Action.READ, Action.EDIT, Action.RUN_AI, Action.REVIEW,
        Action.MANAGE_MEMBERS, Action.DELETE,
    },
}

ALL_ACTIONS = [
    Action.READ, Action.EDIT, Action.RUN_AI,
    Action.REVIEW, Action.MANAGE_MEMBERS, Action.DELETE,
]


def test_role_matrix_matches_doc():
    """_ROLE_PERMISSIONS 与 08§1.3 矩阵一致。"""
    for role, allowed in MATRIX.items():
        assert _ROLE_PERMISSIONS[role] == allowed, f"{role} 矩阵不符 08§1.3"


@pytest.mark.parametrize("role", list(MATRIX))
@pytest.mark.parametrize("action", ALL_ACTIONS)
def test_role_allows_matrix(role, action):
    expected = action in MATRIX[role]
    assert _role_allows(role, action) is expected


def test_unknown_role_denies_all():
    assert _role_allows("ghost", Action.READ) is False


# ---------------------------------------------------------------------------
# check_script_access — personal scope
# ---------------------------------------------------------------------------


async def test_personal_owner_all_actions_allowed():
    user = make_user(user_id="u1", enabled=True)
    script = make_script(scope="personal", owner_user_id="u1", created_by_user_id="u1")
    db = make_mock_db()
    for a in ALL_ACTIONS:
        await check_script_access(db, user, script, a)  # 不应抛异常


async def test_personal_non_owner_forbidden():
    user = make_user(user_id="u2", enabled=True)
    script = make_script(scope="personal", owner_user_id="u1", created_by_user_id="u1")
    db = make_mock_db()
    with pytest.raises(ForbiddenError):
        await check_script_access(db, user, script, Action.READ)


async def test_personal_creator_without_ownership_forbidden():
    """created_by 只记录来源，不能替代 personal 资源所有权。"""
    user = make_user(user_id="u2", enabled=True)
    script = make_script(scope="personal", owner_user_id="u1", created_by_user_id="u2")
    db = make_mock_db()
    with pytest.raises(ForbiddenError):
        await check_script_access(db, user, script, Action.EDIT)


async def test_disabled_user_unauthenticated():
    user = make_user(user_id="u1", enabled=False)
    script = make_script(scope="personal", owner_user_id="u1", created_by_user_id="u1")
    db = make_mock_db()
    with pytest.raises(NotAuthenticatedError):
        await check_script_access(db, user, script, Action.READ)


# ---------------------------------------------------------------------------
# check_script_access — team scope
# ---------------------------------------------------------------------------


async def test_team_cross_org_forbidden():
    user = make_user(user_id="u2", enabled=True, active_org_id="orgA")
    script = make_script(
        scope="team", owner_user_id="u1", org_id="orgB", created_by_user_id="u1"
    )
    db = make_mock_db()
    with pytest.raises(ForbiddenError):
        await check_script_access(db, user, script, Action.READ)


async def test_team_non_member_forbidden():
    """改 URL id 访问他人剧本：非项目成员一律 403（08§4.1 资源级）。"""
    user = make_user(user_id="u2", enabled=True, active_org_id="orgA")
    script = make_script(
        script_id="s1", scope="team", owner_user_id="u1",
        org_id="orgA", created_by_user_id="u1",
    )
    db = make_mock_db(
        execute_results=[_Result(scalar="u2"), _Result(scalar=None)]
    )
    with pytest.raises(ForbiddenError):
        await check_script_access(db, user, script, Action.READ)


@pytest.mark.parametrize("role", list(MATRIX))
@pytest.mark.parametrize("action", ALL_ACTIONS)
async def test_team_member_matrix(role, action):
    user = make_user(user_id="u2", enabled=True, active_org_id="orgA")
    script = make_script(
        script_id="s1", scope="team", owner_user_id="u1",
        org_id="orgA", created_by_user_id="u1",
    )
    db = make_mock_db(
        execute_results=[_Result(scalar="u2"), _Result(scalar=_member(role))]
    )
    expected = action in MATRIX[role]
    if expected:
        await check_script_access(db, user, script, action)
    else:
        with pytest.raises(ForbiddenError):
            await check_script_access(db, user, script, action)


# ---------------------------------------------------------------------------
# require_write 兼容 kwarg
# ---------------------------------------------------------------------------


async def test_require_write_true_maps_edit_for_viewer():
    user = make_user(user_id="u2", enabled=True, active_org_id="orgA")
    script = make_script(
        scope="team", owner_user_id="u1", org_id="orgA", created_by_user_id="u1"
    )
    # viewer 无 EDIT -> 拒绝
    db = make_mock_db(
        execute_results=[_Result(scalar="u2"), _Result(scalar=_member("viewer"))]
    )
    with pytest.raises(ForbiddenError):
        await check_script_access(db, user, script, require_write=True)
    # viewer 有 READ -> 放行
    db2 = make_mock_db(
        execute_results=[_Result(scalar="u2"), _Result(scalar=_member("viewer"))]
    )
    await check_script_access(db2, user, script, require_write=False)


async def test_require_write_and_action_mutually_exclusive():
    user = make_user(user_id="u2", enabled=True, active_org_id="orgA")
    script = make_script(
        scope="team", owner_user_id="u1", org_id="orgA", created_by_user_id="u1"
    )
    db = make_mock_db(execute_results=[_Result(scalar=_member("owner"))])
    with pytest.raises(TypeError):
        await check_script_access(db, user, script, action=Action.EDIT, require_write=True)


# ---------------------------------------------------------------------------
# 端点级测试
# ---------------------------------------------------------------------------


async def _override(client, deps: dict):
    app = client._app  # type: ignore[attr-defined]
    app.dependency_overrides.update(deps)
    return app


async def test_me_unauthenticated_401(client):
    """未登录访问受保护路由 401 {"detail":"未登录"}。"""
    client._app.dependency_overrides[get_db] = lambda: make_mock_db()  # type: ignore[attr-defined]
    try:
        r = await client.get("/v3api/auth/me")
        assert r.status_code == 401
        assert r.json() == {"detail": "未登录"}
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_login_wrong_password_401(client, monkeypatch):
    monkeypatch.setattr("app.api.v1.auth.verify_password", lambda p, h: False)
    user = make_user(
        user_id="u1", enabled=True, username="alice", org_id="orgA"
    )
    db = make_mock_db(execute_results=[_Result(scalar=user)])
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    try:
        r = await client.post(
            "/v3api/auth/login", json={"username": "alice", "password": "bad"}
        )
        assert r.status_code == 401
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_login_user_not_found_401(client, monkeypatch):
    monkeypatch.setattr("app.api.v1.auth.verify_password", lambda p, h: False)
    db = make_mock_db(execute_results=[_Result(scalar=None)])
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    try:
        r = await client.post(
            "/v3api/auth/login", json={"username": "nobody", "password": "x"}
        )
        assert r.status_code == 401
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_login_audit_failure_still_returns_401(client, monkeypatch):
    """认证失败是主结果，审计不可用时仍必须保持 401。"""
    monkeypatch.setattr("app.api.v1.auth.verify_password", lambda p, h: False)
    monkeypatch.setattr(
        "app.services.audit.log",
        AsyncMock(side_effect=RuntimeError("audit unavailable")),
    )
    db = make_mock_db(execute_results=[_Result(scalar=None)])
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    try:
        response = await client.post(
            "/v3api/auth/login", json={"username": "nobody", "password": "x"}
        )
        assert response.status_code == 401
        assert response.json() == {"detail": "用户名或密码错误"}
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_login_audit_commit_failure_still_returns_401(client, monkeypatch):
    monkeypatch.setattr("app.api.v1.auth.verify_password", lambda p, h: False)
    db = make_mock_db(execute_results=[_Result(scalar=None)])
    db.commit = AsyncMock(side_effect=RuntimeError("audit commit unavailable"))
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    try:
        response = await client.post(
            "/v3api/auth/login", json={"username": "nobody", "password": "x"}
        )
        assert response.status_code == 401
        assert response.json() == {"detail": "用户名或密码错误"}
        db.rollback.assert_awaited_once()
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_login_success_returns_user_and_orgs(client, monkeypatch):
    monkeypatch.setattr("app.api.v1.auth.verify_password", lambda p, h: True)
    from app.models.organization import Organization

    user = make_user(
        user_id="u1", enabled=True, username="alice", org_id="orgA",
        account_type="team",
    )
    org = Organization(id="orgA", org_name="Acme", org_code="acme")
    uo_row = (org, MagicMock())
    # login: execute(select(User)) -> user；_build_user_out: execute -> rows
    db = make_mock_db(
        execute_results=[
            _Result(scalar=user),
            _Result(scalar="u1"),
            _Result(all_list=[uo_row]),
        ]
    )
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    try:
        r = await client.post(
            "/v3api/auth/login", json={"username": "alice", "password": "good"}
        )
        assert r.status_code == 200
        body = r.json()
        assert body["user_id"] == "u1"
        assert body["active_org_id"] == "orgA"
        assert len(body["organizations"]) == 1
        assert body["organizations"][0]["org_code"] == "acme"
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_me_returns_organizations(client):
    """登录后 me 返回完整用户对象含 organizations[]（T1 验收）。"""
    from app.models.organization import Organization

    user = make_user(
        user_id="u1", enabled=True, username="alice", org_id="orgA",
        account_type="team", active_org_id="orgA",
    )
    org = Organization(id="orgA", org_name="Acme", org_code="acme")
    uo_row = (org, MagicMock())

    async def _fake_user():
        yield user

    db = make_mock_db(execute_results=[_Result(all_list=[uo_row])])
    client._app.dependency_overrides[get_current_user] = _fake_user  # type: ignore[attr-defined]
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    try:
        r = await client.get("/v3api/auth/me")
        assert r.status_code == 200
        body = r.json()
        assert body["user_id"] == "u1"
        assert body["active_org_id"] == "orgA"
        assert body["organizations"][0]["org_name"] == "Acme"
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_switch_org_forbidden_403(client):
    """切换到无权访问的企业 -> 403（08§1.4 资源级授权）。"""
    user = make_user(user_id="u1", enabled=True, active_org_id="orgA")

    async def _fake_user():
        yield user

    # Organization 存在，但 UserOrganization 查询 -> None（不属于该企业）
    db = make_mock_db(
        execute_results=[_Result(scalar="orgB"), _Result(scalar=None)]
    )
    client._app.dependency_overrides[get_current_user] = _fake_user  # type: ignore[attr-defined]
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    try:
        r = await client.post(
            "/v3api/auth/switch-org", json={"org_id": "orgB"}
        )
        assert r.status_code == 403
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_switch_org_missing_returns_404(client):
    """目标企业真实不存在时返回 404，而不是把不存在伪装成越权。"""
    user = make_user(user_id="u1", enabled=True, active_org_id="orgA")

    async def _fake_user():
        yield user

    db = make_mock_db(execute_results=[_Result(scalar=None)])
    client._app.dependency_overrides[get_current_user] = _fake_user  # type: ignore[attr-defined]
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    try:
        response = await client.post(
            "/v3api/auth/switch-org", json={"org_id": "missing"}
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "企业不存在"}
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_switch_org_forbidden_audit_persists_after_caller_rollback(
    client, monkeypatch, tmp_path
):
    user = make_user(user_id="u1", enabled=True, active_org_id="orgA")

    async def _fake_user():
        yield user

    engine, maker = _make_audit_sqlite_session(tmp_path)
    audit_factory = _AuditSessionFactory(maker)
    monkeypatch.setattr("app.core.database.async_session_factory", audit_factory)
    db = make_mock_db(
        execute_results=[_Result(scalar="orgB"), _Result(scalar=None)]
    )
    client._app.dependency_overrides[get_current_user] = _fake_user  # type: ignore[attr-defined]
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    try:
        response = await client.post(
            "/v3api/auth/switch-org", json={"org_id": "orgB"}
        )
        assert response.status_code == 403
        assert response.json() == {"detail": "无权访问该企业"}

        await db.rollback()
        async with maker() as verification:
            row = (
                await verification.execute(select(AuditEvent))
            ).scalar_one_or_none()

        assert row is not None
        assert row.actor_user_id == "u1"
        assert row.actor_username == "tester"
        assert row.action == "switch_org_denied"
        assert row.resource_type == "organization"
        assert row.resource_id == "orgB"
        assert row.before == {"reason": "not_a_member"}
        assert row.after is None
        assert "audit unavailable" not in str(row.before)
        assert audit_factory.sessions
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]
        engine.dispose()


async def test_switch_org_missing_audit_persists_after_caller_rollback(
    client, monkeypatch, tmp_path
):
    user = make_user(user_id="u1", enabled=True, active_org_id="orgA")

    async def _fake_user():
        yield user

    engine, maker = _make_audit_sqlite_session(tmp_path)
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _AuditSessionFactory(maker),
    )
    db = make_mock_db(execute_results=[_Result(scalar=None)])
    client._app.dependency_overrides[get_current_user] = _fake_user  # type: ignore[attr-defined]
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    try:
        response = await client.post(
            "/v3api/auth/switch-org", json={"org_id": "missing"}
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "企业不存在"}

        await db.rollback()
        async with maker() as verification:
            row = (
                await verification.execute(select(AuditEvent))
            ).scalar_one_or_none()

        assert row is not None
        assert row.actor_user_id == "u1"
        assert row.actor_username == "tester"
        assert row.action == "switch_org_denied"
        assert row.resource_type == "organization"
        assert row.resource_id == "missing"
        assert row.before == {"reason": "organization_not_found"}
        assert row.after is None
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]
        engine.dispose()


async def test_switch_org_denial_audit_failure_still_returns_403(
    client, monkeypatch
):
    user = make_user(user_id="u1", enabled=True, active_org_id="orgA")

    async def _fake_user():
        yield user

    audit = AsyncMock(side_effect=RuntimeError("audit unavailable"))
    monkeypatch.setattr(
        "app.api.v1.auth.audit_log_independently",
        audit,
        raising=False,
    )
    db = make_mock_db(
        execute_results=[_Result(scalar="orgB"), _Result(scalar=None)]
    )
    client._app.dependency_overrides[get_current_user] = _fake_user  # type: ignore[attr-defined]
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    try:
        response = await client.post(
            "/v3api/auth/switch-org", json={"org_id": "orgB"}
        )
        assert response.status_code == 403
        assert response.json() == {"detail": "无权访问该企业"}
        audit.assert_awaited_once()
        db.commit.assert_not_awaited()
        db.rollback.assert_not_awaited()
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_get_current_user_rejects_removed_active_org_membership():
    """会话中的 active_org_id 不是凭据，成员被移除后应立即失效。"""
    request = MagicMock()
    request.session = {"user_id": "u1", "active_org_id": "orgA"}
    user = make_user(user_id="u1", enabled=True, org_id="orgA")
    db = make_mock_db(
        execute_results=[_Result(scalar=user), _Result(scalar=None)]
    )

    current_user = get_current_user(request, db)
    with pytest.raises(NotAuthenticatedError):
        await anext(current_user)


async def test_get_current_user_falls_back_from_removed_primary_org():
    request = MagicMock()
    request.session = {"user_id": "u1"}
    user = make_user(user_id="u1", enabled=True, org_id="orgA")
    db = make_mock_db(
        execute_results=[
            _Result(scalar=user),
            _Result(scalar=None),
            _Result(scalar="orgB"),
        ]
    )

    current_user = await anext(get_current_user(request, db))

    assert current_user.active_org_id == "orgB"


async def test_team_access_rejects_removed_org_member():
    user = make_user(user_id="u2", enabled=True, active_org_id="orgA")
    script = make_script(
        script_id="s1",
        scope="team",
        owner_user_id="u1",
        org_id="orgA",
        created_by_user_id="u1",
    )
    db = make_mock_db(execute_results=[_Result(scalar=None)])

    with pytest.raises(ForbiddenError):
        await check_script_access(db, user, script, Action.READ)


async def test_platform_admin_does_not_bypass_cross_org_boundary():
    user = make_user(
        user_id="platform-admin",
        enabled=True,
        is_admin=True,
        active_org_id="orgA",
    )
    script = make_script(
        scope="team",
        owner_user_id="u1",
        org_id="orgB",
        created_by_user_id="u1",
    )

    with pytest.raises(ForbiddenError):
        await check_script_access(make_mock_db(), user, script, Action.READ)


async def test_audit_failure_does_not_change_forbidden(monkeypatch):
    """审计是 best-effort，故障不得把既定 403 改成 500。"""
    monkeypatch.setattr(
        "app.services.audit.log",
        AsyncMock(side_effect=RuntimeError("audit unavailable")),
    )
    user = make_user(user_id="u2", enabled=True)
    script = make_script(
        scope="personal", owner_user_id="u1", created_by_user_id="u1"
    )

    with pytest.raises(ForbiddenError):
        await check_script_access(make_mock_db(), user, script, Action.READ)


async def test_access_denial_audit_persists_after_caller_rollback(
    monkeypatch, tmp_path
):
    """权限拒绝审计使用独立短事务，调用者事务回滚后第二会话仍可查询。"""
    engine, maker = _make_audit_sqlite_session(tmp_path)
    audit_factory = _AuditSessionFactory(maker)
    monkeypatch.setattr("app.core.database.async_session_factory", audit_factory)
    db = make_mock_db()
    user = make_user(user_id="u2", enabled=True)
    script = make_script(
        scope="personal", owner_user_id="u1", created_by_user_id="u1"
    )

    try:
        with pytest.raises(ForbiddenError) as exc_info:
            await check_script_access(db, user, script, Action.READ)

        assert exc_info.value.status_code == 403
        await db.rollback()
        async with maker() as verification:
            row = (
                await verification.execute(select(AuditEvent))
            ).scalar_one_or_none()

        assert row is not None
        assert row.actor_user_id == "u2"
        assert row.actor_username == "tester"
        assert row.action == "access_denied"
        assert row.resource_type == "script"
        assert row.resource_id == "s1"
        assert row.before == {
            "action": "read",
            "scope": "personal",
            "reason": "非所有者访问个人剧本",
        }
        assert row.after is None
        assert "audit unavailable" not in str(row.before)
        db.commit.assert_not_awaited()
        db.rollback.assert_awaited_once()
        assert audit_factory.sessions
    finally:
        engine.dispose()


async def test_best_effort_audit_failure_does_not_rollback_caller_session(
    monkeypatch,
):
    """commit=False 表示审计附着在调用者事务，失败不得回滚调用者 Session。"""
    monkeypatch.setattr(
        "app.services.audit.log",
        AsyncMock(side_effect=RuntimeError("audit unavailable")),
    )
    db = make_mock_db()

    event = await best_effort_log(
        db,
        actor=None,
        action="access_denied",
        resource_type="script",
        commit=False,
    )

    assert event is None
    db.rollback.assert_not_awaited()


async def test_best_effort_audit_commit_false_flush_failure_keeps_outer_transaction_usable(
    tmp_path,
):
    """commit=False 下审计 flush 失败必须回滚到保存点，而不是污染调用方事务。"""
    engine, maker = _make_audit_sqlite_session(tmp_path)
    try:
        async with maker() as session:
            await session.execute(
                text("INSERT INTO markers (id, value) VALUES (1, 'before')")
            )

            event = await best_effort_log(
                session,
                actor=None,
                action=None,  # type: ignore[arg-type]
                resource_type="script",
                commit=False,
            )

            assert event is None
            await session.execute(
                text("INSERT INTO markers (id, value) VALUES (2, 'after')")
            )
            count = await session.scalar(text("SELECT count(*) FROM markers"))
            assert count == 2
            await session.commit()
    finally:
        engine.dispose()


async def test_best_effort_audit_commit_true_commits_short_transaction(tmp_path):
    engine, maker = _make_audit_sqlite_session(tmp_path)
    try:
        async with maker() as session:
            event = await best_effort_log(
                session,
                actor=None,
                action="access_denied",
                resource_type="script",
                resource_id="s1",
                commit=True,
            )

            assert event is not None
            assert session.in_transaction() is False

        async with maker() as verification:
            count = await verification.scalar(text("SELECT count(*) FROM audit_events"))
            assert count == 1
    finally:
        engine.dispose()


async def test_logout_clears_session_cookie(client):
    from app.core.config import settings

    client.cookies.set(settings.session_cookie_name, "tampered")
    response = await client.post("/v3api/auth/logout")

    assert response.status_code == 200
    assert response.json() == {"detail": "已退出"}
    session_deletions = [
        value
        for value in response.headers.get_list("set-cookie")
        if value.startswith(f"{settings.session_cookie_name}=")
    ]
    assert session_deletions
    assert "Max-Age=0" in session_deletions[0]


async def test_tampered_session_cookie_is_unauthenticated(client):
    from app.core.config import settings

    db = make_mock_db()
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    client.cookies.set(settings.session_cookie_name, "tampered")
    try:
        response = await client.get("/v3api/auth/me")
        assert response.status_code == 401
        assert response.json() == {"detail": "未登录"}
        db.execute.assert_not_awaited()
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_expired_session_cookie_is_unauthenticated(client):
    import json
    from base64 import b64encode

    import itsdangerous

    from app.core.config import settings

    signer = itsdangerous.TimestampSigner(settings.session_secret)
    signer.get_timestamp = lambda: 1
    payload = b64encode(json.dumps({"user_id": "u1"}).encode("utf-8"))
    expired_cookie = signer.sign(payload).decode("utf-8")
    client.cookies.set(settings.session_cookie_name, expired_cookie)
    db = make_mock_db()
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    try:
        response = await client.get("/v3api/auth/me")
        assert response.status_code == 401
        assert response.json() == {"detail": "未登录"}
        db.execute.assert_not_awaited()
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]


async def test_login_clears_stale_active_org(monkeypatch):
    from fastapi import Response

    monkeypatch.setattr("app.api.v1.auth.verify_password", lambda p, h: True)
    user = make_user(user_id="u2", username="personal", org_id=None)
    db = make_mock_db(
        execute_results=[
            _Result(scalar=user),
            _Result(scalar=None),
            _Result(all_list=[]),
        ]
    )
    request = MagicMock()
    request.session = {"user_id": "old", "active_org_id": "org-old"}

    await login(
        LoginIn(username="personal", password="good"),
        request,
        Response(),
        db,
    )

    assert request.session == {"user_id": "u2"}


async def test_login_switch_org_cookie_active_org_reaches_scripts_and_credits(
    client, monkeypatch
):
    """保持同一 Cookie：login -> switch-org -> scripts list/create -> credits。"""
    from datetime import UTC, datetime

    from app.api.v1 import credits as credits_api
    from app.api.v1 import scripts as scripts_api
    from app.models.organization import Organization
    from app.models.script import Script
    from app.models.user_organization import UserOrganization

    monkeypatch.setattr("app.api.v1.auth.verify_password", lambda p, h: True)
    monkeypatch.setattr("app.api.v1.auth.audit_log", AsyncMock(return_value=None))
    user = make_user(
        user_id="u1",
        enabled=True,
        username="alice",
        org_id="orgA",
        account_type="team",
    )
    org_a = Organization(id="orgA", org_name="Org A", org_code="a")
    org_b = Organization(id="orgB", org_name="Org B", org_code="b")
    uo_a = UserOrganization(user_id="u1", org_id="orgA")
    uo_b = UserOrganization(user_id="u1", org_id="orgB")
    db = make_mock_db(
        execute_results=[
            _Result(scalar=user),        # login select(User)
            _Result(scalar="u1"),        # login resolve primary org
            _Result(all_list=[(org_a, uo_a), (org_b, uo_b)]),
            _Result(scalar=user),        # switch get_current_user
            _Result(scalar="u1"),        # switch resolve session active orgA
            _Result(scalar="orgB"),      # switch organization exists
            _Result(scalar=uo_b),        # switch membership
            _Result(all_list=[(org_a, uo_a), (org_b, uo_b)]),
            _Result(scalar=user),        # scripts list get_current_user
            _Result(scalar="u1"),        # resolve active orgB
            _Result(scalar=user),        # scripts create get_current_user
            _Result(scalar="u1"),        # resolve active orgB
            _Result(scalar=user),        # credits get_current_user
            _Result(scalar="u1"),        # resolve active orgB
        ]
    )
    observed: list[tuple[str, str | None]] = []

    async def _list_scripts(db_arg, scope, limit, offset, current_user):
        observed.append(("scripts-list", current_user.active_org_id))
        return [], 0

    async def _create_script(db_arg, title, scope, current_user):
        observed.append(("scripts-create", current_user.active_org_id))
        now = datetime.now(UTC)
        return Script(
            id="script-org-b",
            title=title,
            scope=scope,
            owner_user_id=current_user.user_id,
            org_id=current_user.active_org_id,
            created_by_user_id=current_user.user_id,
            created_at=now,
            updated_at=now,
        )

    async def _get_accounts(db_arg, current_user):
        observed.append(("credits", current_user.active_org_id))
        return []

    monkeypatch.setattr(scripts_api.script_crud, "list_scripts", _list_scripts)
    monkeypatch.setattr(scripts_api.script_crud, "create_script", _create_script)
    monkeypatch.setattr(credits_api, "get_accounts_for_user", _get_accounts)
    client._app.dependency_overrides[get_db] = lambda: db  # type: ignore[attr-defined]
    try:
        login_response = await client.post(
            "/v3api/auth/login", json={"username": "alice", "password": "good"}
        )
        assert login_response.status_code == 200
        assert login_response.json()["active_org_id"] == "orgA"

        switch_response = await client.post(
            "/v3api/auth/switch-org", json={"org_id": "orgB"}
        )
        assert switch_response.status_code == 200
        assert switch_response.json()["active_org_id"] == "orgB"

        list_response = await client.get(
            "/v3api/scripts/page", params={"scope": "team"}
        )
        assert list_response.status_code == 200
        create_response = await client.post(
            "/v3api/scripts", json={"title": "Team B", "scope": "team"}
        )
        assert create_response.status_code == 201
        assert create_response.json()["org_id"] == "orgB"
        credits_response = await client.get("/v3api/credits/accounts")
        assert credits_response.status_code == 200

        assert observed == [
            ("scripts-list", "orgB"),
            ("scripts-create", "orgB"),
            ("credits", "orgB"),
        ]
    finally:
        client._app.dependency_overrides.clear()  # type: ignore[attr-defined]
