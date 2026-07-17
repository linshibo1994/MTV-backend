"""团队项目成员管理 CRUD 专项测试。"""

from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.api.v1 import project_members
from app.models.audit_event import AuditEvent
from app.models.base import Base
from app.models.organization import Organization
from app.models.project_member import ProjectMember
from app.models.script import Script
from app.models.user import User
from app.models.user_organization import UserOrganization


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


class _AsyncSessionAdapter:
    """以真实 SQLite 事务承载成员管理异步调用。"""

    def __init__(self, session: Session) -> None:
        self.session = session

    async def execute(self, statement):
        return self.session.execute(statement)

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


@pytest.fixture(autouse=True)
def _disable_independent_denial_audit(monkeypatch):
    """权限拒绝使用独立数据库会话，专项测试中隔离该外部副作用。"""

    async def _ignore(*_args, **_kwargs) -> None:
        return None

    monkeypatch.setattr("app.core.permissions._audit_access_denied", _ignore)


def _create_tables(engine) -> None:
    Base.metadata.create_all(
        engine,
        tables=[
            Organization.__table__,
            User.__table__,
            UserOrganization.__table__,
            Script.__table__,
            ProjectMember.__table__,
            AuditEvent.__table__,
        ],
    )


def _user(user_id: str, username: str, org_id: str) -> User:
    return User(
        user_id=user_id,
        username=username,
        password_hash="test",
        enabled=True,
        account_type="team",
        org_id=org_id,
    )


def _seed(raw: Session) -> None:
    raw.add_all(
        [
            Organization(id="org-a", org_name="A 组织", org_code="org-a"),
            Organization(id="org-b", org_name="B 组织", org_code="org-b"),
            _user("owner", "项目所有者", "org-a"),
            _user("manager", "管理员", "org-a"),
            _user("viewer", "只读成员", "org-a"),
            _user("candidate", "候选成员", "org-a"),
            _user("backup", "备用管理员", "org-a"),
            _user("outsider", "外部成员", "org-b"),
            UserOrganization(id="uo-owner", user_id="owner", org_id="org-a"),
            UserOrganization(id="uo-manager", user_id="manager", org_id="org-a"),
            UserOrganization(id="uo-viewer", user_id="viewer", org_id="org-a"),
            UserOrganization(id="uo-candidate", user_id="candidate", org_id="org-a"),
            UserOrganization(id="uo-backup", user_id="backup", org_id="org-a"),
            UserOrganization(id="uo-outsider", user_id="outsider", org_id="org-b"),
            Script(
                id="script-a",
                title="A 项目",
                scope="team",
                org_id="org-a",
                owner_user_id="owner",
                created_by_user_id="owner",
            ),
            Script(
                id="script-b",
                title="B 项目",
                scope="team",
                org_id="org-a",
                owner_user_id="owner",
                created_by_user_id="owner",
            ),
            ProjectMember(
                id="member-owner",
                script_id="script-a",
                user_id="owner",
                project_role="owner",
            ),
            ProjectMember(
                id="member-manager",
                script_id="script-a",
                user_id="manager",
                project_role="manager",
            ),
            ProjectMember(
                id="member-viewer",
                script_id="script-a",
                user_id="viewer",
                project_role="viewer",
            ),
            ProjectMember(
                id="member-owner-b",
                script_id="script-b",
                user_id="owner",
                project_role="owner",
            ),
            ProjectMember(
                id="member-manager-b",
                script_id="script-b",
                user_id="manager",
                project_role="manager",
            ),
            ProjectMember(
                id="member-cross",
                script_id="script-b",
                user_id="candidate",
                project_role="viewer",
            ),
        ]
    )
    raw.commit()


def _actor(raw: Session, user_id: str) -> User:
    user = raw.get(User, user_id)
    assert user is not None
    setattr(user, "active_org_id", "org-a")
    return user


def _app(db: _AsyncSessionAdapter, user: User) -> FastAPI:
    app = FastAPI()
    app.include_router(project_members.router)

    async def _db_override() -> AsyncIterator[_AsyncSessionAdapter]:
        yield db

    app.dependency_overrides[project_members.get_db] = _db_override
    app.dependency_overrides[project_members.get_current_user] = lambda: user
    return app


@pytest.mark.asyncio
async def test_manager_can_list_add_change_and_remove_member_with_audit() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        app = _app(_AsyncSessionAdapter(raw), _actor(raw, "manager"))
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            listed = await client.get("/scripts/script-a/members")
            added = await client.post(
                "/scripts/script-a/members",
                json={"user_id": "candidate", "project_role": "editor"},
            )
            member_id = added.json()["member_id"]
            changed = await client.patch(
                f"/scripts/script-a/members/{member_id}",
                json={"project_role": "reviewer"},
            )
            removed = await client.delete(f"/scripts/script-a/members/{member_id}")

        assert listed.status_code == 200
        assert listed.json()["total"] == 3
        assert {
            (item["user_id"], item["project_role"])
            for item in listed.json()["items"]
        } == {
            ("owner", "owner"),
            ("manager", "manager"),
            ("viewer", "viewer"),
        }
        assert added.status_code == 201
        assert added.json()["username"] == "候选成员"
        assert added.json()["project_role"] == "editor"
        assert changed.status_code == 200
        assert changed.json()["project_role"] == "reviewer"
        assert removed.status_code == 200
        assert removed.json() == {"detail": "已移除"}
        assert raw.get(ProjectMember, member_id) is None

        events = list(
            raw.scalars(
                select(AuditEvent)
                .where(AuditEvent.resource_id == member_id)
                .order_by(AuditEvent.created_at.asc())
            )
        )
        assert [event.action for event in events] == [
            "project_member_add",
            "project_member_role_change",
            "project_member_remove",
        ]
        assert events[0].before is None
        assert events[0].after["project_role"] == "editor"
        assert events[1].before["project_role"] == "editor"
        assert events[1].after["project_role"] == "reviewer"
        assert events[2].before["project_role"] == "reviewer"
        assert events[2].after is None


@pytest.mark.asyncio
async def test_only_current_organization_user_can_be_added() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _actor(raw, "manager"))),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/scripts/script-a/members",
                json={"user_id": "outsider", "project_role": "viewer"},
            )

        assert response.status_code == 400
        assert response.json() == {"detail": "目标用户不是当前组织的有效成员"}
        assert raw.query(AuditEvent).count() == 0


@pytest.mark.asyncio
async def test_duplicate_member_returns_conflict() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _actor(raw, "manager"))),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/scripts/script-a/members",
                json={"user_id": "viewer", "project_role": "editor"},
            )

        assert response.status_code == 409
        assert response.json() == {"detail": "该用户已是项目成员"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "payload"),
    [
        ("GET", "/scripts/script-a/members", None),
        (
            "POST",
            "/scripts/script-a/members",
            {"user_id": "backup", "project_role": "viewer"},
        ),
        (
            "PATCH",
            "/scripts/script-a/members/member-manager",
            {"project_role": "editor"},
        ),
        ("DELETE", "/scripts/script-a/members/member-manager", None),
    ],
)
async def test_viewer_cannot_manage_members(
    method: str,
    path: str,
    payload: dict | None,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _actor(raw, "viewer"))),
            base_url="http://test",
        ) as client:
            response = await client.request(method, path, json=payload)

        assert response.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["PATCH", "DELETE"])
async def test_cross_project_member_id_returns_403(method: str) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _actor(raw, "manager"))),
            base_url="http://test",
        ) as client:
            response = await client.request(
                method,
                "/scripts/script-a/members/member-cross",
                json={"project_role": "editor"} if method == "PATCH" else None,
            )

        assert response.status_code == 403
        assert response.json() == {"detail": "项目成员不属于当前项目"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "expected_detail"),
    [
        ("PATCH", "项目所有者必须保留 owner 角色"),
        ("DELETE", "项目所有者不能从成员列表移除；所有权转移需使用专用流程"),
    ],
)
async def test_real_script_owner_cannot_be_demoted_or_removed(
    method: str,
    expected_detail: str,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _actor(raw, "manager"))),
            base_url="http://test",
        ) as client:
            response = await client.request(
                method,
                "/scripts/script-a/members/member-owner",
                json={"project_role": "viewer"} if method == "PATCH" else None,
            )

        assert response.status_code == 409
        assert response.json() == {"detail": expected_detail}
        owner = raw.get(ProjectMember, "member-owner")
        assert owner is not None and owner.project_role == "owner"
        assert raw.scalar(
            select(Script.owner_user_id).where(Script.id == "script-a")
        ) == "owner"


@pytest.mark.asyncio
async def test_manager_can_remove_self_while_real_owner_remains() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _actor(raw, "manager"))),
            base_url="http://test",
        ) as client:
            removed = await client.delete("/scripts/script-a/members/member-manager")

        assert removed.status_code == 200
        assert raw.get(ProjectMember, "member-manager") is None
        owner = raw.get(ProjectMember, "member-owner")
        assert owner is not None and owner.project_role == "owner"
        assert raw.scalar(
            select(Script.owner_user_id).where(Script.id == "script-a")
        ) == "owner"


@pytest.mark.asyncio
async def test_real_owner_can_manage_ordinary_members() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _actor(raw, "owner"))),
            base_url="http://test",
        ) as client:
            added = await client.post(
                "/scripts/script-a/members",
                json={"user_id": "backup", "project_role": "editor"},
            )
            member_id = added.json()["member_id"]
            changed = await client.patch(
                f"/scripts/script-a/members/{member_id}",
                json={"project_role": "manager"},
            )
            removed = await client.delete(f"/scripts/script-a/members/{member_id}")

        assert (added.status_code, changed.status_code, removed.status_code) == (201, 200, 200)
        assert raw.get(ProjectMember, member_id) is None
        assert raw.scalar(
            select(Script.owner_user_id).where(Script.id == "script-a")
        ) == "owner"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "payload"),
    [
        (
            "POST",
            "/scripts/script-a/members",
            {"user_id": "backup", "project_role": "owner"},
        ),
        (
            "PATCH",
            "/scripts/script-a/members/member-viewer",
            {"project_role": "owner"},
        ),
    ],
)
async def test_manager_cannot_assign_owner_role_without_ownership_transfer(
    method: str,
    path: str,
    payload: dict[str, str],
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _actor(raw, "manager"))),
            base_url="http://test",
        ) as client:
            response = await client.request(method, path, json=payload)

        assert response.status_code == 409
        assert response.json() == {
            "detail": "owner 角色仅可由项目所有者持有；所有权转移需使用专用流程"
        }
        owner = raw.get(ProjectMember, "member-owner")
        assert raw.scalar(
            select(Script.owner_user_id).where(Script.id == "script-a")
        ) == "owner"
        assert owner is not None and owner.project_role == "owner"


@pytest.mark.asyncio
async def test_project_role_enum_rejects_unknown_role() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        async with AsyncClient(
            transport=ASGITransport(app=_app(_AsyncSessionAdapter(raw), _actor(raw, "manager"))),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/scripts/script-a/members",
                json={"user_id": "backup", "project_role": "administrator"},
            )

        assert response.status_code == 422
        assert raw.query(ProjectMember).filter_by(script_id="script-a").count() == 3


def test_database_rejects_unknown_project_role() -> None:
    """即使绕过 API，数据库约束也不能接受非法角色。"""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    _create_tables(engine)
    with Session(engine, expire_on_commit=False) as raw:
        _seed(raw)
        raw.add(
            ProjectMember(
                id="member-invalid-role",
                script_id="script-a",
                user_id="candidate",
                project_role="administrator",
            )
        )
        with pytest.raises(IntegrityError):
            raw.commit()


def test_openapi_exposes_project_member_crud() -> None:
    from app.main import app

    schema = app.openapi()["paths"]
    collection = schema["/v3api/scripts/{script_id}/members"]
    item = schema["/v3api/scripts/{script_id}/members/{member_id}"]
    assert {"get", "post"} <= set(collection)
    assert {"patch", "delete"} <= set(item)
