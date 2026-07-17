"""资源级权限边界测试。"""

from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.core.errors import ForbiddenError
from app.core.permissions import Action, _load_script, check_script_access
from app.crud import script as script_crud
from app.crud.script import create_script, list_scripts
from app.models.base import Base
from app.models.episode import Episode
from app.models.project_member import ProjectMember
from app.models.script import Script
from app.models.shot_group import ShotGroup
from app.models.user import User


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


class _Result:
    def __init__(self, scalar=None):
        self._scalar = scalar

    def scalar_one_or_none(self):
        return self._scalar

    def scalar(self):
        return self._scalar

    def scalars(self):
        class _Scalars:
            def all(self):
                return []

        return _Scalars()


def _db(*results):
    db = AsyncMock()
    db.execute = AsyncMock(side_effect=results)
    db.add = MagicMock()
    db.commit = AsyncMock()
    db.refresh = AsyncMock()
    return db


def _entity_name(statement) -> str | None:
    descriptions = getattr(statement, "column_descriptions", None) or []
    if not descriptions:
        return None
    entity = descriptions[0].get("entity")
    return getattr(entity, "__name__", None)


def _routing_db(mapping):
    db = AsyncMock()

    async def _execute(statement):
        return _Result(mapping.get(_entity_name(statement)))

    db.execute = AsyncMock(side_effect=_execute)
    db.add = MagicMock()
    db.commit = AsyncMock()
    db.refresh = AsyncMock()
    return db


class _AsyncSessionAdapter:
    def __init__(self, session: Session) -> None:
        self.session = session

    async def execute(self, statement):
        return self.session.execute(statement)

    async def flush(self) -> None:
        self.session.flush()

    async def commit(self) -> None:
        self.session.commit()


def _episode_resource_db():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine, expire_on_commit=False)
    session.add_all(
        [
            Script(
                id="script-authorized",
                title="已授权剧本",
                scope="personal",
                owner_user_id="u1",
                created_by_user_id="u1",
            ),
            Script(
                id="script-other",
                title="其他剧本",
                scope="personal",
                owner_user_id="u2",
                created_by_user_id="u2",
            ),
            Episode(
                episode_id="episode-cross",
                script_id="script-other",
                episode_no=1,
                title="其他剧本分集",
                episode_version=1,
                client_save_seq=0,
            ),
        ]
    )
    session.commit()
    return session


def _user(active_org_id: str | None = "orgB") -> User:
    user = User(
        user_id="u2",
        username="member",
        password_hash="x",
        enabled=True,
        is_admin=False,
        account_type="team",
        org_id="orgA",
        department_id="deptA",
        can_manage_accounts=False,
        management_scope="none",
    )
    if active_org_id is not None:
        setattr(user, "active_org_id", active_org_id)
    return user


async def test_missing_script_returns_404():
    with pytest.raises(Exception) as exc_info:
        await _load_script(_db(_Result(None)), "missing")

    assert exc_info.value.status_code == 404


async def test_existing_but_unauthorized_script_returns_403(monkeypatch):
    async def _ignore_audit(*args, **kwargs):
        return None

    monkeypatch.setattr(
        "app.core.permissions._audit_access_denied", _ignore_audit
    )
    user = User(
        user_id="u2",
        username="viewer",
        password_hash="x",
        enabled=True,
        is_admin=False,
        account_type="team",
        can_manage_accounts=False,
        management_scope="none",
    )
    setattr(user, "active_org_id", "orgA")
    script = Script(
        id="s1",
        title="existing",
        scope="team",
        org_id="orgA",
        owner_user_id="u1",
        created_by_user_id="u1",
    )
    db = _db(_Result("u2"), _Result(None))

    with pytest.raises(ForbiddenError) as exc_info:
        await check_script_access(db, user, script, Action.READ)

    assert exc_info.value.status_code == 403


async def test_team_script_without_active_org_fails_closed(monkeypatch):
    async def _ignore_audit(*args, **kwargs):
        return None

    monkeypatch.setattr(
        "app.core.permissions._audit_access_denied", _ignore_audit
    )
    user = User(
        user_id="u2",
        username="member",
        password_hash="x",
        enabled=True,
        is_admin=False,
        account_type="team",
        can_manage_accounts=False,
        management_scope="none",
    )
    script = Script(
        id="s1",
        title="existing",
        scope="team",
        org_id="orgA",
        owner_user_id="u1",
        created_by_user_id="u1",
    )
    member = ProjectMember(
        script_id="s1", user_id="u2", project_role="viewer"
    )

    with pytest.raises(ForbiddenError):
        await check_script_access(
            _db(_Result("u2"), _Result(member)), user, script, Action.READ
        )


async def test_unknown_script_scope_fails_closed(monkeypatch):
    async def _ignore_audit(*args, **kwargs):
        return None

    monkeypatch.setattr(
        "app.core.permissions._audit_access_denied", _ignore_audit
    )
    user = User(
        user_id="u2",
        username="member",
        password_hash="x",
        enabled=True,
        is_admin=False,
        account_type="team",
        can_manage_accounts=False,
        management_scope="none",
    )
    member = ProjectMember(
        script_id="s1", user_id="u2", project_role="viewer"
    )
    script = Script(
        id="s1",
        title="invalid",
        scope="other",
        owner_user_id="u1",
        created_by_user_id="u1",
    )

    with pytest.raises(ForbiddenError):
        await check_script_access(_db(_Result(member)), user, script, Action.READ)


async def test_team_script_list_is_limited_to_active_org():
    user = _user(active_org_id="orgB")
    db = _db(_Result(0), _Result())

    await list_scripts(db, "team", 30, 0, user)

    sql_fragments = [
        str(call.args[0].compile(compile_kwargs={"literal_binds": True}))
        for call in db.execute.await_args_list
    ]
    assert all("orgB" in sql for sql in sql_fragments)
    assert all("orgA" not in sql for sql in sql_fragments)


async def test_team_script_creation_uses_active_org_not_primary_org():
    user = _user(active_org_id="orgB")
    db = _db()

    script = await create_script(db, "team script", "team", user)

    assert script.org_id == "orgB"
    added_scripts = [
        call.args[0] for call in db.add.call_args_list if isinstance(call.args[0], Script)
    ]
    assert added_scripts and added_scripts[0].org_id == "orgB"


async def test_team_script_creation_without_active_org_fails_closed():
    user = _user(active_org_id=None)
    db = _db()

    with pytest.raises(Exception) as exc_info:
        await create_script(db, "team script", "team", user)

    assert exc_info.value.status_code == 400


async def test_check_materials_rejects_cross_script_shot_group_idor(monkeypatch):
    """check-materials 必须先绑定 shot_group 归属，不能只信任 query script_id。"""
    from app.api.v1 import video_generation

    async def _ignore_audit(*args, **kwargs):
        return None

    async def _aggregate(*args, **kwargs):
        return SimpleNamespace(missing_subjects=[], missing_audio=[])

    monkeypatch.setattr(
        "app.core.permissions._audit_access_denied", _ignore_audit
    )
    monkeypatch.setattr(
        "app.services.reference_aggregator.aggregate_references",
        _aggregate,
    )
    user = User(
        user_id="u1",
        username="owner",
        password_hash="x",
        enabled=True,
        is_admin=False,
        account_type="personal",
        can_manage_accounts=False,
        management_scope="none",
    )
    script = Script(
        id="script-authorized",
        title="authorized",
        scope="personal",
        owner_user_id="u1",
        created_by_user_id="u1",
    )
    cross_group = ShotGroup(
        shot_group_id="sg-cross",
        script_id="script-other",
        episode_id="ep-other",
        scene_no=1,
        group_no="1",
    )
    db = _routing_db({"ShotGroup": cross_group, "Script": script})

    with pytest.raises(Exception) as exc_info:
        await video_generation.check_materials(
            shot_group_id="sg-cross",
            script_id="script-authorized",
            db=db,
            user=user,
        )

    assert exc_info.value.status_code == 403


async def test_check_materials_missing_shot_group_returns_404(monkeypatch):
    from app.api.v1 import video_generation

    user = User(
        user_id="u1",
        username="owner",
        password_hash="x",
        enabled=True,
        is_admin=False,
        account_type="personal",
        can_manage_accounts=False,
        management_scope="none",
    )

    async def _aggregate(*args, **kwargs):
        return SimpleNamespace(missing_subjects=[], missing_audio=[])

    monkeypatch.setattr(
        "app.services.reference_aggregator.aggregate_references",
        _aggregate,
    )
    script = Script(
        id="script-authorized",
        title="authorized",
        scope="personal",
        owner_user_id="u1",
        created_by_user_id="u1",
    )

    with pytest.raises(Exception) as exc_info:
        await video_generation.check_materials(
            shot_group_id="missing",
            script_id="script-authorized",
            db=_routing_db({"ShotGroup": None, "Script": script}),
            user=user,
        )

    assert exc_info.value.status_code == 404


@pytest.mark.parametrize("entrypoint", ["get_episode", "save_episode", "get_breakdown"])
async def test_cross_script_existing_episode_returns_403(entrypoint: str):
    raw_session = _episode_resource_db()
    try:
        db = cast(AsyncSession, _AsyncSessionAdapter(raw_session))

        with pytest.raises(Exception) as exc_info:
            if entrypoint == "get_episode":
                await script_crud.get_episode(
                    db, "script-authorized", "episode-cross"
                )
            elif entrypoint == "save_episode":
                await script_crud.save_episode(
                    db,
                    "script-authorized",
                    "episode-cross",
                    "新正文",
                    1,
                    0,
                )
            else:
                await script_crud.get_breakdown(
                    db, "script-authorized", "episode-cross"
                )

        assert cast(HTTPException, exc_info.value).status_code == 403
    finally:
        raw_session.close()


@pytest.mark.parametrize("entrypoint", ["get_episode", "save_episode", "get_breakdown"])
async def test_globally_missing_episode_still_returns_404(entrypoint: str):
    raw_session = _episode_resource_db()
    try:
        db = cast(AsyncSession, _AsyncSessionAdapter(raw_session))

        with pytest.raises(Exception) as exc_info:
            if entrypoint == "get_episode":
                await script_crud.get_episode(
                    db, "script-authorized", "episode-missing"
                )
            elif entrypoint == "save_episode":
                await script_crud.save_episode(
                    db,
                    "script-authorized",
                    "episode-missing",
                    "新正文",
                    1,
                    0,
                )
            else:
                await script_crud.get_breakdown(
                    db, "script-authorized", "episode-missing"
                )

        assert cast(HTTPException, exc_info.value).status_code == 404
    finally:
        raw_session.close()
