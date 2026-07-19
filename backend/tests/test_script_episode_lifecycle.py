"""B1/B2 剧本与分集完整生命周期专项测试。"""

import os
import secrets
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import create_engine, delete, select, text, update
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.api.v1 import scripts
from app.core.errors import ConflictError, ForbiddenError
from app.core.permissions import Action, check_script_access
from app.crud import script as script_crud
from app.models.asset_entry import AssetEntry
from app.models.base import Base
from app.models.credit import CreditAccount
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.generation_job import GenerationJob
from app.models.materials import Materials
from app.models.media_storage_job import MediaStorageJob
from app.models.media_upload import MediaUpload
from app.models.pipeline_job import PipelineJob
from app.models.project_member import ProjectMember
from app.models.scene import Scene
from app.models.script import Script
from app.models.shot_group import ShotGroup
from app.models.storyboard_job import StoryboardJob
from app.models.subject import Subject
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.timeline_render_job import TimelineRenderJob
from app.models.user import User
from app.models.video_generation_job import VideoGenerationJob
from app.schemas.script import (
    EpisodeSaveIn,
    LifecycleDependencySummary,
    ScriptCopyIn,
    ScriptUpdateIn,
)
from app.services import media_storage_jobs


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


class _AsyncSessionAdapter:
    """以同步 SQLite Session 承载异步 CRUD 契约。"""

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

    async def refresh(self, row) -> None:
        self.session.refresh(
            row,
            attribute_names=[column.key for column in row.__table__.columns],
        )

    async def delete(self, row) -> None:
        self.session.delete(row)

    async def get(self, *args, **kwargs):
        return self.session.get(*args, **kwargs)

    def add(self, row) -> None:
        self.session.add(row)


class _OneResult:
    def __init__(self, value) -> None:
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class _RowsResult:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    def scalars(self):
        return self

    def all(self) -> list[object]:
        return self.rows


def _script_row(**overrides) -> SimpleNamespace:
    now = datetime.now(UTC)
    values = {
        "id": "script-1",
        "title": "原剧本",
        "scope": "personal",
        "owner_user_id": "user-1",
        "org_id": None,
        "created_by_user_id": "user-1",
        "created_at": now,
        "updated_at": now,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _episode_row(**overrides) -> SimpleNamespace:
    now = datetime.now(UTC)
    values = {
        "episode_id": "episode-1",
        "script_id": "script-1",
        "episode_no": 1,
        "title": "第一集",
        "episode_version": 1,
        "client_save_seq": 0,
        "updated_at": now,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_lifecycle_routes_are_exposed() -> None:
    contracts = {
        (method, route.path)
        for route in scripts.router.routes
        for method in getattr(route, "methods", set())
    }

    assert {
        ("PATCH", "/scripts/{script_id}"),
        ("POST", "/scripts/{script_id}/copy"),
        ("DELETE", "/scripts/{script_id}"),
        ("PUT", "/scripts/{script_id}/episodes/{episode_id}"),
        ("DELETE", "/scripts/{script_id}/episodes/{episode_id}"),
    } <= contracts


async def test_delete_parent_loaders_emit_postgresql_for_update_locks() -> None:
    class CaptureDB:
        def __init__(self, rows: list[object]) -> None:
            self.rows = rows
            self.statements: list[object] = []

        async def execute(self, statement):
            self.statements.append(statement)
            return _OneResult(self.rows.pop(0))

    script = _script_row()
    episode = _episode_row()
    db = CaptureDB([script, episode])

    assert await script_crud.get_script_for_update(db, "script-1") is script
    assert (
        await script_crud.get_episode_for_update(db, "script-1", "episode-1")
        is episode
    )

    compiled = [
        str(statement.compile(dialect=postgresql.dialect()))
        for statement in db.statements
    ]
    assert all("FOR UPDATE" in sql for sql in compiled)
    assert (
        db.statements[1].get_execution_options().get("populate_existing") is True
    )


async def test_video_cleanup_debt_scan_uses_dialect_neutral_json_selection() -> None:
    class CaptureDB:
        def __init__(self) -> None:
            self.statement = None

        async def execute(self, statement):
            self.statement = statement
            return _RowsResult([])

    db = CaptureDB()
    assert (
        await script_crud._count_blocking_video_jobs(
            db,
            VideoGenerationJob.script_id == "script-1",
        )
        == 0
    )
    assert db.statement is not None

    for dialect in (sqlite.dialect(), postgresql.dialect()):
        sql = str(db.statement.compile(dialect=dialect))
        assert "video_generation_jobs.output_json" in sql
        assert "video_generation_jobs.error_type" in sql
        assert "video_generation_jobs.retryable" in sql
        assert "->" not in sql
        assert "JSON_EXTRACT" not in sql


@pytest.mark.parametrize("resource", ["script", "episode"])
async def test_lifecycle_delete_rechecks_cleanup_debt_after_ordered_row_locks(
    resource: str,
) -> None:
    script = _script_row()
    episode = _episode_row()
    video = SimpleNamespace(
        job_id="video-debt",
        attempt=1,
        status="canceled",
        error_type=None,
        retryable=False,
        output_json={
            "provider_task_id": "provider-task-debt",
            "provider_cancellation": {
                "provider_task_id": "provider-task-debt",
                "outcome": "pending",
                "retryable": True,
            },
        },
    )

    class CaptureDB:
        def __init__(self) -> None:
            self.statements: list[object] = []
            if resource == "script":
                self.results = [_OneResult(script), _RowsResult([video])]
            else:
                self.results = [
                    _OneResult(script),
                    _OneResult(episode),
                    _RowsResult([]),
                    _RowsResult([video]),
                ]

        async def execute(self, statement):
            self.statements.append(statement)
            return self.results.pop(0)

    db = CaptureDB()
    with pytest.raises(ConflictError, match="清理债务"):
        if resource == "script":
            await script_crud.delete_script(db, script)
        else:
            await script_crud.delete_episode(
                db,
                script.id,
                episode.episode_id,
            )

    locked_statements = [
        statement
        for statement in db.statements
        if "FOR UPDATE"
        in " ".join(str(statement.compile(dialect=postgresql.dialect())).split())
    ]
    compiled = [
        " ".join(str(statement.compile(dialect=postgresql.dialect())).split())
        for statement in locked_statements
    ]
    expected_tables = ["scripts"]
    if resource == "episode":
        expected_tables.append("episodes")
    expected_tables.append("video_generation_jobs")
    assert len(compiled) == len(expected_tables)
    assert all(
        f"FROM {table}" in sql and "FOR UPDATE" in sql
        for table, sql in zip(expected_tables, compiled, strict=True)
    )
    video_statement = locked_statements[-1]
    assert video_statement.get_execution_options()["populate_existing"] is True


async def test_rename_script_uses_edit_permission_and_writes_audit(monkeypatch) -> None:
    db = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock(), refresh=AsyncMock())
    user = SimpleNamespace(user_id="user-1", username="alice")
    row = _script_row()
    access = AsyncMock()
    audit = AsyncMock()
    monkeypatch.setattr(scripts.script_crud, "get_script", AsyncMock(return_value=row))
    monkeypatch.setattr(scripts, "check_script_access", access)

    async def rename(_db, target, title, *, commit):
        assert commit is False
        target.title = title
        return target

    monkeypatch.setattr(scripts.script_crud, "rename_script", rename)
    monkeypatch.setattr(scripts, "audit_log", audit)

    result = await scripts.rename_script(
        "script-1", ScriptUpdateIn(title="新剧本"), db=db, user=user
    )

    assert result.title == "新剧本"
    assert access.await_args.kwargs["action"] is Action.EDIT
    assert audit.await_args.kwargs["action"] == "script_rename"
    assert audit.await_args.kwargs["before"] == {"title": "原剧本"}
    assert audit.await_args.kwargs["after"] == {"title": "新剧本"}
    db.commit.assert_awaited_once()


async def test_copy_script_uses_edit_permission_and_audits_new_resource(monkeypatch) -> None:
    db = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock(), refresh=AsyncMock())
    user = SimpleNamespace(user_id="user-2", username="bob")
    source = _script_row()
    target = _script_row(
        id="script-copy",
        title="副本",
        owner_user_id="user-2",
        created_by_user_id="user-2",
    )
    access = AsyncMock()
    audit = AsyncMock()
    copy = AsyncMock(return_value=target)
    monkeypatch.setattr(scripts.script_crud, "get_script", AsyncMock(return_value=source))
    monkeypatch.setattr(scripts, "check_script_access", access)
    monkeypatch.setattr(scripts.script_crud, "copy_script", copy)
    monkeypatch.setattr(scripts, "audit_log", audit)

    result = await scripts.copy_script(
        "script-1", ScriptCopyIn(title="副本", scope="personal"), db=db, user=user
    )

    assert result.id == "script-copy"
    assert access.await_args.kwargs["action"] is Action.EDIT
    assert copy.await_args.kwargs == {
        "title": "副本",
        "scope": "personal",
        "commit": False,
    }
    assert audit.await_args.kwargs["resource_id"] == "script-copy"
    assert audit.await_args.kwargs["before"] == {"source_script_id": "script-1"}


async def test_team_editor_cannot_copy_project_across_scope(monkeypatch) -> None:
    db = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock(), refresh=AsyncMock())
    user = SimpleNamespace(user_id="editor-1", username="editor")
    source = _script_row(scope="team", org_id="org-1")
    copy = AsyncMock()
    monkeypatch.setattr(scripts.script_crud, "get_script", AsyncMock(return_value=source))
    monkeypatch.setattr(scripts, "check_script_access", AsyncMock())
    monkeypatch.setattr(
        scripts.script_crud,
        "get_project_role",
        AsyncMock(return_value="editor"),
    )
    monkeypatch.setattr(scripts.script_crud, "copy_script", copy)

    with pytest.raises(ForbiddenError, match="所有者或管理员"):
        await scripts.copy_script(
            "script-1",
            ScriptCopyIn(scope="personal"),
            db=db,
            user=user,
        )
    copy.assert_not_awaited()


async def test_viewer_cannot_copy_project_even_with_same_scope(monkeypatch) -> None:
    db = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock(), refresh=AsyncMock())
    user = SimpleNamespace(user_id="viewer-1", username="viewer")
    source = _script_row(scope="team", org_id="org-1")
    copy = AsyncMock()
    access = AsyncMock(side_effect=ForbiddenError("只读成员无权编辑剧本"))
    monkeypatch.setattr(scripts.script_crud, "get_script", AsyncMock(return_value=source))
    monkeypatch.setattr(scripts, "check_script_access", access)
    monkeypatch.setattr(scripts.script_crud, "copy_script", copy)

    with pytest.raises(ForbiddenError, match="只读成员"):
        await scripts.copy_script(
            "script-1",
            ScriptCopyIn(scope="team"),
            db=db,
            user=user,
        )

    assert access.await_args.kwargs["action"] is Action.EDIT
    copy.assert_not_awaited()


async def test_script_delete_rejects_dependencies_without_confirmation(monkeypatch) -> None:
    db = SimpleNamespace()
    user = SimpleNamespace(user_id="user-1")
    summary = LifecycleDependencySummary(episode_count=2, asset_count=3)
    delete = AsyncMock()
    monkeypatch.setattr(
        scripts.script_crud, "get_script_for_update", AsyncMock(return_value=_script_row())
    )
    monkeypatch.setattr(scripts, "check_script_access", AsyncMock())
    monkeypatch.setattr(
        scripts.script_crud,
        "get_script_dependency_summary",
        AsyncMock(return_value=summary),
    )
    monkeypatch.setattr(scripts.script_crud, "delete_script", delete)

    with pytest.raises(ConflictError, match="confirm=true") as exc_info:
        await scripts.delete_script("script-1", confirm=False, db=db, user=user)

    assert "episode_count=2" in exc_info.value.detail
    assert "asset_count=3" in exc_info.value.detail
    delete.assert_not_awaited()


@pytest.mark.parametrize("resource", ["script", "episode"])
async def test_delete_rejects_active_paid_jobs_even_when_confirmed(
    monkeypatch, resource: str
) -> None:
    db = SimpleNamespace()
    user = SimpleNamespace(user_id="user-1")
    summary = LifecycleDependencySummary(job_count=3, active_job_count=2)
    delete_script = AsyncMock()
    delete_episode = AsyncMock()
    monkeypatch.setattr(
        scripts.script_crud, "get_script_for_update", AsyncMock(return_value=_script_row())
    )
    monkeypatch.setattr(scripts, "check_script_access", AsyncMock())
    monkeypatch.setattr(scripts.script_crud, "delete_script", delete_script)
    monkeypatch.setattr(scripts.script_crud, "delete_episode", delete_episode)

    if resource == "script":
        monkeypatch.setattr(
            scripts.script_crud,
            "get_script_dependency_summary",
            AsyncMock(return_value=summary),
        )
        call = scripts.delete_script("script-1", confirm=True, db=db, user=user)
    else:
        monkeypatch.setattr(
            scripts.script_crud,
            "get_episode_for_update",
            AsyncMock(return_value=_episode_row()),
        )
        monkeypatch.setattr(
            scripts.script_crud,
            "get_episode_dependency_summary",
            AsyncMock(return_value=summary),
        )
        call = scripts.delete_episode(
            "script-1", "episode-1", confirm=True, db=db, user=user
        )

    with pytest.raises(ConflictError, match="请先取消或等待") as exc_info:
        await call
    assert "2 个" in exc_info.value.detail
    delete_script.assert_not_awaited()
    delete_episode.assert_not_awaited()


async def test_episode_delete_uses_delete_permission_and_returns_dependency_summary(
    monkeypatch,
) -> None:
    db = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())
    user = SimpleNamespace(user_id="user-1", username="alice")
    summary = LifecycleDependencySummary(
        subject_count=2,
        asset_count=4,
        shot_count=8,
        video_count=1,
    )
    access = AsyncMock()
    audit = AsyncMock()
    monkeypatch.setattr(
        scripts.script_crud, "get_script_for_update", AsyncMock(return_value=_script_row())
    )
    monkeypatch.setattr(
        scripts.script_crud, "get_episode_for_update", AsyncMock(return_value=_episode_row())
    )
    monkeypatch.setattr(
        scripts.script_crud,
        "get_episode_dependency_summary",
        AsyncMock(return_value=summary),
    )
    monkeypatch.setattr(scripts, "check_script_access", access)
    monkeypatch.setattr(scripts.script_crud, "delete_episode", AsyncMock())
    monkeypatch.setattr(scripts, "audit_log", audit)

    result = await scripts.delete_episode(
        "script-1", "episode-1", confirm=True, db=db, user=user
    )

    assert access.await_args.kwargs["action"] is Action.EDIT
    assert result.dependencies == summary
    assert audit.await_args.kwargs["action"] == "episode_delete"
    assert audit.await_args.kwargs["before"]["dependencies"]["video_count"] == 1
    db.commit.assert_awaited_once()


async def test_team_editor_can_delete_episode_but_cannot_delete_script(monkeypatch) -> None:
    """分集管理属于 EDIT；只有项目删除使用 DELETE。"""
    user = SimpleNamespace(
        user_id="editor-1",
        username="editor",
        enabled=True,
        active_org_id="org-1",
    )
    row = Script(
        id="script-1",
        title="团队剧本",
        scope="team",
        owner_user_id="owner-1",
        org_id="org-1",
        created_by_user_id="owner-1",
    )
    member = ProjectMember(
        id="member-1",
        script_id="script-1",
        user_id="editor-1",
        project_role="editor",
    )
    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[_OneResult("editor-1"), _OneResult(member)]
        )
    )
    monkeypatch.setattr(
        "app.core.permissions._audit_access_denied", AsyncMock()
    )

    with pytest.raises(ForbiddenError, match="无权删除"):
        await check_script_access(db, user, row, action=Action.DELETE)

    db.execute = AsyncMock(side_effect=[_OneResult("editor-1"), _OneResult(member)])
    await check_script_access(db, user, row, action=Action.EDIT)


async def test_change_episode_number_reuses_optimistic_lock_without_running_ai(
    monkeypatch,
) -> None:
    db = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())
    user = SimpleNamespace(user_id="user-1", username="alice")
    old = _episode_row()
    changed = _episode_row(
        episode_no=2,
        episode_version=2,
        client_save_seq=1,
    )
    access = AsyncMock()
    save = AsyncMock(return_value=changed)
    trigger = AsyncMock()
    monkeypatch.setattr(
        scripts.script_crud, "get_script", AsyncMock(return_value=_script_row())
    )
    monkeypatch.setattr(scripts.script_crud, "get_episode", AsyncMock(return_value=old))
    monkeypatch.setattr(scripts.script_crud, "save_episode", save)
    monkeypatch.setattr(scripts, "check_script_access", access)
    monkeypatch.setattr(scripts, "trigger_pipeline", trigger)
    monkeypatch.setattr(scripts, "audit_log", AsyncMock())

    result = await scripts.save_episode(
        "script-1",
        "episode-1",
        EpisodeSaveIn(episode_no=2, episode_version=1, client_save_seq=0),
        db=db,
        user=user,
    )

    assert result.episode_no == 2
    assert save.await_args.args[7] == 2
    assert [item.kwargs["action"] for item in access.await_args_list] == [Action.EDIT]
    trigger.assert_not_awaited()
    db.commit.assert_awaited_once()


def test_copy_only_clones_episode_text_and_resets_versions() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[
            User.__table__,
            Script.__table__,
            Episode.__table__,
            Scene.__table__,
            ProjectMember.__table__,
        ],
    )
    with Session(engine, expire_on_commit=False) as raw:
        user = User(
            user_id="user-1",
            username="alice",
            password_hash="hash",
            enabled=True,
        )
        source = Script(
            id="script-1",
            title="原剧本",
            scope="personal",
            owner_user_id="user-1",
            created_by_user_id="user-1",
        )
        raw.add(user)
        raw.add(source)
        raw.add(
            Episode(
                episode_id="episode-1",
                script_id="script-1",
                episode_no=1,
                title="第一集",
                script_text="正文",
                script_file_storage_key="private/source.docx",
                episode_version=9,
                client_save_seq=12,
            )
        )
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        async def scenario() -> Script:
            copied = await script_crud.copy_script(
                db,
                source,
                user,
                title="原剧本副本",
                scope="personal",
            )
            return copied

        import asyncio

        copied = asyncio.run(scenario())
        copied_episodes = list(
            raw.scalars(select(Episode).where(Episode.script_id == copied.id)).all()
        )

        assert len(copied_episodes) == 1
        clone = copied_episodes[0]
        assert clone.script_text == "正文"
        assert clone.script_file_storage_key is None
        assert clone.episode_version == 1
        assert clone.client_save_seq == 0


async def test_team_copy_without_commit_flushes_and_rolls_back_all_target_rows() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        raw.execute(text("PRAGMA foreign_keys=ON"))
        assert raw.scalar(text("PRAGMA foreign_keys")) == 1
        user = User(
            user_id="user-team-copy",
            username="team-copy-owner",
            password_hash="hash",
            enabled=True,
        )
        setattr(user, "active_org_id", "org-team-copy")
        source = Script(
            id="script-team-source",
            title="团队复制源剧本",
            scope="personal",
            owner_user_id=user.user_id,
            created_by_user_id=user.user_id,
        )
        raw.add_all(
            [
                user,
                source,
                Episode(
                    episode_id="episode-team-source",
                    script_id=source.id,
                    episode_no=1,
                    title="第一集",
                    script_text="待复制正文",
                    episode_version=3,
                    client_save_seq=4,
                ),
            ]
        )
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        commit = AsyncMock(wraps=db.commit)
        db.commit = commit  # type: ignore[method-assign]

        target = await script_crud.copy_script(
            db,
            source,
            user,
            title="团队副本",
            scope="team",
            commit=False,
        )

        owner = raw.scalar(
            select(ProjectMember).where(ProjectMember.script_id == target.id)
        )
        copied_episode = raw.scalar(
            select(Episode).where(Episode.script_id == target.id)
        )
        assert raw.get(Script, target.id) is target
        assert owner is not None
        assert (owner.user_id, owner.project_role) == (user.user_id, "owner")
        assert copied_episode is not None
        assert copied_episode.script_text == "待复制正文"
        commit.assert_not_awaited()

        # 模拟外层审计失败：父行、成员和分集必须由同一次回滚共同撤销。
        await db.rollback()

        assert raw.get(Script, target.id) is None
        assert raw.scalar(
            select(ProjectMember).where(ProjectMember.script_id == target.id)
        ) is None
        assert raw.scalar(select(Episode).where(Episode.script_id == target.id)) is None
        assert raw.get(Script, source.id) is source


def test_episode_number_conflict_is_reported_as_409() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[Script.__table__, Episode.__table__, Scene.__table__],
    )
    with Session(engine, expire_on_commit=False) as raw:
        raw.add(
            Script(
                id="script-1",
                title="剧本",
                scope="personal",
                owner_user_id="user-1",
                created_by_user_id="user-1",
            )
        )
        raw.add_all(
            [
                Episode(
                    episode_id="episode-1",
                    script_id="script-1",
                    episode_no=1,
                    title="第一集",
                ),
                Episode(
                    episode_id="episode-2",
                    script_id="script-1",
                    episode_no=2,
                    title="第二集",
                ),
            ]
        )
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        async def scenario() -> None:
            with pytest.raises(ConflictError, match="集号已存在") as exc_info:
                await script_crud.save_episode(
                    db,
                    "script-1",
                    "episode-1",
                    None,
                    1,
                    0,
                    episode_no=2,
                )
            assert exc_info.value.status_code == 409
            await db.rollback()

        import asyncio

        asyncio.run(scenario())


def test_script_delete_enqueues_all_owned_media_before_database_cleanup() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        user = User(
            user_id="user-1",
            username="alice",
            password_hash="hash",
            enabled=True,
        )
        script = Script(
            id="script-1",
            title="待删除剧本",
            scope="personal",
            owner_user_id="user-1",
            created_by_user_id="user-1",
        )
        episode = Episode(
            episode_id="episode-1",
            script_id="script-1",
            episode_no=1,
            title="第一集",
            script_file_storage_key="upload_images/material-1/script.txt",
        )
        subject = Subject(
            id="subject-1",
            script_id="script-1",
            type="character",
            name="角色",
            episode_nos=[1],
        )
        material = Materials(id="material-1", name="画布", script_id="script-1")
        asset = AssetEntry(
            id="asset-1",
            script_id="script-1",
            subject_id="subject-1",
            kind="image",
            storage_key="generate_images/material-1/asset.png",
            mime_type="image/png",
            size_bytes=10,
            source="generate_images",
        )
        group = ShotGroup(
            shot_group_id="group-1",
            script_id="script-1",
            episode_id="episode-1",
            scene_no=1,
            group_no="G1",
            reference_frames={
                "last_frame": "generate_images/video/frame.png",
            },
        )
        video = VideoGenerationJob(
            job_id="video-1",
            script_id="script-1",
            episode_id="episode-1",
            shot_group_id="group-1",
            scene_no=1,
            unit_id="group:group-1",
            unit_runtime_key="episode-1:1:group:group-1",
            provider_key="modelsai",
            job_type="all_reference",
            status="succeeded",
            output_json={
                "video_storage_key": "generate_images/video/video-1.mp4"
            },
        )
        render = TimelineRenderJob(
            job_id="render-1",
            script_id="script-1",
            episode_id="episode-1",
            owner_user_id="user-1",
            idempotency_key="render-delete-test",
            request_hash="a" * 64,
            input_json={},
            output_storage_key="generate_outputs/script-1/episode-1/render-1.mp4",
            status="succeeded",
        )
        generation = GenerationJob(
            job_id="generation-1",
            owner_user_id="user-1",
            material_id="material-1",
            script_id="script-1",
            subject_id="subject-1",
            episode_id="episode-1",
            idempotency_key="generation-delete-test",
            request_hash="b" * 64,
            input_version=1,
            attempt=1,
            job_type="generate_images",
            status="succeeded",
            progress=100,
            input_json={},
            metrics={
                "archived_output": {
                    "storage_key": "generate_images/material-1/generation-metrics.png"
                }
            },
        )
        published_generation = GenerationJob(
            job_id="generation-published",
            owner_user_id="user-1",
            material_id="material-1",
            script_id="script-1",
            subject_id="subject-1",
            episode_id="episode-1",
            idempotency_key="generation-published-delete-test",
            request_hash="c" * 64,
            input_version=1,
            attempt=2,
            job_type="generate_images",
            status="succeeded",
            progress=100,
            input_json={},
        )
        storyboard = StoryboardJob(
            job_id="storyboard-1",
            script_id="script-1",
            episode_id="episode-1",
            triggered_by_user_id="user-1",
            owner_user_id="user-1",
            episode_version=1,
            input_version=1,
            attempt=1,
            idempotency_key="storyboard-delete-test",
            request_hash="d" * 64,
            status="succeeded",
            progress=100,
        )
        pipeline = PipelineJob(
            id="pipeline-job-1",
            script_id="script-1",
            episode_id="episode-1",
            stage="split",
            dispatch_id="pipeline-stage-dispatch",
            run_id="pipeline-run-1",
            status="done",
            input_version=1,
            attempt=0,
        )
        now = datetime.now(UTC)
        outboxes = [
            TaskDispatchOutbox(
                id="dispatch-generation-delete",
                task_type="generation.generate_images",
                job_id=generation.job_id,
                attempt=generation.attempt,
                payload={"job_id": generation.job_id},
                status="pending",
            ),
            TaskDispatchOutbox(
                id="dispatch-video-delete",
                task_type="video.generate",
                job_id=video.job_id,
                attempt=video.attempt,
                payload={"job_id": video.job_id},
                status="retry",
            ),
            TaskDispatchOutbox(
                id="dispatch-timeline-delete",
                task_type="timeline.render",
                job_id=render.job_id,
                attempt=render.attempt,
                payload={"job_id": render.job_id},
                status="running",
                lease_owner="dispatcher",
                lease_token="dispatch-token",
                lease_until=now,
            ),
            TaskDispatchOutbox(
                id="dispatch-storyboard-delete",
                task_type="storyboard.generate",
                job_id=storyboard.job_id,
                attempt=storyboard.attempt,
                payload={"job_id": storyboard.job_id},
                status="pending",
            ),
            TaskDispatchOutbox(
                id="dispatch-pipeline-delete",
                task_type="pipeline.dag",
                job_id=pipeline.run_id,
                attempt=pipeline.input_version,
                payload={"run_id": pipeline.run_id},
                status="retry",
            ),
            TaskDispatchOutbox(
                id="dispatch-published-delete",
                task_type="generation.generate_images",
                job_id=published_generation.job_id,
                attempt=published_generation.attempt,
                payload={"job_id": published_generation.job_id},
                status="published",
                published_at=now,
            ),
            TaskDispatchOutbox(
                id="dispatch-unrelated",
                task_type="video.generate",
                job_id="other-script-video",
                attempt=1,
                payload={"job_id": "other-script-video"},
                status="pending",
            ),
        ]
        retained_script = Script(
            id="script-retained",
            title="保留剧本",
            scope="personal",
            owner_user_id=user.user_id,
            created_by_user_id=user.user_id,
        )
        retained_subject = Subject(
            id="subject-retained",
            script_id=retained_script.id,
            type="character",
            name="保留角色",
            episode_nos=[],
        )
        retained_asset = AssetEntry(
            id="asset-retained",
            script_id=retained_script.id,
            subject_id=retained_subject.id,
            kind="image",
            storage_key=asset.storage_key,
            mime_type="image/png",
            size_bytes=10,
            source="generate_images",
        )
        raw.add_all(
            [
                user,
                script,
                retained_script,
                episode,
                subject,
                retained_subject,
                material,
                asset,
                retained_asset,
                group,
                video,
                render,
                generation,
                published_generation,
                storyboard,
                pipeline,
                *outboxes,
            ]
        )
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        import asyncio

        asyncio.run(script_crud.delete_script(db, script))

        queued = set(raw.scalars(select(MediaStorageJob.source_key)))
        assert queued == {
            "upload_images/material-1/script.txt",
            "generate_images/material-1/generation-metrics.png",
            "generate_images/video/frame.png",
            "generate_images/video/video-1.mp4",
            "generate_outputs/script-1/episode-1/render-1.mp4",
        }
        assert raw.get(Script, "script-1") is None
        assert {
            raw.get(TaskDispatchOutbox, dispatch_id).status
            for dispatch_id in {
                "dispatch-generation-delete",
                "dispatch-video-delete",
                "dispatch-timeline-delete",
                "dispatch-storyboard-delete",
                "dispatch-pipeline-delete",
            }
        } == {"canceled"}
        timeline_dispatch = raw.get(TaskDispatchOutbox, "dispatch-timeline-delete")
        assert timeline_dispatch.lease_owner is None
        assert timeline_dispatch.lease_token is None
        assert timeline_dispatch.lease_until is None
        assert raw.get(TaskDispatchOutbox, "dispatch-published-delete").status == "published"
        assert raw.get(TaskDispatchOutbox, "dispatch-unrelated").status == "pending"
        asset_task = raw.scalar(
            select(MediaStorageJob).where(
                MediaStorageJob.source_key == "generate_images/material-1/asset.png"
            )
        )
        assert asset_task is None
        assert raw.get(AssetEntry, retained_asset.id) is not None


def test_episode_delete_cancels_all_owned_task_dispatches() -> None:
    """分集任务行删除前必须收敛无外键 outbox，已发布和无关投递保持不变。"""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        script = Script(
            id="script-episode-dispatch",
            title="分集投递删除测试",
            scope="personal",
            owner_user_id="user-dispatch",
            created_by_user_id="user-dispatch",
        )
        episode = Episode(
            episode_id="episode-dispatch",
            script_id=script.id,
            episode_no=1,
            title="第一集",
        )
        group = ShotGroup(
            shot_group_id="group-dispatch",
            script_id=script.id,
            episode_id=episode.episode_id,
            scene_no=1,
            group_no="G1",
        )
        generation = GenerationJob(
            job_id="generation-episode-dispatch",
            owner_user_id="user-dispatch",
            material_id="material-dispatch",
            script_id=script.id,
            subject_id="subject-dispatch",
            episode_id=episode.episode_id,
            idempotency_key="generation-episode-dispatch",
            request_hash="a" * 64,
            input_version=1,
            attempt=1,
            job_type="generate_images",
            status="succeeded",
            progress=100,
            input_json={},
        )
        legacy_subject = Subject(
            id="subject-legacy-dispatch",
            script_id=script.id,
            type="character",
            name="历史角色",
            episode_nos=[1],
        )
        legacy_generation = GenerationJob(
            job_id="generation-legacy-dispatch",
            owner_user_id="user-dispatch",
            material_id="material-dispatch",
            script_id=script.id,
            subject_id=legacy_subject.id,
            episode_id=None,
            idempotency_key="generation-legacy-dispatch",
            request_hash="9" * 64,
            input_version=1,
            attempt=1,
            job_type="generate_images",
            status="succeeded",
            progress=100,
            input_json={},
        )
        video = VideoGenerationJob(
            job_id="video-episode-dispatch",
            script_id=script.id,
            episode_id=episode.episode_id,
            shot_group_id=group.shot_group_id,
            scene_no=1,
            unit_id=f"group:{group.shot_group_id}",
            unit_runtime_key=f"{episode.episode_id}:1:group:{group.shot_group_id}",
            provider_key="modelsai",
            job_type="all_reference",
            status="succeeded",
        )
        timeline = TimelineRenderJob(
            job_id="timeline-episode-dispatch",
            script_id=script.id,
            episode_id=episode.episode_id,
            owner_user_id="user-dispatch",
            idempotency_key="timeline-episode-dispatch",
            request_hash="b" * 64,
            input_json={},
            output_storage_key=(
                "generate_outputs/script-episode-dispatch/episode-dispatch/render.mp4"
            ),
            status="succeeded",
        )
        storyboard = StoryboardJob(
            job_id="storyboard-episode-dispatch",
            script_id=script.id,
            episode_id=episode.episode_id,
            episode_version=1,
            input_version=1,
            attempt=1,
            idempotency_key="storyboard-episode-dispatch",
            request_hash="c" * 64,
            status="succeeded",
            progress=100,
        )
        pipeline = PipelineJob(
            id="pipeline-episode-dispatch",
            script_id=script.id,
            episode_id=episode.episode_id,
            stage="extract",
            dispatch_id="pipeline-stage-dispatch",
            run_id="pipeline-run-episode-dispatch",
            status="done",
            input_version=1,
            attempt=0,
        )
        now = datetime.now(UTC)
        dispatches = [
            TaskDispatchOutbox(
                id="episode-dispatch-generation",
                task_type="generation.generate_images",
                job_id=generation.job_id,
                attempt=generation.attempt,
                payload={"job_id": generation.job_id},
                status="pending",
            ),
            TaskDispatchOutbox(
                id="episode-dispatch-video",
                task_type="video.generate",
                job_id=video.job_id,
                attempt=video.attempt,
                payload={"job_id": video.job_id},
                status="retry",
            ),
            TaskDispatchOutbox(
                id="episode-dispatch-generation-legacy",
                task_type="generation.generate_images",
                job_id=legacy_generation.job_id,
                attempt=legacy_generation.attempt,
                payload={"job_id": legacy_generation.job_id},
                status="pending",
            ),
            TaskDispatchOutbox(
                id="episode-dispatch-timeline",
                task_type="timeline.render",
                job_id=timeline.job_id,
                attempt=timeline.attempt,
                payload={"job_id": timeline.job_id},
                status="running",
                lease_owner="dispatcher",
                lease_token="lease-token",
                lease_until=now,
            ),
            TaskDispatchOutbox(
                id="episode-dispatch-storyboard",
                task_type="storyboard.generate",
                job_id=storyboard.job_id,
                attempt=storyboard.attempt,
                payload={"job_id": storyboard.job_id},
                status="pending",
            ),
            TaskDispatchOutbox(
                id="episode-dispatch-pipeline",
                task_type="pipeline.dag",
                job_id=pipeline.run_id,
                attempt=pipeline.input_version,
                payload={"run_id": pipeline.run_id},
                status="retry",
            ),
            TaskDispatchOutbox(
                id="episode-dispatch-unrelated",
                task_type="video.generate",
                job_id="video-other-episode",
                attempt=1,
                payload={"job_id": "video-other-episode"},
                status="pending",
            ),
        ]
        raw.add_all(
            [
                script,
                episode,
                group,
                generation,
                legacy_subject,
                legacy_generation,
                video,
                timeline,
                storyboard,
                pipeline,
                *dispatches,
            ]
        )
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        import asyncio

        asyncio.run(
            script_crud.delete_episode(
                db,
                script.id,
                episode.episode_id,
            )
        )

        assert raw.get(Episode, episode.episode_id) is None
        assert {
            raw.get(TaskDispatchOutbox, dispatch_id).status
            for dispatch_id in {
                "episode-dispatch-generation",
                "episode-dispatch-generation-legacy",
                "episode-dispatch-video",
                "episode-dispatch-timeline",
                "episode-dispatch-storyboard",
                "episode-dispatch-pipeline",
            }
        } == {"canceled"}
        timeline_dispatch = raw.get(
            TaskDispatchOutbox,
            "episode-dispatch-timeline",
        )
        assert timeline_dispatch.lease_owner is None
        assert timeline_dispatch.lease_token is None
        assert timeline_dispatch.lease_until is None
        assert (
            raw.get(TaskDispatchOutbox, "episode-dispatch-unrelated").status
            == "pending"
        )


def _seed_nonvideo_financial_cleanup_graph(
    raw: Session,
    *,
    kind: str,
    cleared: bool = False,
) -> tuple[Script, Episode, object]:
    user = User(
        user_id="user-financial",
        username="financial-owner",
        password_hash="hash",
        enabled=True,
    )
    script = Script(
        id="script-financial",
        title="财务清理债务测试",
        scope="personal",
        owner_user_id=user.user_id,
        created_by_user_id=user.user_id,
    )
    episode = Episode(
        episode_id="episode-financial",
        script_id=script.id,
        episode_no=1,
        title="第一集",
    )
    subject = Subject(
        id="subject-financial",
        script_id=script.id,
        type="character",
        name="角色",
        episode_nos=[1] if kind == "generation_legacy" else [],
    )
    material = Materials(
        id="material-financial",
        name="画布",
        script_id=script.id,
    )

    if kind in {"generation", "generation_legacy"}:
        job: object = GenerationJob(
            job_id=f"{kind}-financial",
            owner_user_id=user.user_id,
            material_id=material.id,
            script_id=script.id,
            subject_id=subject.id,
            episode_id=(None if kind == "generation_legacy" else episode.episode_id),
            idempotency_key=f"{kind}-financial",
            request_hash="d" * 64,
            input_version=1,
            attempt=1,
            job_type="generate_images",
            status="failed",
            error_type="financial_cleanup",
            retryable=not cleared,
            input_json={"reservation_id": "reservation-generation"},
            metrics={
                "financial_cleanup": {
                    "action": "release",
                    "reservation_id": "reservation-generation",
                }
            },
        )
    elif kind == "storyboard":
        job = StoryboardJob(
            job_id="storyboard-financial",
            script_id=script.id,
            episode_id=episode.episode_id,
            episode_version=1,
            input_version=1,
            attempt=1,
            idempotency_key="storyboard-financial",
            request_hash="e" * 64,
            status="failed",
            retryable=True,
            reservation_id="reservation-storyboard",
            financial_cleanup_status="done" if cleared else "pending",
        )
    elif kind == "timeline":
        job = TimelineRenderJob(
            job_id="timeline-financial",
            script_id=script.id,
            episode_id=episode.episode_id,
            owner_user_id=user.user_id,
            idempotency_key="timeline-financial",
            request_hash="f" * 64,
            input_json={},
            output_storage_key=(
                "generate_outputs/script-financial/episode-financial/render.mp4"
            ),
            status="failed",
            output_json={
                "financial_cleanup": {
                    "status": "done" if cleared else "pending",
                    "action": "release",
                    "reservation_id": "reservation-timeline",
                }
            },
        )
    else:
        raise AssertionError(f"未知测试任务类型: {kind}")

    raw.add_all([user, script, episode, subject, material, job])
    raw.commit()
    return script, episode, job


@pytest.mark.parametrize("resource", ["script", "episode"])
@pytest.mark.parametrize(
    "kind",
    ["generation", "generation_legacy", "storyboard", "timeline"],
)
def test_lifecycle_delete_preserves_nonvideo_financial_cleanup_debt(
    resource: str,
    kind: str,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        script, episode, job = _seed_nonvideo_financial_cleanup_graph(
            raw,
            kind=kind,
        )
        db = _AsyncSessionAdapter(raw)

        async def scenario() -> None:
            if resource == "script":
                summary = await script_crud.get_script_dependency_summary(db, script.id)
            else:
                summary = await script_crud.get_episode_dependency_summary(
                    db,
                    script.id,
                    episode.episode_id,
                )
            assert summary.active_job_count == 1

            with pytest.raises(ConflictError, match="财务清理债务"):
                if resource == "script":
                    await script_crud.delete_script(db, script)
                else:
                    await script_crud.delete_episode(
                        db,
                        script.id,
                        episode.episode_id,
                    )
            await db.rollback()

        import asyncio

        asyncio.run(scenario())

        assert raw.get(Script, script.id) is not None
        assert raw.get(Episode, episode.episode_id) is not None
        assert raw.get(type(job), job.job_id) is not None


@pytest.mark.parametrize(
    "kind",
    ["generation", "generation_legacy", "storyboard", "timeline"],
)
def test_lifecycle_delete_allows_cleared_nonvideo_financial_cleanup(
    kind: str,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        script, _, _ = _seed_nonvideo_financial_cleanup_graph(
            raw,
            kind=kind,
            cleared=True,
        )
        db = _AsyncSessionAdapter(raw)

        import asyncio

        summary = asyncio.run(
            script_crud.get_script_dependency_summary(db, script.id)
        )
        assert summary.active_job_count == 0
        asyncio.run(script_crud.delete_script(db, script))
        assert raw.get(Script, script.id) is None


def _seed_pipeline_financial_cleanup_graph(
    raw: Session,
    *,
    reservation_status: str | None,
) -> tuple[Script, Episode, PipelineJob, CreditReservation | None]:
    user = User(
        user_id="user-pipeline-financial",
        username="pipeline-financial-owner",
        password_hash="hash",
        enabled=True,
    )
    script = Script(
        id="script-pipeline-financial",
        title="流水线财务清理债务测试",
        scope="personal",
        owner_user_id=user.user_id,
        created_by_user_id=user.user_id,
    )
    episode = Episode(
        episode_id="episode-pipeline-financial",
        script_id=script.id,
        episode_no=1,
        title="第一集",
    )
    job = PipelineJob(
        id="pipeline-financial-job",
        script_id=script.id,
        episode_id=episode.episode_id,
        stage="merge",
        dispatch_id="pipeline-financial-dispatch",
        run_id="pipeline-financial-run",
        status="failed",
        input_version=1,
        attempt=1,
        error_type="provider_timeout",
        error_message="上游服务超时",
        retryable=False,
    )
    rows: list[object] = [user, script, episode, job]
    reservation = None
    if reservation_status is not None:
        account = CreditAccount(
            id="account-pipeline-financial",
            owner_type="user",
            owner_id=user.user_id,
            balance_points=10,
            frozen_points=1 if reservation_status == "reserved" else 0,
            status="active",
        )
        reservation = CreditReservation(
            id="reservation-pipeline-financial",
            account_id=account.id,
            source_type="pipeline",
            source_id=job.run_id,
            reserved_points=1,
            actual_points=(1 if reservation_status in {"captured", "refunded"} else None),
            status=reservation_status,
        )
        rows.extend([account, reservation])
    raw.add_all(rows)
    raw.commit()
    return script, episode, job, reservation


@pytest.mark.parametrize("resource", ["script", "episode"])
def test_lifecycle_delete_preserves_failed_pipeline_reserved_credit_debt(
    resource: str,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        script, episode, job, reservation = _seed_pipeline_financial_cleanup_graph(
            raw,
            reservation_status="reserved",
        )
        db = _AsyncSessionAdapter(raw)

        async def scenario() -> None:
            if resource == "script":
                summary = await script_crud.get_script_dependency_summary(db, script.id)
            else:
                summary = await script_crud.get_episode_dependency_summary(
                    db,
                    script.id,
                    episode.episode_id,
                )
            assert summary.active_job_count == 1

            with pytest.raises(ConflictError, match="财务清理债务"):
                if resource == "script":
                    await script_crud.delete_script(db, script)
                else:
                    await script_crud.delete_episode(
                        db,
                        script.id,
                        episode.episode_id,
                    )
            await db.rollback()

        import asyncio

        asyncio.run(scenario())

        assert raw.get(Script, script.id) is not None
        assert raw.get(Episode, episode.episode_id) is not None
        assert raw.get(PipelineJob, job.id) is not None
        assert reservation is not None
        assert raw.get(CreditReservation, reservation.id) is not None


@pytest.mark.parametrize("reservation_status", ["released", "captured", None])
def test_lifecycle_delete_allows_failed_pipeline_without_reserved_credit_debt(
    reservation_status: str | None,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        script, _, _, _ = _seed_pipeline_financial_cleanup_graph(
            raw,
            reservation_status=reservation_status,
        )
        db = _AsyncSessionAdapter(raw)

        import asyncio

        summary = asyncio.run(
            script_crud.get_script_dependency_summary(db, script.id)
        )
        assert summary.active_job_count == 0
        asyncio.run(script_crud.delete_script(db, script))
        assert raw.get(Script, script.id) is None


def _seed_video_cleanup_debt_graph(
    raw: Session,
    *,
    status: str = "canceled",
    error_type: str | None = None,
    retryable: bool = False,
    output_json: dict | None = None,
) -> tuple[Script, Episode, VideoGenerationJob]:
    script = Script(
        id="script-debt",
        title="远端清理债务测试",
        scope="personal",
        owner_user_id="user-debt",
        created_by_user_id="user-debt",
    )
    episode = Episode(
        episode_id="episode-debt",
        script_id=script.id,
        episode_no=1,
        title="第一集",
    )
    shot_group = ShotGroup(
        shot_group_id="group-debt",
        script_id=script.id,
        episode_id=episode.episode_id,
        scene_no=1,
        group_no="G1",
    )
    video = VideoGenerationJob(
        job_id="video-debt",
        script_id=script.id,
        episode_id=episode.episode_id,
        shot_group_id=shot_group.shot_group_id,
        scene_no=1,
        unit_id=f"group:{shot_group.shot_group_id}",
        unit_runtime_key=f"{episode.episode_id}:1:group:{shot_group.shot_group_id}",
        provider_key="modelsai",
        job_type="all_reference",
        status=status,
        error_type=error_type,
        retryable=retryable,
        output_json=output_json,
    )
    raw.add_all([script, episode, shot_group, video])
    raw.commit()
    return script, episode, video


@pytest.mark.parametrize(
    ("resource", "output_json"),
    [
        (
            "script",
            {
                "provider_task_id": "provider-task-pending",
                "provider_cancellation": {
                    "provider_task_id": "provider-task-pending",
                    "outcome": "pending",
                    "retryable": True,
                    "attempts": 0,
                },
            },
        ),
        (
            "episode",
            {
                "provider_task_id": "provider-task-failed",
                "provider_cancellation": {
                    "provider_task_id": "provider-task-failed",
                    "outcome": "failed",
                    "retryable": True,
                    "attempts": 2,
                },
            },
        ),
        (
            "script",
            {
                "attempt_output_cleanup": {
                    "attempt-1": {
                        "storage_attempt_id": "attempt-1",
                        "video_storage_key": "generate_images/video/orphan.mp4",
                        "retryable": True,
                    }
                }
            },
        ),
        (
            "episode",
            {
                "attempt_output_cleanup": {
                    "attempt-2": {
                        "storage_attempt_id": "attempt-2",
                        "last_frame_storage_key": "generate_images/video/orphan.jpg",
                        "retryable": True,
                    }
                }
            },
        ),
    ],
)
def test_lifecycle_delete_preserves_retryable_video_cleanup_debt(
    resource: str,
    output_json: dict,
) -> None:
    """confirm=true 也不能删除仍需 reconciler 处理的外部副作用事实。"""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        script, episode, video = _seed_video_cleanup_debt_graph(
            raw,
            output_json=output_json,
        )
        db = _AsyncSessionAdapter(raw)

        async def scenario() -> None:
            if resource == "script":
                summary = await script_crud.get_script_dependency_summary(db, script.id)
            else:
                summary = await script_crud.get_episode_dependency_summary(
                    db,
                    script.id,
                    episode.episode_id,
                )
            assert summary.active_job_count == 1
            with pytest.raises(ConflictError, match="清理|取消|债务"):
                if resource == "script":
                    await script_crud.delete_script(db, script)
                else:
                    await script_crud.delete_episode(
                        db,
                        script.id,
                        episode.episode_id,
                    )
            await db.rollback()

        import asyncio

        asyncio.run(scenario())

        assert raw.get(Script, script.id) is not None
        assert raw.get(Episode, episode.episode_id) is not None
        assert raw.get(VideoGenerationJob, video.job_id) is not None


@pytest.mark.parametrize("resource", ["script", "episode"])
def test_lifecycle_delete_preserves_retryable_video_financial_cleanup_debt(
    resource: str,
) -> None:
    """财务释放失败仍依赖 VideoJob 供 reconciler 定位，不得硬删除。"""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        script, episode, video = _seed_video_cleanup_debt_graph(
            raw,
            status="failed",
            error_type="financial_cleanup",
            retryable=True,
        )
        db = _AsyncSessionAdapter(raw)

        async def scenario() -> None:
            if resource == "script":
                summary = await script_crud.get_script_dependency_summary(db, script.id)
            else:
                summary = await script_crud.get_episode_dependency_summary(
                    db,
                    script.id,
                    episode.episode_id,
                )
            assert summary.active_job_count == 1

            with pytest.raises(ConflictError, match="财务清理债务"):
                if resource == "script":
                    await script_crud.delete_script(db, script)
                else:
                    await script_crud.delete_episode(
                        db,
                        script.id,
                        episode.episode_id,
                    )
            await db.rollback()

        import asyncio

        asyncio.run(scenario())

        assert raw.get(Script, script.id) is not None
        assert raw.get(Episode, episode.episode_id) is not None
        assert raw.get(VideoGenerationJob, video.job_id) is not None


@pytest.mark.parametrize(
    ("status", "output_json"),
    [
        (
            "canceled",
            {
                "provider_task_id": "provider-canceled",
                "provider_cancellation": {
                    "provider_task_id": "provider-canceled",
                    "outcome": "canceled",
                    "retryable": False,
                },
            },
        ),
        (
            "canceled",
            {
                "provider_task_id": "provider-unsupported",
                "provider_cancellation": {
                    "provider_task_id": "provider-unsupported",
                    "outcome": "unsupported",
                    "retryable": False,
                },
            },
        ),
        (
            "canceled",
            {
                "provider_cancellation": {
                    "outcome": "pending",
                    "retryable": True,
                },
            },
        ),
        (
            "succeeded",
            {
                "provider_task_id": "provider-succeeded",
                "provider_cancellation": {
                    "provider_task_id": "provider-succeeded",
                    "outcome": "failed",
                    "retryable": True,
                },
            },
        ),
        (
            "failed",
            {
                "provider_task_id": "provider-failed",
                "provider_cancellation": {
                    "provider_task_id": "provider-failed",
                    "outcome": "pending",
                    "retryable": True,
                },
            },
        ),
    ],
)
def test_lifecycle_delete_does_not_block_safe_video_terminal_state(
    status: str,
    output_json: dict,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        script, _, _ = _seed_video_cleanup_debt_graph(
            raw,
            status=status,
            output_json=output_json,
        )
        db = _AsyncSessionAdapter(raw)

        import asyncio

        summary = asyncio.run(
            script_crud.get_script_dependency_summary(db, script.id)
        )
        assert summary.active_job_count == 0
        asyncio.run(script_crud.delete_script(db, script))
        assert raw.get(Script, script.id) is None


@pytest.mark.parametrize(
    ("error_type", "retryable"),
    [
        ("financial_cleanup", False),
        ("provider_timeout", True),
    ],
)
def test_lifecycle_delete_does_not_block_nonfinancial_or_nonretryable_failure(
    error_type: str,
    retryable: bool,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        script, _, _ = _seed_video_cleanup_debt_graph(
            raw,
            status="failed",
            error_type=error_type,
            retryable=retryable,
        )
        db = _AsyncSessionAdapter(raw)

        import asyncio

        summary = asyncio.run(
            script_crud.get_script_dependency_summary(db, script.id)
        )
        assert summary.active_job_count == 0
        asyncio.run(script_crud.delete_script(db, script))
        assert raw.get(Script, script.id) is None


def test_terminal_video_orphan_last_frame_is_enqueued_for_delete() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        orphan_key = "generate_images/video/orphan-last-frame.jpg"
        script, _, _ = _seed_video_cleanup_debt_graph(
            raw,
            status="failed",
            error_type="provider_timeout",
            retryable=False,
            output_json={
                "attempt_output_cleanup": {
                    "attempt-terminal": {
                        "storage_attempt_id": "attempt-terminal",
                        "last_frame_storage_key": orphan_key,
                        "retryable": False,
                    }
                }
            },
        )

        import asyncio

        asyncio.run(script_crud.delete_script(_AsyncSessionAdapter(raw), script))

        assert orphan_key in set(
            raw.scalars(
                select(MediaStorageJob.source_key).where(
                    MediaStorageJob.job_type == "delete"
                )
            )
        )


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 视频清理债务删除测试",
)
@pytest.mark.parametrize(
    ("debt_values", "message"),
    [
        (
            {
                "output_json": {
                    "attempt_output_cleanup": {
                        "attempt-pg": {
                            "storage_attempt_id": "attempt-pg",
                            "video_storage_key": "generate_images/video/orphan-pg.mp4",
                            "retryable": True,
                        }
                    }
                }
            },
            "媒体清理债务",
        ),
        (
            {
                "status": "failed",
                "error_type": "financial_cleanup",
                "retryable": True,
            },
            "财务清理债务",
        ),
    ],
)
def test_postgres_episode_delete_rechecks_cleanup_debt_after_precheck(
    debt_values: dict,
    message: str,
) -> None:
    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = secrets.token_hex(6)
        script_id = f"script-debt-{suffix}"
        episode_id = f"episode-debt-{suffix}"
        group_id = f"group-debt-{suffix}"
        job_id = f"video-debt-{suffix}"

        try:
            async with session_factory() as db:
                try:
                    db.add(
                        Script(
                            id=script_id,
                            title="PostgreSQL 清理债务测试",
                            scope="personal",
                            owner_user_id="user-debt",
                            created_by_user_id="user-debt",
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
                            unit_id=f"group:{group_id}",
                            unit_runtime_key=f"{episode_id}:1:group:{group_id}",
                            provider_key="modelsai",
                            job_type="all_reference",
                            status="succeeded",
                            output_json={},
                        )
                    )
                    await db.commit()

                    await script_crud.get_script_for_update(db, script_id)
                    await script_crud.get_episode_for_update(
                        db,
                        script_id,
                        episode_id,
                    )
                    summary = await script_crud.get_episode_dependency_summary(
                        db,
                        script_id,
                        episode_id,
                    )
                    assert summary.active_job_count == 0

                    async with session_factory() as writer:
                        await writer.execute(text("SET LOCAL lock_timeout = '2s'"))
                        await writer.execute(
                            update(VideoGenerationJob)
                            .where(VideoGenerationJob.job_id == job_id)
                            .values(**debt_values)
                        )
                        await writer.commit()

                    with pytest.raises(ConflictError, match=message):
                        await script_crud.delete_episode(
                            db,
                            script_id,
                            episode_id,
                            commit=False,
                        )
                    await db.rollback()

                    assert await db.get(Script, script_id) is not None
                    assert await db.get(Episode, episode_id) is not None
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

    import asyncio

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 分集 identity map 刷新测试",
)
def test_postgres_get_episode_for_update_refreshes_selectin_identity_map() -> None:
    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = secrets.token_hex(6)
        script_id = f"script-identity-{suffix}"
        episode_id = f"episode-identity-{suffix}"

        try:
            async with session_factory() as db:
                try:
                    db.add(
                        Script(
                            id=script_id,
                            title="分集 identity map 测试",
                            scope="personal",
                            owner_user_id="user-identity",
                            created_by_user_id="user-identity",
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
                    await db.commit()

                    locked_script = await script_crud.get_script_for_update(
                        db,
                        script_id,
                    )
                    preloaded_episode = next(
                        item
                        for item in locked_script.episodes
                        if item.episode_id == episode_id
                    )
                    assert preloaded_episode.episode_no == 1

                    async with session_factory() as writer:
                        await writer.execute(
                            update(Episode)
                            .where(Episode.episode_id == episode_id)
                            .values(episode_no=2)
                        )
                        await writer.commit()

                    refreshed_episode = await script_crud.get_episode_for_update(
                        db,
                        script_id,
                        episode_id,
                    )
                    assert refreshed_episode is preloaded_episode
                    assert refreshed_episode.episode_no == 2
                finally:
                    await db.rollback()
                    await db.execute(
                        delete(Episode).where(Episode.episode_id == episode_id)
                    )
                    await db.execute(delete(Script).where(Script.id == script_id))
                    await db.commit()
        finally:
            await engine.dispose()

    import asyncio

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 非视频财务债务竞态测试",
)
@pytest.mark.parametrize(
    "kind",
    ["generation", "generation_legacy", "storyboard", "timeline"],
)
def test_postgres_episode_delete_rechecks_nonvideo_financial_debt_after_precheck(
    kind: str,
) -> None:
    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = secrets.token_hex(6)
        user_id = f"user-debt-{suffix}"
        script_id = f"script-task-debt-{suffix}"
        episode_id = f"episode-task-debt-{suffix}"
        subject_id = f"subject-debt-{suffix}"
        material_id = f"material-debt-{suffix}"
        job_id = f"{kind}-debt-{suffix}"

        try:
            async with session_factory() as db:
                try:
                    db.add(
                        User(
                            user_id=user_id,
                            username=f"debt-{suffix}",
                            password_hash="hash",
                            enabled=True,
                        )
                    )
                    db.add(
                        Script(
                            id=script_id,
                            title="PostgreSQL 非视频财务债务测试",
                            scope="personal",
                            owner_user_id=user_id,
                            created_by_user_id=user_id,
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
                    db.add(
                        Subject(
                            id=subject_id,
                            script_id=script_id,
                            type="character",
                            name="角色",
                            episode_nos=(
                                [1] if kind == "generation_legacy" else []
                            ),
                        )
                    )
                    db.add(
                        Materials(
                            id=material_id,
                            name="画布",
                            script_id=script_id,
                        )
                    )
                    await db.flush()

                    if kind in {"generation", "generation_legacy"}:
                        job_model = GenerationJob
                        db.add(
                            GenerationJob(
                                job_id=job_id,
                                owner_user_id=user_id,
                                material_id=material_id,
                                script_id=script_id,
                                subject_id=subject_id,
                                episode_id=(
                                    None
                                    if kind == "generation_legacy"
                                    else episode_id
                                ),
                                idempotency_key=f"idem-{job_id}",
                                request_hash="1" * 64,
                                input_version=1,
                                attempt=1,
                                job_type="generate_images",
                                status="succeeded",
                                progress=100,
                                input_json={},
                            )
                        )
                    elif kind == "storyboard":
                        job_model = StoryboardJob
                        db.add(
                            StoryboardJob(
                                job_id=job_id,
                                script_id=script_id,
                                episode_id=episode_id,
                                episode_version=1,
                                input_version=1,
                                attempt=1,
                                idempotency_key=f"idem-{job_id}",
                                request_hash="2" * 64,
                                status="succeeded",
                                progress=100,
                            )
                        )
                    else:
                        job_model = TimelineRenderJob
                        db.add(
                            TimelineRenderJob(
                                job_id=job_id,
                                script_id=script_id,
                                episode_id=episode_id,
                                owner_user_id=user_id,
                                idempotency_key=f"idem-{job_id}",
                                request_hash="3" * 64,
                                input_json={},
                                output_storage_key=(
                                    f"generate_outputs/{script_id}/{episode_id}/{job_id}.mp4"
                                ),
                                status="succeeded",
                            )
                        )
                    await db.commit()

                    await script_crud.get_script_for_update(db, script_id)
                    await script_crud.get_episode_for_update(
                        db,
                        script_id,
                        episode_id,
                    )
                    summary = await script_crud.get_episode_dependency_summary(
                        db,
                        script_id,
                        episode_id,
                    )
                    assert summary.active_job_count == 0

                    async with session_factory() as writer:
                        if kind in {"generation", "generation_legacy"}:
                            await writer.execute(
                                update(GenerationJob)
                                .where(GenerationJob.job_id == job_id)
                                .values(
                                    status="failed",
                                    error_type="financial_cleanup",
                                    retryable=True,
                                )
                            )
                        elif kind == "storyboard":
                            await writer.execute(
                                update(StoryboardJob)
                                .where(StoryboardJob.job_id == job_id)
                                .values(
                                    status="failed",
                                    financial_cleanup_status="pending",
                                )
                            )
                        else:
                            await writer.execute(
                                update(TimelineRenderJob)
                                .where(TimelineRenderJob.job_id == job_id)
                                .values(
                                    status="failed",
                                    output_json={
                                        "financial_cleanup": {
                                            "status": "pending",
                                            "action": "release",
                                            "reservation_id": "reservation-pg",
                                        }
                                    },
                                )
                            )
                        await writer.commit()

                    with pytest.raises(ConflictError, match="财务清理债务"):
                        await script_crud.delete_episode(
                            db,
                            script_id,
                            episode_id,
                            commit=False,
                        )
                    await db.rollback()

                    assert await db.get(Script, script_id) is not None
                    assert await db.get(Episode, episode_id) is not None
                    assert (
                        await db.get(job_model, job_id, populate_existing=True)
                        is not None
                    )
                finally:
                    await db.rollback()
                    await db.execute(
                        delete(GenerationJob).where(
                            GenerationJob.script_id == script_id
                        )
                    )
                    await db.execute(
                        delete(StoryboardJob).where(
                            StoryboardJob.script_id == script_id
                        )
                    )
                    await db.execute(
                        delete(TimelineRenderJob).where(
                            TimelineRenderJob.script_id == script_id
                        )
                    )
                    await db.execute(
                        delete(Episode).where(Episode.episode_id == episode_id)
                    )
                    await db.execute(
                        delete(Subject).where(Subject.id == subject_id)
                    )
                    await db.execute(
                        delete(Materials).where(Materials.id == material_id)
                    )
                    await db.execute(delete(Script).where(Script.id == script_id))
                    await db.execute(delete(User).where(User.user_id == user_id))
                    await db.commit()
        finally:
            await engine.dispose()

    import asyncio

    asyncio.run(scenario())


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 流水线财务债务竞态测试",
)
def test_postgres_episode_delete_rechecks_pipeline_reserved_credit_after_precheck() -> None:
    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = secrets.token_hex(6)
        user_id = f"u-pd-{suffix}"
        script_id = f"s-pd-{suffix}"
        episode_id = f"e-pd-{suffix}"
        account_id = f"a-pd-{suffix}"
        reservation_id = f"r-pd-{suffix}"
        run_id = f"run-pd-{suffix}"
        job_id = f"j-pd-{suffix}"

        try:
            async with session_factory() as db:
                try:
                    db.add(
                        User(
                            user_id=user_id,
                            username=f"pipeline-debt-{suffix}",
                            password_hash="hash",
                            enabled=True,
                        )
                    )
                    db.add(
                        Script(
                            id=script_id,
                            title="PostgreSQL 流水线财务债务测试",
                            scope="personal",
                            owner_user_id=user_id,
                            created_by_user_id=user_id,
                        )
                    )
                    db.add(
                        CreditAccount(
                            id=account_id,
                            owner_type="user",
                            owner_id=user_id,
                            balance_points=10,
                            frozen_points=1,
                            status="active",
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
                    db.add(
                        CreditReservation(
                            id=reservation_id,
                            account_id=account_id,
                            source_type="pipeline",
                            source_id=run_id,
                            reserved_points=1,
                            status="reserved",
                        )
                    )
                    await db.flush()
                    db.add(
                        PipelineJob(
                            id=job_id,
                            script_id=script_id,
                            episode_id=episode_id,
                            stage="merge",
                            dispatch_id=f"dispatch-{suffix}",
                            run_id=run_id,
                            status="done",
                            input_version=1,
                            attempt=1,
                        )
                    )
                    await db.commit()

                    await script_crud.get_script_for_update(db, script_id)
                    await script_crud.get_episode_for_update(
                        db,
                        script_id,
                        episode_id,
                    )
                    summary = await script_crud.get_episode_dependency_summary(
                        db,
                        script_id,
                        episode_id,
                    )
                    assert summary.active_job_count == 0

                    async with session_factory() as writer:
                        await writer.execute(
                            update(PipelineJob)
                            .where(PipelineJob.id == job_id)
                            .values(
                                status="failed",
                                error_type="provider_timeout",
                                error_message="上游服务超时",
                                retryable=False,
                            )
                        )
                        await writer.commit()

                    with pytest.raises(ConflictError, match="财务清理债务"):
                        await script_crud.delete_episode(
                            db,
                            script_id,
                            episode_id,
                            commit=False,
                        )
                    await db.rollback()

                    assert await db.get(Episode, episode_id) is not None
                    assert await db.get(PipelineJob, job_id) is not None
                    locked_reservation = await db.get(
                        CreditReservation,
                        reservation_id,
                        populate_existing=True,
                    )
                    assert locked_reservation is not None
                    assert locked_reservation.status == "reserved"
                finally:
                    await db.rollback()
                    await db.execute(
                        delete(PipelineJob).where(PipelineJob.script_id == script_id)
                    )
                    await db.execute(
                        delete(Episode).where(Episode.episode_id == episode_id)
                    )
                    await db.execute(delete(Script).where(Script.id == script_id))
                    await db.execute(
                        delete(CreditReservation).where(
                            CreditReservation.id == reservation_id
                        )
                    )
                    await db.execute(
                        delete(CreditAccount).where(CreditAccount.id == account_id)
                    )
                    await db.execute(delete(User).where(User.user_id == user_id))
                    await db.commit()
        finally:
            await engine.dispose()

    import asyncio

    asyncio.run(scenario())


def test_completed_promote_then_delete_enqueues_source_and_final_cleanup() -> None:
    """已运行的晋升先完成；删除随后等待到其完成态并清理源、目标对象。"""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        promote = MediaStorageJob(
            id="promote-1",
            idempotency_key="promote:asset-1",
            job_type="promote",
            source_key="upload_images/_tmp/material-1/asset.png",
            final_key="generate_images/material-1/asset.png",
            asset_id="asset-1",
            status="pending",
        )
        raw.add(promote)
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        async def scenario() -> None:
            claim = await media_storage_jobs.claim_job(
                db,
                promote.id,
                owner="test-worker",
            )
            assert claim is not None
            assert await media_storage_jobs.mark_succeeded(
                db,
                claim.id,
                claim.lease_token,
            )
            await db.commit()
            await script_crud._enqueue_storage_deletes(
                db,
                {promote.final_key},
                asset_ids_by_key={promote.final_key: "asset-1"},
            )
            await db.commit()

        import asyncio

        asyncio.run(scenario())

        raw.refresh(promote)
        assert promote.status == "succeeded"
        delete_keys = set(
            raw.scalars(
                select(MediaStorageJob.source_key).where(
                    MediaStorageJob.job_type == "delete"
                )
            )
        )
        assert delete_keys == {promote.source_key, promote.final_key}


def test_delete_cancels_pending_promote_and_prevents_later_claim() -> None:
    """删除先取得晋升行锁并取消任务，之后 worker 不得再领取该晋升。"""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        promote = MediaStorageJob(
            id="promote-1",
            idempotency_key="promote:asset-1",
            job_type="promote",
            source_key="upload_images/_tmp/material-1/asset.png",
            final_key="generate_images/material-1/asset.png",
            asset_id="asset-1",
            status="pending",
        )
        raw.add(promote)
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        async def scenario() -> None:
            await script_crud._enqueue_storage_deletes(
                db,
                {promote.final_key},
                asset_ids_by_key={promote.final_key: "asset-1"},
            )
            await db.commit()
            claim = await media_storage_jobs.claim_job(
                db,
                promote.id,
                owner="late-worker",
            )
            assert claim is None

        import asyncio

        asyncio.run(scenario())

        raw.refresh(promote)
        assert promote.status == "canceled"
        assert promote.completed_at is not None
        assert promote.lease_owner is None
        assert promote.lease_token is None
        assert promote.lease_until is None
        delete_keys = set(
            raw.scalars(
                select(MediaStorageJob.source_key).where(
                    MediaStorageJob.job_type == "delete"
                )
            )
        )
        assert delete_keys == {promote.source_key, promote.final_key}


def test_delete_filters_protected_source_returned_by_promote_cancel() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        source_key = "upload_images/_tmp/material-1/protected.png"
        final_key = "generate_images/material-1/deleted.png"
        promote = MediaStorageJob(
            id="promote-protected-source",
            idempotency_key="promote:protected-source",
            job_type="promote",
            source_key=source_key,
            final_key=final_key,
            asset_id="asset-protected-source",
            status="pending",
        )
        raw.add(promote)
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        async def scenario() -> None:
            await script_crud._enqueue_storage_deletes(
                db,
                {final_key},
                asset_ids_by_key={final_key: "asset-protected-source"},
                protected_storage_keys={source_key},
            )
            await db.commit()

        import asyncio

        asyncio.run(scenario())
        raw.refresh(promote)
        assert promote.status == "canceled"
        delete_keys = set(
            raw.scalars(
                select(MediaStorageJob.source_key).where(
                    MediaStorageJob.job_type == "delete"
                )
            )
        )
        assert delete_keys == {final_key}


async def test_running_promote_cancellation_uses_blocking_ordered_row_lock() -> None:
    """运行中任务的删除协调必须等待行锁，并在取得锁后清除租约。"""

    class CaptureDB:
        def __init__(self, row: MediaStorageJob) -> None:
            self.row = row
            self.statements: list[object] = []
            self.flushed = False

        async def execute(self, statement):
            self.statements.append(statement)
            return _RowsResult([self.row])

        async def flush(self) -> None:
            self.flushed = True

    now = datetime.now(UTC)
    promote = MediaStorageJob(
        id="promote-running",
        idempotency_key="promote:running",
        job_type="promote",
        source_key="upload_images/_tmp/material-1/running.png",
        final_key="generate_images/material-1/running.png",
        asset_id="asset-running",
        status="running",
        lease_owner="worker-1",
        lease_token="lease-1",
        lease_until=now,
        created_at=now,
        updated_at=now,
    )
    db = CaptureDB(promote)

    cleanup_keys = await media_storage_jobs.cancel_promotes_for_delete(
        db,
        asset_ids={"asset-running"},
        final_keys={"generate_images/material-1/running.png"},
    )

    sql = str(db.statements[0].compile(dialect=postgresql.dialect()))
    assert "FOR UPDATE" in sql
    assert "SKIP LOCKED" not in sql
    assert "ORDER BY media_storage_jobs.id ASC" in sql
    assert cleanup_keys == {promote.source_key, promote.final_key}
    assert promote.status == "canceled"
    assert promote.completed_at is not None
    assert promote.lease_owner is None
    assert promote.lease_token is None
    assert promote.lease_until is None
    assert db.flushed is True


async def test_promote_worker_commits_claim_before_io_and_result_afterward(monkeypatch) -> None:
    """worker 不得把领取事务的 Job 行锁带入存储 I/O。"""
    from app.workers import media_storage as media_worker

    events: list[str] = []
    claim = media_storage_jobs.MediaStorageJobClaim(
        id="promote-running",
        idempotency_key="promote:running",
        job_type="promote",
        source_key="upload_images/_tmp/material-1/running.png",
        final_key="generate_images/material-1/running.png",
        asset_id="asset-running",
        status="running",
        attempt=1,
        lease_token="lease-1",
    )

    async def claim_next_job(*_args, **_kwargs):
        events.append("claim_with_row_lock")
        return claim

    async def mark_succeeded(*_args, **_kwargs):
        events.append("mark_succeeded")
        return True

    class DB:
        async def commit(self) -> None:
            events.append("commit_releases_row_lock")

    class Storage:
        async def promote_if_absent(self, source_key: str, final_key: str) -> bool:
            assert (source_key, final_key) == (claim.source_key, claim.final_key)
            events.append("storage_promote")
            return True

    monkeypatch.setattr(media_storage_jobs, "claim_next_job", claim_next_job)
    monkeypatch.setattr(media_storage_jobs, "mark_succeeded", mark_succeeded)

    assert await media_worker.process_one(DB(), Storage(), owner="worker-1") == claim.id
    assert events == [
        "claim_with_row_lock",
        "commit_releases_row_lock",
        "storage_promote",
        "mark_succeeded",
        "commit_releases_row_lock",
    ]


def test_episode_delete_keeps_shared_subject_media_and_queues_single_episode_media(
    monkeypatch,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        raw.execute(text("PRAGMA foreign_keys=ON"))
        assert raw.scalar(text("PRAGMA foreign_keys")) == 1
        user = User(
            user_id="user-1",
            username="episode-delete-owner",
            password_hash="hash",
            enabled=True,
        )
        script = Script(
            id="script-1",
            title="分集删除测试",
            scope="personal",
            owner_user_id="user-1",
            created_by_user_id="user-1",
        )
        episode_one = Episode(
            episode_id="episode-1",
            script_id="script-1",
            episode_no=1,
            title="第一集",
            script_file_storage_key="upload_images/material-1/episode-1.txt",
        )
        episode_two = Episode(
            episode_id="episode-2",
            script_id="script-1",
            episode_no=2,
            title="第二集",
        )
        shared = Subject(
            id="subject-shared",
            script_id="script-1",
            type="character",
            name="跨集角色",
            episode_nos=[1, 2],
        )
        single = Subject(
            id="subject-single",
            script_id="script-1",
            type="prop",
            name="单集道具",
            episode_nos=[1],
        )
        deleted = Subject(
            id="subject-deleted",
            script_id="script-1",
            type="prop",
            name="待删除道具",
            episode_nos=[1],
        )
        shared_asset = AssetEntry(
            id="asset-shared",
            script_id="script-1",
            subject_id="subject-shared",
            kind="image",
            storage_key="generate_images/material-1/shared.png",
            mime_type="image/png",
            size_bytes=10,
            source="generate_images",
        )
        single_asset = AssetEntry(
            id="asset-single",
            script_id="script-1",
            subject_id="subject-single",
            kind="image",
            storage_key="generate_images/material-1/legacy-output.png",
            mime_type="image/png",
            size_bytes=10,
            source="generate_images",
        )
        deleted_asset = AssetEntry(
            id="asset-deleted",
            script_id="script-1",
            subject_id="subject-deleted",
            kind="image",
            storage_key="generate_images/material-1/deleted-asset.png",
            mime_type="image/png",
            size_bytes=10,
            source="generate_images",
        )
        material = Materials(id="material-1", name="画布", script_id=script.id)
        legacy_generation = GenerationJob(
            job_id="generation-legacy-single",
            owner_user_id="user-1",
            material_id=material.id,
            script_id=script.id,
            subject_id=single.id,
            episode_id=None,
            idempotency_key="generation-legacy-single",
            request_hash="1" * 64,
            input_version=1,
            attempt=1,
            job_type="generate_images",
            status="processing",
            progress=50,
            input_json={},
            output_json={
                "imageOutputs": [
                    {
                        "storageKey": (
                            "generate_images/material-1/legacy-output.png"
                        )
                    }
                ]
            },
            metrics={
                "archived_output": {
                    "storageKey": "generate_images/material-1/legacy-metrics.png"
                },
                "retained_job_output": {
                    "storageKey": "generate_images/material-1/retained-job.png"
                },
            },
        )
        deleted_legacy_generation = GenerationJob(
            job_id="generation-legacy-deleted",
            owner_user_id="user-1",
            material_id=material.id,
            script_id=script.id,
            subject_id=deleted.id,
            episode_id=None,
            idempotency_key="generation-legacy-deleted",
            request_hash="4" * 64,
            input_version=1,
            attempt=1,
            job_type="generate_images",
            status="processing",
            progress=50,
            input_json={},
            output_json={
                "storageKey": "generate_images/material-1/deleted-output.png"
            },
            metrics={
                "storageKey": "generate_images/material-1/deleted-metrics.png"
            },
        )
        legacy_dispatch = TaskDispatchOutbox(
            id="dispatch-generation-legacy-single",
            task_type="generation.generate_images",
            job_id=legacy_generation.job_id,
            attempt=legacy_generation.attempt,
            payload={"job_id": legacy_generation.job_id},
            status="pending",
        )
        episode_one_generation = GenerationJob(
            job_id="generation-episode-one-shared-subject",
            owner_user_id="user-1",
            material_id=material.id,
            script_id=script.id,
            subject_id=shared.id,
            episode_id=episode_one.episode_id,
            idempotency_key="generation-episode-one-shared-subject",
            request_hash="3" * 64,
            input_version=1,
            attempt=1,
            job_type="generate_images",
            status="succeeded",
            progress=100,
            input_json={},
            output_json={"storageKey": shared_asset.storage_key},
        )
        episode_two_generation = GenerationJob(
            job_id="generation-episode-two-single-subject",
            owner_user_id="user-1",
            material_id=material.id,
            script_id=script.id,
            subject_id=single.id,
            episode_id=episode_two.episode_id,
            idempotency_key="generation-episode-two-single-subject",
            request_hash="2" * 64,
            input_version=1,
            attempt=1,
            job_type="generate_images",
            status="succeeded",
            progress=100,
            input_json={},
            output_json={
                "imageOutputs": [
                    {
                        "storageKey": "generate_images/material-1/retained-job.png"
                    }
                ]
            },
        )
        episode_two_dispatch = TaskDispatchOutbox(
            id="dispatch-generation-episode-two-single-subject",
            task_type="generation.generate_images",
            job_id=episode_two_generation.job_id,
            attempt=episode_two_generation.attempt,
            payload={"job_id": episode_two_generation.job_id},
            status="pending",
        )
        raw.add_all(
            [
                user,
                script,
                episode_one,
                episode_two,
                shared,
                single,
                deleted,
                material,
                shared_asset,
                single_asset,
                deleted_asset,
                legacy_generation,
                deleted_legacy_generation,
                legacy_dispatch,
                episode_one_generation,
                episode_two_generation,
                episode_two_dispatch,
            ]
        )
        raw.commit()
        db = _AsyncSessionAdapter(raw)
        monkeypatch.setattr(scripts, "check_script_access", AsyncMock())
        audit = AsyncMock()
        monkeypatch.setattr(scripts, "audit_log", audit)

        import asyncio

        async def scenario() -> LifecycleDependencySummary:
            with pytest.raises(ConflictError, match="仍有 1 个活动"):
                await scripts.delete_episode(
                    "script-1",
                    "episode-1",
                    confirm=True,
                    db=db,
                    user=user,
                )
            await db.rollback()

            deleted_job = raw.get(GenerationJob, deleted_legacy_generation.job_id)
            assert deleted_job is not None
            deleted_job.status = "succeeded"
            deleted_job.progress = 100
            raw.commit()

            result = await scripts.delete_episode(
                "script-1",
                "episode-1",
                confirm=True,
                db=db,
                user=user,
            )
            return result.dependencies

        summary = asyncio.run(scenario())
        assert summary.subject_count == 3
        assert summary.asset_count == 1
        assert summary.job_count == 2
        assert summary.active_job_count == 0
        audit.assert_awaited_once()

        queued = set(raw.scalars(select(MediaStorageJob.source_key)))
        assert queued == {
            "upload_images/material-1/episode-1.txt",
            "generate_images/material-1/deleted-asset.png",
            "generate_images/material-1/deleted-output.png",
            "generate_images/material-1/deleted-metrics.png",
        }
        assert raw.get(Episode, "episode-1") is None
        assert raw.get(Episode, "episode-2") is not None
        preserved = raw.get(Subject, "subject-shared")
        assert preserved is not None and preserved.episode_nos == [2]
        assert raw.get(AssetEntry, "asset-shared") is not None
        archived = raw.get(Subject, "subject-single")
        assert archived is not None
        assert archived.episode_nos == []
        assert archived.is_active is False
        assert raw.get(AssetEntry, "asset-single") is not None
        assert raw.get(GenerationJob, legacy_generation.job_id) is not None
        assert raw.get(TaskDispatchOutbox, legacy_dispatch.id).status == "pending"
        assert raw.get(Subject, deleted.id) is None
        assert raw.get(AssetEntry, deleted_asset.id) is None
        assert raw.get(GenerationJob, deleted_legacy_generation.job_id) is None
        assert raw.get(GenerationJob, episode_one_generation.job_id) is None
        assert raw.get(GenerationJob, episode_two_generation.job_id) is not None
        assert raw.get(TaskDispatchOutbox, episode_two_dispatch.id).status == "pending"


def test_episode_delete_preserves_referenced_pending_promote_and_cleans_orphan() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
        raw.execute(text("PRAGMA foreign_keys=ON"))
        user = User(
            user_id="user-promote-retain",
            username="promote-retain",
            password_hash="hash",
            enabled=True,
        )
        script = Script(
            id="script-promote-retain",
            title="共享晋升保留测试",
            scope="personal",
            owner_user_id=user.user_id,
            created_by_user_id=user.user_id,
        )
        deleting_episode = Episode(
            episode_id="episode-promote-delete",
            script_id=script.id,
            episode_no=1,
            title="第一集",
        )
        retained_episode = Episode(
            episode_id="episode-promote-retain",
            script_id=script.id,
            episode_no=2,
            title="第二集",
        )
        deleting_subject = Subject(
            id="subject-promote-delete",
            script_id=script.id,
            type="prop",
            name="待删除道具",
            episode_nos=[1],
        )
        retained_subject = Subject(
            id="subject-promote-retain",
            script_id=script.id,
            type="prop",
            name="保留道具",
            episode_nos=[2],
        )
        external_script = Script(
            id="script-promote-external",
            title="外部保留剧本",
            scope="personal",
            owner_user_id=user.user_id,
            created_by_user_id=user.user_id,
        )
        external_episode = Episode(
            episode_id="episode-promote-external",
            script_id=external_script.id,
            episode_no=1,
            title="外部保留分集",
        )
        external_subject = Subject(
            id="subject-promote-external",
            script_id=external_script.id,
            type="prop",
            name="外部保留道具",
            episode_nos=[1],
        )
        material = Materials(
            id="material-promote-retain",
            script_id=script.id,
            name="画布",
        )
        external_material = Materials(
            id="material-promote-external",
            script_id=external_script.id,
            name="外部画布",
        )
        protected_final = "upload_images/material-promote-retain/shared.png"
        protected_temp = "upload_images/_tmp/material-promote-retain/shared.png"
        orphan_final = "upload_images/material-promote-retain/orphan.png"
        orphan_temp = "upload_images/_tmp/material-promote-retain/orphan.png"
        protected_asset = AssetEntry(
            id="asset-promote-protected",
            script_id=script.id,
            subject_id=deleting_subject.id,
            kind="image",
            storage_key=f"/{protected_final}",
            mime_type="image/png",
            size_bytes=4,
            source="upload_images",
        )
        orphan_asset = AssetEntry(
            id="asset-promote-orphan",
            script_id=script.id,
            subject_id=deleting_subject.id,
            kind="image",
            storage_key=f"/{orphan_final}",
            mime_type="image/png",
            size_bytes=4,
            source="upload_images",
        )
        protected_upload = MediaUpload(
            id="upload-promote-protected",
            script_id=script.id,
            material_id=material.id,
            subject_id=deleting_subject.id,
            asset_id=protected_asset.id,
            temp_storage_key=protected_temp,
            final_storage_key=protected_final,
            original_filename="shared.png",
            mime_type="image/png",
            size_bytes=4,
            sha256="a" * 64,
            status="staging",
        )
        orphan_upload = MediaUpload(
            id="upload-promote-orphan",
            script_id=script.id,
            material_id=material.id,
            subject_id=deleting_subject.id,
            asset_id=orphan_asset.id,
            temp_storage_key=orphan_temp,
            final_storage_key=orphan_final,
            original_filename="orphan.png",
            mime_type="image/png",
            size_bytes=4,
            sha256="b" * 64,
            status="staging",
        )
        protected_promote = MediaStorageJob(
            id="promote-protected",
            idempotency_key="upload-promote:upload-promote-protected",
            job_type="promote",
            source_key=protected_temp,
            final_key=protected_final,
            asset_id=protected_asset.id,
            upload_id=protected_upload.id,
            status="pending",
        )
        orphan_promote = MediaStorageJob(
            id="promote-orphan",
            idempotency_key="upload-promote:upload-promote-orphan",
            job_type="promote",
            source_key=orphan_temp,
            final_key=orphan_final,
            asset_id=orphan_asset.id,
            upload_id=orphan_upload.id,
            status="pending",
        )
        retained_job = GenerationJob(
            job_id="generation-promote-retain",
            owner_user_id=user.user_id,
            material_id=external_material.id,
            script_id=external_script.id,
            subject_id=external_subject.id,
            episode_id=external_episode.episode_id,
            idempotency_key="generation-promote-retain",
            request_hash="c" * 64,
            input_version=1,
            attempt=1,
            job_type="generate_images",
            status="succeeded",
            progress=100,
            input_json={},
            output_json=None,
            metrics={"retained_storage": {"storage_key": protected_final}},
        )
        raw.add_all([user, script, external_script])
        raw.flush()
        raw.add_all(
            [
                deleting_episode,
                retained_episode,
                external_episode,
                deleting_subject,
                retained_subject,
                external_subject,
                material,
                external_material,
            ]
        )
        raw.flush()
        raw.add_all(
            [
                protected_asset,
                orphan_asset,
                retained_job,
            ]
        )
        raw.flush()
        raw.add_all(
            [
                protected_upload,
                orphan_upload,
            ]
        )
        raw.flush()
        raw.add_all(
            [
                protected_promote,
                orphan_promote,
            ]
        )
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        import asyncio

        async def scenario() -> None:
            await script_crud.delete_episode(
                db,
                script.id,
                deleting_episode.episode_id,
            )
            raw.refresh(protected_upload)
            raw.refresh(orphan_upload)
            raw.refresh(protected_promote)
            raw.refresh(orphan_promote)

            assert raw.get(Subject, deleting_subject.id) is None
            assert raw.get(AssetEntry, protected_asset.id) is None
            assert raw.get(AssetEntry, orphan_asset.id) is None
            assert protected_upload.status == "staging"
            assert protected_upload.subject_id is None
            assert protected_upload.asset_id is None
            assert protected_promote.status == "pending"
            assert orphan_upload.status == "deleted"
            assert orphan_promote.status == "canceled"

            delete_keys = set(
                raw.scalars(
                    select(MediaStorageJob.source_key).where(
                        MediaStorageJob.job_type == "delete"
                    )
                )
            )
            assert {orphan_temp, orphan_final} <= delete_keys
            assert {protected_temp, protected_final}.isdisjoint(delete_keys)

            claim = await media_storage_jobs.claim_job(
                db,
                protected_promote.id,
                owner="late-promote-worker",
            )
            assert claim is not None
            await db.commit()
            asset_id = await media_storage_jobs.finalize_upload_ready(db, claim)
            assert asset_id is None
            assert await media_storage_jobs.mark_succeeded(
                db,
                claim.id,
                claim.lease_token,
                asset_id=asset_id,
            )
            await db.commit()

        asyncio.run(scenario())

        raw.refresh(protected_upload)
        raw.refresh(protected_promote)
        assert protected_upload.status == "ready"
        assert protected_promote.status == "succeeded"
        assert raw.get(GenerationJob, retained_job.job_id) is not None
        assert retained_job.metrics == {
            "retained_storage": {"storage_key": protected_final}
        }


def test_episode_delete_generation_scans_do_not_scale_with_subject_count() -> None:
    async def run_case(subject_count: int) -> int:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        Base.metadata.create_all(engine)
        with Session(engine, expire_on_commit=False) as raw:
            user = User(
                user_id=f"user-scan-{subject_count}",
                username=f"scan-{subject_count}",
                password_hash="unused",
            )
            script = Script(
                id=f"script-scan-{subject_count}",
                title="批量扫描测试",
                scope="personal",
                owner_user_id=user.user_id,
                created_by_user_id=user.user_id,
            )
            episode = Episode(
                episode_id=f"episode-scan-{subject_count}",
                script_id=script.id,
                episode_no=1,
                title="第一集",
            )
            material = Materials(
                id=f"material-scan-{subject_count}",
                script_id=script.id,
                name="画布",
            )
            raw.add_all([user, script, episode, material])
            raw.flush()
            subjects = [
                Subject(
                    id=f"subject-scan-{subject_count}-{index}",
                    script_id=script.id,
                    type="prop",
                    name=f"道具-{index}",
                    episode_nos=[1],
                )
                for index in range(subject_count)
            ]
            raw.add_all(subjects)
            raw.flush()
            raw.add_all(
                [
                    GenerationJob(
                        job_id=f"generation-scan-{subject_count}-{index}",
                        owner_user_id=user.user_id,
                        material_id=material.id,
                        script_id=script.id,
                        subject_id=subject.id,
                        episode_id=episode.episode_id,
                        idempotency_key=f"generation-scan-{subject_count}-{index}",
                        request_hash=f"{index:x}".rjust(64, "0"),
                        input_version=1,
                        attempt=1,
                        job_type="generate_images",
                        status="succeeded",
                        progress=100,
                        input_json={},
                    )
                    for index, subject in enumerate(subjects)
                ]
            )
            raw.commit()

            class CountingAdapter(_AsyncSessionAdapter):
                def __init__(self, session: Session) -> None:
                    super().__init__(session)
                    self.generation_select_count = 0

                async def execute(self, statement):
                    if getattr(statement, "is_select", False):
                        sql = str(statement.compile(dialect=sqlite.dialect())).lower()
                        if "generation_jobs" in sql:
                            self.generation_select_count += 1
                    return await super().execute(statement)

            db = CountingAdapter(raw)
            await script_crud.delete_episode(
                db,
                script.id,
                episode.episode_id,
            )
            return db.generation_select_count

    import asyncio

    single_count = asyncio.run(run_case(1))
    batch_count = asyncio.run(run_case(4))
    assert single_count == batch_count


@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过跨集图片任务主体外键测试",
)
def test_postgres_episode_delete_preserves_cross_episode_generation_subject_fk() -> None:
    async def scenario() -> None:
        database_url = os.environ["MTV_TEST_POSTGRES_URL"]
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        suffix = secrets.token_hex(6)
        user_id = f"user-xgen-{suffix}"
        script_id = f"script-xgen-{suffix}"
        episode_one_id = f"ep1-xgen-{suffix}"
        episode_two_id = f"ep2-xgen-{suffix}"
        subject_id = f"subject-xgen-{suffix}"
        deleted_subject_id = f"subject-deleted-xgen-{suffix}"
        material_id = f"material-xgen-{suffix}"
        asset_id = f"asset-xgen-{suffix}"
        deleted_asset_id = f"asset-deleted-xgen-{suffix}"
        legacy_job_id = f"legacy-xgen-{suffix}"
        deleted_job_id = f"legacy-deleted-xgen-{suffix}"
        cross_job_id = f"cross-xgen-{suffix}"
        dispatch_id = f"dispatch-xgen-{suffix}"
        legacy_dispatch_id = f"dispatch-legacy-xgen-{suffix}"
        episode_key = f"upload_images/{suffix}/episode-one.txt"
        asset_key = f"generate_images/{suffix}/subject.png"
        legacy_metrics_key = f"generate_images/{suffix}/legacy-metrics.png"
        cross_key = f"generate_images/{suffix}/cross.png"
        deleted_asset_key = f"generate_images/{suffix}/deleted-asset.png"
        deleted_output_key = f"generate_images/{suffix}/deleted-output.png"
        deleted_metrics_key = f"generate_images/{suffix}/deleted-metrics.png"
        storage_keys = {
            episode_key,
            asset_key,
            legacy_metrics_key,
            cross_key,
            deleted_asset_key,
            deleted_output_key,
            deleted_metrics_key,
        }

        try:
            async with session_factory() as setup:
                setup.add_all(
                    [
                        User(
                            user_id=user_id,
                            username=f"xgen-{suffix}",
                            password_hash="hash",
                            enabled=True,
                        ),
                        Script(
                            id=script_id,
                            title="跨集图片主体外键测试",
                            scope="personal",
                            owner_user_id=user_id,
                            created_by_user_id=user_id,
                        ),
                    ]
                )
                await setup.flush()
                setup.add_all(
                    [
                        Episode(
                            episode_id=episode_one_id,
                            script_id=script_id,
                            episode_no=1,
                            title="第一集",
                            script_file_storage_key=episode_key,
                        ),
                        Episode(
                            episode_id=episode_two_id,
                            script_id=script_id,
                            episode_no=2,
                            title="第二集",
                        ),
                        Subject(
                            id=subject_id,
                            script_id=script_id,
                            type="prop",
                            name="跨集任务引用道具",
                            episode_nos=[1],
                        ),
                        Subject(
                            id=deleted_subject_id,
                            script_id=script_id,
                            type="prop",
                            name="真正删除的单集道具",
                            episode_nos=[1],
                        ),
                        Materials(
                            id=material_id,
                            script_id=script_id,
                            name="画布",
                        ),
                    ]
                )
                await setup.flush()
                setup.add_all(
                    [
                        AssetEntry(
                            id=asset_id,
                            script_id=script_id,
                            subject_id=subject_id,
                            kind="image",
                            storage_key=asset_key,
                            mime_type="image/png",
                            size_bytes=10,
                            source="generate_images",
                        ),
                        AssetEntry(
                            id=deleted_asset_id,
                            script_id=script_id,
                            subject_id=deleted_subject_id,
                            kind="image",
                            storage_key=deleted_asset_key,
                            mime_type="image/png",
                            size_bytes=10,
                            source="generate_images",
                        ),
                        GenerationJob(
                            job_id=legacy_job_id,
                            owner_user_id=user_id,
                            material_id=material_id,
                            script_id=script_id,
                            subject_id=subject_id,
                            episode_id=None,
                            idempotency_key=f"legacy-{suffix}",
                            request_hash="1" * 64,
                            input_version=1,
                            attempt=1,
                            job_type="generate_images",
                            status="processing",
                            progress=50,
                            input_json={},
                            output_json={"storage_key": asset_key},
                            metrics={
                                "storage_key": legacy_metrics_key,
                                "retained_job_output": {"storage_key": cross_key},
                            },
                        ),
                        GenerationJob(
                            job_id=deleted_job_id,
                            owner_user_id=user_id,
                            material_id=material_id,
                            script_id=script_id,
                            subject_id=deleted_subject_id,
                            episode_id=None,
                            idempotency_key=f"deleted-{suffix}",
                            request_hash="3" * 64,
                            input_version=1,
                            attempt=1,
                            job_type="generate_images",
                            status="succeeded",
                            progress=100,
                            input_json={},
                            output_json={"storage_key": deleted_output_key},
                            metrics={"storage_key": deleted_metrics_key},
                        ),
                        GenerationJob(
                            job_id=cross_job_id,
                            owner_user_id=user_id,
                            material_id=material_id,
                            script_id=script_id,
                            subject_id=subject_id,
                            episode_id=episode_two_id,
                            idempotency_key=f"cross-{suffix}",
                            request_hash="2" * 64,
                            input_version=1,
                            attempt=1,
                            job_type="generate_images",
                            status="succeeded",
                            progress=100,
                            input_json={},
                            output_json={"storage_key": cross_key},
                        ),
                        TaskDispatchOutbox(
                            id=dispatch_id,
                            task_type="generation.generate_images",
                            job_id=cross_job_id,
                            attempt=1,
                            payload={"job_id": cross_job_id},
                            status="pending",
                        ),
                        TaskDispatchOutbox(
                            id=legacy_dispatch_id,
                            task_type="generation.generate_images",
                            job_id=legacy_job_id,
                            attempt=1,
                            payload={"job_id": legacy_job_id},
                            status="pending",
                        ),
                    ]
                )
                await setup.commit()

            async with session_factory() as precheck:
                summary = await script_crud.get_episode_dependency_summary(
                    precheck,
                    script_id,
                    episode_one_id,
                )
                assert summary.subject_count == 2
                assert summary.asset_count == 1
                assert summary.job_count == 1
                assert summary.active_job_count == 0

            async with session_factory() as deleting:
                await script_crud.delete_episode(
                    deleting,
                    script_id,
                    episode_one_id,
                )

            async with session_factory() as verification:
                assert await verification.get(Episode, episode_one_id) is None
                assert await verification.get(Episode, episode_two_id) is not None
                archived = await verification.get(Subject, subject_id)
                assert archived is not None
                assert archived.episode_nos == []
                assert archived.is_active is False
                assert await verification.get(AssetEntry, asset_id) is not None
                assert await verification.get(GenerationJob, legacy_job_id) is not None
                assert await verification.get(GenerationJob, cross_job_id) is not None
                assert await verification.get(Subject, deleted_subject_id) is None
                assert await verification.get(AssetEntry, deleted_asset_id) is None
                assert await verification.get(GenerationJob, deleted_job_id) is None
                dispatch = await verification.get(TaskDispatchOutbox, dispatch_id)
                assert dispatch is not None and dispatch.status == "pending"
                legacy_dispatch = await verification.get(
                    TaskDispatchOutbox,
                    legacy_dispatch_id,
                )
                assert legacy_dispatch is not None and legacy_dispatch.status == "pending"
                queued = set(
                    (
                        await verification.execute(
                            select(MediaStorageJob.source_key).where(
                                MediaStorageJob.source_key.in_(storage_keys)
                            )
                        )
                    ).scalars()
                )
                assert queued == {
                    episode_key,
                    deleted_asset_key,
                    deleted_output_key,
                    deleted_metrics_key,
                }
        finally:
            async with session_factory() as cleanup:
                await cleanup.execute(
                    delete(MediaStorageJob).where(
                        MediaStorageJob.source_key.in_(storage_keys)
                    )
                )
                await cleanup.execute(
                    delete(TaskDispatchOutbox).where(
                        TaskDispatchOutbox.id.in_({dispatch_id, legacy_dispatch_id})
                    )
                )
                await cleanup.execute(
                    delete(GenerationJob).where(GenerationJob.script_id == script_id)
                )
                await cleanup.execute(
                    delete(AssetEntry).where(AssetEntry.script_id == script_id)
                )
                await cleanup.execute(
                    delete(Subject).where(Subject.script_id == script_id)
                )
                await cleanup.execute(
                    delete(Materials).where(Materials.script_id == script_id)
                )
                await cleanup.execute(
                    delete(Episode).where(Episode.script_id == script_id)
                )
                await cleanup.execute(delete(Script).where(Script.id == script_id))
                await cleanup.execute(delete(User).where(User.user_id == user_id))
                await cleanup.commit()
            await engine.dispose()

    import asyncio

    asyncio.run(scenario())
