"""B1/B2 剧本与分集完整生命周期专项测试。"""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.api.v1 import scripts
from app.core.errors import ConflictError, ForbiddenError
from app.core.permissions import Action, check_script_access
from app.crud import script as script_crud
from app.models.asset_entry import AssetEntry
from app.models.base import Base
from app.models.episode import Episode
from app.models.media_storage_job import MediaStorageJob
from app.models.project_member import ProjectMember
from app.models.scene import Scene
from app.models.script import Script
from app.models.shot_group import ShotGroup
from app.models.subject import Subject
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
        raw.add_all([user, script, episode, subject, asset, group, video, render])
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        import asyncio

        asyncio.run(script_crud.delete_script(db, script))

        queued = set(raw.scalars(select(MediaStorageJob.source_key)))
        assert queued == {
            "upload_images/material-1/script.txt",
            "generate_images/material-1/asset.png",
            "generate_images/video/frame.png",
            "generate_images/video/video-1.mp4",
            "generate_outputs/script-1/episode-1/render-1.mp4",
        }
        assert raw.get(Script, "script-1") is None
        asset_task = raw.scalar(
            select(MediaStorageJob).where(
                MediaStorageJob.source_key == "generate_images/material-1/asset.png"
            )
        )
        assert asset_task is not None and asset_task.asset_id == "asset-1"


def test_completed_promote_then_delete_enqueues_source_and_final_cleanup() -> None:
    """已运行的晋升先完成；删除随后等待到其完成态并清理源、目标对象。"""
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[MediaStorageJob.__table__])
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
    Base.metadata.create_all(engine, tables=[MediaStorageJob.__table__])
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


async def test_promote_worker_commits_only_after_io_and_status_update(monkeypatch) -> None:
    """worker 的领取事务必须跨越存储 I/O，保证删除方会等待完成态。"""
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
        async def promote(self, source_key: str, final_key: str) -> str:
            assert (source_key, final_key) == (claim.source_key, claim.final_key)
            events.append("storage_promote")
            return final_key

    monkeypatch.setattr(media_storage_jobs, "claim_next_job", claim_next_job)
    monkeypatch.setattr(media_storage_jobs, "mark_succeeded", mark_succeeded)

    assert await media_worker.process_one(DB(), Storage(), owner="worker-1") == claim.id
    assert events == [
        "claim_with_row_lock",
        "storage_promote",
        "mark_succeeded",
        "commit_releases_row_lock",
    ]


def test_episode_delete_keeps_shared_subject_media_and_queues_single_episode_media() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as raw:
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
            storage_key="generate_images/material-1/single.png",
            mime_type="image/png",
            size_bytes=10,
            source="generate_images",
        )
        raw.add_all(
            [
                script,
                episode_one,
                episode_two,
                shared,
                single,
                shared_asset,
                single_asset,
            ]
        )
        raw.commit()
        db = _AsyncSessionAdapter(raw)

        import asyncio

        asyncio.run(script_crud.delete_episode(db, "script-1", "episode-1"))

        queued = set(raw.scalars(select(MediaStorageJob.source_key)))
        assert queued == {
            "upload_images/material-1/episode-1.txt",
            "generate_images/material-1/single.png",
        }
        assert raw.get(Episode, "episode-1") is None
        assert raw.get(Episode, "episode-2") is not None
        preserved = raw.get(Subject, "subject-shared")
        assert preserved is not None and preserved.episode_nos == [2]
        assert raw.get(AssetEntry, "asset-shared") is not None
        assert raw.get(Subject, "subject-single") is None
