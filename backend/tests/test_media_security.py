"""跨模块生成产物的媒体代理安全契约。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.schema import CreateIndex

from app.api.v1 import media
from app.core.errors import ForbiddenError, NotFoundError


class _Result:
    def __init__(self, value: object) -> None:
        self.value = value

    def scalar_one_or_none(self) -> object:
        return self.value


async def test_video_media_authorizes_video_job_by_exact_json_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = SimpleNamespace(execute=AsyncMock(return_value=_Result("script-1")))
    user = SimpleNamespace()
    authorize = AsyncMock()
    monkeypatch.setattr(media, "_authorize_script", authorize)

    await media._authorize_video_media_key(
        db,
        "generate_images/video/video-job-1.mp4",
        user,
        False,
    )

    authorize.assert_awaited_once_with(db, "script-1", user, False)
    assert db.execute.await_count == 1


@pytest.mark.parametrize("frame_field", ["first_frame", "last_frame", "reference_frame"])
async def test_video_media_authorizes_each_shot_group_frame_field(
    monkeypatch: pytest.MonkeyPatch,
    frame_field: str,
) -> None:
    db = SimpleNamespace(
        execute=AsyncMock(side_effect=[_Result(None), _Result("script-1")])
    )
    user = SimpleNamespace()
    authorize = AsyncMock()
    monkeypatch.setattr(media, "_authorize_script", authorize)

    await media._authorize_video_media_key(
        db,
        f"generate_images/video/frames/group-1_{frame_field}.png",
        user,
        False,
    )

    authorize.assert_awaited_once_with(db, "script-1", user, False)
    assert db.execute.await_count == 2


async def test_video_media_propagates_script_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = SimpleNamespace(execute=AsyncMock(return_value=_Result("script-1")))
    denied = ForbiddenError("无权访问剧本")
    authorize = AsyncMock(side_effect=denied)
    monkeypatch.setattr(media, "_authorize_script", authorize)

    with pytest.raises(ForbiddenError) as exc_info:
        await media._authorize_video_media_key(
            db,
            "generate_images/video/video-job-1.mp4",
            SimpleNamespace(),
            False,
        )

    assert exc_info.value is denied


async def test_video_media_rejects_unknown_key_with_bounded_json_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = SimpleNamespace(execute=AsyncMock(side_effect=[_Result(None), _Result(None)]))
    authorize = AsyncMock()
    monkeypatch.setattr(media, "_authorize_script", authorize)

    with pytest.raises(NotFoundError, match="媒体资源不存在"):
        await media._authorize_video_media_key(
            db,
            "generate_images/video/not-found.mp4",
            SimpleNamespace(),
            False,
        )

    authorize.assert_not_awaited()
    assert db.execute.await_count == 2
    video_statement = db.execute.await_args_list[0].args[0]
    shot_group_statement = db.execute.await_args_list[1].args[0]

    video_sqlite_sql = str(
        video_statement.compile(
            dialect=sqlite.dialect(), compile_kwargs={"render_postcompile": True}
        )
    )
    shot_group_sqlite_sql = str(
        shot_group_statement.compile(
            dialect=sqlite.dialect(), compile_kwargs={"render_postcompile": True}
        )
    )
    video_postgresql_sql = str(
        video_statement.compile(
            dialect=postgresql.dialect(), compile_kwargs={"render_postcompile": True}
        )
    )
    shot_group_postgresql_sql = str(
        shot_group_statement.compile(
            dialect=postgresql.dialect(), compile_kwargs={"render_postcompile": True}
        )
    )
    video_literal_sql = str(
        video_statement.compile(
            dialect=sqlite.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    shot_group_literal_sql = str(
        shot_group_statement.compile(
            dialect=sqlite.dialect(), compile_kwargs={"literal_binds": True}
        )
    )

    assert "SELECT video_generation_jobs.script_id" in video_sqlite_sql
    assert (
        "JSON_EXTRACT(video_generation_jobs.output_json, '$.\"video_storage_key\"')"
        in video_sqlite_sql
    )
    assert "LIMIT 1" in video_literal_sql
    assert "SELECT shot_groups.script_id" in shot_group_sqlite_sql
    for frame_field in ("first_frame", "last_frame", "reference_frame"):
        assert (
            f"JSON_EXTRACT(shot_groups.reference_frames, '$.\"{frame_field}\"')"
            in shot_group_sqlite_sql
        )
    assert "LIMIT 1" in shot_group_literal_sql
    assert "video_generation_jobs.output_json ->> 'video_storage_key'" in video_postgresql_sql
    for frame_field in ("first_frame", "last_frame", "reference_frame"):
        assert f"shot_groups.reference_frames ->> '{frame_field}'" in shot_group_postgresql_sql


def test_video_media_json_lookup_indexes_cover_fixed_fields() -> None:
    video_indexes = {index.name: index for index in media.VideoGenerationJob.__table__.indexes}
    shot_group_indexes = {index.name: index for index in media.ShotGroup.__table__.indexes}
    expected_video_index = "ix_video_generation_jobs_output_video_storage_key"
    expected_shot_group_indexes = {
        "ix_shot_groups_reference_frames_first_frame": "first_frame",
        "ix_shot_groups_reference_frames_last_frame": "last_frame",
        "ix_shot_groups_reference_frames_reference_frame": "reference_frame",
    }

    assert expected_video_index in video_indexes
    assert expected_shot_group_indexes.keys() <= shot_group_indexes.keys()

    video_postgresql_ddl = str(
        CreateIndex(video_indexes[expected_video_index]).compile(
            dialect=postgresql.dialect()
        )
    )
    video_sqlite_ddl = str(
        CreateIndex(video_indexes[expected_video_index]).compile(dialect=sqlite.dialect())
    )
    assert "output_json ->> 'video_storage_key'" in video_postgresql_ddl
    assert "JSON_EXTRACT(output_json" in video_sqlite_ddl

    for index_name, frame_field in expected_shot_group_indexes.items():
        postgresql_ddl = str(
            CreateIndex(shot_group_indexes[index_name]).compile(
                dialect=postgresql.dialect()
            )
        )
        sqlite_ddl = str(
            CreateIndex(shot_group_indexes[index_name]).compile(dialect=sqlite.dialect())
        )
        assert f"reference_frames ->> '{frame_field}'" in postgresql_ddl
        assert "JSON_EXTRACT(reference_frames" in sqlite_ddl


@pytest.mark.parametrize("status", ["starting", "processing", "failed", "canceled"])
async def test_timeline_media_rejects_every_non_succeeded_render(
    monkeypatch: pytest.MonkeyPatch, status: str
) -> None:
    job = SimpleNamespace(
        status=status,
        script_id="script-1",
        output_storage_key="generate_outputs/script-1/episode-1/render-1.mp4",
    )
    db = SimpleNamespace(execute=AsyncMock(return_value=_Result(job)))
    authorize = AsyncMock()
    monkeypatch.setattr(media, "_authorize_script", authorize)

    with pytest.raises(NotFoundError, match="媒体资源不存在"):
        await media._authorize_timeline_media_key(
            db,
            "generate_outputs/script-1/episode-1/render-1.mp4",
            SimpleNamespace(),
            False,
        )

    authorize.assert_not_awaited()


async def test_timeline_media_authorizes_succeeded_render(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = SimpleNamespace(
        status="succeeded",
        script_id="script-1",
        output_storage_key="generate_outputs/script-1/episode-1/render-1.mp4",
    )
    db = SimpleNamespace(execute=AsyncMock(return_value=_Result(job)))
    authorize = AsyncMock()
    monkeypatch.setattr(media, "_authorize_script", authorize)

    await media._authorize_timeline_media_key(
        db,
        "generate_outputs/script-1/episode-1/render-1.mp4",
        SimpleNamespace(),
        False,
    )

    authorize.assert_awaited_once()
