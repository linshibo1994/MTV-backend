"""T9 时间线 remediation 行为测试。

仅使用 fake / SQLite / mock，不连接真实 DB、Redis、ffmpeg、服务或网络。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from sqlalchemy import UniqueConstraint, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.core.errors import ConflictError
from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.script import Script
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.timeline_render_job import TimelineRenderJob
from app.models.user import User
from app.models.video_generation_job import VideoGenerationJob
from app.models.video_timeline_arrangement import VideoTimelineArrangement
from app.schemas.timeline import TimelineUpdateIn


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


class _AsyncNested:
    def __init__(self, transaction) -> None:
        self.transaction = transaction

    async def __aenter__(self):
        return self.transaction.__enter__()

    async def __aexit__(self, exc_type, exc, tb):
        return self.transaction.__exit__(exc_type, exc, tb)


class _AsyncSessionAdapter:
    def __init__(self, session: Session) -> None:
        self.session = session

    async def execute(self, statement):
        return self.session.execute(statement)

    async def get(self, *args, **kwargs):
        return self.session.get(*args, **kwargs)

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

    def begin_nested(self):
        return _AsyncNested(self.session.begin_nested())


def _constraint_columns(model) -> list[set[str]]:
    return [
        {column.name for column in constraint.columns}
        for constraint in model.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    ]


def _make_db() -> tuple[object, Session]:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    for table in (
        User.__table__,
        Script.__table__,
        Episode.__table__,
        VideoGenerationJob.__table__,
        VideoTimelineArrangement.__table__,
        CreditAccount.__table__,
        CreditReservation.__table__,
        CreditLog.__table__,
        TimelineRenderJob.__table__,
        TaskDispatchOutbox.__table__,
    ):
        table.create(engine)  # type: ignore[attr-defined]
    raw = Session(engine, expire_on_commit=False)
    return engine, raw


def _seed(raw: Session) -> SimpleNamespace:
    owner = User(
        user_id="user-1",
        username="owner",
        password_hash="hash",
        enabled=True,
        account_type="personal",
    )
    other = User(
        user_id="user-2",
        username="other",
        password_hash="hash",
        enabled=True,
        account_type="personal",
    )
    script = Script(
        id="script-1",
        title="剧本",
        scope="personal",
        owner_user_id=owner.user_id,
        created_by_user_id=owner.user_id,
    )
    other_script = Script(
        id="script-2",
        title="其他剧本",
        scope="personal",
        owner_user_id=other.user_id,
        created_by_user_id=other.user_id,
    )
    episode = Episode(
        episode_id="episode-1",
        script_id=script.id,
        episode_no=1,
        title="第一集",
        episode_version=7,
    )
    other_episode = Episode(
        episode_id="episode-2",
        script_id=other_script.id,
        episode_no=1,
        title="其他第一集",
        episode_version=3,
    )
    video_a = VideoGenerationJob(
        job_id="video-1",
        script_id=script.id,
        episode_id=episode.episode_id,
        shot_group_id="shot-group-1",
        scene_no=1,
        unit_id="group:g1",
        unit_runtime_key="runtime:1:group:g1",
        provider_key="modelsai",
        job_type="all_reference",
        status="succeeded",
        progress=100,
        output_json={"video_storage_key": "generate_images/mat/video-1.mp4"},
    )
    video_b = VideoGenerationJob(
        job_id="video-2",
        script_id=script.id,
        episode_id=episode.episode_id,
        shot_group_id="shot-group-2",
        scene_no=1,
        unit_id="group:g2",
        unit_runtime_key="runtime:1:group:g2",
        provider_key="modelsai",
        job_type="all_reference",
        status="succeeded",
        progress=100,
        output_json={"storage_key": "generate_images/mat/video-2.mp4"},
    )
    other_video = VideoGenerationJob(
        job_id="video-other",
        script_id=other_script.id,
        episode_id=other_episode.episode_id,
        shot_group_id="shot-group-other",
        scene_no=1,
        unit_id="group:g3",
        unit_runtime_key="runtime:1:group:g3",
        provider_key="modelsai",
        job_type="all_reference",
        status="succeeded",
        progress=100,
        output_json={"storage_key": "generate_images/mat/video-other.mp4"},
    )
    account = CreditAccount(
        id="account-1",
        owner_type="user",
        owner_id=owner.user_id,
        balance_points=100,
        frozen_points=0,
        status="active",
    )
    raw.add_all([
        owner,
        other,
        script,
        other_script,
        episode,
        other_episode,
        video_a,
        video_b,
        other_video,
        account,
    ])
    raw.commit()
    return SimpleNamespace(
        owner=owner,
        other=other,
        script=script,
        other_script=other_script,
        episode=episode,
        other_episode=other_episode,
        video_a=video_a,
        video_b=video_b,
        other_video=other_video,
        account=account,
    )


def test_timeline_models_declare_constraints_and_render_job_contract() -> None:
    arrangement_columns = set(VideoTimelineArrangement.__table__.columns.keys())
    assert {
        "script_id",
        "episode_id",
        "job_id",
        "row_order",
        "trim_start_seconds",
        "trim_end_seconds",
        "version",
    } <= arrangement_columns
    assert {"script_id", "episode_id", "row_order"} in _constraint_columns(
        VideoTimelineArrangement
    )
    assert VideoTimelineArrangement.__table__.c.job_id.nullable is False

    job_columns = set(TimelineRenderJob.__table__.columns.keys())
    assert {
        "job_id",
        "script_id",
        "episode_id",
        "owner_user_id",
        "idempotency_key",
        "request_hash",
        "input_version",
        "attempt",
        "reservation_id",
        "dispatch_id",
        "lease_owner",
        "lease_token",
        "leased_until",
        "heartbeat_at",
        "started_at",
        "finished_at",
        "canceled_at",
        "error_type",
        "retryable",
        "output_storage_key",
    } <= job_columns
    assert {"owner_user_id", "idempotency_key"} in _constraint_columns(TimelineRenderJob)


async def test_patch_explicit_null_clears_trim_end_but_omitted_keeps_value() -> None:
    from app.crud import timeline as timeline_crud

    _, raw = _make_db()
    seed = _seed(raw)
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))
    arrangement = await timeline_crud.add_arrangement(
        db,
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
        unit_runtime_key=seed.video_a.unit_runtime_key,
        job_id=seed.video_a.job_id,
    )
    await timeline_crud.update_arrangement(
        db,
        arrangement,
        trim_start_seconds=1.0,
        trim_end_seconds=3.0,
        expected_version=0,
        fields_set={"trim_start_seconds", "trim_end_seconds"},
    )
    omitted = TimelineUpdateIn(is_selected=False, expected_version=1)
    arrangement = await timeline_crud.update_arrangement(
        db,
        arrangement,
        is_selected=omitted.is_selected,
        trim_end_seconds=omitted.trim_end_seconds,
        expected_version=omitted.expected_version,
        fields_set=omitted.model_fields_set,
    )
    assert arrangement.trim_end_seconds == 3.0

    explicit_null = TimelineUpdateIn(trim_end_seconds=None, expected_version=2)
    arrangement = await timeline_crud.update_arrangement(
        db,
        arrangement,
        trim_end_seconds=explicit_null.trim_end_seconds,
        expected_version=explicit_null.expected_version,
        fields_set=explicit_null.model_fields_set,
    )
    assert arrangement.trim_end_seconds is None


async def test_patch_requires_version_and_uses_db_cas() -> None:
    from app.crud import timeline as timeline_crud

    _, raw = _make_db()
    seed = _seed(raw)
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))
    arrangement = await timeline_crud.add_arrangement(
        db,
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
        unit_runtime_key=seed.video_a.unit_runtime_key,
        job_id=seed.video_a.job_id,
    )
    await db.commit()

    with pytest.raises(ConflictError):
        await timeline_crud.update_arrangement(
            db,
            arrangement,
            is_selected=False,
            expected_version=None,
            fields_set={"is_selected"},
        )

    raw.rollback()
    stale = raw.get(VideoTimelineArrangement, arrangement.id)
    assert stale is not None
    stale.version = 1
    raw.commit()

    with pytest.raises(ConflictError):
        await timeline_crud.update_arrangement(
            db,
            arrangement,
            is_selected=False,
            expected_version=0,
            fields_set={"is_selected"},
        )

    fresh = await timeline_crud.get_arrangement(db, arrangement.id)
    assert fresh is not None
    updated = await timeline_crud.update_arrangement(
        db,
        fresh,
        is_selected=False,
        expected_version=1,
        fields_set={"is_selected"},
    )
    assert updated.version == 2
    assert updated.is_selected is False


async def test_duplicate_row_order_returns_conflict_and_rolls_back_reorder() -> None:
    from app.crud import timeline as timeline_crud

    _, raw = _make_db()
    seed = _seed(raw)
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))
    first = await timeline_crud.add_arrangement(
        db,
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
        unit_runtime_key=seed.video_a.unit_runtime_key,
        job_id=seed.video_a.job_id,
    )
    second = await timeline_crud.add_arrangement(
        db,
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
        unit_runtime_key=seed.video_b.unit_runtime_key,
        job_id=seed.video_b.job_id,
    )
    await db.commit()

    with pytest.raises(ConflictError):
        await timeline_crud.reorder(
            db,
            [{"id": first.id, "row_order": 1}, {"id": second.id, "row_order": 1}],
            script_id=seed.script.id,
            episode_id=seed.episode.episode_id,
        )

    raw.rollback()
    rows = (
        raw.execute(
            select(VideoTimelineArrangement).order_by(VideoTimelineArrangement.row_order)
        )
        .scalars()
        .all()
    )
    assert [(row.id, row.row_order) for row in rows] == [(first.id, 0), (second.id, 1)]


async def test_reorder_requires_versions_and_uses_db_cas() -> None:
    from app.crud import timeline as timeline_crud

    _, raw = _make_db()
    seed = _seed(raw)
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))
    first = await timeline_crud.add_arrangement(
        db,
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
        unit_runtime_key=seed.video_a.unit_runtime_key,
        job_id=seed.video_a.job_id,
    )
    second = await timeline_crud.add_arrangement(
        db,
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
        unit_runtime_key=seed.video_b.unit_runtime_key,
        job_id=seed.video_b.job_id,
    )
    await db.commit()

    with pytest.raises(ConflictError):
        await timeline_crud.reorder(
            db,
            [{"id": first.id, "row_order": 1}, {"id": second.id, "row_order": 0}],
            script_id=seed.script.id,
            episode_id=seed.episode.episode_id,
        )

    raw.rollback()
    first.version = 3
    raw.commit()

    with pytest.raises(ConflictError):
        await timeline_crud.reorder(
            db,
            [
                {"id": first.id, "row_order": 1, "expected_version": 0},
                {"id": second.id, "row_order": 0, "expected_version": 0},
            ],
            script_id="script-1",
            episode_id="episode-1",
        )

    raw.rollback()
    await timeline_crud.reorder(
        db,
        [
            {"id": first.id, "row_order": 1, "expected_version": 3},
            {"id": second.id, "row_order": 0, "expected_version": 0},
        ],
        script_id="script-1",
        episode_id="episode-1",
    )
    assert first.version == 4
    assert second.version == 1


async def test_render_creates_persistent_job_and_outbox_idempotently_without_delay(monkeypatch) -> None:
    from app.crud import timeline as timeline_crud
    from app.services import timeline_render

    _, raw = _make_db()
    seed = _seed(raw)
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))
    second = await timeline_crud.add_arrangement(
        db,
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
        unit_runtime_key=seed.video_b.unit_runtime_key,
        job_id=seed.video_b.job_id,
    )
    first = await timeline_crud.add_arrangement(
        db,
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
        unit_runtime_key=seed.video_a.unit_runtime_key,
        job_id=seed.video_a.job_id,
    )
    await timeline_crud.reorder(
        db,
        [
            {"id": first.id, "row_order": 0, "expected_version": 0},
            {"id": second.id, "row_order": 1, "expected_version": 0},
        ],
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
    )
    await db.commit()

    async def allow(*args, **kwargs):
        return seed.script

    monkeypatch.setattr(timeline_render, "check_timeline_script_access", allow)
    result = await timeline_render.render(
        db,
        seed.script.id,
        seed.episode.episode_id,
        seed.owner,
        idempotency_key="idem-1",
    )
    await db.commit()
    same = await timeline_render.render(
        db,
        seed.script.id,
        seed.episode.episode_id,
        seed.owner,
        idempotency_key="idem-1",
    )

    assert same["job_id"] == result["job_id"]
    job = raw.get(TimelineRenderJob, result["job_id"])
    assert job is not None
    assert job.owner_user_id == seed.owner.user_id
    assert job.status == "starting"
    assert job.output_storage_key == f"generate_outputs/{seed.script.id}/{seed.episode.episode_id}/{job.job_id}.mp4"
    assert [seg["job_id"] for seg in job.input_json["segments"]] == ["video-1", "video-2"]
    assert [seg["row_order"] for seg in job.input_json["segments"]] == [0, 1]

    outboxes = raw.execute(select(TaskDispatchOutbox)).scalars().all()
    assert len(outboxes) == 1
    assert outboxes[0].task_type == "timeline.render"
    assert outboxes[0].job_id == job.job_id
    assert outboxes[0].payload["job_id"] == job.job_id
    reservations = raw.execute(select(CreditReservation)).scalars().all()
    assert len(reservations) == 1
    assert reservations[0].source_type == "timeline_render_job"
    assert reservations[0].source_id == job.job_id
    assert reservations[0].id == job.reservation_id
    account = raw.get(CreditAccount, seed.account.id)
    assert account is not None
    assert account.frozen_points == reservations[0].reserved_points

    job.request_hash = "changed-by-concurrent-edit"
    with pytest.raises(ConflictError):
        await timeline_render.render(
            db,
            seed.script.id,
            seed.episode.episode_id,
            seed.owner,
            idempotency_key="idem-1",
        )


async def test_add_arrangement_requires_succeeded_video_and_matching_runtime_key() -> None:
    from app.crud import timeline as timeline_crud

    _, raw = _make_db()
    seed = _seed(raw)
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))
    seed.video_a.status = "processing"
    raw.commit()

    with pytest.raises(ConflictError, match="尚未成功"):
        await timeline_crud.add_arrangement(
            db,
            script_id=seed.script.id,
            episode_id=seed.episode.episode_id,
            unit_runtime_key=seed.video_a.unit_runtime_key,
            job_id=seed.video_a.job_id,
        )

    raw.rollback()
    seed.video_a.status = "succeeded"
    raw.commit()
    with pytest.raises(ConflictError, match="unit_runtime_key"):
        await timeline_crud.add_arrangement(
            db,
            script_id="script-1",
            episode_id="episode-1",
            unit_runtime_key="runtime:mismatch",
            job_id=seed.video_a.job_id,
        )


async def test_render_rejects_cross_episode_video_job_and_missing_idempotency() -> None:
    from app.crud import timeline as timeline_crud
    from app.services import timeline_render

    _, raw = _make_db()
    seed = _seed(raw)
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))
    with pytest.raises(ConflictError):
        await timeline_render.render(
            db,
            seed.script.id,
            seed.episode.episode_id,
            seed.owner,
            idempotency_key=None,
        )

    with pytest.raises(Exception) as exc_info:
        await timeline_crud.add_arrangement(
            db,
            script_id=seed.script.id,
            episode_id=seed.episode.episode_id,
            unit_runtime_key=seed.other_video.unit_runtime_key,
            job_id=seed.other_video.job_id,
        )
    assert getattr(exc_info.value, "status_code", None) == 403


async def test_render_status_is_persistent_and_cancel_requires_owner_or_run_ai(monkeypatch) -> None:
    from app.services import timeline_render

    _, raw = _make_db()
    seed = _seed(raw)
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))
    job = TimelineRenderJob(
        job_id="render-1",
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
        owner_user_id=seed.owner.user_id,
        idempotency_key="idem",
        request_hash="hash",
        input_version=1,
        attempt=1,
        status="starting",
        progress=0,
        input_json={"segments": []},
        output_storage_key="generate_outputs/script-1/episode-1/render-1.mp4",
    )
    raw.add(job)
    raw.commit()

    async def fake_check(db, user, script_id, action):
        checks.append((user.user_id, action))
        if user.user_id == seed.owner.user_id and allow_owner:
            return seed.script
        raise timeline_render.ForbiddenError("无权访问该剧本")

    checks: list[tuple[str, object]] = []
    allow_owner = True
    monkeypatch.setattr(timeline_render, "check_timeline_script_access", fake_check)
    assert (await timeline_render.get_render_status("render-1", db, seed.owner))["status"] == "starting"
    with pytest.raises(timeline_render.ForbiddenError):
        await timeline_render.cancel_render("render-1", db, seed.other)

    allow_owner = False
    with pytest.raises(timeline_render.ForbiddenError):
        await timeline_render.cancel_render("render-1", db, seed.owner)

    allow_owner = True
    canceled = await timeline_render.cancel_render("render-1", db, seed.owner)
    assert canceled["status"] == "canceled"
    assert (seed.owner.user_id, timeline_render.Action.RUN_AI) in checks
    canceled_job = raw.get(TimelineRenderJob, "render-1")
    assert canceled_job is not None
    assert canceled_job.canceled_at is not None


def test_trim_command_uses_duration_and_concat_manifest_is_safe(tmp_path, monkeypatch) -> None:
    from app.workers import timeline_render as worker

    commands: list[list[str]] = []

    def fake_run(cmd, check, capture_output):
        commands.append(cmd)
        Path(cmd[-1]).write_bytes(b"mp4")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(worker.subprocess, "run", fake_run)

    worker._trim_segment(str(tmp_path / "input.mp4"), 1.5, 4.0, str(tmp_path / "out.mp4"))
    worker._trim_segment(str(tmp_path / "input.mp4"), 2.0, None, str(tmp_path / "out2.mp4"))
    worker._concat_segments([str(tmp_path / "out.mp4"), str(tmp_path / "out2.mp4")], str(tmp_path / "final.mp4"))

    assert commands[0][0] == worker.FFMPEG_BIN
    assert "-ss" in commands[0]
    assert commands[0][commands[0].index("-ss") + 1] == "1.500"
    assert "-t" in commands[0]
    assert commands[0][commands[0].index("-t") + 1] == "2.500"
    assert "-to" not in commands[0]
    assert "-t" not in commands[1]
    assert commands[2][commands[2].index("-f") + 1] == "concat"
    assert commands[2][commands[2].index("-safe") + 1] == "1"


async def test_worker_uses_async_storage_and_token_cas_then_cleans_temp(monkeypatch, tmp_path) -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    seed = _seed(raw)
    job = TimelineRenderJob(
        job_id="render-worker",
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
        owner_user_id=seed.owner.user_id,
        idempotency_key="idem-worker",
        request_hash="hash",
        input_version=1,
        attempt=1,
        status="starting",
        progress=0,
        input_json={
            "timeline_version": 1,
            "segments": [
                {
                    "storage_key": "generate_images/mat/video-1.mp4",
                    "trim_start_seconds": 0.5,
                    "trim_end_seconds": 2.0,
                }
            ],
        },
        output_storage_key="generate_outputs/script-1/episode-1/render-worker.mp4",
    )
    raw.add(job)
    raw.commit()

    class FakeSessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            self.raw = Session(raw.get_bind(), expire_on_commit=False)
            return cast(AsyncSession, _AsyncSessionAdapter(self.raw))

        async def __aexit__(self, exc_type, exc, tb):
            self.raw.close()

    stored: dict[str, bytes] = {"generate_images/mat/video-1.mp4": b"source"}

    class FakeStorage:
        async def get(self, key: str) -> bytes:
            return stored[key]

        async def put(self, key: str, data: bytes) -> str:
            assert key.endswith(".tmp")
            stored[key] = data
            return key

        async def promote(self, source_key: str, final_key: str) -> str:
            stored[final_key] = stored.pop(source_key)
            return final_key

        async def delete(self, key: str) -> None:
            stored.pop(key, None)

    def fake_trim(src, start, end, out):
        assert Path(src).read_bytes() == b"source"
        assert (start, end) == (0.5, 2.0)
        Path(out).write_bytes(b"trimmed")

    def fake_concat(paths, out):
        assert len(paths) == 1
        Path(out).write_bytes(b"final")

    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())
    monkeypatch.setattr(worker, "get_storage", lambda: FakeStorage())
    monkeypatch.setattr(worker, "_trim_segment", fake_trim)
    monkeypatch.setattr(worker, "_concat_segments", fake_concat)

    result = await worker.run_render_job("render-worker", owner="worker-a", temp_root=tmp_path)

    raw.expire_all()
    saved = raw.get(TimelineRenderJob, "render-worker")
    assert saved is not None
    assert result["status"] == "succeeded"
    assert saved.status == "succeeded"
    assert saved.output_storage_key in stored
    assert stored[saved.output_storage_key] == b"final"
    assert not any(key.endswith(".tmp") for key in stored)
    assert saved.lease_token is None
    assert not any(tmp_path.iterdir())


async def test_worker_settle_failure_releases_reservation_and_deletes_output(monkeypatch, tmp_path) -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    seed = _seed(raw)
    raw.add(
        CreditReservation(
            id="reservation-settle-fail",
            account_id=seed.account.id,
            source_type="timeline_render_job",
            source_id="render-settle-fail",
            reserved_points=1,
            status="reserved",
        )
    )
    seed.account.frozen_points = 1
    raw.add(
        TimelineRenderJob(
            job_id="render-settle-fail",
            script_id=seed.script.id,
            episode_id=seed.episode.episode_id,
            owner_user_id=seed.owner.user_id,
            idempotency_key="idem-settle-fail",
            request_hash="hash",
            input_version=1,
            attempt=1,
            reservation_id="reservation-settle-fail",
            status="starting",
            progress=0,
            input_json={"segments": [{"storage_key": "generate_images/mat/video-1.mp4"}]},
            output_storage_key="generate_outputs/script-1/episode-1/render-settle-fail.mp4",
        )
    )
    raw.commit()

    class FakeSessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            self.raw = Session(raw.get_bind(), expire_on_commit=False)
            return cast(AsyncSession, _AsyncSessionAdapter(self.raw))

        async def __aexit__(self, exc_type, exc, tb):
            self.raw.close()

    stored: dict[str, bytes] = {"generate_images/mat/video-1.mp4": b"source"}
    deleted: list[str] = []

    class FakeStorage:
        async def get(self, key: str) -> bytes:
            return stored[key]

        async def put(self, key: str, data: bytes) -> str:
            stored[key] = data
            return key

        async def promote(self, source_key: str, final_key: str) -> str:
            stored[final_key] = stored.pop(source_key)
            return final_key

        async def delete(self, key: str) -> None:
            deleted.append(key)
            stored.pop(key, None)

    async def failing_settle(*args, **kwargs):
        raise RuntimeError("settle backend down")

    def fake_trim(src, start, end, out):
        Path(out).write_bytes(b"trimmed")

    def fake_concat(paths, out):
        Path(out).write_bytes(b"final")

    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())
    monkeypatch.setattr(worker, "get_storage", lambda: FakeStorage())
    monkeypatch.setattr(worker, "_trim_segment", fake_trim)
    monkeypatch.setattr(worker, "_concat_segments", fake_concat)
    monkeypatch.setattr(worker, "credits_settle", failing_settle)

    result = await worker.run_render_job("render-settle-fail", owner="worker-a", temp_root=tmp_path)

    raw.expire_all()
    saved = raw.get(TimelineRenderJob, "render-settle-fail")
    reservation = raw.get(CreditReservation, "reservation-settle-fail")
    account = raw.get(CreditAccount, seed.account.id)
    assert saved is not None and reservation is not None and account is not None
    assert result["status"] == "skipped"
    assert saved.status == "failed"
    assert saved.output_json is None
    assert reservation.status == "released"
    assert reservation.actual_points is None
    assert account.balance_points == 100
    assert account.frozen_points == 0
    assert saved.output_storage_key not in stored
    assert saved.output_storage_key in deleted


async def test_worker_release_failure_records_pending_cleanup_and_reconciler_releases(monkeypatch) -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    seed = _seed(raw)
    raw.add(
        CreditReservation(
            id="reservation-release-fail",
            account_id=seed.account.id,
            source_type="timeline_render_job",
            source_id="render-release-fail",
            reserved_points=1,
            status="reserved",
        )
    )
    seed.account.frozen_points = 1
    raw.add(
        TimelineRenderJob(
            job_id="render-release-fail",
            script_id=seed.script.id,
            episode_id=seed.episode.episode_id,
            owner_user_id=seed.owner.user_id,
            idempotency_key="idem-release-fail",
            request_hash="hash",
            input_version=1,
            attempt=1,
            reservation_id="reservation-release-fail",
            status="processing",
            progress=10,
            input_json={"segments": []},
            output_storage_key="generate_outputs/script-1/episode-1/render-release-fail.mp4",
            lease_owner="worker-a",
            lease_token="token-current",
            leased_until=datetime.now(UTC) + timedelta(seconds=60),
        )
    )
    raw.commit()

    class FakeSessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            self.raw = Session(raw.get_bind(), expire_on_commit=False)
            return cast(AsyncSession, _AsyncSessionAdapter(self.raw))

        async def __aexit__(self, exc_type, exc, tb):
            self.raw.close()

    async def failing_release(*args, **kwargs):
        raise RuntimeError("release backend down")

    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())
    monkeypatch.setattr(worker, "credits_release", failing_release)

    ok = await worker._finish(
        "render-release-fail",
        "token-current",
        status="failed",
        error=RuntimeError("ffmpeg failed"),
    )

    saved = raw.get(TimelineRenderJob, "render-release-fail")
    reservation = raw.get(CreditReservation, "reservation-release-fail")
    assert saved is not None and reservation is not None
    assert ok is False
    assert saved.status == "failed"
    assert reservation.status == "reserved"
    assert saved.output_json == {
        "financial_cleanup": {
            "status": "pending",
            "action": "release",
            "reservation_id": "reservation-release-fail",
            "error": "RuntimeError",
        }
    }

    monkeypatch.setattr(worker, "credits_release", worker.credits.release)
    released = await worker.reconcile_render_financial_cleanup(limit=10)

    raw.expire_all()
    saved = raw.get(TimelineRenderJob, "render-release-fail")
    reservation = raw.get(CreditReservation, "reservation-release-fail")
    account = raw.get(CreditAccount, seed.account.id)
    assert saved is not None and reservation is not None and account is not None
    assert released == 1
    assert saved.status == "failed"
    assert saved.output_json is not None
    assert saved.output_json["financial_cleanup"]["status"] == "done"
    assert reservation.status == "released"
    assert account.frozen_points == 0


async def test_worker_old_lease_does_not_trigger_financial_side_effect(monkeypatch) -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    seed = _seed(raw)
    raw.add(
        CreditReservation(
            id="reservation-old-lease",
            account_id=seed.account.id,
            source_type="timeline_render_job",
            source_id="render-old-lease",
            reserved_points=1,
            status="reserved",
        )
    )
    raw.add(
        TimelineRenderJob(
            job_id="render-old-lease",
            script_id=seed.script.id,
            episode_id=seed.episode.episode_id,
            owner_user_id=seed.owner.user_id,
            idempotency_key="idem-old-lease",
            request_hash="hash",
            input_version=1,
            attempt=1,
            reservation_id="reservation-old-lease",
            status="processing",
            progress=10,
            input_json={"segments": []},
            output_storage_key="generate_outputs/script-1/episode-1/render-old-lease.mp4",
            lease_owner="worker-b",
            lease_token="new-token",
            leased_until=datetime.now(UTC) + timedelta(seconds=60),
        )
    )
    raw.commit()

    class FakeSessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            self.raw = Session(raw.get_bind(), expire_on_commit=False)
            return cast(AsyncSession, _AsyncSessionAdapter(self.raw))

        async def __aexit__(self, exc_type, exc, tb):
            self.raw.close()

    financial_calls: list[str] = []

    async def record_settle(*args, **kwargs):
        financial_calls.append("settle")

    async def record_release(*args, **kwargs):
        financial_calls.append("release")

    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())
    monkeypatch.setattr(worker, "credits_settle", record_settle)
    monkeypatch.setattr(worker, "credits_release", record_release)

    ok = await worker._finish("render-old-lease", "old-token", status="succeeded")

    saved = raw.get(TimelineRenderJob, "render-old-lease")
    reservation = raw.get(CreditReservation, "reservation-old-lease")
    assert saved is not None and reservation is not None
    assert ok is False
    assert financial_calls == []
    assert saved.status == "processing"
    assert reservation.status == "reserved"


async def test_cancel_release_failure_records_pending_cleanup_and_reconciler_releases(monkeypatch) -> None:
    from app.services import timeline_render
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    seed = _seed(raw)
    raw.add(
        CreditReservation(
            id="reservation-cancel-release-fail",
            account_id=seed.account.id,
            source_type="timeline_render_job",
            source_id="render-cancel-release-fail",
            reserved_points=1,
            status="reserved",
        )
    )
    seed.account.frozen_points = 1
    raw.add(
        TimelineRenderJob(
            job_id="render-cancel-release-fail",
            script_id=seed.script.id,
            episode_id=seed.episode.episode_id,
            owner_user_id=seed.owner.user_id,
            idempotency_key="idem-cancel-release-fail",
            request_hash="hash",
            input_version=1,
            attempt=1,
            reservation_id="reservation-cancel-release-fail",
            status="starting",
            progress=0,
            input_json={"segments": []},
            output_storage_key="generate_outputs/script-1/episode-1/render-cancel-release-fail.mp4",
        )
    )
    raw.commit()
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))

    async def allow(*args, **kwargs):
        return seed.script

    async def failing_release(*args, **kwargs):
        raise RuntimeError("release backend down")

    monkeypatch.setattr(timeline_render, "check_timeline_script_access", allow)
    monkeypatch.setattr(timeline_render, "credits_release", failing_release)

    canceled = await timeline_render.cancel_render("render-cancel-release-fail", db, seed.owner)
    await db.commit()

    raw.expire_all()
    saved = raw.get(TimelineRenderJob, "render-cancel-release-fail")
    reservation = raw.get(CreditReservation, "reservation-cancel-release-fail")
    assert saved is not None and reservation is not None
    assert canceled["status"] == "canceled"
    assert saved.status == "canceled"
    assert reservation.status == "reserved"
    assert saved.output_json is not None
    assert saved.output_json["financial_cleanup"]["status"] == "pending"
    assert saved.output_json["financial_cleanup"]["action"] == "release"

    class FakeSessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            self.raw = Session(raw.get_bind(), expire_on_commit=False)
            return cast(AsyncSession, _AsyncSessionAdapter(self.raw))

        async def __aexit__(self, exc_type, exc, tb):
            self.raw.close()

    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())
    released = await worker.reconcile_render_financial_cleanup(limit=10)

    raw.expire_all()
    saved = raw.get(TimelineRenderJob, "render-cancel-release-fail")
    reservation = raw.get(CreditReservation, "reservation-cancel-release-fail")
    account = raw.get(CreditAccount, seed.account.id)
    assert saved is not None and reservation is not None and account is not None
    assert released == 1
    assert saved.status == "canceled"
    assert saved.output_json is not None
    assert saved.output_json["financial_cleanup"]["status"] == "done"
    assert reservation.status == "released"
    assert account.frozen_points == 0


def test_timeline_financial_cleanup_task_name_is_available_for_beat() -> None:
    from app.workers import timeline_render as worker

    assert worker.reconcile_render_financial_cleanup_task.name == "timeline.render.financial_cleanup"


async def test_worker_lease_reclaim_cancel_and_sanitized_failure() -> None:
    from app.crud import timeline as timeline_crud

    _, raw = _make_db()
    seed = _seed(raw)
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))
    job = TimelineRenderJob(
        job_id="render-lease",
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
        owner_user_id=seed.owner.user_id,
        idempotency_key="idem-lease",
        request_hash="hash",
        input_version=1,
        attempt=1,
        status="processing",
        progress=10,
        input_json={"segments": []},
        output_storage_key="generate_outputs/script-1/episode-1/render-lease.mp4",
        lease_owner="old",
        lease_token="old-token",
        leased_until=datetime.now(UTC) - timedelta(seconds=30),
    )
    raw.add(job)
    raw.commit()

    claim = await timeline_crud.claim_render_job(db, "render-lease", owner="new")
    assert claim is not None
    assert claim.lease_token != "old-token"
    assert not await timeline_crud.finish_render_job(
        db,
        "render-lease",
        "old-token",
        status="succeeded",
        output_storage_key=job.output_storage_key,
    )
    assert await timeline_crud.finish_render_job(
        db,
        "render-lease",
        claim.lease_token,
        status="failed",
        error=RuntimeError("Authorization=secret-token /tmp/private/file.mp4"),
    )
    assert job.status == "failed"
    assert job.error_type == "internal_error"
    assert "secret-token" not in (job.error_message or "")
    assert "/tmp/private" not in (job.error_message or "")

    stale = await timeline_crud.list_stale_render_jobs(db, limit=10)
    assert stale == []


async def test_worker_lost_lease_deletes_temp_and_final_orphans(monkeypatch, tmp_path) -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    seed = _seed(raw)
    raw.add(
        TimelineRenderJob(
            job_id="render-lost-lease",
            script_id=seed.script.id,
            episode_id=seed.episode.episode_id,
            owner_user_id=seed.owner.user_id,
            idempotency_key="idem-lost-lease",
            request_hash="hash",
            input_version=1,
            attempt=1,
            status="starting",
            progress=0,
            input_json={"segments": [{"storage_key": "generate_images/mat/video-1.mp4"}]},
            output_storage_key="generate_outputs/script-1/episode-1/render-lost-lease.mp4",
        )
    )
    raw.commit()

    class FakeSessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            self.raw = Session(raw.get_bind(), expire_on_commit=False)
            return cast(AsyncSession, _AsyncSessionAdapter(self.raw))

        async def __aexit__(self, exc_type, exc, tb):
            self.raw.close()

    stored: dict[str, bytes] = {"generate_images/mat/video-1.mp4": b"source"}
    deleted: list[str] = []

    class FakeStorage:
        async def get(self, key: str) -> bytes:
            return stored[key]

        async def put(self, key: str, data: bytes) -> str:
            stored[key] = data
            claimed = raw.get(TimelineRenderJob, "render-lost-lease")
            assert claimed is not None
            claimed.lease_token = "stolen-token"
            raw.commit()
            return key

        async def promote(self, source_key: str, final_key: str) -> str:
            stored[final_key] = stored[source_key]
            return final_key

        async def delete(self, key: str) -> None:
            deleted.append(key)
            stored.pop(key, None)

    def fake_trim(src, start, end, out):
        Path(out).write_bytes(b"trimmed")

    def fake_concat(paths, out):
        Path(out).write_bytes(b"final")

    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())
    monkeypatch.setattr(worker, "get_storage", lambda: FakeStorage())
    monkeypatch.setattr(worker, "_trim_segment", fake_trim)
    monkeypatch.setattr(worker, "_concat_segments", fake_concat)

    result = await worker.run_render_job("render-lost-lease", owner="worker-a", temp_root=tmp_path)

    saved = raw.get(TimelineRenderJob, "render-lost-lease")
    assert saved is not None
    assert saved.status == "processing"
    assert result["status"] == "skipped"
    assert saved.output_storage_key not in stored
    assert all(not key.endswith(".tmp") for key in stored)
    assert any(key.endswith(".tmp") for key in deleted)


async def test_stale_reconciler_requeues_starting_processing_and_missing_outbox() -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    seed = _seed(raw)
    expired = datetime.now(UTC) - timedelta(seconds=10)
    raw.add_all(
        [
            TimelineRenderJob(
                job_id="render-starting",
                script_id=seed.script.id,
                episode_id=seed.episode.episode_id,
                owner_user_id=seed.owner.user_id,
                idempotency_key="idem-starting",
                request_hash="hash-starting",
                input_version=1,
                attempt=1,
                status="starting",
                progress=0,
                input_json={"segments": []},
                output_storage_key="generate_outputs/script-1/episode-1/render-starting.mp4",
            ),
            TimelineRenderJob(
                job_id="render-processing",
                script_id=seed.script.id,
                episode_id=seed.episode.episode_id,
                owner_user_id=seed.owner.user_id,
                idempotency_key="idem-processing",
                request_hash="hash-processing",
                input_version=1,
                attempt=1,
                status="processing",
                progress=10,
                input_json={"segments": []},
                output_storage_key="generate_outputs/script-1/episode-1/render-processing.mp4",
                lease_owner="old",
                lease_token="old-token",
                leased_until=expired,
            ),
        ]
    )
    raw.add(
        TaskDispatchOutbox(
            id="outbox-processing",
            task_type="timeline.render",
            job_id="render-processing",
            attempt=1,
            payload={"job_id": "render-processing", "dispatch_id": "outbox-processing", "run_id": "render-processing"},
            status="published",
            published_at=expired,
        )
    )
    raw.commit()
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))

    recovered = await worker.reconcile_stale_render_jobs(db, limit=10)

    assert recovered == 2
    processing = raw.get(TimelineRenderJob, "render-processing")
    assert processing is not None
    assert processing.status == "starting"
    assert processing.lease_token is None
    outboxes = raw.scalars(select(TaskDispatchOutbox)).all()
    assert len(outboxes) == 2
    assert all(row.status == "pending" for row in outboxes)


async def test_retry_render_creates_new_immutable_attempt_and_outbox(monkeypatch) -> None:
    from app.services import timeline_render

    _, raw = _make_db()
    seed = _seed(raw)
    failed = TimelineRenderJob(
        job_id="render-failed",
        script_id=seed.script.id,
        episode_id=seed.episode.episode_id,
        owner_user_id=seed.owner.user_id,
        idempotency_key="idem-failed",
        request_hash="hash",
        input_version=7,
        attempt=1,
        status="failed",
        progress=10,
        retryable=True,
        input_json={
            "schema_version": "timeline.render.v1",
            "segments": [{"job_id": "video-1", "storage_key": "generate_images/mat/video-1.mp4"}],
        },
        output_storage_key="generate_outputs/script-1/episode-1/render-failed.mp4",
        reservation_id="reservation-old",
    )
    raw.add(failed)
    raw.commit()
    db = cast(AsyncSession, _AsyncSessionAdapter(raw))

    async def allow(*args, **kwargs):
        return seed.script

    monkeypatch.setattr(timeline_render, "check_timeline_script_access", allow)
    result = await timeline_render.retry_render("render-failed", db, seed.owner)

    assert result["status"] == "starting"
    assert result["job_id"] != "render-failed"
    retried = raw.get(TimelineRenderJob, result["job_id"])
    assert retried is not None
    assert retried.attempt == 2
    assert retried.request_hash == failed.request_hash
    assert retried.idempotency_key.endswith(":retry:2")
    assert retried.input_json == failed.input_json
    assert retried.reservation_id != failed.reservation_id
    outbox = raw.scalars(
        select(TaskDispatchOutbox).where(TaskDispatchOutbox.job_id == retried.job_id)
    ).one()
    assert outbox.attempt == 2
