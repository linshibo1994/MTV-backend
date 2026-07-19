"""T9 时间线 remediation 行为测试。

仅使用 fake / SQLite / mock，不连接真实 DB、Redis、ffmpeg、服务或网络。
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from sqlalchemy import UniqueConstraint, create_engine, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.core.errors import ConflictError
from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.media_storage_job import MediaStorageJob
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
        MediaStorageJob.__table__,
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
    active_outbox = TaskDispatchOutbox(
        id="dispatch-timeline-cancel",
        task_type="timeline.render",
        job_id=job.job_id,
        attempt=job.attempt,
        payload={"job_id": job.job_id},
        status="running",
        lease_owner="dispatcher",
        lease_token="dispatch-token",
        lease_until=datetime.now(UTC) + timedelta(minutes=1),
    )
    wrong_attempt = TaskDispatchOutbox(
        id="dispatch-timeline-wrong-attempt",
        task_type="timeline.render",
        job_id=job.job_id,
        attempt=job.attempt + 1,
        payload={"job_id": job.job_id},
        status="published",
        published_at=datetime.now(UTC),
    )
    wrong_task_type = TaskDispatchOutbox(
        id="dispatch-timeline-wrong-type",
        task_type="video.generate",
        job_id=job.job_id,
        attempt=job.attempt,
        payload={"job_id": job.job_id},
        status="pending",
    )
    raw.add_all([job, active_outbox, wrong_attempt, wrong_task_type])
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
    await db.commit()
    raw.expire_all()
    active_outbox = raw.get(TaskDispatchOutbox, "dispatch-timeline-cancel")
    wrong_attempt = raw.get(TaskDispatchOutbox, "dispatch-timeline-wrong-attempt")
    wrong_task_type = raw.get(TaskDispatchOutbox, "dispatch-timeline-wrong-type")
    assert active_outbox is not None
    assert wrong_attempt is not None
    assert wrong_task_type is not None
    assert canceled["status"] == "canceled"
    assert (seed.owner.user_id, timeline_render.Action.RUN_AI) in checks
    canceled_job = raw.get(TimelineRenderJob, "render-1")
    assert canceled_job is not None
    assert canceled_job.canceled_at is not None
    assert active_outbox.status == "canceled"
    assert active_outbox.lease_owner is None
    assert active_outbox.lease_token is None
    assert active_outbox.lease_until is None
    assert wrong_attempt.status == "published"
    assert wrong_task_type.status == "pending"


async def test_trim_command_uses_duration_and_concat_manifest_is_safe(tmp_path, monkeypatch) -> None:
    from app.workers import timeline_render as worker

    commands: list[list[str]] = []

    async def fake_run(cmd, *, output_path: str, **_kwargs):
        commands.append(cmd)
        Path(output_path).write_bytes(b"mp4")

    monkeypatch.setattr(worker, "_run_ffmpeg_command", fake_run)

    await worker._trim_segment(
        str(tmp_path / "input.mp4"),
        1.5,
        4.0,
        str(tmp_path / "out.mp4"),
        job_id="render-command",
        token="lease-token",
    )
    await worker._trim_segment(
        str(tmp_path / "input.mp4"),
        2.0,
        None,
        str(tmp_path / "out2.mp4"),
        job_id="render-command",
        token="lease-token",
    )
    await worker._concat_segments(
        [str(tmp_path / "out.mp4"), str(tmp_path / "out2.mp4")],
        str(tmp_path / "final.mp4"),
        job_id="render-command",
        token="lease-token",
    )

    assert commands[0][0] == worker.FFMPEG_BIN
    assert "-ss" in commands[0]
    assert commands[0][commands[0].index("-ss") + 1] == "1.500"
    assert "-t" in commands[0]
    assert commands[0][commands[0].index("-t") + 1] == "2.500"
    assert "-to" not in commands[0]
    assert "-t" not in commands[1]
    assert commands[2][commands[2].index("-f") + 1] == "concat"
    assert commands[2][commands[2].index("-safe") + 1] == "1"


async def test_ffmpeg_long_step_renews_lease_until_process_exits(
    monkeypatch,
    tmp_path,
) -> None:
    from app.workers import timeline_render as worker

    done = asyncio.Event()
    output_path = tmp_path / "output.mp4"
    output_path.write_bytes(b"video")
    heartbeats = 0

    class Process:
        returncode: int | None = None

        async def wait(self) -> int:
            await done.wait()
            self.returncode = 0
            return 0

        def terminate(self) -> None:
            raise AssertionError("活跃租约不得终止 ffmpeg")

        def kill(self) -> None:
            raise AssertionError("活跃租约不得杀死 ffmpeg")

    async def spawn(_command: list[str]):
        return Process()

    async def active(*_args, **_kwargs):
        return "active"

    async def heartbeat(*_args, **_kwargs):
        nonlocal heartbeats
        heartbeats += 1
        if heartbeats == 2:
            done.set()
        return True

    monkeypatch.setattr(worker, "_spawn_ffmpeg_process", spawn, raising=False)
    monkeypatch.setattr(worker, "_runtime_state", active)
    monkeypatch.setattr(worker, "_heartbeat", heartbeat)

    await worker._run_ffmpeg_command(
        [worker.FFMPEG_BIN, "-version"],
        output_path=str(output_path),
        job_id="render-long-step",
        token="lease-token",
        heartbeat_interval=0.002,
        poll_interval=0.001,
    )

    assert heartbeats >= 2


async def test_ffmpeg_checks_cancellation_on_each_poll_before_heartbeat(
    monkeypatch,
    tmp_path,
) -> None:
    from app.workers import timeline_render as worker

    done = asyncio.Event()
    output_path = tmp_path / "output-canceled.mp4"
    output_path.write_bytes(b"video")
    state_checks = 0
    terminated = False
    wait_completed = False

    class Process:
        returncode: int | None = None

        async def wait(self) -> int:
            nonlocal wait_completed
            try:
                await asyncio.wait_for(done.wait(), timeout=0.02)
            except TimeoutError:
                self.returncode = 0
                wait_completed = True
                return 0
            wait_completed = True
            self.returncode = -15 if terminated else 0
            return self.returncode

        def terminate(self) -> None:
            nonlocal terminated
            terminated = True
            done.set()

        def kill(self) -> None:
            done.set()

    async def spawn(_command: list[str]):
        return Process()

    async def state(*_args, **_kwargs):
        nonlocal state_checks
        state_checks += 1
        return "active" if state_checks == 1 else "canceled"

    async def heartbeat(*_args, **_kwargs):
        raise AssertionError("60 秒心跳尚未到期，不应续租")

    monkeypatch.setattr(worker, "_spawn_ffmpeg_process", spawn)
    monkeypatch.setattr(worker, "_runtime_state", state)
    monkeypatch.setattr(worker, "_heartbeat", heartbeat)

    with pytest.raises(worker._FfmpegStateChanged) as caught:
        await worker._run_ffmpeg_command(
            [worker.FFMPEG_BIN, "-version"],
            output_path=str(output_path),
            job_id="render-canceled-between-heartbeats",
            token="lease-token",
            heartbeat_interval=60.0,
            poll_interval=0.001,
        )

    assert caught.value.state == "canceled"
    assert state_checks >= 2
    assert terminated is True
    assert wait_completed is True


@pytest.mark.parametrize("output_limit_bytes", [0, -1])
async def test_ffmpeg_nonpositive_output_limit_does_not_spawn(
    monkeypatch,
    tmp_path,
    output_limit_bytes: int,
) -> None:
    from app.workers import timeline_render as worker

    spawned = False

    class Process:
        returncode = 0

        async def wait(self) -> int:
            return 0

        def terminate(self) -> None:
            raise AssertionError("非正输出上限不得启动或终止 ffmpeg")

        def kill(self) -> None:
            raise AssertionError("非正输出上限不得启动或杀死 ffmpeg")

    async def spawn(_command: list[str]):
        nonlocal spawned
        spawned = True
        return Process()

    monkeypatch.setattr(worker, "_spawn_ffmpeg_process", spawn)

    with pytest.raises(ValueError, match="文件过大"):
        await worker._run_ffmpeg_command(
            [worker.FFMPEG_BIN, "-version"],
            output_path=str(tmp_path / "output.mp4"),
            job_id="render-invalid-output-limit",
            token="lease-token",
            output_limit_bytes=output_limit_bytes,
        )

    assert spawned is False


async def test_ffmpeg_lost_lease_terminates_and_reaps_process(
    monkeypatch,
    tmp_path,
) -> None:
    from app.workers import timeline_render as worker

    done = asyncio.Event()
    output_path = tmp_path / "output.mp4"
    output_path.write_bytes(b"video")
    terminated = False
    wait_completed = False

    class Process:
        returncode: int | None = None

        async def wait(self) -> int:
            nonlocal wait_completed
            await done.wait()
            wait_completed = True
            self.returncode = -15
            return -15

        def terminate(self) -> None:
            nonlocal terminated
            terminated = True
            done.set()

        def kill(self) -> None:
            done.set()

    async def spawn(_command: list[str]):
        return Process()

    async def lost(*_args, **_kwargs):
        return "lost_lease"

    monkeypatch.setattr(worker, "_spawn_ffmpeg_process", spawn, raising=False)
    monkeypatch.setattr(worker, "_runtime_state", lost)

    with pytest.raises(worker._FfmpegStateChanged) as caught:
        await worker._run_ffmpeg_command(
            [worker.FFMPEG_BIN, "-version"],
            output_path=str(output_path),
            job_id="render-lost-lease",
            token="lease-token",
            heartbeat_interval=0.001,
            poll_interval=0.001,
        )

    assert caught.value.state == "lost_lease"
    assert terminated is True
    assert wait_completed is True


async def test_ffmpeg_state_check_failure_terminates_and_reaps_process(
    monkeypatch,
    tmp_path,
) -> None:
    from app.workers import timeline_render as worker

    done = asyncio.Event()
    output_path = tmp_path / "output.mp4"
    output_path.write_bytes(b"video")
    terminated = False
    wait_completed = False

    class Process:
        returncode: int | None = None

        async def wait(self) -> int:
            nonlocal wait_completed
            await done.wait()
            wait_completed = True
            self.returncode = -15
            return -15

        def terminate(self) -> None:
            nonlocal terminated
            terminated = True
            done.set()

        def kill(self) -> None:
            done.set()

    async def spawn(_command: list[str]):
        return Process()

    async def fail_state_check(*_args, **_kwargs):
        raise RuntimeError("heartbeat database unavailable")

    monkeypatch.setattr(worker, "_spawn_ffmpeg_process", spawn)
    monkeypatch.setattr(worker, "_runtime_state", fail_state_check)

    with pytest.raises(RuntimeError, match="heartbeat database unavailable"):
        await worker._run_ffmpeg_command(
            [worker.FFMPEG_BIN, "-version"],
            output_path=str(output_path),
            job_id="render-state-error",
            token="lease-token",
            heartbeat_interval=0.001,
            poll_interval=0.001,
        )

    assert terminated is True
    assert wait_completed is True


@pytest.mark.parametrize("fatal_error", [KeyboardInterrupt, SystemExit])
async def test_ffmpeg_fatal_base_exception_terminates_and_reaps_before_reraise(
    monkeypatch,
    tmp_path,
    fatal_error: type[BaseException],
) -> None:
    from app.workers import timeline_render as worker

    done = asyncio.Event()
    output_path = tmp_path / "output-fatal.mp4"
    output_path.write_bytes(b"video")
    terminated = False
    wait_completed = False

    class Process:
        returncode: int | None = None

        async def wait(self) -> int:
            nonlocal wait_completed
            await done.wait()
            wait_completed = True
            self.returncode = -15
            return -15

        def terminate(self) -> None:
            nonlocal terminated
            terminated = True
            done.set()

        def kill(self) -> None:
            done.set()

    async def spawn(_command: list[str]):
        return Process()

    async def fail_state_check(*_args, **_kwargs):
        raise fatal_error("fatal worker interruption")

    monkeypatch.setattr(worker, "_spawn_ffmpeg_process", spawn)
    monkeypatch.setattr(worker, "_runtime_state", fail_state_check)

    with pytest.raises(fatal_error, match="fatal worker interruption"):
        await worker._run_ffmpeg_command(
            [worker.FFMPEG_BIN, "-version"],
            output_path=str(output_path),
            job_id="render-fatal-state-error",
            token="lease-token",
            heartbeat_interval=0.001,
            poll_interval=0.001,
        )

    assert terminated is True
    assert wait_completed is True


async def test_ffmpeg_signal_failure_does_not_replace_caller_cancellation(
    monkeypatch,
    tmp_path,
) -> None:
    from app.workers import timeline_render as worker

    done = asyncio.Event()
    spawned = asyncio.Event()
    output_path = tmp_path / "output-signal-failure.mp4"
    output_path.write_bytes(b"video")
    kill_called = False
    wait_completed = False

    class Process:
        returncode: int | None = None

        async def wait(self) -> int:
            nonlocal wait_completed
            await done.wait()
            wait_completed = True
            self.returncode = -9
            return -9

        def terminate(self) -> None:
            raise PermissionError("terminate denied")

        def kill(self) -> None:
            nonlocal kill_called
            kill_called = True
            done.set()

    async def spawn(_command: list[str]):
        spawned.set()
        return Process()

    monkeypatch.setattr(worker, "_spawn_ffmpeg_process", spawn)
    task = asyncio.create_task(
        worker._run_ffmpeg_command(
            [worker.FFMPEG_BIN, "-version"],
            output_path=str(output_path),
            job_id="render-signal-failure",
            token="lease-token",
            heartbeat_interval=60.0,
            poll_interval=1.0,
        )
    )
    await spawned.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert kill_called is True
    assert wait_completed is True


async def test_ffmpeg_cancellation_during_termination_wait_is_preserved(
    monkeypatch,
    tmp_path,
) -> None:
    from app.workers import timeline_render as worker

    done = asyncio.Event()
    termination_started = asyncio.Event()
    output_path = tmp_path / "output-cancel-during-termination.mp4"
    output_path.write_bytes(b"video")
    terminate_calls = 0
    wait_completed = False

    class Process:
        returncode: int | None = None

        async def wait(self) -> int:
            nonlocal wait_completed
            await done.wait()
            wait_completed = True
            self.returncode = -15
            return -15

        def terminate(self) -> None:
            nonlocal terminate_calls
            terminate_calls += 1
            termination_started.set()
            if terminate_calls >= 2:
                done.set()

        def kill(self) -> None:
            done.set()

    async def spawn(_command: list[str]):
        return Process()

    async def lost(*_args, **_kwargs):
        return "lost_lease"

    monkeypatch.setattr(worker, "_spawn_ffmpeg_process", spawn)
    monkeypatch.setattr(worker, "_runtime_state", lost)
    task = asyncio.create_task(
        worker._run_ffmpeg_command(
            [worker.FFMPEG_BIN, "-version"],
            output_path=str(output_path),
            job_id="render-cancel-during-termination",
            token="lease-token",
            heartbeat_interval=0.001,
            poll_interval=0.001,
        )
    )
    await termination_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert terminate_calls >= 2
    assert wait_completed is True


async def test_ffmpeg_cancellation_and_output_limit_terminate_and_reap(
    monkeypatch,
    tmp_path,
) -> None:
    from app.core.config import settings
    from app.workers import timeline_render as worker

    async def run_case(*, oversized: bool) -> tuple[bool, bool]:
        done = asyncio.Event()
        spawned = asyncio.Event()
        output_path = tmp_path / f"output-{oversized}.mp4"
        output_path.write_bytes(b"123456" if oversized else b"video")
        terminated = False
        wait_completed = False

        class Process:
            returncode: int | None = None

            async def wait(self) -> int:
                nonlocal wait_completed
                await done.wait()
                wait_completed = True
                self.returncode = -15
                return -15

            def terminate(self) -> None:
                nonlocal terminated
                terminated = True
                done.set()

            def kill(self) -> None:
                done.set()

        async def spawn(_command: list[str]):
            spawned.set()
            return Process()

        async def active(*_args, **_kwargs):
            return "active"

        async def heartbeat(*_args, **_kwargs):
            return True

        monkeypatch.setattr(worker, "_spawn_ffmpeg_process", spawn, raising=False)
        monkeypatch.setattr(worker, "_runtime_state", active)
        monkeypatch.setattr(worker, "_heartbeat", heartbeat)
        task = asyncio.create_task(
            worker._run_ffmpeg_command(
                [worker.FFMPEG_BIN, "-version"],
                output_path=str(output_path),
                job_id="render-ffmpeg-stop",
                token="lease-token",
                heartbeat_interval=0.002,
                poll_interval=0.001,
            )
        )
        await spawned.wait()
        if oversized:
            with pytest.raises(ValueError, match="文件过大"):
                await task
        else:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        return terminated, wait_completed

    monkeypatch.setattr(settings, "max_media_read_bytes", 5)

    assert await run_case(oversized=False) == (True, True)
    assert await run_case(oversized=True) == (True, True)


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

    async def fake_trim(src, start, end, out, **_kwargs):
        assert Path(src).read_bytes() == b"source"
        assert (start, end) == (0.5, 2.0)
        Path(out).write_bytes(b"trimmed")

    async def fake_concat(paths, out, **_kwargs):
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


async def test_finish_commit_after_apply_preserves_authoritative_succeeded_output(
    monkeypatch,
    tmp_path,
) -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    seed = _seed(raw)
    job_id = "render-commit-after-apply"
    base_key = f"generate_outputs/script-1/episode-1/{job_id}.mp4"
    raw.add(
        TimelineRenderJob(
            job_id=job_id,
            script_id=seed.script.id,
            episode_id=seed.episode.episode_id,
            owner_user_id=seed.owner.user_id,
            idempotency_key=f"idem-{job_id}",
            request_hash="hash",
            input_version=1,
            attempt=1,
            status="starting",
            progress=0,
            input_json={"segments": [{"storage_key": "generate_images/mat/source.mp4"}]},
            output_storage_key=base_key,
        )
    )
    raw.commit()
    disconnect_raised = False

    class CommitAfterApplyAdapter(_AsyncSessionAdapter):
        async def commit(self) -> None:
            nonlocal disconnect_raised
            self.session.commit()
            saved = self.session.get(TimelineRenderJob, job_id)
            if saved is not None and saved.status == "succeeded" and not disconnect_raised:
                disconnect_raised = True
                raise ConnectionError("commit response lost")

    class FakeSessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            self.raw = Session(raw.get_bind(), expire_on_commit=False)
            return cast(AsyncSession, CommitAfterApplyAdapter(self.raw))

        async def __aexit__(self, exc_type, exc, tb):
            self.raw.close()

    stored = {"generate_images/mat/source.mp4": b"source"}
    deleted: list[str] = []

    class Storage:
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

    async def fake_trim(_src, _start, _end, out, **_kwargs):
        Path(out).write_bytes(b"trimmed")

    async def fake_concat(_paths, out, **_kwargs):
        Path(out).write_bytes(b"final")

    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())
    monkeypatch.setattr(worker, "get_storage", lambda: Storage())
    monkeypatch.setattr(worker, "_trim_segment", fake_trim)
    monkeypatch.setattr(worker, "_concat_segments", fake_concat)

    result = await worker.run_render_job(job_id, owner="worker-a", temp_root=tmp_path)

    raw.expire_all()
    saved = raw.get(TimelineRenderJob, job_id)
    intents = raw.scalars(select(MediaStorageJob)).all()
    assert disconnect_raised is True
    assert saved is not None and saved.status == "succeeded"
    assert result == {
        "job_id": job_id,
        "status": "succeeded",
        "storage_key": saved.output_storage_key,
    }
    assert stored[saved.output_storage_key] == b"final"
    assert deleted == []
    assert len(intents) == 2
    assert {intent.status for intent in intents} == {"canceled"}


@pytest.mark.parametrize(
    ("intent_status", "finish_allowed"),
    [
        ("pending", True),
        ("retry", True),
        ("canceled", True),
        ("running", True),
        ("succeeded", False),
        ("failed", False),
    ],
)
async def test_finish_respects_delete_intent_state_race(
    monkeypatch,
    intent_status: str,
    finish_allowed: bool,
) -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    seed = _seed(raw)
    job_id = f"render-delete-intent-{intent_status}"
    lease_token = f"lease-{intent_status}"
    output_key = f"generate_outputs/script-1/episode-1/{job_id}.attempt.mp4"
    intent_key = worker._delete_intent_key(output_key)
    raw.add_all(
        [
            TimelineRenderJob(
                job_id=job_id,
                script_id=seed.script.id,
                episode_id=seed.episode.episode_id,
                owner_user_id=seed.owner.user_id,
                idempotency_key=f"idem-{job_id}",
                request_hash="hash",
                input_version=1,
                attempt=1,
                status="processing",
                progress=95,
                input_json={"segments": []},
                # succeeded 是 durable tombstone；先用无关 key 构造合法 processing 行，
                # 再由 _finish 更新到目标 key，验证 writer flush 被 tombstone 拒绝。
                output_storage_key=(
                    f"{output_key}.initial" if intent_status == "succeeded" else output_key
                ),
                lease_owner="render-worker",
                lease_token=lease_token,
                leased_until=datetime.now(UTC) + timedelta(seconds=60),
            ),
            MediaStorageJob(
                id=f"intent-{intent_status}",
                idempotency_key=intent_key,
                job_type="delete",
                source_key=output_key,
                status=intent_status,
                lease_owner="cleanup-worker" if intent_status == "running" else None,
                lease_token="cleanup-lease" if intent_status == "running" else None,
                lease_until=(
                    datetime.now(UTC) + timedelta(seconds=60)
                    if intent_status == "running"
                    else None
                ),
                completed_at=(
                    datetime.now(UTC)
                    if intent_status in {"succeeded", "failed", "canceled"}
                    else None
                ),
            ),
        ]
    )
    raw.commit()
    original_intent = raw.get(MediaStorageJob, f"intent-{intent_status}")
    assert original_intent is not None
    original_fields = (
        original_intent.status,
        original_intent.available_at,
        original_intent.lease_owner,
        original_intent.lease_token,
        original_intent.lease_until,
        original_intent.completed_at,
    )

    class FakeSessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            self.raw = Session(raw.get_bind(), expire_on_commit=False)
            return cast(AsyncSession, _AsyncSessionAdapter(self.raw))

        async def __aexit__(self, exc_type, exc, tb):
            self.raw.close()

    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())

    conflict: RuntimeError | ConflictError | None = None
    try:
        finished = await worker._finish(
            job_id,
            lease_token,
            status="succeeded",
            output_storage_key=output_key,
            output_json={"storage_key": output_key},
            delete_intent_keys=(intent_key,),
        )
    except (RuntimeError, ConflictError) as exc:
        conflict = exc
        finished = False

    raw.expire_all()
    saved_job = raw.get(TimelineRenderJob, job_id)
    saved_intent = raw.get(MediaStorageJob, f"intent-{intent_status}")
    assert saved_job is not None and saved_intent is not None
    if finish_allowed:
        assert conflict is None
        assert finished is True
        assert saved_job.status == "succeeded"
        assert saved_intent.status == "canceled"
        if intent_status == "canceled":
            assert (
                saved_intent.status,
                saved_intent.available_at,
                saved_intent.lease_owner,
                saved_intent.lease_token,
                saved_intent.lease_until,
                saved_intent.completed_at,
            ) == original_fields
        else:
            assert saved_intent.lease_owner is None
            assert saved_intent.lease_token is None
            assert saved_intent.lease_until is None
            assert saved_intent.completed_at is not None
    else:
        assert conflict is not None
        if intent_status == "succeeded":
            assert isinstance(conflict, ConflictError)
            assert "正在删除或已删除" in str(conflict)
        else:
            assert str(conflict) == "timeline_delete_intent_state_conflict"
        assert finished is False
        assert saved_job.status == "processing"
        assert saved_job.lease_token == lease_token
        assert (
            saved_intent.status,
            saved_intent.available_at,
            saved_intent.lease_owner,
            saved_intent.lease_token,
            saved_intent.lease_until,
            saved_intent.completed_at,
        ) == original_fields


@pytest.mark.parametrize(
    "intent_status",
    ["pending", "retry", "canceled", "running", "succeeded", "failed"],
)
async def test_cleanup_attempt_objects_preserves_active_and_terminal_delete_intents(
    monkeypatch,
    intent_status: str,
) -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    source_key = f"generate_outputs/script-1/cleanup-{intent_status}.mp4"
    intent_key = worker._delete_intent_key(source_key)
    lease_until = datetime.now(UTC) + timedelta(minutes=5)
    completed_at = (
        datetime.now(UTC) - timedelta(minutes=1)
        if intent_status in {"canceled", "succeeded", "failed"}
        else None
    )
    raw.add(
        MediaStorageJob(
            id=f"cleanup-{intent_status}",
            idempotency_key=intent_key,
            job_type="delete",
            source_key=source_key,
            status=intent_status,
            attempt=3,
            available_at=datetime.now(UTC) + timedelta(hours=1),
            lease_owner="cleanup-worker" if intent_status == "running" else None,
            lease_token="cleanup-lease" if intent_status == "running" else None,
            lease_until=lease_until if intent_status == "running" else None,
            last_error_type="StoredError",
            last_error_message="safe error",
            completed_at=completed_at,
        )
    )
    raw.commit()
    original = raw.get(MediaStorageJob, f"cleanup-{intent_status}")
    assert original is not None
    original_fields = (
        original.status,
        original.attempt,
        original.available_at,
        original.lease_owner,
        original.lease_token,
        original.lease_until,
        original.last_error_type,
        original.last_error_message,
        original.completed_at,
    )

    class FakeSessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            self.raw = Session(raw.get_bind(), expire_on_commit=False)
            return cast(AsyncSession, _AsyncSessionAdapter(self.raw))

        async def __aexit__(self, exc_type, exc, tb):
            self.raw.close()

    deleted: list[str] = []

    class Storage:
        async def delete(self, key: str) -> None:
            deleted.append(key)

    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())

    await worker._cleanup_attempt_objects(Storage(), (intent_key,), source_key)

    raw.expire_all()
    saved = raw.get(MediaStorageJob, f"cleanup-{intent_status}")
    assert saved is not None
    assert deleted == [source_key]
    if intent_status in {"pending", "retry"}:
        assert saved.status == "pending"
        assert saved.available_at <= datetime.now(UTC).replace(tzinfo=None)
        assert saved.lease_owner is None
        assert saved.lease_token is None
        assert saved.lease_until is None
        assert saved.completed_at is None
        assert saved.attempt == 3
        assert saved.last_error_type == "StoredError"
        assert saved.last_error_message == "safe error"
    else:
        assert (
            saved.status,
            saved.attempt,
            saved.available_at,
            saved.lease_owner,
            saved.lease_token,
            saved.lease_until,
            saved.last_error_type,
            saved.last_error_message,
            saved.completed_at,
        ) == original_fields


@pytest.mark.parametrize(
    "intent_status",
    ["pending", "retry", "canceled", "running", "succeeded", "failed"],
)
async def test_cleanup_delete_failure_preserves_intent_state_and_retries_failed_generation(
    monkeypatch,
    intent_status: str,
) -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    source_key = f"generate_outputs/script-1/cleanup-failure-{intent_status}.mp4"
    intent_key = worker._delete_intent_key(source_key)
    completed_at = (
        datetime.now(UTC) - timedelta(minutes=1)
        if intent_status in {"canceled", "succeeded", "failed"}
        else None
    )
    raw.add(
        MediaStorageJob(
            id=f"cleanup-failure-{intent_status}",
            idempotency_key=intent_key,
            job_type="delete",
            source_key=source_key,
            status=intent_status,
            attempt=8 if intent_status == "failed" else 3,
            max_attempts=8,
            available_at=datetime.now(UTC) + timedelta(hours=1),
            lease_owner="cleanup-worker" if intent_status == "running" else None,
            lease_token="cleanup-lease" if intent_status == "running" else None,
            lease_until=(
                datetime.now(UTC) + timedelta(minutes=5)
                if intent_status == "running"
                else None
            ),
            last_error_type="StoredError",
            last_error_message="safe error",
            completed_at=completed_at,
        )
    )
    raw.commit()
    original = raw.get(MediaStorageJob, f"cleanup-failure-{intent_status}")
    assert original is not None
    original_fields = (
        original.status,
        original.attempt,
        original.available_at,
        original.lease_owner,
        original.lease_token,
        original.lease_until,
        original.last_error_type,
        original.last_error_message,
        original.completed_at,
    )

    class FakeSessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            self.raw = Session(raw.get_bind(), expire_on_commit=False)
            return cast(AsyncSession, _AsyncSessionAdapter(self.raw))

        async def __aexit__(self, exc_type, exc, tb):
            self.raw.close()

    deleted: list[str] = []

    class Storage:
        async def delete(self, key: str) -> None:
            deleted.append(key)
            raise RuntimeError("storage delete failed")

    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())

    await worker._cleanup_attempt_objects(Storage(), (intent_key,), source_key)

    raw.expire_all()
    rows = list(raw.scalars(select(MediaStorageJob).order_by(MediaStorageJob.idempotency_key)))
    saved = raw.get(MediaStorageJob, f"cleanup-failure-{intent_status}")
    assert saved is not None
    assert deleted == [source_key]
    if intent_status in {"pending", "retry"}:
        assert len(rows) == 1
        assert saved.status == "pending"
        assert saved.available_at <= datetime.now(UTC).replace(tzinfo=None)
        assert saved.lease_owner is None
        assert saved.lease_token is None
        assert saved.lease_until is None
        assert saved.completed_at is None
        assert saved.attempt == original_fields[1]
    elif intent_status == "failed":
        assert len(rows) == 2
        assert (
            saved.status,
            saved.attempt,
            saved.available_at,
            saved.lease_owner,
            saved.lease_token,
            saved.lease_until,
            saved.last_error_type,
            saved.last_error_message,
            saved.completed_at,
        ) == original_fields
        retry = next(row for row in rows if row.id != saved.id)
        assert retry.idempotency_key == f"{intent_key}:retry:1"
        assert retry.source_key == source_key
        assert retry.status == "pending"
        assert retry.available_at <= datetime.now(UTC).replace(tzinfo=None)
        assert retry.lease_owner is None
        assert retry.lease_token is None
        assert retry.lease_until is None
        assert retry.completed_at is None
    else:
        assert len(rows) == 1
        assert (
            saved.status,
            saved.attempt,
            saved.available_at,
            saved.lease_owner,
            saved.lease_token,
            saved.lease_until,
            saved.last_error_type,
            saved.last_error_message,
            saved.completed_at,
        ) == original_fields


async def test_cleanup_delete_failure_advances_past_failed_compensation_generations(
    monkeypatch,
) -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    source_key = "generate_outputs/script-1/cleanup-failure-generations.mp4"
    intent_key = worker._delete_intent_key(source_key)
    completed_at = datetime.now(UTC) - timedelta(minutes=1)
    failed_rows = [
        MediaStorageJob(
            id=f"cleanup-generation-{generation}",
            idempotency_key=(
                intent_key if generation == 0 else f"{intent_key}:retry:{generation}"
            ),
            job_type="delete",
            source_key=source_key,
            status="failed",
            attempt=8,
            max_attempts=8,
            available_at=datetime.now(UTC) - timedelta(hours=1),
            last_error_type=f"StoredError{generation}",
            last_error_message="safe error",
            completed_at=completed_at,
        )
        for generation in range(2)
    ]
    raw.add_all(failed_rows)
    raw.commit()
    raw.expire_all()
    original_fields = {
        row.id: (
            row.status,
            row.attempt,
            row.last_error_type,
            row.last_error_message,
            row.completed_at,
        )
        for row in failed_rows
    }

    class FakeSessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            self.raw = Session(raw.get_bind(), expire_on_commit=False)
            return cast(AsyncSession, _AsyncSessionAdapter(self.raw))

        async def __aexit__(self, exc_type, exc, tb):
            self.raw.close()

    class Storage:
        async def delete(self, _key: str) -> None:
            raise RuntimeError("storage delete failed")

    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())

    await worker._cleanup_attempt_objects(Storage(), (intent_key,), source_key)

    raw.expire_all()
    rows = list(raw.scalars(select(MediaStorageJob).order_by(MediaStorageJob.idempotency_key)))
    assert len(rows) == 3
    for row_id, expected in original_fields.items():
        saved = raw.get(MediaStorageJob, row_id)
        assert saved is not None
        assert (
            saved.status,
            saved.attempt,
            saved.last_error_type,
            saved.last_error_message,
            saved.completed_at,
        ) == expected
    retry = next(row for row in rows if row.id not in original_fields)
    assert retry.idempotency_key == f"{intent_key}:retry:2"
    assert retry.source_key == source_key
    assert retry.status == "pending"
    assert retry.available_at <= datetime.now(UTC).replace(tzinfo=None)


async def test_authoritative_success_read_waits_for_inflight_commit(monkeypatch) -> None:
    from app.workers import timeline_render as worker

    read_started = asyncio.Event()
    commit_applied = asyncio.Event()
    attempt_key = "generate_outputs/script-1/render.attempt-1-lock.mp4"
    processing = SimpleNamespace(status="processing", output_storage_key="base.mp4")
    succeeded = SimpleNamespace(status="succeeded", output_storage_key=attempt_key)

    class Result:
        def __init__(self, row) -> None:
            self.row = row

        def scalar_one_or_none(self):
            return self.row

    class Db:
        async def execute(self, statement):
            read_started.set()
            if statement._for_update_arg is None:  # noqa: SLF001 - 模拟 PostgreSQL MVCC 锁语义
                return Result(processing)
            await commit_applied.wait()
            return Result(succeeded)

    class Factory:
        def __call__(self):
            return self

        async def __aenter__(self):
            return Db()

        async def __aexit__(self, exc_type, exc, tb):
            return None

    monkeypatch.setattr(worker, "async_session_factory", Factory())
    check = asyncio.create_task(
        worker._authoritative_attempt_succeeded("render-inflight-commit", attempt_key)
    )
    await read_started.wait()
    await asyncio.sleep(0)
    assert check.done() is False

    commit_applied.set()
    assert await check is True


@pytest.mark.parametrize("cancel_during_download", [False, True])
async def test_worker_streams_video_files_without_bytes_storage_api(
    monkeypatch,
    tmp_path,
    cancel_during_download: bool,
) -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    seed = _seed(raw)
    job_id = "render-stream-cancel" if cancel_during_download else "render-stream"
    output_key = f"generate_outputs/script-1/episode-1/{job_id}.mp4"
    raw.add(
        TimelineRenderJob(
            job_id=job_id,
            script_id=seed.script.id,
            episode_id=seed.episode.episode_id,
            owner_user_id=seed.owner.user_id,
            idempotency_key=f"idem-{job_id}",
            request_hash="hash",
            input_version=1,
            attempt=1,
            status="starting",
            progress=0,
            input_json={
                "segments": [
                    {"storage_key": "generate_images/mat/video-stream.mp4"}
                ]
            },
            output_storage_key=output_key,
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

    stored: dict[str, bytes] = {
        "generate_images/mat/video-stream.mp4": b"source-video"
    }
    iter_calls: list[tuple[str, int, int, int | None]] = []
    upload_paths: list[str] = []
    source_paths: list[str] = []

    class FakeStorage:
        async def get(self, _key: str) -> bytes:
            raise AssertionError("大视频下载不得调用 get(bytes)")

        async def put(self, _key: str, _data: bytes) -> str:
            raise AssertionError("最终视频上传不得调用 put(bytes)")

        async def iter_chunks(
            self,
            key: str,
            *,
            chunk_size: int,
            start: int,
            end: int | None,
        ):
            iter_calls.append((key, chunk_size, start, end))
            yield b"source-"
            if cancel_during_download:
                claimed = raw.get(TimelineRenderJob, job_id)
                assert claimed is not None
                claimed.status = "canceled"
                claimed.lease_owner = None
                claimed.lease_token = None
                claimed.leased_until = None
                raw.commit()
            yield b"video"

        async def put_file(self, key: str, path: str) -> str:
            upload_paths.append(path)
            stored[key] = Path(path).read_bytes()
            return key

        async def promote(self, source_key: str, final_key: str) -> str:
            stored[final_key] = stored.pop(source_key)
            return final_key

        async def delete(self, key: str) -> None:
            stored.pop(key, None)

    async def fake_trim(src, start, end, out, **_kwargs):
        assert Path(src).read_bytes() == b"source-video"
        source_paths.append(src)
        Path(out).write_bytes(b"trimmed")

    async def fake_concat(paths, out, **_kwargs):
        assert len(paths) == 1
        assert source_paths
        assert all(not Path(path).exists() for path in source_paths)
        Path(out).write_bytes(b"final-video")

    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())
    monkeypatch.setattr(worker, "get_storage", lambda: FakeStorage())
    monkeypatch.setattr(worker, "_trim_segment", fake_trim)
    monkeypatch.setattr(worker, "_concat_segments", fake_concat)

    result = await worker.run_render_job(job_id, owner="worker-a", temp_root=tmp_path)

    raw.expire_all()
    saved = raw.get(TimelineRenderJob, job_id)
    assert saved is not None
    assert len(iter_calls) == 1
    assert iter_calls[0][0] == "generate_images/mat/video-stream.mp4"
    assert iter_calls[0][2:] == (0, None)
    assert not any(tmp_path.iterdir())
    if cancel_during_download:
        assert result["status"] == "canceled"
        assert saved.status == "canceled"
        assert upload_paths == []
        assert output_key not in stored
    else:
        assert result["status"] == "succeeded"
        assert saved.status == "succeeded"
        assert saved.lease_token is None
        assert saved.output_storage_key != output_key
        assert ".attempt-1-" in saved.output_storage_key
        assert stored[saved.output_storage_key] == b"final-video"
        assert output_key not in stored
        assert len(upload_paths) == 1
        assert not Path(upload_paths[0]).exists()


async def test_worker_streaming_upload_error_cleans_partial_temp_object(
    monkeypatch,
    tmp_path,
) -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    seed = _seed(raw)
    job_id = "render-stream-upload-fail"
    output_key = f"generate_outputs/script-1/episode-1/{job_id}.mp4"
    raw.add(
        TimelineRenderJob(
            job_id=job_id,
            script_id=seed.script.id,
            episode_id=seed.episode.episode_id,
            owner_user_id=seed.owner.user_id,
            idempotency_key=f"idem-{job_id}",
            request_hash="hash",
            input_version=1,
            attempt=1,
            status="starting",
            progress=0,
            input_json={
                "segments": [
                    {"storage_key": "generate_images/mat/video-stream.mp4"}
                ]
            },
            output_storage_key=output_key,
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

    stored: dict[str, bytes] = {}
    upload_keys: list[str] = []
    deleted: list[str] = []

    class FakeStorage:
        async def get(self, _key: str) -> bytes:
            raise AssertionError("大视频下载不得调用 get(bytes)")

        async def put(self, _key: str, _data: bytes) -> str:
            raise AssertionError("最终视频上传不得调用 put(bytes)")

        async def iter_chunks(self, _key: str, **_kwargs):
            yield b"source-video"

        async def put_file(self, key: str, path: str) -> str:
            upload_keys.append(key)
            stored[key] = Path(path).read_bytes()
            raise RuntimeError("streaming upload failed")

        async def promote(self, _source_key: str, _final_key: str) -> str:
            raise AssertionError("上传失败后不得 promote")

        async def delete(self, key: str) -> None:
            deleted.append(key)
            stored.pop(key, None)

    async def fake_trim(src, start, end, out, **_kwargs):
        Path(out).write_bytes(Path(src).read_bytes())

    async def fake_concat(paths, out, **_kwargs):
        Path(out).write_bytes(Path(paths[0]).read_bytes())

    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())
    monkeypatch.setattr(worker, "get_storage", lambda: FakeStorage())
    monkeypatch.setattr(worker, "_trim_segment", fake_trim)
    monkeypatch.setattr(worker, "_concat_segments", fake_concat)

    result = await worker.run_render_job(job_id, owner="worker-a", temp_root=tmp_path)

    raw.expire_all()
    saved = raw.get(TimelineRenderJob, job_id)
    intents = list(raw.scalars(select(MediaStorageJob)))
    assert saved is not None
    assert result["status"] == "failed"
    assert saved.status == "failed"
    assert saved.lease_token is None
    assert len(upload_keys) == 1
    assert upload_keys[0].endswith(".tmp")
    assert upload_keys[0] in deleted
    assert output_key not in deleted
    assert len(intents) == 2
    assert {intent.source_key for intent in intents} == {
        upload_keys[0],
        upload_keys[0].removesuffix(".tmp"),
    }
    assert {intent.status for intent in intents} == {"pending"}
    now = datetime.now(UTC) + timedelta(seconds=1)
    assert all(
        (
            intent.available_at.replace(tzinfo=UTC)
            if intent.available_at.tzinfo is None
            else intent.available_at
        )
        <= now
        for intent in intents
    )
    assert stored == {}
    assert not any(tmp_path.iterdir())


async def test_timeline_stream_rejects_empty_source_and_removes_partial_file(tmp_path) -> None:
    from app.workers import timeline_render as worker

    class Storage:
        async def get(self, _key: str) -> bytes:
            raise AssertionError("存在流式接口时不得 fallback")

        async def iter_chunks(self, _key: str, **_kwargs):
            if False:
                yield b""

    local_path = tmp_path / "empty.mp4"
    with pytest.raises(ValueError, match="为空"):
        await worker._download_storage_file(
            Storage(),
            "generate_images/video/empty.mp4",
            str(local_path),
        )

    assert not local_path.exists()


async def test_timeline_upload_rejects_oversized_final_before_put_file(
    monkeypatch,
    tmp_path,
) -> None:
    from app.core.config import settings
    from app.workers import timeline_render as worker

    uploaded = False

    class Storage:
        async def put_file(self, _key: str, _path: str) -> str:
            nonlocal uploaded
            uploaded = True
            return "unexpected"

    final_path = tmp_path / "oversized-final.mp4"
    final_path.write_bytes(b"123456")
    monkeypatch.setattr(settings, "max_media_read_bytes", 5)

    with pytest.raises(ValueError, match="文件过大"):
        await worker._upload_storage_file(
            Storage(),
            "generate_outputs/script-1/oversized.mp4",
            str(final_path),
        )

    assert uploaded is False


async def test_timeline_rejects_cumulative_trimmed_size_before_concat(
    monkeypatch,
    tmp_path,
) -> None:
    from app.core.config import settings
    from app.workers import timeline_render as worker

    job = SimpleNamespace(
        input_json={
            "segments": [
                {"storage_key": "generate_images/video/one.mp4"},
                {"storage_key": "generate_images/video/two.mp4"},
            ]
        },
        output_storage_key="generate_outputs/script-1/render-limit.mp4",
        attempt=1,
    )
    concat_called = False
    upload_called = False

    async def load_claimed(*_args, **_kwargs):
        return job, "lease-token"

    async def active(*_args, **_kwargs):
        return "active"

    async def heartbeat(*_args, **_kwargs):
        return True

    async def finish(*_args, **_kwargs):
        return True

    class Storage:
        async def get(self, _key: str) -> bytes:
            raise AssertionError("大视频下载不得调用 get(bytes)")

        async def iter_chunks(self, _key: str, **_kwargs):
            yield b"x"

        async def put_file(self, _key: str, _path: str) -> str:
            nonlocal upload_called
            upload_called = True
            return "unexpected"

        async def delete(self, _key: str) -> None:
            return None

    async def fake_trim(_src, _start, _end, out, **_kwargs):
        Path(out).write_bytes(b"abc")

    async def fake_concat(_paths, _out, **_kwargs):
        nonlocal concat_called
        concat_called = True
        raise AssertionError("累计超限后不得 concat")

    monkeypatch.setattr(settings, "max_media_read_bytes", 5)
    monkeypatch.setattr(worker, "_load_claimed_job", load_claimed)
    monkeypatch.setattr(worker, "_runtime_state", active)
    monkeypatch.setattr(worker, "_heartbeat", heartbeat)
    monkeypatch.setattr(worker, "_finish", finish)
    monkeypatch.setattr(worker, "get_storage", lambda: Storage())
    monkeypatch.setattr(worker, "_trim_segment", fake_trim)
    monkeypatch.setattr(worker, "_concat_segments", fake_concat)

    result = await worker.run_render_job(
        "render-limit",
        owner="worker-a",
        temp_root=tmp_path,
    )

    assert result["status"] == "failed"
    assert concat_called is False
    assert upload_called is False
    assert not any(tmp_path.iterdir())


async def test_two_timeline_workers_keep_only_current_attempt_output(
    monkeypatch,
    tmp_path,
) -> None:
    from app.workers import timeline_render as worker

    _, raw = _make_db()
    seed = _seed(raw)
    job_id = "render-two-workers"
    base_key = f"generate_outputs/script-1/episode-1/{job_id}.mp4"
    raw.add(
        TimelineRenderJob(
            job_id=job_id,
            script_id=seed.script.id,
            episode_id=seed.episode.episode_id,
            owner_user_id=seed.owner.user_id,
            idempotency_key="idem-two-workers",
            request_hash="hash",
            input_version=1,
            attempt=1,
            status="starting",
            progress=0,
            input_json={
                "segments": [
                    {"storage_key": "generate_images/mat/video-stream.mp4"}
                ]
            },
            output_storage_key=base_key,
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

    stored: dict[str, bytes] = {}
    promotions: list[tuple[str, str]] = []
    deleted: list[str] = []

    class Storage:
        async def get(self, _key: str) -> bytes:
            raise AssertionError("大视频下载不得调用 get(bytes)")

        async def put(self, _key: str, _data: bytes) -> str:
            raise AssertionError("最终视频上传不得调用 put(bytes)")

        async def iter_chunks(self, _key: str, **_kwargs):
            yield b"source-video"

        async def put_file(self, key: str, path: str) -> str:
            stored[key] = Path(path).read_bytes()
            return key

        async def promote(self, source_key: str, final_key: str) -> str:
            promotions.append((source_key, final_key))
            stored[final_key] = stored.pop(source_key)
            if len(promotions) == 1:
                claimed = raw.get(TimelineRenderJob, job_id)
                assert claimed is not None
                claimed.lease_owner = "worker-b"
                claimed.lease_token = "replacement-token"
                raw.commit()
            return final_key

        async def delete(self, key: str) -> None:
            deleted.append(key)
            stored.pop(key, None)

    async def fake_trim(src, start, end, out, **_kwargs):
        Path(out).write_bytes(Path(src).read_bytes())

    async def fake_concat(paths, out, **_kwargs):
        Path(out).write_bytes(Path(paths[0]).read_bytes())

    storage = Storage()
    monkeypatch.setattr(worker, "async_session_factory", FakeSessionFactory())
    monkeypatch.setattr(worker, "get_storage", lambda: storage)
    monkeypatch.setattr(worker, "_trim_segment", fake_trim)
    monkeypatch.setattr(worker, "_concat_segments", fake_concat)

    stale_result = await worker.run_render_job(
        job_id,
        owner="worker-a",
        temp_root=tmp_path,
    )
    assert stale_result["status"] == "skipped"

    raw.expire_all()
    retry = raw.get(TimelineRenderJob, job_id)
    assert retry is not None
    retry.status = "starting"
    retry.lease_owner = None
    retry.lease_token = None
    retry.leased_until = None
    raw.commit()

    current_result = await worker.run_render_job(
        job_id,
        owner="worker-b",
        temp_root=tmp_path,
    )

    raw.expire_all()
    saved = raw.get(TimelineRenderJob, job_id)
    assert saved is not None
    assert current_result["status"] == "succeeded"
    assert len(promotions) == 2
    stale_final = promotions[0][1]
    current_final = promotions[1][1]
    assert stale_final != current_final
    assert all(".attempt-1-" in final_key for _, final_key in promotions)
    assert stale_final in deleted
    assert stale_final not in stored
    assert current_final in stored
    assert saved.output_storage_key == current_final
    assert current_result["storage_key"] == current_final
    assert base_key not in stored
    assert base_key not in deleted
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

    async def fake_trim(src, start, end, out, **_kwargs):
        Path(out).write_bytes(b"trimmed")

    async def fake_concat(paths, out, **_kwargs):
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
    assert saved.output_storage_key not in deleted
    assert any(".attempt-1-" in key and not key.endswith(".tmp") for key in deleted)


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
    render_job = TimelineRenderJob(
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
    outbox = TaskDispatchOutbox(
        id="dispatch-render-cancel-release-fail",
        task_type="timeline.render",
        job_id=render_job.job_id,
        attempt=render_job.attempt,
        payload={"job_id": render_job.job_id},
        status="running",
        lease_owner="dispatcher",
        lease_token="dispatch-token",
        lease_until=datetime.now(UTC) + timedelta(minutes=1),
    )
    raw.add_all([render_job, outbox])
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
    outbox = raw.get(TaskDispatchOutbox, "dispatch-render-cancel-release-fail")
    assert saved is not None and reservation is not None and outbox is not None
    assert canceled["status"] == "canceled"
    assert saved.status == "canceled"
    assert reservation.status == "reserved"
    assert saved.output_json is not None
    assert saved.output_json["financial_cleanup"]["status"] == "pending"
    assert saved.output_json["financial_cleanup"]["action"] == "release"
    assert outbox.status == "canceled"
    assert outbox.lease_owner is None
    assert outbox.lease_token is None
    assert outbox.lease_until is None

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

    async def fake_trim(src, start, end, out, **_kwargs):
        Path(out).write_bytes(b"trimmed")

    async def fake_concat(paths, out, **_kwargs):
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
    class RecordingSession(_AsyncSessionAdapter):
        def __init__(self, session: Session) -> None:
            super().__init__(session)
            self.statements: list[object] = []

        async def execute(self, statement):
            self.statements.append(statement)
            return await super().execute(statement)

    recording_db = RecordingSession(raw)
    db = cast(AsyncSession, recording_db)

    recovered = await worker.reconcile_stale_render_jobs(db, limit=10)

    assert recovered == 2
    processing = raw.get(TimelineRenderJob, "render-processing")
    assert processing is not None
    assert processing.status == "starting"
    assert processing.lease_token is None
    outboxes = raw.scalars(select(TaskDispatchOutbox)).all()
    assert len(outboxes) == 2
    assert all(row.status == "pending" for row in outboxes)
    statements = [
        str(statement.compile(dialect=postgresql.dialect()))
        for statement in recording_db.statements
    ]
    job_lock_index = next(
        index
        for index, statement in enumerate(statements)
        if "FROM timeline_render_jobs" in statement
        and "FOR UPDATE SKIP LOCKED" in statement
    )
    outbox_lock_index = next(
        index
        for index, statement in enumerate(statements)
        if "FROM task_dispatch_outbox" in statement and "FOR UPDATE" in statement
    )
    assert job_lock_index < outbox_lock_index


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
