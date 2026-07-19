"""流水线人工重新提取与失败重试专项测试。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from app.api.v1 import pipeline_runs as pipeline_runs_api
from app.core.errors import ConflictError, ForbiddenError
from app.core.permissions import Action
from app.models.pipeline_job import PipelineJob
from app.schemas.pipeline_runs import PipelineRunCreateIn, PipelineRunMode, PipelineRunOut
from app.services import pipeline_runs as pipeline_runs_service
from app.services.pipeline import STAGE_ORDER


class _Session:
    def __init__(self) -> None:
        self.added: list[object] = []
        self.flush = AsyncMock()
        self.commit = AsyncMock()
        self.rollback = AsyncMock()

    def add(self, row: object) -> None:
        self.added.append(row)


class _RowsResult:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows

    def scalars(self) -> "_RowsResult":
        return self

    def all(self) -> list[object]:
        return self.rows


def _body(**overrides) -> PipelineRunCreateIn:
    values = {
        "mode": PipelineRunMode.MANUAL_REEXTRACT,
        "episode_version": 4,
        "client_save_seq": 9,
    }
    values.update(overrides)
    return PipelineRunCreateIn(**values)


def _job(**overrides) -> SimpleNamespace:
    values = {
        "id": "old-job",
        "script_id": "script-1",
        "episode_id": "episode-1",
        "run_id": "failed-run",
        "dispatch_id": "dispatch-1",
        "stage": "extract",
        "status": "failed",
        "attempt": 2,
        "input_version": 4,
        "input_client_save_seq": 9,
        "retryable": True,
        "error_type": "provider_timeout",
        "error_message": "供应商暂时不可用",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _valid_history(
    *,
    run_id: str = "failed-run",
    baseline_attempt: int = 2,
) -> list[SimpleNamespace]:
    return [
        _job(
            id=f"{stage}-{baseline_attempt}",
            run_id=run_id,
            stage=stage,
            status="done",
            attempt=baseline_attempt,
            retryable=None,
            error_type=None,
            error_message=None,
        )
        for stage in STAGE_ORDER
    ]


def test_retry_schema_requires_failed_run_and_manual_forbids_it() -> None:
    with pytest.raises(ValidationError, match="failed_run_id"):
        _body(mode=PipelineRunMode.RETRY_FAILED)
    with pytest.raises(ValidationError, match="failed_run_id"):
        _body(failed_run_id="failed-run")


def test_retry_validation_rejects_nonretryable_and_stale_input() -> None:
    jobs = _valid_history()
    jobs[3].status = "failed"
    jobs[3].retryable = False
    jobs[4].status = "ready"
    with pytest.raises(ConflictError, match="不可重试"):
        pipeline_runs_service._validate_retry_source(
            jobs, input_version=4, input_client_save_seq=9
        )

    jobs[3].retryable = True
    with pytest.raises(ConflictError, match="输入版本已过期"):
        pipeline_runs_service._validate_retry_source(
            jobs, input_version=5, input_client_save_seq=10
        )


def test_replay_status_uses_latest_attempts() -> None:
    jobs = [
        _job(stage="split", status="failed", attempt=1),
        _job(stage="split", status="done", attempt=2),
        _job(stage="extract", status="running", attempt=2),
    ]
    assert pipeline_runs_service._derive_run_status(jobs) == "running"
    jobs[2].status = "failed"
    assert pipeline_runs_service._derive_run_status(jobs) == "failed"


@pytest.mark.parametrize(
    "invalid_history",
    [
        "missing-baseline",
        "below-baseline",
        "duplicate-attempt",
        "attempt-gap",
        "scope-mismatch",
    ],
)
def test_replay_job_history_rejects_invalid_retry_sequences(
    invalid_history: str,
) -> None:
    def history_job(stage: str, attempt: int, **overrides) -> SimpleNamespace:
        values = {
            "id": f"{stage}-{attempt}",
            "script_id": "script-1",
            "episode_id": "episode-1",
            "run_id": "run-1",
            "stage": stage,
            "dispatch_id": "dispatch-1",
            "status": "done",
            "attempt": attempt,
            "input_version": 4,
            "input_client_save_seq": 9,
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    jobs = [history_job(stage, 2) for stage in STAGE_ORDER]
    assert pipeline_runs_service._pipeline_job_history_is_valid(
        jobs,
        script_id="script-1",
        episode_id="episode-1",
        run_id="run-1",
        dispatch_id="dispatch-1",
        baseline_attempt=2,
        input_version=4,
        input_client_save_seq=9,
    )

    retry_stage = STAGE_ORDER[0]
    if invalid_history == "missing-baseline":
        jobs[0].attempt = 3
    elif invalid_history == "below-baseline":
        jobs.append(history_job(retry_stage, 1))
    elif invalid_history == "duplicate-attempt":
        jobs.append(history_job(retry_stage, 2, id="duplicate"))
    elif invalid_history == "attempt-gap":
        jobs.append(history_job(retry_stage, 4))
    else:
        jobs.append(history_job(retry_stage, 3, dispatch_id="other-dispatch"))

    assert not pipeline_runs_service._pipeline_job_history_is_valid(
        jobs,
        script_id="script-1",
        episode_id="episode-1",
        run_id="run-1",
        dispatch_id="dispatch-1",
        baseline_attempt=2,
        input_version=4,
        input_client_save_seq=9,
    )


@pytest.mark.parametrize(
    "invalid_history",
    [
        "unknown-stage",
        "missing-stage",
        "done-before-retry",
        "nonretryable-before-retry",
        "unknown-status",
        "nonleaf-ready",
        "extract-before-split",
        "merge-before-extract",
        "leaf-before-merge",
        "single-leaf-done",
    ],
)
def test_pipeline_history_rejects_impossible_worker_state(
    invalid_history: str,
) -> None:
    jobs = _valid_history(run_id="run-1")

    if invalid_history == "unknown-stage":
        jobs.append(_job(id="unknown-2", run_id="run-1", stage="unknown"))
    elif invalid_history == "missing-stage":
        jobs = [job for job in jobs if job.stage != "parents"]
    elif invalid_history in {"done-before-retry", "nonretryable-before-retry"}:
        source = next(job for job in jobs if job.stage == "enrich")
        source.status = "done" if invalid_history == "done-before-retry" else "failed"
        source.retryable = invalid_history == "done-before-retry"
        jobs.append(
            _job(
                id="enrich-3",
                run_id="run-1",
                stage="enrich",
                status="running",
                attempt=3,
                retryable=None,
                error_type=None,
                error_message=None,
            )
        )
        next(job for job in jobs if job.stage == "parents").status = "ready"
    elif invalid_history == "unknown-status":
        jobs[0].status = "retry"
    elif invalid_history == "nonleaf-ready":
        jobs[1].status = "ready"
    elif invalid_history == "extract-before-split":
        jobs[0].status = "pending"
        jobs[1].status = "running"
        for job in jobs[2:]:
            job.status = "pending"
    elif invalid_history == "merge-before-extract":
        jobs[1].status = "pending"
        jobs[2].status = "failed"
        jobs[2].retryable = True
        for job in jobs[3:]:
            job.status = "pending"
    elif invalid_history == "leaf-before-merge":
        jobs[2].status = "pending"
        jobs[3].status = "running"
        jobs[4].status = "pending"
    else:
        jobs[4].status = "ready"

    assert not pipeline_runs_service._pipeline_job_history_is_valid(
        jobs,
        script_id="script-1",
        episode_id="episode-1",
        run_id="run-1",
        dispatch_id="dispatch-1",
        baseline_attempt=2,
        input_version=4,
        input_client_save_seq=9,
    )


def test_retry_source_reuses_authoritative_history_validation() -> None:
    jobs = _valid_history()
    enrich = next(job for job in jobs if job.stage == "enrich")
    parents = next(job for job in jobs if job.stage == "parents")
    enrich.status = "failed"
    enrich.retryable = True
    parents.status = "ready"

    assert (
        pipeline_runs_service._validate_retry_source(
            jobs,
            input_version=4,
            input_client_save_seq=9,
        )
        == 3
    )

    enrich.status = "done"
    jobs.append(
        _job(
            id="enrich-3",
            stage="enrich",
            status="failed",
            attempt=3,
            retryable=True,
        )
    )
    with pytest.raises(ConflictError, match="历史已失效"):
        pipeline_runs_service._validate_retry_source(
            jobs,
            input_version=4,
            input_client_save_seq=9,
        )


def test_pipeline_history_requires_prerequisites_for_any_execution_evidence() -> None:
    invalid = _valid_history()
    for job in invalid:
        job.status = "pending"
        job.retryable = None
    split = next(job for job in invalid if job.stage == "split")
    extract = next(job for job in invalid if job.stage == "extract")
    split.status = "failed"
    split.retryable = True
    extract.status = "failed"
    extract.retryable = True
    invalid.append(
        _job(
            id="extract-3",
            stage="extract",
            status="pending",
            attempt=3,
            retryable=None,
            error_type=None,
            error_message=None,
        )
    )

    assert not pipeline_runs_service._pipeline_job_history_is_valid(
        invalid,
        script_id="script-1",
        episode_id="episode-1",
        run_id="failed-run",
        dispatch_id="dispatch-1",
        baseline_attempt=2,
        input_version=4,
        input_client_save_seq=9,
    )
    with pytest.raises(ConflictError, match="历史已失效"):
        pipeline_runs_service._validate_retry_source(
            invalid,
            input_version=4,
            input_client_save_seq=9,
        )

    waiting = _valid_history(run_id="waiting-run")
    for job in waiting:
        job.status = "pending"
        job.retryable = None
    assert pipeline_runs_service._pipeline_job_history_is_valid(
        waiting,
        script_id="script-1",
        episode_id="episode-1",
        run_id="waiting-run",
        dispatch_id="dispatch-1",
        baseline_attempt=2,
        input_version=4,
        input_client_save_seq=9,
    )

    legal_retry = _valid_history(run_id="retry-run")
    for job in legal_retry:
        job.status = "pending"
        job.retryable = None
    next(job for job in legal_retry if job.stage == "split").status = "done"
    extract = next(job for job in legal_retry if job.stage == "extract")
    extract.status = "failed"
    extract.retryable = True
    legal_retry.append(
        _job(
            id="extract-retry-3",
            run_id="retry-run",
            stage="extract",
            status="pending",
            attempt=3,
            retryable=None,
            error_type=None,
            error_message=None,
        )
    )
    assert pipeline_runs_service._pipeline_job_history_is_valid(
        legal_retry,
        script_id="script-1",
        episode_id="episode-1",
        run_id="retry-run",
        dispatch_id="dispatch-1",
        baseline_attempt=2,
        input_version=4,
        input_client_save_seq=9,
    )


async def test_active_detection_ignores_failed_pending_leftovers_but_not_running() -> None:
    failed = _job(stage="split", status="failed", attempt=1)
    leftover = _job(stage="extract", status="pending", attempt=1)
    db = SimpleNamespace(execute=AsyncMock(return_value=_RowsResult([failed, leftover])))
    assert await pipeline_runs_service._find_active_run(db, "script-1") is None

    sibling_running = _job(stage="parents", status="running", attempt=1)
    db.execute = AsyncMock(
        return_value=_RowsResult([failed, leftover, sibling_running])
    )
    assert await pipeline_runs_service._find_active_run(db, "script-1") == "failed-run"


async def test_route_requires_run_ai_and_blocks_cross_resource(monkeypatch) -> None:
    db = SimpleNamespace()
    user = SimpleNamespace(user_id="user-1")
    script = SimpleNamespace(id="script-1")
    access = AsyncMock()
    create = AsyncMock()
    monkeypatch.setattr(
        pipeline_runs_api.script_crud,
        "get_script",
        AsyncMock(return_value=script),
    )
    monkeypatch.setattr(pipeline_runs_api, "check_script_access", access)
    monkeypatch.setattr(
        pipeline_runs_api.script_crud,
        "get_episode",
        AsyncMock(side_effect=ForbiddenError("无权访问该分集")),
    )
    monkeypatch.setattr(pipeline_runs_api, "create_pipeline_run", create)

    with pytest.raises(ForbiddenError):
        await pipeline_runs_api.create_manual_pipeline_run(
            "script-1",
            "episode-other",
            _body(),
            "idem-1",
            db,
            user,
        )

    assert access.await_args.kwargs["action"] is Action.RUN_AI
    create.assert_not_awaited()


async def test_route_propagates_viewer_denial_before_creating_run(monkeypatch) -> None:
    db = SimpleNamespace()
    user = SimpleNamespace(user_id="viewer")
    monkeypatch.setattr(
        pipeline_runs_api.script_crud,
        "get_script",
        AsyncMock(return_value=SimpleNamespace(id="script-1")),
    )
    monkeypatch.setattr(
        pipeline_runs_api,
        "check_script_access",
        AsyncMock(side_effect=ForbiddenError("只读成员无权运行 AI 任务")),
    )
    create = AsyncMock()
    monkeypatch.setattr(pipeline_runs_api, "create_pipeline_run", create)

    with pytest.raises(ForbiddenError, match="只读成员"):
        await pipeline_runs_api.create_manual_pipeline_run(
            "script-1",
            "episode-1",
            _body(),
            "idem-viewer",
            db,
            user,
        )
    create.assert_not_awaited()


async def test_idempotent_replay_reloads_episode_before_active_run_check(
    monkeypatch,
) -> None:
    db = _Session()
    expected = PipelineRunOut(
        script_id="script-1",
        episode_id="episode-1",
        run_id="existing-run",
        dispatch_id="dispatch-1",
        reservation_id="reservation-1",
        mode=PipelineRunMode.MANUAL_REEXTRACT,
        retry_of_run_id=None,
        attempt=1,
        input_version=4,
        input_client_save_seq=9,
        estimated=10,
        idempotent_replay=True,
    )
    reload_episode = AsyncMock()
    load_idempotent_result = AsyncMock(return_value=expected)
    audit = AsyncMock()
    monkeypatch.setattr(pipeline_runs_service, "_acquire_script_lock", AsyncMock())
    monkeypatch.setattr(
        pipeline_runs_service,
        "_load_idempotent_result",
        load_idempotent_result,
    )
    monkeypatch.setattr(pipeline_runs_service, "_reload_episode", reload_episode)
    monkeypatch.setattr(pipeline_runs_service, "audit_log", audit)

    actual = await pipeline_runs_service.create_pipeline_run(
        db,
        script=SimpleNamespace(id="script-1"),
        episode_id="episode-1",
        actor=SimpleNamespace(user_id="user-1"),
        body=_body(),
        idempotency_key="same-key",
    )

    assert actual == expected
    reload_episode.assert_awaited_once_with(
        db,
        script_id="script-1",
        episode_id="episode-1",
    )
    assert load_idempotent_result.await_args.kwargs["script_id"] == "script-1"
    assert load_idempotent_result.await_args.kwargs["episode_id"] == "episode-1"
    audit.assert_not_awaited()
    db.commit.assert_awaited_once()


async def test_active_run_and_version_fence_return_conflict(monkeypatch) -> None:
    db = _Session()
    monkeypatch.setattr(pipeline_runs_service, "_acquire_script_lock", AsyncMock())
    monkeypatch.setattr(
        pipeline_runs_service,
        "_load_idempotent_result",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        pipeline_runs_service,
        "_reload_episode",
        AsyncMock(
            return_value=SimpleNamespace(
                episode_version=4,
                client_save_seq=9,
            )
        ),
    )
    active = AsyncMock(return_value="active-run")
    monkeypatch.setattr(pipeline_runs_service, "_find_active_run", active)

    with pytest.raises(ConflictError, match="活动 run_id=active-run"):
        await pipeline_runs_service.create_pipeline_run(
            db,
            script=SimpleNamespace(id="script-1"),
            episode_id="episode-1",
            actor=SimpleNamespace(user_id="user-1"),
            body=_body(),
            idempotency_key="new-key",
        )
    db.rollback.assert_awaited_once()

    db.rollback.reset_mock()
    with pytest.raises(ConflictError, match="分集版本已变化"):
        await pipeline_runs_service.create_pipeline_run(
            db,
            script=SimpleNamespace(id="script-1"),
            episode_id="episode-1",
            actor=SimpleNamespace(user_id="user-1"),
            body=_body(episode_version=3),
            idempotency_key="stale-key",
        )
    active.assert_awaited_once()
    db.rollback.assert_awaited_once()


async def test_retry_creates_new_run_attempt_reservation_outbox_without_mutating_old(
    monkeypatch,
) -> None:
    db = _Session()
    source_jobs = _valid_history()
    source_jobs[3].status = "failed"
    source_jobs[3].retryable = True
    source_jobs[4].status = "ready"
    old_snapshots = [vars(source).copy() for source in source_jobs]
    episode = SimpleNamespace(
        episode_id="episode-1",
        script_id="script-1",
        episode_version=4,
        client_save_seq=9,
    )
    script = SimpleNamespace(id="script-1", owner_user_id="owner-1")
    actor = SimpleNamespace(user_id="editor-1", username="alice")
    reserve = AsyncMock(return_value="reservation-new")
    enqueue = AsyncMock(return_value=SimpleNamespace(id="dispatch-new"))
    audit = AsyncMock()
    monkeypatch.setattr(pipeline_runs_service, "_acquire_script_lock", AsyncMock())
    monkeypatch.setattr(
        pipeline_runs_service,
        "_load_idempotent_result",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        pipeline_runs_service, "_reload_episode", AsyncMock(return_value=episode)
    )
    monkeypatch.setattr(
        pipeline_runs_service, "_find_active_run", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        pipeline_runs_service,
        "_load_retry_source",
        AsyncMock(return_value=source_jobs),
    )
    monkeypatch.setattr(
        pipeline_runs_service,
        "_load_credit_account",
        AsyncMock(return_value=SimpleNamespace(id="account-1")),
    )
    monkeypatch.setattr(pipeline_runs_service, "_estimate_cost", AsyncMock(return_value=17))
    monkeypatch.setattr(pipeline_runs_service, "reserve", reserve)
    monkeypatch.setattr(pipeline_runs_service, "enqueue_task", enqueue)
    monkeypatch.setattr(pipeline_runs_service, "audit_log", audit)
    monkeypatch.setattr(
        pipeline_runs_service, "_reset_pipeline_stages", AsyncMock()
    )

    result = await pipeline_runs_service.create_pipeline_run(
        db,
        script=script,
        episode_id="episode-1",
        actor=actor,
        body=_body(mode=PipelineRunMode.RETRY_FAILED, failed_run_id="failed-run"),
        idempotency_key="retry-secret-key",
    )

    assert result.run_id != "failed-run"
    assert result.retry_of_run_id == "failed-run"
    assert result.attempt == 3
    assert [vars(source) for source in source_jobs] == old_snapshots
    new_jobs = [row for row in db.added if isinstance(row, PipelineJob)]
    assert {row.stage for row in new_jobs} == set(STAGE_ORDER)
    assert {row.attempt for row in new_jobs} == {3}
    assert {row.run_id for row in new_jobs} == {result.run_id}
    assert reserve.await_args.kwargs["source_id"] == result.run_id
    assert reserve.await_args.kwargs["commit"] is False
    outbox_payload = enqueue.await_args.kwargs["payload"]
    assert outbox_payload["retry_of_run_id"] == "failed-run"
    assert outbox_payload["pipeline_attempt"] == 3
    assert outbox_payload["idempotency_key_hash"] != "retry-secret-key"
    assert "retry-secret-key" not in str(outbox_payload)
    assert audit.await_args.kwargs["action"] == "pipeline_retry_failed"
    assert audit.await_args.kwargs["before"] == {"run_id": "failed-run"}
    assert audit.await_args.kwargs["after"]["reservation_id"] == "reservation-new"
    assert audit.await_args.kwargs["after"]["dispatch_id"] == "dispatch-new"
    db.commit.assert_awaited_once()
    db.rollback.assert_not_awaited()


async def test_manual_reextract_creates_fresh_attempt_and_manual_audit(monkeypatch) -> None:
    db = _Session()
    audit = AsyncMock()
    monkeypatch.setattr(pipeline_runs_service, "_acquire_script_lock", AsyncMock())
    monkeypatch.setattr(
        pipeline_runs_service,
        "_load_idempotent_result",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        pipeline_runs_service,
        "_reload_episode",
        AsyncMock(
            return_value=SimpleNamespace(
                episode_version=4,
                client_save_seq=9,
            )
        ),
    )
    monkeypatch.setattr(
        pipeline_runs_service, "_find_active_run", AsyncMock(return_value=None)
    )
    retry_source = AsyncMock()
    monkeypatch.setattr(pipeline_runs_service, "_load_retry_source", retry_source)
    monkeypatch.setattr(
        pipeline_runs_service,
        "_load_credit_account",
        AsyncMock(return_value=SimpleNamespace(id="account-1")),
    )
    monkeypatch.setattr(pipeline_runs_service, "_estimate_cost", AsyncMock(return_value=5))
    monkeypatch.setattr(
        pipeline_runs_service, "reserve", AsyncMock(return_value="reservation-manual")
    )
    monkeypatch.setattr(
        pipeline_runs_service,
        "enqueue_task",
        AsyncMock(return_value=SimpleNamespace(id="dispatch-manual")),
    )
    monkeypatch.setattr(
        pipeline_runs_service, "_reset_pipeline_stages", AsyncMock()
    )
    monkeypatch.setattr(pipeline_runs_service, "audit_log", audit)

    result = await pipeline_runs_service.create_pipeline_run(
        db,
        script=SimpleNamespace(id="script-1", owner_user_id="owner-1"),
        episode_id="episode-1",
        actor=SimpleNamespace(user_id="editor-1", username="alice"),
        body=_body(),
        idempotency_key="manual-key",
    )

    assert result.attempt == 1
    assert result.retry_of_run_id is None
    retry_source.assert_not_awaited()
    assert audit.await_args.kwargs["action"] == "pipeline_manual_reextract"
    assert audit.await_args.kwargs["before"] is None
