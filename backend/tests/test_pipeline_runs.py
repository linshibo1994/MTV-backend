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


def test_retry_schema_requires_failed_run_and_manual_forbids_it() -> None:
    with pytest.raises(ValidationError, match="failed_run_id"):
        _body(mode=PipelineRunMode.RETRY_FAILED)
    with pytest.raises(ValidationError, match="failed_run_id"):
        _body(failed_run_id="failed-run")


def test_retry_validation_rejects_nonretryable_and_stale_input() -> None:
    with pytest.raises(ConflictError, match="不可重试"):
        pipeline_runs_service._validate_retry_source(
            [_job(retryable=False)], input_version=4, input_client_save_seq=9
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
    with pytest.raises(ConflictError, match="输入版本已过期"):
        pipeline_runs_service._validate_retry_source(
            [_job()], input_version=5, input_client_save_seq=10
        )


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


async def test_idempotent_replay_returns_before_active_run_check(monkeypatch) -> None:
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
    audit = AsyncMock()
    monkeypatch.setattr(pipeline_runs_service, "_acquire_script_lock", AsyncMock())
    monkeypatch.setattr(
        pipeline_runs_service,
        "_load_idempotent_result",
        AsyncMock(return_value=expected),
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
    reload_episode.assert_not_awaited()
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
    source = _job()
    old_snapshot = vars(source).copy()
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
        pipeline_runs_service, "_load_retry_source", AsyncMock(return_value=[source])
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
    assert vars(source) == old_snapshot
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
