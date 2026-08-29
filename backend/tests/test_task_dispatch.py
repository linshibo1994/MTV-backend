import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

from sqlalchemy import UniqueConstraint, create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.core.job_state import LeaseState, claim_lease, complete_lease, heartbeat_lease
from app.models.credit_reservation import CreditReservation
from app.models.pipeline_job import PipelineJob
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.services import task_dispatch as task_dispatch_service
from app.services.task_dispatch import (
    backoff_seconds,
    dispatch_outbox_batch,
    enqueue_task,
)
from app.workers import outbox as outbox_worker
from app.workers.outbox import build_pipeline_canvas, celery_app, publish_outbox_payload


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

    def add(self, row) -> None:
        self.session.add(row)

    def begin_nested(self):
        return _AsyncNested(self.session.begin_nested())


def test_outbox_has_idempotency_constraint_and_exponential_backoff() -> None:
    constraints = [
        constraint
        for constraint in TaskDispatchOutbox.__table__.constraints
        if isinstance(constraint, UniqueConstraint)
    ]
    assert any(
        {column.name for column in constraint.columns} == {"task_type", "job_id", "attempt"}
        for constraint in constraints
    )
    assert backoff_seconds(1) == 1
    assert backoff_seconds(4) == 8


def test_outbox_dispatch_is_actually_installed_in_beat_schedule() -> None:
    schedule = celery_app.conf.beat_schedule["outbox-dispatch"]
    assert schedule["task"] == "outbox.dispatch"
    assert 0 < schedule["schedule"] <= 30


def test_pipeline_reconciler_is_installed_in_beat_schedule() -> None:
    schedule = celery_app.conf.beat_schedule["pipeline-dispatch-reconcile"]
    assert schedule["task"] == "outbox.reconcile-pipeline"
    assert 0 < schedule["schedule"] <= 60


def test_generation_worker_and_outbox_are_registered_in_celery() -> None:
    includes = set(celery_app.conf.include)
    assert "app.workers.generation" in includes
    assert "app.workers.outbox" in includes
    schedule = celery_app.conf.beat_schedule["generation-financial-cleanup"]
    assert schedule["task"] == "generation.reconcile-financial-cleanup"
    assert 0 < schedule["schedule"] <= 120


def test_generation_outbox_row_builds_real_task_signature_without_broker(monkeypatch) -> None:
    class _GenerationTask:
        def s(self, job_id: str):
            return {"task": "generation.generate_images", "args": (job_id,)}

    signature = outbox_worker.build_generation_canvas(
        {"job_id": "job-1", "dispatch_id": "dispatch-1", "run_id": "job-1"},
        _GenerationTask(),
    )
    assert signature == {"task": "generation.generate_images", "args": ("job-1",)}

    published: list[object] = []

    class _Signature:
        def apply_async(self):
            published.append(self)
            return "async-result"

    class _Task:
        def s(self, job_id: str):
            assert job_id == "job-1"
            return _Signature()

    import app.workers.generation as generation_worker

    monkeypatch.setattr(generation_worker, "generate_images_task", _Task())
    assert publish_outbox_payload(
        "generation.generate_images",
        {"job_id": "job-1", "dispatch_id": "dispatch-1", "run_id": "job-1"},
    ) == "async-result"
    assert len(published) == 1


def test_generation_outbox_real_row_dispatches_and_marks_published(monkeypatch) -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        TaskDispatchOutbox.__table__.create(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            row = await enqueue_task(
                db,
                task_type="generation.generate_images",
                job_id="job-real",
                attempt=1,
                payload={"job_id": "job-real"},
            )
            await db.commit()
            applied: list[str] = []

            class _Signature:
                def __init__(self, job_id: str) -> None:
                    self.job_id = job_id

                def apply_async(self):
                    applied.append(self.job_id)

            class _Task:
                def s(self, job_id: str):
                    return _Signature(job_id)

            import app.workers.generation as generation_worker

            monkeypatch.setattr(generation_worker, "generate_images_task", _Task())
            assert await dispatch_outbox_batch(db, publish_outbox_payload, owner="worker") == 1
            assert applied == ["job-real"]
            assert raw_session.get(TaskDispatchOutbox, row.id).status == "published"

    asyncio.run(scenario())


def test_expired_lease_can_be_reclaimed_but_old_token_cannot_complete() -> None:
    now = datetime(2026, 7, 17, tzinfo=UTC)
    state = LeaseState(status="pending")

    first = claim_lease(state, "worker-a", now, timedelta(seconds=10))
    assert first is not None
    assert state.status == "running"
    assert heartbeat_lease(state, first, now + timedelta(seconds=1), timedelta(seconds=10))

    second = claim_lease(state, "worker-b", now + timedelta(seconds=12), timedelta(seconds=10))
    assert second is not None and second != first
    assert not complete_lease(state, first, "done")
    assert complete_lease(state, second, "done")
    assert state.status == "done"


class _Result:
    def __init__(self, row) -> None:
        self.row = row

    def scalar_one_or_none(self):
        return self.row


class _FakeSession:
    def __init__(self, row) -> None:
        self.row = row
        self.available = True
        self.events: list[str] = []

    async def execute(self, query):
        self.events.append("select")
        if self.available:
            self.available = False
            return _Result(self.row)
        return _Result(None)

    async def get(self, model, row_id, **kwargs):
        return self.row

    async def commit(self) -> None:
        self.events.append("commit")


def test_dispatch_commits_before_publish_and_expired_delivery_is_republished() -> None:
    async def dispatch_once(status: str, until: datetime | None) -> tuple[object, list[str]]:
        row = TaskDispatchOutbox(
            id="outbox-1",
            task_type="pipeline.dag",
            job_id="job-1",
            attempt=1,
            payload={
                "script_id": "script",
                "dispatch_id": "outbox-1",
                "run_id": "job-1",
            },
            status=status,
            publish_attempts=0,
            lease_until=until,
        )
        db = _FakeSession(row)
        events = db.events

        def publisher(task_type: str, payload: dict) -> None:
            events.append("publish")

        assert await dispatch_outbox_batch(db, publisher, owner="worker", limit=1) == 1
        return row, events

    first, first_events = asyncio.run(dispatch_once("pending", None))
    retry, retry_events = asyncio.run(
        dispatch_once("running", datetime(2026, 1, 1, tzinfo=UTC))
    )
    assert first.status == retry.status == "published"
    assert first_events.index("commit") < first_events.index("publish")
    assert retry_events.count("publish") == 1
    assert first.payload == retry.payload


def test_outbox_real_transaction_reuses_stable_dispatch_and_publishes_once() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        TaskDispatchOutbox.__table__.create(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            first = await enqueue_task(
                db,
                task_type="pipeline.dag",
                job_id="run-1",
                attempt=3,
                payload={"script_id": "script-1", "episode_id": "episode-1"},
            )
            duplicate = await enqueue_task(
                db,
                task_type="pipeline.dag",
                job_id="run-1",
                attempt=3,
                payload={"script_id": "changed", "episode_id": "changed"},
            )
            await db.commit()
            assert duplicate.id == first.id
            assert duplicate.payload["dispatch_id"] == first.id
            assert duplicate.payload["run_id"] == "run-1"
            assert duplicate.payload["script_id"] == "script-1"

            published: list[dict] = []

            def publisher(task_type: str, payload: dict) -> None:
                published.append(payload)

            assert await dispatch_outbox_batch(db, publisher, owner="worker-1") == 1
            assert await dispatch_outbox_batch(db, publisher, owner="worker-1") == 0
            assert published == [first.payload]
            assert raw_session.get(TaskDispatchOutbox, first.id).status == "published"

    asyncio.run(scenario())


def test_enqueue_task_persists_explicit_available_at() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        TaskDispatchOutbox.__table__.create(engine)
        scheduled_at = datetime(2026, 7, 23, 3, 16, 32, tzinfo=UTC)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            row = await enqueue_task(
                db,
                task_type="generation.generate_images",
                job_id="job-scheduled",
                attempt=1,
                payload={"job_id": "job-scheduled"},
                available_at=scheduled_at,
            )
            await db.commit()

            persisted = raw_session.get(TaskDispatchOutbox, row.id)
            assert persisted is not None
            assert persisted.available_at.replace(tzinfo=UTC) == scheduled_at

    asyncio.run(scenario())


class _SignatureTask:
    def __init__(self, name: str) -> None:
        self.name = name

    def s(self, *args, **kwargs):
        return self.name, args, kwargs


def test_pipeline_canvas_uses_chain_group_and_unique_finalizer_without_broker() -> None:
    seen: dict[str, object] = {}

    def fake_chain(*items):
        seen["chain"] = items
        return items

    def fake_group(*items):
        seen["group"] = items
        return "group", items

    def fake_chord(header, body):
        seen["chord"] = (header, body)
        return "chord", header, body

    canvas = build_pipeline_canvas(
        {
            "script_id": "script",
            "episode_id": "episode",
            "input_version": 2,
            "reservation_id": "reservation",
            "estimated": 3,
            "dispatch_id": "dispatch",
            "run_id": "run",
        },
        _SignatureTask("split"),
        _SignatureTask("extract"),
        _SignatureTask("merge"),
        _SignatureTask("enrich"),
        _SignatureTask("parents"),
        _SignatureTask("finalize"),
        chain_factory=fake_chain,
        group_factory=fake_group,
        chord_factory=fake_chord,
    )
    assert canvas == seen["chain"]
    assert [item[0] for item in seen["group"]] == ["enrich", "parents"]
    assert seen["chord"][1][0] == "finalize"
    assert seen["chain"][-1][0] == "chord"
    for signature in (
        seen["chain"][0],
        seen["chain"][1],
        seen["chain"][2],
        *seen["group"],
    ):
        args = signature[1]
        assert "dispatch" in args
        assert "run" in args
        assert "episode" in args
        # 请求级 LLM 路由键贯穿每个阶段任务；历史 payload 未配置时为 None。
        kwargs = signature[2]
        assert kwargs == {"llm_provider": None, "llm_model": None}


def test_pipeline_canvas_forwards_request_llm_selection_to_every_stage() -> None:
    """请求级 provider/model 必须随 canvas 关键字参数贯穿全部阶段。"""
    seen: dict[str, object] = {}

    def fake_chain(*items):
        seen["chain"] = items
        return items

    def fake_group(*items):
        seen["group"] = items
        return "group", items

    def fake_chord(header, body):
        seen["chord"] = (header, body)
        return "chord", header, body

    build_pipeline_canvas(
        {
            "script_id": "script",
            "episode_id": "episode",
            "input_version": 2,
            "reservation_id": "reservation",
            "estimated": 3,
            "dispatch_id": "dispatch",
            "run_id": "run",
            "llm_provider": "bailian",
            "llm_model": "qwen-max",
        },
        _SignatureTask("split"),
        _SignatureTask("extract"),
        _SignatureTask("merge"),
        _SignatureTask("enrich"),
        _SignatureTask("parents"),
        _SignatureTask("finalize"),
        chain_factory=fake_chain,
        group_factory=fake_group,
        chord_factory=fake_chord,
    )
    for signature in (
        seen["chain"][0],
        seen["chain"][1],
        seen["chain"][2],
        *seen["group"],
        seen["chord"][1],
    ):
        assert signature[2] == {"llm_provider": "bailian", "llm_model": "qwen-max"}


def test_broker_failure_persists_only_stable_sanitized_error() -> None:
    row = TaskDispatchOutbox(
        id="outbox-secret",
        task_type="pipeline.dag",
        job_id="run-secret",
        attempt=1,
        payload={"dispatch_id": "outbox-secret", "run_id": "run-secret"},
        status="pending",
        publish_attempts=0,
    )
    db = _FakeSession(row)

    def publisher(task_type: str, payload: dict) -> None:
        raise RuntimeError("broker response Authorization=secret-token")

    assert asyncio.run(
        dispatch_outbox_batch(db, publisher, owner="worker", limit=1)
    ) == 0
    assert row.status == "retry"
    assert row.last_error == "broker_unavailable: 消息队列暂不可用"
    assert "secret" not in row.last_error


def test_reconciler_recovers_published_half_dag_and_missing_outbox() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        PipelineJob.__table__.create(engine)
        CreditReservation.__table__.create(engine)
        TaskDispatchOutbox.__table__.create(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            published = TaskDispatchOutbox(
                id="dispatch-existing",
                task_type="pipeline.dag",
                job_id="run-existing",
                attempt=3,
                payload={
                    "script_id": "script-1",
                    "episode_id": "episode-1",
                    "input_version": 3,
                    "reservation_id": "reservation-existing",
                    "estimated": 9,
                    "dispatch_id": "dispatch-existing",
                    "run_id": "run-existing",
                },
                status="published",
                publish_attempts=1,
            )
            db.add(published)
            db.add(
                PipelineJob(
                    id="job-upstream-done",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="merge",
                    dispatch_id="dispatch-existing",
                    run_id="run-existing",
                    status="done",
                    input_version=3,
                    attempt=1,
                )
            )
            db.add(
                PipelineJob(
                    id="job-downstream-pending",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="parents",
                    dispatch_id="dispatch-existing",
                    run_id="run-existing",
                    status="pending",
                    input_version=3,
                    attempt=1,
                )
            )
            db.add(
                PipelineJob(
                    id="job-missing-outbox",
                    script_id="script-2",
                    episode_id="episode-2",
                    stage="split",
                    dispatch_id="dispatch-missing",
                    run_id="run-missing",
                    status="pending",
                    input_version=4,
                    input_client_save_seq=10,
                    attempt=1,
                )
            )
            db.add(
                CreditReservation(
                    id="reservation-missing",
                    account_id="account-1",
                    source_type="pipeline",
                    source_id="run-missing",
                    reserved_points=11,
                    status="reserved",
                )
            )
            await db.commit()

            assert await task_dispatch_service.reconcile_pipeline_dispatches(db) == 2
            await db.commit()

            assert raw_session.get(TaskDispatchOutbox, "dispatch-existing").status == "pending"
            recovered = raw_session.get(TaskDispatchOutbox, "dispatch-missing")
            assert recovered is not None
            assert recovered.status == "pending"
            assert recovered.payload["input_version"] == 4
            assert recovered.payload["input_client_save_seq"] == 10
            assert recovered.payload["reservation_id"] == "reservation-missing"
            assert recovered.payload["estimated"] == 11

    asyncio.run(scenario())


def test_reconciler_does_not_republish_a_normally_running_dag() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        PipelineJob.__table__.create(engine)
        CreditReservation.__table__.create(engine)
        TaskDispatchOutbox.__table__.create(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            db.add(
                TaskDispatchOutbox(
                    id="dispatch-active",
                    task_type="pipeline.dag",
                    job_id="run-active",
                    attempt=1,
                    payload={"dispatch_id": "dispatch-active", "run_id": "run-active"},
                    status="published",
                    publish_attempts=1,
                )
            )
            db.add(
                PipelineJob(
                    id="job-split-active",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="split",
                    dispatch_id="dispatch-active",
                    run_id="run-active",
                    status="running",
                    input_version=1,
                    attempt=1,
                    lease_until=datetime.now(UTC) + timedelta(minutes=5),
                )
            )
            db.add(
                PipelineJob(
                    id="job-extract-waiting",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="extract",
                    dispatch_id="dispatch-active",
                    run_id="run-active",
                    status="pending",
                    input_version=1,
                    attempt=1,
                )
            )
            await db.commit()

            assert await task_dispatch_service.reconcile_pipeline_dispatches(db) == 0
            assert raw_session.get(TaskDispatchOutbox, "dispatch-active").status == "published"

    asyncio.run(scenario())


def test_reconciler_republishes_stale_job_after_credit_release_rollback() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        PipelineJob.__table__.create(engine)
        CreditReservation.__table__.create(engine)
        TaskDispatchOutbox.__table__.create(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            reservation = CreditReservation(
                id="reservation-release-retry",
                account_id="account-1",
                source_type="pipeline",
                source_id="run-release-retry",
                reserved_points=9,
                status="reserved",
            )
            outbox = TaskDispatchOutbox(
                id="dispatch-release-retry",
                task_type="pipeline.dag",
                job_id="run-release-retry",
                attempt=1,
                payload={
                    "dispatch_id": "dispatch-release-retry",
                    "run_id": "run-release-retry",
                },
                status="published",
                publish_attempts=1,
            )
            job = PipelineJob(
                id="job-release-retry",
                script_id="script-1",
                episode_id="episode-1",
                stage="merge",
                dispatch_id="dispatch-release-retry",
                run_id="run-release-retry",
                status="running",
                input_version=1,
                attempt=1,
                lease_owner="pipeline-worker:merge",
                lease_token="expired-token",
                lease_until=datetime.now(UTC) - timedelta(seconds=1),
            )
            db.add(reservation)
            db.add(outbox)
            db.add(job)
            await db.commit()

            assert await task_dispatch_service.reconcile_pipeline_dispatches(db) == 1
            await db.commit()

            assert outbox.status == "pending"
            assert reservation.status == "reserved"
            assert job.status == "running"
            assert await task_dispatch_service.reconcile_pipeline_dispatches(db) == 0

    asyncio.run(scenario())


def test_reconciler_releases_failed_run_and_closes_ready_leaf(monkeypatch) -> None:
    async def scenario() -> None:
        from app.models.base import Base
        from app.models.pipeline_stage import PipelineStage

        engine = create_engine("sqlite+pysqlite:///:memory:")
        Base.metadata.create_all(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            reservation = CreditReservation(
                id="reservation-failed",
                account_id="account-1",
                source_type="pipeline",
                source_id="run-failed",
                reserved_points=9,
                status="reserved",
            )
            db.add(reservation)
            db.add(
                TaskDispatchOutbox(
                    id="dispatch-failed",
                    task_type="pipeline.dag",
                    job_id="run-failed",
                    attempt=1,
                    payload={"dispatch_id": "dispatch-failed", "run_id": "run-failed"},
                    status="published",
                    publish_attempts=1,
                )
            )
            db.add(
                PipelineJob(
                    id="job-enrich-failed",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="enrich",
                    dispatch_id="dispatch-failed",
                    run_id="run-failed",
                    status="failed",
                    input_version=1,
                    attempt=1,
                    error_type="provider_contract_error",
                    error_message="供应商响应不符合约定",
                    retryable=False,
                )
            )
            db.add(
                PipelineJob(
                    id="job-parents-ready",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="parents",
                    dispatch_id="dispatch-failed",
                    run_id="run-failed",
                    status="ready",
                    input_version=1,
                    attempt=1,
                )
            )
            db.add(
                PipelineStage(
                    id="stage-enrich-failed",
                    script_id="script-1",
                    stage="enrich",
                    status="failed",
                )
            )
            db.add(
                PipelineStage(
                    id="stage-parents-ready",
                    script_id="script-1",
                    stage="parents",
                    status="running",
                    detail={"count": 25, "input_version": 1},
                )
            )
            await db.commit()

            async def release(db, reservation_id, reason, *, commit):
                assert reservation_id == "reservation-failed"
                assert reason == "流水线执行失败"
                assert commit is False
                reservation.status = "released"

            release_mock = AsyncMock(side_effect=release)
            monkeypatch.setattr(task_dispatch_service, "credits_release", release_mock)

            assert await task_dispatch_service.reconcile_pipeline_dispatches(db) == 1
            await db.commit()

            release_mock.assert_awaited_once()
            assert reservation.status == "released"
            sibling_status = raw_session.execute(
                select(PipelineJob.status).where(
                    PipelineJob.id == "job-parents-ready"
                )
            ).scalar_one()
            # ready 表示父主体计算和私有结果已成功；兄弟叶失败时保留成功事实。
            assert sibling_status == "ready"
            failed_stage = raw_session.get(PipelineStage, "stage-enrich-failed")
            assert failed_stage.error == {
                "error_type": "provider_contract_error",
                "error_message": "供应商响应不符合约定",
                "retryable": False,
            }
            completed_stage = raw_session.get(PipelineStage, "stage-parents-ready")
            assert completed_stage.status == "ready"
            assert completed_stage.error is None
            assert completed_stage.detail["count"] == 25
            assert raw_session.get(TaskDispatchOutbox, "dispatch-failed").status == "published"

    asyncio.run(scenario())


def test_reconciler_closes_released_failed_run_and_allows_real_retrigger(monkeypatch) -> None:
    async def scenario() -> None:
        from app.models.base import Base
        from app.models.credit import CreditAccount
        from app.models.episode import Episode
        from app.models.pipeline_stage import PipelineStage
        from app.models.script import Script
        from app.services.pipeline import orchestrator

        class _TriggerSessionAdapter(_AsyncSessionAdapter):
            async def execute(self, statement):
                if "pg_advisory_xact_lock" in str(statement):
                    return self.session.execute(select(1))
                return await super().execute(statement)

        engine = create_engine("sqlite+pysqlite:///:memory:")
        Base.metadata.create_all(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            db.add(
                Script(
                    id="script-1",
                    title="测试剧本",
                    scope="personal",
                    owner_user_id="user-1",
                    created_by_user_id="user-1",
                )
            )
            db.add(
                Episode(
                    episode_id="episode-1",
                    script_id="script-1",
                    episode_no=1,
                    title="第一集",
                    episode_version=1,
                )
            )
            db.add(
                CreditAccount(
                    id="account-1",
                    owner_type="user",
                    owner_id="user-1",
                    balance_points=100,
                    frozen_points=0,
                    status="active",
                )
            )
            reservation = CreditReservation(
                id="reservation-running-leaf",
                account_id="account-1",
                source_type="pipeline",
                source_id="run-running-leaf",
                reserved_points=9,
                status="released",
            )
            db.add(reservation)
            db.add(
                TaskDispatchOutbox(
                    id="dispatch-running-leaf",
                    task_type="pipeline.dag",
                    job_id="run-running-leaf",
                    attempt=1,
                    payload={
                        "dispatch_id": "dispatch-running-leaf",
                        "run_id": "run-running-leaf",
                    },
                    status="published",
                    publish_attempts=1,
                )
            )
            for row in [
                    PipelineJob(
                        id="job-enrich-failed",
                        script_id="script-1",
                        episode_id="episode-1",
                        stage="enrich",
                        dispatch_id="dispatch-running-leaf",
                        run_id="run-running-leaf",
                        status="failed",
                        input_version=1,
                        attempt=1,
                        error_message="Authorization=secret-token",
                    ),
                    PipelineJob(
                        id="job-parents-running",
                        script_id="script-1",
                        episode_id="episode-1",
                        stage="parents",
                        dispatch_id="dispatch-running-leaf",
                        run_id="run-running-leaf",
                        status="running",
                        input_version=1,
                        attempt=1,
                        lease_owner="worker-secret",
                        lease_token="lease-secret",
                        lease_until=datetime.now(UTC) + timedelta(minutes=5),
                    ),
                    PipelineStage(
                        id="stage-enrich",
                        script_id="script-1",
                        stage="enrich",
                        status="failed",
                    ),
                    PipelineStage(
                        id="stage-parents",
                        script_id="script-1",
                        stage="parents",
                        status="running",
                        detail={"input_version": 1},
                    ),
                ]:
                db.add(row)
            await db.commit()

            release_mock = AsyncMock()
            monkeypatch.setattr(task_dispatch_service, "credits_release", release_mock)

            assert await task_dispatch_service.reconcile_pipeline_dispatches(db) == 1
            await db.commit()

            failed = raw_session.get(PipelineJob, "job-enrich-failed")
            sibling = raw_session.get(PipelineJob, "job-parents-running")
            assert failed.status == "failed"
            assert failed.error_type is None
            assert failed.error_message == "Authorization=secret-token"
            assert failed.finished_at is not None
            assert sibling.status == "skipped"
            assert sibling.finished_at is not None
            assert sibling.lease_owner is None
            assert sibling.lease_token is None
            assert sibling.lease_until is None
            assert sibling.error_type == "internal_error"
            assert sibling.error_message == "任务执行失败"
            assert raw_session.get(PipelineStage, "stage-enrich").error == {
                "error_type": "internal_error",
                "error_message": "任务执行失败",
                "retryable": False,
            }
            parents_stage = raw_session.get(PipelineStage, "stage-parents")
            assert parents_stage.status == "failed"
            assert parents_stage.error == {
                "error_type": "internal_error",
                "error_message": "任务执行失败",
                "retryable": False,
            }
            assert reservation.status == "released"
            assert not await orchestrator._is_running(db, "script-1")
            release_mock.assert_not_awaited()

            reserve_mock = AsyncMock(return_value="reservation-retriggered")
            monkeypatch.setattr(orchestrator, "reserve", reserve_mock)
            monkeypatch.setattr(
                orchestrator,
                "_estimate_cost",
                AsyncMock(return_value=3),
            )
            retriggered = await orchestrator.trigger_pipeline(
                _TriggerSessionAdapter(raw_session),
                "script-1",
                "episode-1",
            )
            assert retriggered["run_id"] != "run-running-leaf"
            new_jobs = raw_session.execute(
                select(PipelineJob).where(
                    PipelineJob.run_id == retriggered["run_id"]
                )
            ).scalars().all()
            assert {job.stage for job in new_jobs} == {
                "split",
                "extract",
                "merge",
                "enrich",
                "parents",
            }
            assert all(job.attempt == 1 for job in new_jobs)
            assert all(job.started_at is None for job in new_jobs)
            assert all(job.finished_at is None for job in new_jobs)
            assert raw_session.get(
                TaskDispatchOutbox, retriggered["dispatch_id"]
            ).status == "pending"
            assert await orchestrator._is_running(
                _TriggerSessionAdapter(raw_session), "script-1"
            )
            reserve_mock.assert_awaited_once()

    asyncio.run(scenario())


def test_reconciler_does_not_fail_current_stage_when_old_attempt_seq_differs(monkeypatch) -> None:
    async def scenario() -> None:
        from app.models.base import Base
        from app.models.pipeline_stage import PipelineStage

        engine = create_engine("sqlite+pysqlite:///:memory:")
        Base.metadata.create_all(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            reservation = CreditReservation(
                id="reservation-old-seq",
                account_id="account-1",
                source_type="pipeline",
                source_id="run-old-seq",
                reserved_points=9,
                status="reserved",
            )
            db.add(reservation)
            db.add(
                TaskDispatchOutbox(
                    id="dispatch-old-seq",
                    task_type="pipeline.dag",
                    job_id="run-old-seq",
                    attempt=1,
                    payload={"dispatch_id": "dispatch-old-seq", "run_id": "run-old-seq"},
                    status="published",
                    publish_attempts=1,
                )
            )
            db.add(
                PipelineJob(
                    id="job-old-seq-failed",
                    script_id="script-1",
                    episode_id="episode-1",
                    stage="enrich",
                    dispatch_id="dispatch-old-seq",
                    run_id="run-old-seq",
                    status="failed",
                    input_version=3,
                    input_client_save_seq=9,
                    attempt=1,
                    error_type="provider_timeout",
                    error_message="上游服务超时",
                    retryable=True,
                )
            )
            db.add(
                PipelineStage(
                    id="stage-current-enrich",
                    script_id="script-1",
                    stage="enrich",
                    status="running",
                    detail={"input_version": 3, "input_client_save_seq": 10},
                )
            )
            await db.commit()

            monkeypatch.setattr(task_dispatch_service, "credits_release", AsyncMock())

            assert await task_dispatch_service.reconcile_pipeline_dispatches(db) == 1
            await db.commit()

            stage = raw_session.get(PipelineStage, "stage-current-enrich")
            assert stage.status == "running"
            assert stage.error is None
            failed = raw_session.get(PipelineJob, "job-old-seq-failed")
            assert failed.error_type == "provider_timeout"
            assert failed.error_message == "上游服务超时"

    asyncio.run(scenario())


def test_reconciler_republishes_stale_root_that_never_started() -> None:
    async def scenario() -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        PipelineJob.__table__.create(engine)
        CreditReservation.__table__.create(engine)
        TaskDispatchOutbox.__table__.create(engine)
        with Session(engine, expire_on_commit=False) as raw_session:
            db = _AsyncSessionAdapter(raw_session)
            outbox = TaskDispatchOutbox(
                id="dispatch-stale-root",
                task_type="pipeline.dag",
                job_id="run-stale-root",
                attempt=1,
                payload={"dispatch_id": "dispatch-stale-root", "run_id": "run-stale-root"},
                status="published",
                published_at=datetime.now(UTC) - timedelta(minutes=11),
                publish_attempts=1,
            )
            db.add(outbox)
            for stage in ("split", "extract", "merge", "enrich", "parents"):
                db.add(
                    PipelineJob(
                        id=f"job-stale-{stage}",
                        script_id="script-1",
                        episode_id="episode-1",
                        stage=stage,
                        dispatch_id="dispatch-stale-root",
                        run_id="run-stale-root",
                        status="pending",
                        input_version=1,
                        attempt=1,
                    )
                )
            await db.commit()

            assert await task_dispatch_service.reconcile_pipeline_dispatches(db) == 1
            await db.commit()
            assert outbox.status == "pending"

    asyncio.run(scenario())
