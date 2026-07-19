"""视频取消提交结果不确定性的专项测试。"""

from __future__ import annotations

import importlib
import os
import secrets
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded
from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.credit_reservation import CreditReservation
from app.models.episode import Episode
from app.models.script import Script
from app.models.shot_group import ShotGroup
from app.models.task_dispatch_outbox import TaskDispatchOutbox
from app.models.video_generation_job import VideoGenerationJob
from app.providers.video_gen import VideoCancellationOutcome, VideoCancellationResult


@compiles(JSONB, "sqlite")
def _jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


class CommitUncertainError(ConnectionError):
    """模拟数据库提交结果未知。"""


class RollbackUncertainError(ConnectionError):
    """模拟回滚已执行但响应丢失。"""


class _AsyncSessionAdapter:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.commit_count = 0
        self.rollback_count = 0

    async def execute(self, statement):
        return self.session.execute(statement)

    async def flush(self) -> None:
        self.session.flush()

    async def commit(self) -> None:
        self.commit_count += 1
        self.session.commit()

    async def rollback(self) -> None:
        self.rollback_count += 1
        self.session.rollback()

    def add(self, row) -> None:
        self.session.add(row)


class _CommitFaultSession(_AsyncSessionAdapter):
    def __init__(
        self,
        session: Session,
        *,
        fault_on_commit: int,
        apply_before_raise: bool,
        rollback_raises: bool,
    ) -> None:
        super().__init__(session)
        self.fault_on_commit = fault_on_commit
        self.apply_before_raise = apply_before_raise
        self.rollback_raises = rollback_raises

    async def commit(self) -> None:
        self.commit_count += 1
        if self.commit_count != self.fault_on_commit:
            self.session.commit()
            return
        if self.apply_before_raise:
            self.session.commit()
        raise CommitUncertainError(f"commit-{self.commit_count}-result-unknown")

    async def rollback(self) -> None:
        self.rollback_count += 1
        self.session.rollback()
        if self.rollback_raises:
            raise RollbackUncertainError("rollback-result-unknown")


class _IndependentSessionFactory:
    def __init__(
        self,
        engine,
        *,
        commit_apply_before_raise: bool | None = None,
        rollback_raises: bool = False,
    ) -> None:
        self.engine = engine
        self.calls = 0
        self.commit_apply_before_raise = commit_apply_before_raise
        self.commit_fault_pending = commit_apply_before_raise is not None
        self.commit_fault_raised = False
        self.rollback_raises = rollback_raises
        self.rollback_fault_pending = rollback_raises

    def __call__(self):
        factory = self

        class _Context:
            async def __aenter__(self):
                factory.calls += 1
                self.raw = Session(factory.engine, expire_on_commit=False)

                class _FactorySession(_AsyncSessionAdapter):
                    async def commit(inner_self) -> None:
                        inner_self.commit_count += 1
                        if factory.commit_fault_pending:
                            factory.commit_fault_pending = False
                            factory.commit_fault_raised = True
                            if factory.commit_apply_before_raise:
                                inner_self.session.commit()
                            raise CommitUncertainError("independent-commit-result-unknown")
                        inner_self.session.commit()

                    async def rollback(inner_self) -> None:
                        inner_self.rollback_count += 1
                        inner_self.session.rollback()
                        if factory.commit_fault_raised and factory.rollback_fault_pending:
                            factory.rollback_fault_pending = False
                            raise RollbackUncertainError("independent-rollback-result-unknown")

                return _FactorySession(self.raw)

            async def __aexit__(self, exc_type, exc, tb):
                self.raw.close()

        return _Context()


class _UnavailableSessionFactory:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self):
        factory = self

        class _Context:
            async def __aenter__(self):
                factory.calls += 1
                raise ConnectionError("independent-database-unavailable")

            async def __aexit__(self, exc_type, exc, tb):
                return None

        return _Context()


def _make_job_db(*, job_id: str):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    VideoGenerationJob.__table__.create(engine)
    TaskDispatchOutbox.__table__.create(engine)
    raw = Session(engine, expire_on_commit=False)
    provider_task_id = f"provider-{job_id}"
    job = VideoGenerationJob(
        job_id=job_id,
        script_id="script-cancel",
        episode_id="episode-cancel",
        shot_group_id="group-cancel",
        owner_user_id="user-cancel",
        scene_no=1,
        unit_id="group:group-cancel",
        unit_runtime_key="episode-cancel:1:group:group-cancel",
        provider_key="modelsai",
        job_type="all_reference",
        idempotency_key=f"idem-{job_id}",
        request_hash="request-hash",
        input_version=1,
        attempt=1,
        reservation_id=f"reservation-{job_id}",
        status="starting",
        output_json={"provider_task_id": provider_task_id},
        metrics={"provider_task_id": provider_task_id},
    )
    outbox = TaskDispatchOutbox(
        id=f"dispatch-{job_id}",
        task_type="video.generate",
        job_id=job_id,
        attempt=1,
        payload={"job_id": job_id},
        status="pending",
    )
    raw.add_all([job, outbox])
    raw.commit()
    return engine, raw, job, outbox, provider_task_id


def _configure_cancel_dependencies(monkeypatch, engine, provider_task_id: str):
    # worker 在模块导入时缓存 credits.release；必须先加载，避免下面的服务级 mock
    # 被 worker 永久捕获并污染同一 pytest 进程中的后续视频测试。
    importlib.import_module("app.workers.video_generation")

    from app.services import video_generation as service

    release = AsyncMock()
    cancel = AsyncMock(
        return_value=VideoCancellationResult(
            provider_task_id=provider_task_id,
            outcome=VideoCancellationOutcome.CANCELED,
        )
    )
    factory = _IndependentSessionFactory(engine)
    monkeypatch.setattr(service, "_credits_reserve", object())
    monkeypatch.setattr(
        service,
        "_lookup_reservation_for_cancel",
        AsyncMock(return_value="reservation-cancel"),
    )
    monkeypatch.setattr("app.services.credits.release", release)
    monkeypatch.setattr(service, "get_video_provider", lambda _key: SimpleNamespace(cancel=cancel))
    monkeypatch.setattr("app.core.database.async_session_factory", factory)
    return service, release, cancel, factory


def _volcengine_provider_with_close_signal(
    monkeypatch,
    signal: BaseException,
):
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    delete = AsyncMock()
    close = AsyncMock(side_effect=signal)
    client = SimpleNamespace(
        content_generation=SimpleNamespace(
            tasks=SimpleNamespace(delete=delete),
        ),
        close=close,
    )
    provider = VolcengineVideoGenProvider(provider_key="modelsai")
    monkeypatch.setattr(provider, "_check_keys", lambda: None)
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)
    return provider, delete, close


@pytest.mark.parametrize("timeout_type", [SoftTimeLimitExceeded, TimeLimitExceeded])
@pytest.mark.asyncio
async def test_remote_cancel_helper_propagates_provider_close_time_limit(
    monkeypatch,
    timeout_type,
) -> None:
    from app.services import video_generation as service

    timeout_error = timeout_type()
    provider, delete, close = _volcengine_provider_with_close_signal(
        monkeypatch,
        timeout_error,
    )
    monkeypatch.setattr(service, "get_video_provider", lambda _key: provider)

    with pytest.raises(timeout_type) as exc_info:
        await service._best_effort_cancel_remote_video_task(
            "modelsai",
            "provider-helper-time-limit",
        )

    assert exc_info.value is timeout_error
    delete.assert_awaited_once_with(task_id="provider-helper-time-limit")
    close.assert_awaited_once()


@pytest.mark.parametrize("timeout_type", [SoftTimeLimitExceeded, TimeLimitExceeded])
@pytest.mark.asyncio
async def test_cancel_video_job_propagates_provider_close_time_limit(
    monkeypatch,
    timeout_type,
) -> None:
    engine, raw, job, _, provider_task_id = _make_job_db(
        job_id=f"job-{timeout_type.__name__.lower()}"
    )
    service, release, _cancel, _factory = _configure_cancel_dependencies(
        monkeypatch,
        engine,
        provider_task_id,
    )
    timeout_error = timeout_type()
    provider, delete, close = _volcengine_provider_with_close_signal(
        monkeypatch,
        timeout_error,
    )
    monkeypatch.setattr(service, "get_video_provider", lambda _key: provider)
    db = _AsyncSessionAdapter(raw)

    try:
        with pytest.raises(timeout_type) as exc_info:
            await service.cancel_video_job(
                db,
                script_id=job.script_id,
                job_id=job.job_id,
                actor_user_id=job.owner_user_id,
            )

        assert exc_info.value is timeout_error
        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        assert saved is not None
        assert saved.status == "canceled"
        assert (saved.output_json or {})["provider_cancellation"] == {
            "provider_task_id": provider_task_id,
            "outcome": "pending",
            "detail": None,
            "retryable": True,
            "attempts": 0,
        }
        release.assert_awaited_once()
        delete.assert_awaited_once_with(task_id=provider_task_id)
        close.assert_awaited_once()
    finally:
        raw.close()
        engine.dispose()


@pytest.mark.asyncio
async def test_repeated_cancel_of_canceled_job_is_idempotent(monkeypatch) -> None:
    engine, raw, job, _, provider_task_id = _make_job_db(job_id="job-idempotent")
    service, release, remote_cancel, _ = _configure_cancel_dependencies(
        monkeypatch,
        engine,
        provider_task_id,
    )
    db = _AsyncSessionAdapter(raw)

    first = await service.cancel_video_job(
        db,
        script_id=job.script_id,
        job_id=job.job_id,
        actor_user_id=job.owner_user_id,
    )
    second = await service.cancel_video_job(
        db,
        script_id=job.script_id,
        job_id=job.job_id,
        actor_user_id=job.owner_user_id,
    )

    assert first.status == second.status == "canceled"
    release.assert_awaited_once()
    remote_cancel.assert_awaited_once_with(provider_task_id)

    raw.close()
    engine.dispose()


@pytest.mark.asyncio
async def test_uncertain_local_commit_pending_debt_is_reconciled(monkeypatch) -> None:
    engine, raw, job, _, provider_task_id = _make_job_db(job_id="job-debt-reconcile")
    service, release, request_cancel, _ = _configure_cancel_dependencies(
        monkeypatch,
        engine,
        provider_task_id,
    )
    db = _CommitFaultSession(
        raw,
        fault_on_commit=1,
        apply_before_raise=True,
        rollback_raises=True,
    )

    result = await service.cancel_video_job(
        db,
        script_id=job.script_id,
        job_id=job.job_id,
        actor_user_id=job.owner_user_id,
    )
    assert result.status == "canceled"
    request_cancel.assert_not_awaited()
    release.assert_awaited_once()

    from app.workers import video_generation as worker

    reconcile_cancel = AsyncMock(
        return_value=VideoCancellationResult(
            provider_task_id=provider_task_id,
            outcome=VideoCancellationOutcome.CANCELED,
        )
    )
    monkeypatch.setattr(
        worker,
        "get_video_provider",
        lambda _key: SimpleNamespace(cancel=reconcile_cancel),
    )
    reconcile_raw = Session(engine, expire_on_commit=False)
    assert await worker.reconcile_video_remote_cancellations(_AsyncSessionAdapter(reconcile_raw)) == 1
    reconcile_cancel.assert_awaited_once_with(provider_task_id)

    reconcile_raw.expire_all()
    saved = reconcile_raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    debt = (saved.output_json or {})["provider_cancellation"]
    assert debt["outcome"] == "canceled"
    assert debt["retryable"] is False

    reconcile_raw.close()
    raw.close()
    engine.dispose()


@pytest.mark.asyncio
async def test_processing_cancel_commit_uncertainty_preserves_checkpoint_generation(
    monkeypatch,
) -> None:
    engine, raw, job, _, provider_task_id = _make_job_db(
        job_id="job-processing-cancel-generation-uncertain"
    )
    active_lease = "lease-processing-cancel-generation"
    job.status = "processing"
    job.lease_owner = "worker-processing-cancel-generation"
    job.lease_token = active_lease
    raw.commit()
    service, release, request_cancel, _ = _configure_cancel_dependencies(
        monkeypatch,
        engine,
        provider_task_id,
    )
    db = _CommitFaultSession(
        raw,
        fault_on_commit=1,
        apply_before_raise=True,
        rollback_raises=True,
    )

    result = await service.cancel_video_job(
        db,
        script_id=job.script_id,
        job_id=job.job_id,
        actor_user_id=job.owner_user_id,
    )

    assert result.status == "canceled"
    request_cancel.assert_not_awaited()
    release.assert_awaited_once()
    with Session(engine, expire_on_commit=False) as verify:
        saved = verify.get(VideoGenerationJob, job.job_id)
        assert saved is not None
        assert saved.status == "canceled"
        assert saved.lease_token is None
        output_authority = (saved.output_json or {})[
            "provider_task_checkpoint_lease"
        ]
        metrics_authority = (saved.metrics or {})[
            "provider_task_checkpoint_lease"
        ]
        assert output_authority == metrics_authority
        assert output_authority.startswith("sha256:")
        assert active_lease not in output_authority

    raw.close()
    engine.dispose()


@pytest.mark.asyncio
async def test_canceled_checkpoint_commit_uncertainty_replays_idempotently(
    monkeypatch,
) -> None:
    engine, raw, job, _, provider_task_id = _make_job_db(
        job_id="job-canceled-checkpoint-commit-uncertain"
    )
    active_lease = "lease-canceled-checkpoint-commit-uncertain"
    job.status = "processing"
    job.lease_owner = "worker-canceled-checkpoint-commit-uncertain"
    job.lease_token = active_lease
    job.output_json = {}
    job.metrics = {}
    raw.commit()
    service, _, remote_cancel, _ = _configure_cancel_dependencies(
        monkeypatch,
        engine,
        provider_task_id,
    )
    await service.cancel_video_job(
        _AsyncSessionAdapter(raw),
        script_id=job.script_id,
        job_id=job.job_id,
        actor_user_id=job.owner_user_id,
    )
    remote_cancel.assert_not_awaited()

    from app.workers import video_generation as worker

    uncertain_factory = _IndependentSessionFactory(
        engine,
        commit_apply_before_raise=True,
    )
    publish = Mock()
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        uncertain_factory,
    )
    monkeypatch.setattr(worker, "_publish_video_task_checkpoint", publish)

    await worker._ensure_video_task_checkpoint_durable(
        job.job_id,
        provider_task_id,
        active_lease,
    )
    publish.assert_called_once_with(
        job_id=job.job_id,
        provider_task_id=provider_task_id,
        lease_token=active_lease,
    )
    with Session(engine, expire_on_commit=False) as verify:
        saved = verify.get(VideoGenerationJob, job.job_id)
        assert saved is not None
        assert (saved.output_json or {})["provider_task_id"] == provider_task_id
        checkpoint_version = saved.version

    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _IndependentSessionFactory(engine),
    )
    replayed = await worker._run_video_task_checkpoint_async(
        job.job_id,
        provider_task_id,
        active_lease,
    )
    assert replayed["status"] == "persisted"
    with Session(engine, expire_on_commit=False) as verify:
        saved = verify.get(VideoGenerationJob, job.job_id)
        assert saved is not None
        assert saved.version == checkpoint_version

    raw.close()
    engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("apply_before_raise", [False, True])
@pytest.mark.parametrize("rollback_raises", [False, True])
async def test_first_commit_uncertainty_uses_authoritative_state(
    monkeypatch,
    apply_before_raise: bool,
    rollback_raises: bool,
) -> None:
    engine, raw, job, _, provider_task_id = _make_job_db(
        job_id=f"job-first-{int(apply_before_raise)}-{int(rollback_raises)}"
    )
    service, release, remote_cancel, factory = _configure_cancel_dependencies(
        monkeypatch,
        engine,
        provider_task_id,
    )
    db = _CommitFaultSession(
        raw,
        fault_on_commit=1,
        apply_before_raise=apply_before_raise,
        rollback_raises=rollback_raises,
    )

    if apply_before_raise:
        result = await service.cancel_video_job(
            db,
            script_id=job.script_id,
            job_id=job.job_id,
            actor_user_id=job.owner_user_id,
        )
        assert result.status == "canceled"
    else:
        with pytest.raises(CommitUncertainError, match="commit-1-result-unknown"):
            await service.cancel_video_job(
                db,
                script_id=job.script_id,
                job_id=job.job_id,
                actor_user_id=job.owner_user_id,
            )

    assert db.rollback_count == 1
    assert factory.calls == 1
    with Session(engine, expire_on_commit=False) as verify:
        saved = verify.get(VideoGenerationJob, job.job_id)
        outbox = verify.get(TaskDispatchOutbox, f"dispatch-{job.job_id}")
        assert saved is not None
        assert outbox is not None
        if apply_before_raise:
            assert saved.status == "canceled"
            assert outbox.status == "canceled"
            debt = (saved.output_json or {})["provider_cancellation"]
            assert debt["outcome"] == "pending"
            assert debt["retryable"] is True
            release.assert_awaited_once()
        else:
            assert saved.status == "starting"
            assert outbox.status == "pending"

    # 提交归属不确定时请求路径不重复触发远端取消，已提交的 pending 债务由后台恢复。
    remote_cancel.assert_not_awaited()
    raw.close()
    engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("apply_before_raise", [False, True])
@pytest.mark.parametrize("rollback_raises", [False, True])
async def test_remote_result_commit_uncertainty_keeps_local_cancel_success(
    monkeypatch,
    apply_before_raise: bool,
    rollback_raises: bool,
) -> None:
    engine, raw, job, _, provider_task_id = _make_job_db(
        job_id=f"job-remote-{int(apply_before_raise)}-{int(rollback_raises)}"
    )
    service, release, remote_cancel, _factory = _configure_cancel_dependencies(
        monkeypatch,
        engine,
        provider_task_id,
    )
    db = _CommitFaultSession(
        raw,
        fault_on_commit=2,
        apply_before_raise=apply_before_raise,
        rollback_raises=rollback_raises,
    )

    result = await service.cancel_video_job(
        db,
        script_id=job.script_id,
        job_id=job.job_id,
        actor_user_id=job.owner_user_id,
    )

    assert result.status == "canceled"
    assert db.rollback_count == 1
    release.assert_awaited_once()
    remote_cancel.assert_awaited_once_with(provider_task_id)

    with Session(engine, expire_on_commit=False) as verify:
        saved = verify.get(VideoGenerationJob, job.job_id)
        assert saved is not None and saved.status == "canceled"
        debt = (saved.output_json or {})["provider_cancellation"]
        assert debt["outcome"] == "canceled"
        assert debt["retryable"] is False

    from app.workers import video_generation as worker

    reconcile_cancel = AsyncMock(
        return_value=VideoCancellationResult(
            provider_task_id=provider_task_id,
            outcome=VideoCancellationOutcome.CANCELED,
        )
    )
    monkeypatch.setattr(
        worker,
        "get_video_provider",
        lambda _key: SimpleNamespace(cancel=reconcile_cancel),
    )
    reconcile_raw = Session(engine, expire_on_commit=False)
    assert await worker.reconcile_video_remote_cancellations(_AsyncSessionAdapter(reconcile_raw)) == 0
    reconcile_cancel.assert_not_awaited()

    # 客户端重放只返回权威终态，不再次释放积分或调用 Provider。
    replay_raw = Session(engine, expire_on_commit=False)
    replay = await service.cancel_video_job(
        _AsyncSessionAdapter(replay_raw),
        script_id=job.script_id,
        job_id=job.job_id,
        actor_user_id=job.owner_user_id,
    )
    assert replay.status == "canceled"
    release.assert_awaited_once()
    remote_cancel.assert_awaited_once_with(provider_task_id)

    reconcile_raw.close()
    replay_raw.close()
    raw.close()
    engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("independent_apply_before_raise", [False, True])
@pytest.mark.parametrize("independent_rollback_raises", [False, True])
async def test_independent_result_commit_uncertainty_does_not_repeat_provider(
    monkeypatch,
    independent_apply_before_raise: bool,
    independent_rollback_raises: bool,
) -> None:
    engine, raw, job, _, provider_task_id = _make_job_db(
        job_id=(f"job-independent-{int(independent_apply_before_raise)}-{int(independent_rollback_raises)}")
    )
    service, release, remote_cancel, _factory = _configure_cancel_dependencies(
        monkeypatch,
        engine,
        provider_task_id,
    )
    independent_factory = _IndependentSessionFactory(
        engine,
        commit_apply_before_raise=independent_apply_before_raise,
        rollback_raises=independent_rollback_raises,
    )
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        independent_factory,
    )
    db = _CommitFaultSession(
        raw,
        fault_on_commit=2,
        apply_before_raise=False,
        rollback_raises=True,
    )

    result = await service.cancel_video_job(
        db,
        script_id=job.script_id,
        job_id=job.job_id,
        actor_user_id=job.owner_user_id,
    )

    assert result.status == "canceled"
    assert independent_factory.commit_fault_pending is False
    release.assert_awaited_once()
    remote_cancel.assert_awaited_once_with(provider_task_id)
    with Session(engine, expire_on_commit=False) as verify:
        saved = verify.get(VideoGenerationJob, job.job_id)
        assert saved is not None
        debt = (saved.output_json or {})["provider_cancellation"]
        assert debt["outcome"] == "canceled"
        assert debt["retryable"] is False

    from app.workers import video_generation as worker

    reconcile_cancel = AsyncMock()
    monkeypatch.setattr(
        worker,
        "get_video_provider",
        lambda _key: SimpleNamespace(cancel=reconcile_cancel),
    )
    reconcile_raw = Session(engine, expire_on_commit=False)
    assert await worker.reconcile_video_remote_cancellations(_AsyncSessionAdapter(reconcile_raw)) == 0
    reconcile_cancel.assert_not_awaited()

    reconcile_raw.close()
    raw.close()
    engine.dispose()


@pytest.mark.asyncio
async def test_unavailable_independent_database_keeps_pending_debt_and_logs_risk(
    monkeypatch,
    caplog,
) -> None:
    engine, raw, job, _, provider_task_id = _make_job_db(job_id="job-independent-unavailable")
    service, release, remote_cancel, _factory = _configure_cancel_dependencies(
        monkeypatch,
        engine,
        provider_task_id,
    )
    unavailable_factory = _UnavailableSessionFactory()
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        unavailable_factory,
    )
    db = _CommitFaultSession(
        raw,
        fault_on_commit=2,
        apply_before_raise=False,
        rollback_raises=True,
    )

    result = await service.cancel_video_job(
        db,
        script_id=job.script_id,
        job_id=job.job_id,
        actor_user_id=job.owner_user_id,
    )

    assert result.status == "canceled"
    assert unavailable_factory.calls >= 1
    release.assert_awaited_once()
    remote_cancel.assert_awaited_once_with(provider_task_id)
    with Session(engine, expire_on_commit=False) as verify:
        saved = verify.get(VideoGenerationJob, job.job_id)
        assert saved is not None
        debt = (saved.output_json or {})["provider_cancellation"]
        assert debt["outcome"] == "pending"
        assert debt["retryable"] is True
    assert "reconciler 后续可能再次调用 Provider" in caplog.text

    raw.close()
    engine.dispose()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not os.getenv("MTV_TEST_POSTGRES_URL"),
    reason="未提供 MTV_TEST_POSTGRES_URL，跳过真实 PostgreSQL 取消提交不确定性测试",
)
@pytest.mark.parametrize(
    ("fault_on_commit", "apply_before_raise"),
    [
        (1, True),
        (2, False),
    ],
)
async def test_postgres_commit_uncertainty_preserves_cancel_without_duplicate_provider(
    monkeypatch,
    fault_on_commit: int,
    apply_before_raise: bool,
) -> None:
    from app.services import video_generation as service
    from app.workers import video_generation as worker

    database_url = os.environ["MTV_TEST_POSTGRES_URL"]
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    suffix = secrets.token_hex(6)
    script_id = f"cancel-script-{suffix}"
    episode_id = f"cancel-episode-{suffix}"
    group_id = f"cancel-group-{suffix}"
    job_id = f"cancel-job-{suffix}"
    account_id = f"cancel-account-{suffix}"
    reservation_id = f"cancel-resv-{suffix}"
    dispatch_id = f"cancel-dispatch-{suffix}"
    user_id = f"cancel-user-{suffix}"
    provider_task_id = f"provider-{suffix}"

    request_cancel = AsyncMock(
        return_value=VideoCancellationResult(
            provider_task_id=provider_task_id,
            outcome=VideoCancellationOutcome.CANCELED,
        )
    )
    reconcile_cancel = AsyncMock(
        return_value=VideoCancellationResult(
            provider_task_id=provider_task_id,
            outcome=VideoCancellationOutcome.CANCELED,
        )
    )
    monkeypatch.setattr(service, "_credits_reserve", object())
    monkeypatch.setattr(
        service,
        "get_video_provider",
        lambda _key: SimpleNamespace(cancel=request_cancel),
    )
    monkeypatch.setattr(
        worker,
        "get_video_provider",
        lambda _key: SimpleNamespace(cancel=reconcile_cancel),
    )
    monkeypatch.setattr("app.core.database.async_session_factory", session_factory)

    class _PostgresCommitFaultSession:
        def __init__(self, session) -> None:
            self.session = session
            self.commit_count = 0
            self.rollback_count = 0

        def __getattr__(self, name):
            return getattr(self.session, name)

        async def commit(self) -> None:
            self.commit_count += 1
            if self.commit_count == fault_on_commit:
                if apply_before_raise:
                    await self.session.commit()
                raise CommitUncertainError("postgres-commit-response-lost")
            await self.session.commit()

        async def rollback(self) -> None:
            self.rollback_count += 1
            await self.session.rollback()
            if self.rollback_count == 1:
                raise RollbackUncertainError("postgres-rollback-response-lost")

    try:
        async with session_factory() as seed_db:
            seed_db.add_all(
                [
                    Script(
                        id=script_id,
                        title="取消提交不确定性",
                        scope="personal",
                        owner_user_id=user_id,
                        created_by_user_id=user_id,
                    ),
                    CreditAccount(
                        id=account_id,
                        owner_type="user",
                        owner_id=user_id,
                        balance_points=1000,
                        frozen_points=210,
                        status="active",
                    ),
                ]
            )
            await seed_db.flush()
            seed_db.add(
                Episode(
                    episode_id=episode_id,
                    script_id=script_id,
                    episode_no=1,
                    title="第一集",
                )
            )
            await seed_db.flush()
            seed_db.add(
                ShotGroup(
                    shot_group_id=group_id,
                    script_id=script_id,
                    episode_id=episode_id,
                    scene_no=1,
                    group_no="G1",
                )
            )
            await seed_db.flush()
            seed_db.add_all(
                [
                    CreditReservation(
                        id=reservation_id,
                        account_id=account_id,
                        source_type="generation_job",
                        source_id=job_id,
                        reserved_points=210,
                        status="reserved",
                    ),
                    VideoGenerationJob(
                        job_id=job_id,
                        script_id=script_id,
                        episode_id=episode_id,
                        shot_group_id=group_id,
                        owner_user_id=user_id,
                        scene_no=1,
                        unit_id=f"group:{group_id}",
                        unit_runtime_key=f"{episode_id}:1:group:{group_id}",
                        provider_key="modelsai",
                        job_type="all_reference",
                        idempotency_key=f"idem-{suffix}",
                        request_hash="request-hash",
                        input_version=1,
                        attempt=1,
                        reservation_id=reservation_id,
                        status="starting",
                        output_json={"provider_task_id": provider_task_id},
                        metrics={"provider_task_id": provider_task_id},
                    ),
                    TaskDispatchOutbox(
                        id=dispatch_id,
                        task_type="video.generate",
                        job_id=job_id,
                        attempt=1,
                        payload={"job_id": job_id},
                        status="pending",
                    ),
                ]
            )
            await seed_db.commit()

        async with session_factory() as primary_db:
            uncertain_db = _PostgresCommitFaultSession(primary_db)
            canceled = await service.cancel_video_job(
                uncertain_db,
                script_id=script_id,
                job_id=job_id,
                actor_user_id=user_id,
            )
            assert canceled.status == "canceled"
            assert uncertain_db.commit_count == fault_on_commit
            assert uncertain_db.rollback_count == 1

        if fault_on_commit == 1:
            request_cancel.assert_not_awaited()
        else:
            request_cancel.assert_awaited_once_with(provider_task_id)
        async with session_factory() as replay_db:
            replay = await service.cancel_video_job(
                replay_db,
                script_id=script_id,
                job_id=job_id,
                actor_user_id=user_id,
            )
            assert replay.status == "canceled"
        if fault_on_commit == 1:
            request_cancel.assert_not_awaited()
        else:
            request_cancel.assert_awaited_once_with(provider_task_id)

        async with session_factory() as reconcile_db:
            reconciled = await worker.reconcile_video_remote_cancellations(reconcile_db)
        if fault_on_commit == 1:
            assert reconciled == 1
            reconcile_cancel.assert_awaited_once_with(provider_task_id)
        else:
            assert reconciled == 0
            reconcile_cancel.assert_not_awaited()

        async with session_factory() as verify_db:
            saved = await verify_db.get(VideoGenerationJob, job_id)
            reservation = await verify_db.get(CreditReservation, reservation_id)
            account = await verify_db.get(CreditAccount, account_id)
            outbox = await verify_db.get(TaskDispatchOutbox, dispatch_id)
            release_count = await verify_db.scalar(
                select(func.count())
                .select_from(CreditLog)
                .where(
                    CreditLog.source_id == job_id,
                    CreditLog.log_type == "release",
                )
            )
            assert saved is not None and saved.status == "canceled"
            assert reservation is not None and reservation.status == "released"
            assert account is not None and account.frozen_points == 0
            assert outbox is not None and outbox.status == "canceled"
            assert release_count == 1
            debt = (saved.output_json or {})["provider_cancellation"]
            assert debt["outcome"] == "canceled"
            assert debt["retryable"] is False
    finally:
        async with session_factory() as cleanup_db:
            await cleanup_db.execute(delete(CreditLog).where(CreditLog.source_id == job_id))
            await cleanup_db.execute(delete(CreditReservation).where(CreditReservation.id == reservation_id))
            await cleanup_db.execute(delete(TaskDispatchOutbox).where(TaskDispatchOutbox.id == dispatch_id))
            await cleanup_db.execute(delete(VideoGenerationJob).where(VideoGenerationJob.job_id == job_id))
            await cleanup_db.execute(delete(ShotGroup).where(ShotGroup.shot_group_id == group_id))
            await cleanup_db.execute(delete(Episode).where(Episode.episode_id == episode_id))
            await cleanup_db.execute(delete(Script).where(Script.id == script_id))
            await cleanup_db.execute(delete(CreditAccount).where(CreditAccount.id == account_id))
            await cleanup_db.commit()
        await engine.dispose()
