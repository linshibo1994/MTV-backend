"""视频 worker 终审补偿与租约边界回归测试。"""

from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded
from celery.exceptions import Reject, Retry
from sqlalchemy import update
from sqlalchemy.orm import Session

from app.models.credit_reservation import CreditReservation
from app.models.video_generation_job import VideoGenerationJob
from tests.test_video_generation import (
    _add_worker_job,
    _AsyncSessionAdapter,
    _make_db,
    _seed,
)


def test_sync_wrapper_retries_cancellation_recovery_unavailable(monkeypatch) -> None:
    from app.workers import video_generation as worker

    provider_task_id = "provider-cancellation-recovery-unavailable"
    recovery_error = worker.VideoCancellationRecoveryUnavailable(
        "video_cancellation_recovery_unavailable",
        provider_task_id=provider_task_id,
    )
    retry = Mock(wraps=worker.run_video_generation.retry)
    retry_signature = Mock()
    signature_from_request = Mock(return_value=retry_signature)

    async_calls = 0

    def run_async(awaitable):
        nonlocal async_calls
        awaitable.close()
        async_calls += 1
        if async_calls == 1:
            raise recovery_error
        return True

    monkeypatch.setattr(worker, "run_worker_async", Mock(side_effect=run_async))
    monkeypatch.setattr(worker.run_video_generation, "retry", retry)
    monkeypatch.setattr(
        worker.run_video_generation,
        "signature_from_request",
        signature_from_request,
    )

    worker.run_video_generation.push_request(
        called_directly=False,
        is_eager=False,
        retries=0,
    )
    task_request = worker.run_video_generation.request
    try:
        with pytest.raises(Retry) as exc_info:
            worker.run_video_generation.run("job-cancellation-recovery-unavailable")
    finally:
        worker.run_video_generation.pop_request()

    assert exc_info.value.exc is recovery_error
    assert exc_info.value.when == worker._ATTEMPT_CLEANUP_RETRY_SECONDS
    assert exc_info.value.__cause__ is recovery_error
    retry.assert_called_once_with(
        exc=recovery_error,
        countdown=worker._ATTEMPT_CLEANUP_RETRY_SECONDS,
        kwargs={"provider_task_id": provider_task_id},
        throw=False,
    )
    signature_from_request.assert_called_once_with(
        task_request,
        None,
        {"provider_task_id": provider_task_id},
        countdown=worker._ATTEMPT_CLEANUP_RETRY_SECONDS,
        eta=None,
        retries=1,
    )
    retry_signature.apply_async.assert_called_once_with()
    assert worker.run_video_generation.max_retries is None


def test_sync_wrapper_publishes_id_message_when_pending_debt_is_unavailable(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    provider_task_id = "provider-recovery-database-unavailable"
    recovery_error = worker.VideoCancellationRecoveryUnavailable(
        "video_cancellation_recovery_unavailable",
        provider_task_id=provider_task_id,
    )
    retry = Mock(wraps=worker.run_video_generation.retry)
    retry_signature = Mock()
    signature_from_request = Mock(return_value=retry_signature)
    async_calls = 0

    def run_async(awaitable):
        nonlocal async_calls
        awaitable.close()
        async_calls += 1
        if async_calls == 1:
            raise recovery_error
        return False

    monkeypatch.setattr(worker, "run_worker_async", Mock(side_effect=run_async))
    monkeypatch.setattr(worker.run_video_generation, "retry", retry)
    monkeypatch.setattr(
        worker.run_video_generation,
        "signature_from_request",
        signature_from_request,
    )

    worker.run_video_generation.push_request(
        called_directly=False,
        is_eager=False,
        retries=0,
    )
    task_request = worker.run_video_generation.request
    try:
        with pytest.raises(Retry) as exc_info:
            worker.run_video_generation.run("job-recovery-database-unavailable")
    finally:
        worker.run_video_generation.pop_request()

    assert exc_info.value.exc is recovery_error
    retry.assert_called_once_with(
        exc=recovery_error,
        countdown=worker._ATTEMPT_CLEANUP_RETRY_SECONDS,
        kwargs={"provider_task_id": provider_task_id},
        throw=False,
    )
    signature_from_request.assert_called_once_with(
        task_request,
        None,
        {"provider_task_id": provider_task_id},
        countdown=worker._ATTEMPT_CLEANUP_RETRY_SECONDS,
        eta=None,
        retries=1,
    )
    retry_signature.apply_async.assert_called_once_with()


def test_sync_wrapper_retries_both_channels_after_database_and_broker_failure(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    provider_task_id = "provider-double-channel-recovery"
    recovery_error = worker.VideoCancellationRecoveryUnavailable(
        "video_cancellation_recovery_unavailable",
        provider_task_id=provider_task_id,
    )
    events: list[str] = []
    async_calls = 0

    def run_async(awaitable):
        nonlocal async_calls
        awaitable.close()
        async_calls += 1
        if async_calls == 1:
            raise recovery_error
        events.append("database_failed")
        return False

    first_signature = Mock()
    second_signature = Mock()

    def first_publish() -> None:
        events.append("broker_failed")
        raise ConnectionError("broker unavailable")

    def second_publish() -> None:
        events.append("broker_recovered")

    first_signature.apply_async.side_effect = first_publish
    second_signature.apply_async.side_effect = second_publish
    signature_from_request = Mock(side_effect=[first_signature, second_signature])
    sleep = Mock(side_effect=lambda _seconds: events.append("backoff"))
    monkeypatch.setattr(worker, "run_worker_async", Mock(side_effect=run_async))
    monkeypatch.setattr(
        worker.run_video_generation,
        "signature_from_request",
        signature_from_request,
    )
    monkeypatch.setattr(worker, "sleep", sleep, raising=False)

    worker.run_video_generation.push_request(
        called_directly=False,
        is_eager=False,
        retries=0,
    )
    try:
        with pytest.raises(Retry) as exc_info:
            worker.run_video_generation.run("job-double-channel-recovery")
    finally:
        worker.run_video_generation.pop_request()

    assert exc_info.value.exc is recovery_error
    assert events == [
        "database_failed",
        "broker_failed",
        "backoff",
        "database_failed",
        "broker_recovered",
    ]
    assert signature_from_request.call_count == 2
    assert all(
        call.args[2] == {"provider_task_id": provider_task_id}
        for call in signature_from_request.call_args_list
    )
    sleep.assert_called_once_with(worker._CANCELLATION_RECOVERY_CHANNEL_RETRY_SECONDS)


def test_sync_wrapper_keeps_recovery_loop_after_soft_time_limit(monkeypatch) -> None:
    from app.workers import video_generation as worker

    provider_task_id = "provider-soft-limit-recovery"
    recovery_error = worker.VideoCancellationRecoveryUnavailable(
        "video_cancellation_recovery_unavailable",
        provider_task_id=provider_task_id,
    )
    async_calls = 0

    def run_async(awaitable):
        nonlocal async_calls
        awaitable.close()
        async_calls += 1
        if async_calls == 1:
            raise recovery_error
        if async_calls == 2:
            raise SoftTimeLimitExceeded()
        return False

    first_signature = Mock()
    first_signature.apply_async.side_effect = ConnectionError("broker unavailable")
    second_signature = Mock()
    signature_from_request = Mock(side_effect=[first_signature, second_signature])
    monkeypatch.setattr(worker, "run_worker_async", Mock(side_effect=run_async))
    monkeypatch.setattr(
        worker.run_video_generation,
        "signature_from_request",
        signature_from_request,
    )
    monkeypatch.setattr(worker, "sleep", Mock(), raising=False)

    worker.run_video_generation.push_request(
        called_directly=False,
        is_eager=False,
        retries=0,
    )
    try:
        with pytest.raises(Retry) as exc_info:
            worker.run_video_generation.run("job-soft-limit-recovery")
    finally:
        worker.run_video_generation.pop_request()

    assert exc_info.value.exc is recovery_error
    assert signature_from_request.call_count == 2


def test_explicit_recovery_message_republishes_same_id_after_repeated_db_errors(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    provider_task_id = "provider-explicit-recovery-db-errors"
    retry = Mock(wraps=worker.run_video_generation.retry)
    retry_signature = Mock()
    signature_from_request = Mock(return_value=retry_signature)
    effects = iter(
        [
            ConnectionError("primary database unavailable 1"),
            False,
            ConnectionError("primary database unavailable 2"),
            False,
        ]
    )

    def run_async(awaitable):
        awaitable.close()
        effect = next(effects)
        if isinstance(effect, Exception):
            raise effect
        return effect

    monkeypatch.setattr(worker, "run_worker_async", Mock(side_effect=run_async))
    monkeypatch.setattr(worker.run_video_generation, "retry", retry)
    monkeypatch.setattr(
        worker.run_video_generation,
        "signature_from_request",
        signature_from_request,
    )

    worker.run_video_generation.push_request(
        called_directly=False,
        is_eager=False,
        retries=0,
    )
    retry_errors: list[Retry] = []
    try:
        for _ in range(2):
            with pytest.raises(Retry) as exc_info:
                worker.run_video_generation.run(
                    "job-explicit-recovery-db-errors",
                    provider_task_id=provider_task_id,
                )
            retry_errors.append(exc_info.value)
    finally:
        worker.run_video_generation.pop_request()

    assert len(retry_errors) == 2
    assert all(
        isinstance(retry_error.exc, worker.VideoCancellationRecoveryUnavailable)
        and retry_error.exc.provider_task_id == provider_task_id
        for retry_error in retry_errors
    )
    assert retry.call_count == 2
    assert signature_from_request.call_count == 2
    assert all(
        call.args[2] == {"provider_task_id": provider_task_id}
        for call in signature_from_request.call_args_list
    )
    assert retry_signature.apply_async.call_count == 2


def test_explicit_recovery_message_turns_entry_soft_limit_into_id_retry(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    provider_task_id = "provider-explicit-recovery-soft-limit"
    timeout_error = SoftTimeLimitExceeded()
    retry = Mock(wraps=worker.run_video_generation.retry)
    retry_signature = Mock()
    signature_from_request = Mock(return_value=retry_signature)
    effects = iter([timeout_error, False])

    def run_async(awaitable):
        awaitable.close()
        effect = next(effects)
        if isinstance(effect, Exception):
            raise effect
        return effect

    monkeypatch.setattr(worker, "run_worker_async", Mock(side_effect=run_async))
    monkeypatch.setattr(worker.run_video_generation, "retry", retry)
    monkeypatch.setattr(
        worker.run_video_generation,
        "signature_from_request",
        signature_from_request,
    )

    worker.run_video_generation.push_request(
        called_directly=False,
        is_eager=False,
        retries=0,
    )
    try:
        with pytest.raises(Retry) as exc_info:
            worker.run_video_generation.run(
                "job-explicit-recovery-soft-limit",
                provider_task_id=provider_task_id,
            )
    finally:
        worker.run_video_generation.pop_request()

    recovery_error = exc_info.value.exc
    assert isinstance(recovery_error, worker.VideoCancellationRecoveryUnavailable)
    assert recovery_error.provider_task_id == provider_task_id
    assert recovery_error.__cause__ is timeout_error
    signature_from_request.assert_called_once()
    assert signature_from_request.call_args.args[2] == {
        "provider_task_id": provider_task_id
    }
    retry_signature.apply_async.assert_called_once_with()


def test_sync_wrapper_persists_pending_debt_before_retry_publish_failure(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    provider_task_id = "provider-retry-publish-failure"
    recovery_error = worker.VideoCancellationRecoveryUnavailable(
        "video_cancellation_recovery_unavailable",
        provider_task_id=provider_task_id,
    )
    events: list[str] = []
    async_calls = 0

    def run_async(awaitable):
        nonlocal async_calls
        awaitable.close()
        async_calls += 1
        if async_calls == 1:
            raise recovery_error
        events.append("pending_debt_committed")
        return True

    retry_signature = Mock()

    def fail_publish() -> None:
        events.append("retry_publish")
        raise ConnectionError("broker unavailable")

    retry_signature.apply_async.side_effect = fail_publish
    monkeypatch.setattr(worker, "run_worker_async", Mock(side_effect=run_async))
    monkeypatch.setattr(
        worker.run_video_generation,
        "signature_from_request",
        Mock(return_value=retry_signature),
    )

    worker.run_video_generation.push_request(
        called_directly=False,
        is_eager=False,
        retries=0,
    )
    try:
        with pytest.raises(Reject) as exc_info:
            worker.run_video_generation.run("job-retry-publish-failure")
    finally:
        worker.run_video_generation.pop_request()

    assert events == ["pending_debt_committed", "retry_publish"]
    assert exc_info.value.requeue is False


@pytest.mark.parametrize("timeout_type", [SoftTimeLimitExceeded, TimeLimitExceeded])
def test_sync_wrapper_does_not_retry_time_limits(monkeypatch, timeout_type) -> None:
    from app.workers import video_generation as worker

    timeout_error = timeout_type()
    retry = Mock()

    def raise_timeout(awaitable) -> None:
        awaitable.close()
        raise timeout_error

    monkeypatch.setattr(worker, "run_worker_async", Mock(side_effect=raise_timeout))
    monkeypatch.setattr(worker.run_video_generation, "retry", retry)

    with pytest.raises(timeout_type) as exc_info:
        worker.run_video_generation.run(f"job-{timeout_type.__name__}")

    assert exc_info.value is timeout_error
    retry.assert_not_called()


def test_sync_wrapper_propagates_hard_limit_with_explicit_recovery_id(monkeypatch) -> None:
    from app.workers import video_generation as worker

    timeout_error = TimeLimitExceeded()
    retry = Mock()

    def raise_timeout(awaitable) -> None:
        awaitable.close()
        raise timeout_error

    monkeypatch.setattr(worker, "run_worker_async", Mock(side_effect=raise_timeout))
    monkeypatch.setattr(worker.run_video_generation, "retry", retry)

    with pytest.raises(TimeLimitExceeded) as exc_info:
        worker.run_video_generation.run(
            "job-explicit-recovery-hard-limit",
            provider_task_id="provider-explicit-recovery-hard-limit",
        )

    assert exc_info.value is timeout_error
    retry.assert_not_called()


class _SQLiteAsyncSessionFactory:
    def __init__(self, engine) -> None:
        self.engine = engine

    def __call__(self):
        engine = self.engine

        class _Context:
            async def __aenter__(self):
                self.raw = Session(engine, expire_on_commit=False)
                return _AsyncSessionAdapter(self.raw)

            async def __aexit__(self, exc_type, exc, tb):
                self.raw.close()

        return _Context()


@pytest.mark.asyncio
@pytest.mark.parametrize("authority_source", ["output", "metrics", "debt"])
async def test_stale_cancellation_result_cannot_overwrite_authoritative_provider_id(
    authority_source: str,
) -> None:
    from app.providers.video_gen import VideoCancellationOutcome, VideoCancellationResult
    from app.services.video_remote_cancellation import persist_provider_cancellation_result

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id=f"job-stale-cancellation-result-{authority_source}",
        reservation_id=f"reservation-stale-cancellation-result-{authority_source}",
    )
    authoritative_task_id = "provider-authoritative-a"
    debt = {
        "provider_task_id": authoritative_task_id,
        "outcome": "failed",
        "detail": "previous failure",
        "retryable": True,
        "attempts": 7,
    }
    job.status = "canceled"
    job.lease_owner = None
    job.lease_token = None
    job.lease_until = None
    job.output_json = (
        {"provider_task_id": authoritative_task_id}
        if authority_source == "output"
        else {"provider_cancellation": debt}
        if authority_source == "debt"
        else {}
    )
    job.metrics = (
        {"provider_task_id": authoritative_task_id}
        if authority_source == "metrics"
        else {}
    )
    raw.commit()
    before_output = deepcopy(job.output_json)
    before_metrics = deepcopy(job.metrics)
    before_version = job.version
    db = _AsyncSessionAdapter(raw)

    persisted = await persist_provider_cancellation_result(
        db,
        job.job_id,
        VideoCancellationResult(
            provider_task_id="provider-stale-b",
            outcome=VideoCancellationOutcome.FAILED,
            detail="stale result",
        ),
    )

    assert persisted is False
    await db.commit()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert saved.output_json == before_output
    assert saved.metrics == before_metrics
    assert saved.version == before_version
    if authority_source == "debt":
        assert (saved.output_json or {})["provider_cancellation"]["attempts"] == 7


@pytest.mark.asyncio
async def test_reconciler_does_not_call_provider_for_conflicting_authority(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-conflicting-cancellation-authority",
        reservation_id="reservation-conflicting-cancellation-authority",
    )
    job.status = "canceled"
    job.output_json = {
        "provider_task_id": "provider-authoritative-a",
        "provider_cancellation": {
            "provider_task_id": "provider-stale-b",
            "outcome": "failed",
            "detail": "previous failure",
            "retryable": True,
            "attempts": 4,
        },
    }
    job.metrics = {"provider_task_id": "provider-authoritative-a"}
    raw.commit()
    db = _AsyncSessionAdapter(raw)
    cancel = AsyncMock()
    monkeypatch.setattr(
        worker,
        "get_video_provider",
        lambda _provider_key: SimpleNamespace(cancel=cancel),
    )

    assert await worker.reconcile_video_remote_cancellations(db) == 0

    cancel.assert_not_awaited()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert (saved.output_json or {})["provider_task_id"] == "provider-authoritative-a"
    assert (saved.output_json or {})["provider_cancellation"]["attempts"] == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_outcome", ["canceled", "unsupported"])
async def test_same_id_record_conflict_does_not_repeat_or_downgrade_provider_cancel(
    monkeypatch,
    terminal_outcome: str,
) -> None:
    from app.providers.video_gen import VideoCancellationOutcome, VideoCancellationResult
    from app.services.video_remote_cancellation import persist_provider_cancellation_result
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id=f"job-same-id-record-conflict-{terminal_outcome}",
        reservation_id=f"reservation-same-id-record-conflict-{terminal_outcome}",
    )
    provider_task_id = f"provider-same-id-record-conflict-{terminal_outcome}"
    job.status = "canceled"
    job.output_json = {
        "provider_task_id": provider_task_id,
        "provider_cancellation": {
            "provider_task_id": provider_task_id,
            "outcome": "failed",
            "detail": "older retryable failure",
            "retryable": True,
            "attempts": 5,
        },
    }
    job.metrics = {
        "provider_task_id": provider_task_id,
        "provider_cancellation": {
            "provider_task_id": provider_task_id,
            "outcome": terminal_outcome,
            "detail": "confirmed terminal result",
            "retryable": False,
            "attempts": 6,
        },
    }
    raw.commit()
    before_output = deepcopy(job.output_json)
    before_metrics = deepcopy(job.metrics)
    before_version = job.version
    db = _AsyncSessionAdapter(raw)

    persisted = await persist_provider_cancellation_result(
        db,
        job.job_id,
        VideoCancellationResult(
            provider_task_id=provider_task_id,
            outcome=VideoCancellationOutcome.FAILED,
            detail="late duplicate failure",
        ),
    )

    assert persisted is False
    await db.rollback()
    cancel = AsyncMock()
    monkeypatch.setattr(
        worker,
        "get_video_provider",
        lambda _provider_key: SimpleNamespace(cancel=cancel),
    )
    assert await worker.reconcile_video_remote_cancellations(db) == 0

    cancel.assert_not_awaited()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert saved.output_json == before_output
    assert saved.metrics == before_metrics
    assert saved.version == before_version


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout_type", [SoftTimeLimitExceeded, TimeLimitExceeded])
async def test_independent_cancellation_persistence_propagates_time_limit(
    monkeypatch,
    timeout_type: type[Exception],
) -> None:
    from app.providers.video_gen import VideoCancellationOutcome, VideoCancellationResult
    from app.services import video_remote_cancellation
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )
    timeout_error = timeout_type()
    monkeypatch.setattr(
        video_remote_cancellation,
        "persist_provider_cancellation_result",
        AsyncMock(side_effect=timeout_error),
    )

    with pytest.raises(timeout_type) as exc_info:
        await worker._persist_video_cancellation_result_independently(
            "job-independent-cancellation-timeout",
            VideoCancellationResult(
                provider_task_id="provider-independent-cancellation-timeout",
                outcome=VideoCancellationOutcome.FAILED,
            ),
        )

    assert exc_info.value is timeout_error
    raw.close()
    engine.dispose()


@pytest.mark.asyncio
async def test_retried_canceled_job_repairs_missing_pending_debt(monkeypatch) -> None:
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-retried-cancellation-recovery",
        reservation_id="reservation-retried-cancellation-recovery",
    )
    provider_task_id = "provider-retried-cancellation-recovery"
    job.status = "canceled"
    job.progress_stage = "canceled"
    job.lease_owner = None
    job.lease_token = None
    job.lease_until = None
    job.output_json = {"provider_task_id": provider_task_id}
    job.metrics = {"provider_task_id": provider_task_id}
    raw.commit()
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )

    result = await worker._run_video_generation_async(
        job.job_id,
        provider_task_id="provider-stale-retry-message",
    )

    assert result == {
        "job_id": job.job_id,
        "status": "canceled",
        "storage_key": None,
    }
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert (saved.output_json or {})["provider_cancellation"] == {
        "provider_task_id": provider_task_id,
        "outcome": "pending",
        "detail": None,
        "retryable": True,
        "attempts": 0,
    }
    version_after_repair = saved.version

    await worker._run_video_generation_async(
        job.job_id,
        provider_task_id="provider-stale-retry-message",
    )

    raw.expire_all()
    replayed = raw.get(VideoGenerationJob, job.job_id)
    assert replayed is not None
    assert replayed.version == version_after_repair


@pytest.mark.asyncio
@pytest.mark.parametrize("record_side", ["metrics", "output"])
async def test_claim_miss_mirrors_one_sided_cancellation_debt_before_reconcile(
    monkeypatch,
    record_side: str,
) -> None:
    from app.providers.video_gen import VideoCancellationOutcome, VideoCancellationResult
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id=f"job-one-sided-cancellation-debt-{record_side}",
        reservation_id=f"reservation-one-sided-cancellation-debt-{record_side}",
    )
    provider_task_id = f"provider-one-sided-cancellation-debt-{record_side}"
    record = {
        "provider_task_id": provider_task_id,
        "outcome": "failed",
        "detail": "existing provider failure",
        "retryable": True,
        "attempts": 5,
    }
    job.status = "canceled"
    job.progress_stage = "canceled"
    job.lease_owner = None
    job.lease_token = None
    job.lease_until = None
    job.output_json = (
        {
            "provider_task_id": provider_task_id,
            "provider_cancellation": deepcopy(record),
        }
        if record_side == "output"
        else {}
    )
    job.metrics = (
        {
            "provider_task_id": provider_task_id,
            "provider_cancellation": deepcopy(record),
        }
        if record_side == "metrics"
        else {}
    )
    raw.commit()
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )

    await worker._run_video_generation_async(job.job_id)

    raw.expire_all()
    mirrored = raw.get(VideoGenerationJob, job.job_id)
    assert mirrored is not None
    assert (mirrored.output_json or {})["provider_task_id"] == provider_task_id
    assert (mirrored.metrics or {})["provider_task_id"] == provider_task_id
    assert (mirrored.output_json or {})["provider_cancellation"] == record
    assert (mirrored.metrics or {})["provider_cancellation"] == record
    mirrored_version = mirrored.version

    await worker._run_video_generation_async(job.job_id)

    raw.expire_all()
    replayed = raw.get(VideoGenerationJob, job.job_id)
    assert replayed is not None
    assert replayed.version == mirrored_version
    assert (replayed.output_json or {})["provider_cancellation"]["attempts"] == 5
    assert (replayed.metrics or {})["provider_cancellation"]["attempts"] == 5

    cancel = AsyncMock(
        return_value=VideoCancellationResult(
            provider_task_id=provider_task_id,
            outcome=VideoCancellationOutcome.CANCELED,
        )
    )
    monkeypatch.setattr(
        worker,
        "get_video_provider",
        lambda _provider_key: SimpleNamespace(cancel=cancel),
    )
    db = _AsyncSessionAdapter(raw)

    assert await worker.reconcile_video_remote_cancellations(db) == 1
    assert await worker.reconcile_video_remote_cancellations(db) == 0

    cancel.assert_awaited_once_with(provider_task_id)
    raw.expire_all()
    reconciled = raw.get(VideoGenerationJob, job.job_id)
    assert reconciled is not None
    assert (reconciled.output_json or {})["provider_cancellation"]["attempts"] == 6
    assert (reconciled.metrics or {})["provider_cancellation"]["attempts"] == 6


@pytest.mark.asyncio
async def test_ensure_pending_rejects_conflicting_record_ids_without_writing() -> None:
    from app.services.video_remote_cancellation import ensure_pending_provider_cancellation

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-conflicting-one-sided-cancellation-records",
        reservation_id="reservation-conflicting-one-sided-cancellation-records",
    )
    job.status = "canceled"
    job.lease_owner = None
    job.lease_token = None
    job.lease_until = None
    job.output_json = {
        "provider_cancellation": {
            "provider_task_id": "provider-authoritative-a",
            "outcome": "failed",
            "detail": "output failure",
            "retryable": True,
            "attempts": 3,
        }
    }
    job.metrics = {
        "provider_cancellation": {
            "provider_task_id": "provider-stale-b",
            "outcome": "failed",
            "detail": "metrics failure",
            "retryable": True,
            "attempts": 8,
        }
    }
    raw.commit()
    before_output = deepcopy(job.output_json)
    before_metrics = deepcopy(job.metrics)
    before_version = job.version
    db = _AsyncSessionAdapter(raw)

    recovered = await ensure_pending_provider_cancellation(
        db,
        job.job_id,
        "provider-authoritative-a",
    )

    assert recovered is False
    await db.commit()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert saved.output_json == before_output
    assert saved.metrics == before_metrics
    assert saved.version == before_version


@pytest.mark.asyncio
async def test_retried_canceled_job_uses_message_provider_id_when_database_has_none(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-retried-cancellation-message-authority",
        reservation_id="reservation-retried-cancellation-message-authority",
    )
    provider_task_id = "provider-retried-cancellation-message-authority"
    job.status = "canceled"
    job.progress_stage = "canceled"
    job.lease_owner = None
    job.lease_token = None
    job.lease_until = None
    job.output_json = {}
    job.metrics = {}
    raw.commit()
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )

    result = await worker._run_video_generation_async(
        job.job_id,
        provider_task_id=provider_task_id,
    )

    assert result["status"] == "canceled"
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert (saved.output_json or {})["provider_task_id"] == provider_task_id
    assert (saved.metrics or {})["provider_task_id"] == provider_task_id
    assert (saved.output_json or {})["provider_cancellation"] == {
        "provider_task_id": provider_task_id,
        "outcome": "pending",
        "detail": None,
        "retryable": True,
        "attempts": 0,
    }


@pytest.mark.asyncio
async def test_delayed_message_leaves_expired_processing_lease_for_stale_recovery(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-delayed-stale",
        reservation_id="reservation-delayed-stale",
    )
    old_lease_token = "old-expired-video-lease"
    attempt_id = hashlib.sha256(old_lease_token.encode("utf-8")).hexdigest()[:16]
    job.status = "processing"
    job.lease_owner = "crashed-worker"
    job.lease_token = old_lease_token
    job.lease_until = datetime.now(UTC) - timedelta(minutes=1)
    job.output_json = {"provider_task_id": "provider-old-lease"}
    raw.commit()
    db = _AsyncSessionAdapter(raw)

    class Provider:
        calls = 0

        async def generate(self, _request, _on_progress=None):
            self.calls += 1
            raise AssertionError("延迟消息不得直接接管过期 processing 租约")

    provider = Provider()
    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="delayed-celery-message",
        provider=provider,
        extract_last_frame=AsyncMock(),
    )

    raw.expire_all()
    before_recovery = raw.get(VideoGenerationJob, job.job_id)
    assert before_recovery is not None
    assert before_recovery.status == "processing"
    assert before_recovery.lease_token == old_lease_token
    assert (before_recovery.output_json or {})["provider_task_id"] == "provider-old-lease"
    assert provider.calls == 0

    assert await worker.reconcile_stale_video_jobs(db) == [job.job_id]
    raw.expire_all()
    recovered = raw.get(VideoGenerationJob, job.job_id)
    assert recovered is not None
    assert recovered.status == "starting"
    assert recovered.lease_token is None
    assert recovered.lease_owner is None
    assert recovered.lease_until is None
    assert (recovered.output_json or {})["provider_task_id"] == "provider-old-lease"
    assert (recovered.output_json or {})[worker._ATTEMPT_CLEANUP_DEBT_KEY][attempt_id] == {
        "storage_attempt_id": attempt_id,
        "shot_group_id": job.shot_group_id,
        "video_storage_key": f"generate_images/video/{job.job_id}-{attempt_id}.mp4",
        # cleanup debt 中的持久化引用同样使用 canonical 相对 key。
        "last_frame_storage_key": (
            f"generate_images/video/frames/{job.shot_group_id}_{attempt_id}_last.png"
        ),
        "provider_task_id": "provider-old-lease",
        "retryable": True,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout_type", [SoftTimeLimitExceeded, TimeLimitExceeded])
async def test_provider_timeout_propagates_and_preserves_processing_lease(
    monkeypatch,
    timeout_type: type[Exception],
) -> None:
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id=f"job-provider-timeout-{timeout_type.__name__}",
        reservation_id=f"reservation-provider-timeout-{timeout_type.__name__}",
    )
    db = _AsyncSessionAdapter(raw)
    provider_task_id = f"provider-timeout-{timeout_type.__name__}"

    class Provider:
        def __init__(self) -> None:
            self.cancel = AsyncMock()

        async def generate(self, _request, on_progress):
            on_progress(
                SimpleNamespace(
                    progress=35,
                    stage="polling",
                    message="等待远端结果",
                    provider_task_id=provider_task_id,
                )
            )
            raise timeout_type()

    provider = Provider()
    release = AsyncMock()
    monkeypatch.setattr(worker, "_credits_release", release)

    with pytest.raises(timeout_type):
        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="timeout-worker",
            provider=provider,
            extract_last_frame=AsyncMock(),
        )

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert saved.status == "processing"
    assert saved.lease_token is not None
    assert (saved.output_json or {})["provider_task_id"] == provider_task_id
    assert (saved.metrics or {})["provider_task_id"] == provider_task_id
    assert reservation is not None
    assert reservation.status == "reserved"
    provider.cancel.assert_not_awaited()
    release.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cancellation_outcome", "expected_status", "expected_reservation_status"),
    [
        ("canceled", "failed", "released"),
        ("failed", "processing", "reserved"),
        ("unsupported", "processing", "reserved"),
    ],
)
async def test_generate_failure_uses_authoritative_provider_id_before_release(
    cancellation_outcome: str,
    expected_status: str,
    expected_reservation_status: str,
) -> None:
    from app.providers.video_gen import VideoCancellationOutcome, VideoCancellationResult
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id=f"job-generate-failure-{cancellation_outcome}",
        reservation_id=f"reservation-generate-failure-{cancellation_outcome}",
    )
    db = _AsyncSessionAdapter(raw)
    provider_task_id = f"provider-generate-failure-{cancellation_outcome}"

    class Provider:
        def __init__(self) -> None:
            self.cancel = AsyncMock(
                return_value=VideoCancellationResult(
                    provider_task_id=provider_task_id,
                    outcome=VideoCancellationOutcome(cancellation_outcome),
                )
            )

        async def generate(self, _request, on_progress):
            on_progress(
                SimpleNamespace(
                    progress=25,
                    stage="polling",
                    message="远端任务已提交",
                    provider_task_id=provider_task_id,
                )
            )
            raise RuntimeError("provider polling failed")

    provider = Provider()
    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="provider-failure-worker",
        provider=provider,
        extract_last_frame=AsyncMock(),
    )

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert saved.status == expected_status
    assert (saved.output_json or {})["provider_task_id"] == provider_task_id
    assert (saved.metrics or {})["provider_task_id"] == provider_task_id
    assert reservation is not None
    assert reservation.status == expected_reservation_status
    provider.cancel.assert_awaited_once_with(provider_task_id)


@pytest.mark.asyncio
async def test_generate_failure_keeps_lease_when_remote_cancel_channel_fails() -> None:
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-generate-failure-cancel-error",
        reservation_id="reservation-generate-failure-cancel-error",
    )
    db = _AsyncSessionAdapter(raw)
    provider_task_id = "provider-generate-failure-cancel-error"

    class Provider:
        def __init__(self) -> None:
            self.cancel = AsyncMock(side_effect=ConnectionError("cancel unavailable"))

        async def generate(self, _request, on_progress):
            on_progress(
                SimpleNamespace(
                    progress=25,
                    stage="polling",
                    message="远端任务已提交",
                    provider_task_id=provider_task_id,
                )
            )
            raise RuntimeError("provider polling failed")

    provider = Provider()
    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="provider-failure-worker",
        provider=provider,
        extract_last_frame=AsyncMock(),
    )

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert saved.status == "processing"
    assert saved.lease_token is not None
    assert reservation is not None
    assert reservation.status == "reserved"
    provider.cancel.assert_awaited_once_with(provider_task_id)


@pytest.mark.asyncio
async def test_cancellation_fact_commits_before_attempt_cleanup_failure(monkeypatch) -> None:
    from app.providers.video_gen import (
        VideoCancellationOutcome,
        VideoCancellationResult,
        VideoGenerationCancelled,
    )
    from app.services import video_remote_cancellation
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-cancellation-fact-before-cleanup",
        reservation_id="reservation-cancellation-fact-before-cleanup",
    )
    db = _AsyncSessionAdapter(raw)
    provider_task_id = "provider-cancellation-fact-before-cleanup"
    events: list[str] = []

    class Provider:
        async def generate(self, request, _on_progress=None):
            saved = raw.get(VideoGenerationJob, request.job_id)
            assert saved is not None
            saved.status = "canceled"
            saved.progress_stage = "canceled"
            saved.lease_owner = None
            saved.lease_token = None
            saved.lease_until = None
            raw.commit()
            raise VideoGenerationCancelled(
                VideoCancellationResult(
                    provider_task_id=provider_task_id,
                    outcome=VideoCancellationOutcome.FAILED,
                    detail="remote cancellation unavailable",
                )
            )

    original_persist = video_remote_cancellation.persist_provider_cancellation_result

    async def persist_then_record(*args, **kwargs):
        events.append("persist")
        return await original_persist(*args, **kwargs)

    async def fail_cleanup(**_kwargs):
        events.append("cleanup")
        raise RuntimeError("video_attempt_cleanup_unavailable")

    cleanup = AsyncMock(side_effect=fail_cleanup)
    release = AsyncMock()
    monkeypatch.setattr(
        video_remote_cancellation,
        "persist_provider_cancellation_result",
        persist_then_record,
    )
    monkeypatch.setattr(worker, "_cleanup_or_schedule_video_attempt_outputs", cleanup)
    monkeypatch.setattr(worker, "_credits_release", release)

    with pytest.raises(RuntimeError, match="video_attempt_cleanup_unavailable"):
        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="cancellation-worker",
            provider=Provider(),
            extract_last_frame=AsyncMock(),
        )

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert saved.status == "canceled"
    assert (saved.output_json or {})["provider_task_id"] == provider_task_id
    assert (saved.output_json or {})["provider_cancellation"] == {
        "provider_task_id": provider_task_id,
        "outcome": "failed",
        "detail": "remote cancellation unavailable",
        "retryable": True,
        "attempts": 1,
    }
    assert events == ["persist", "cleanup"]
    assert reservation is not None
    assert reservation.status == "reserved"
    cleanup.assert_awaited_once()
    release.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("rolled_back", [True, False], ids=["primary", "independent"])
@pytest.mark.parametrize("persist_mode", ["false", "error"])
async def test_cancellation_persistence_failure_preserves_pending_debt_without_cleanup(
    monkeypatch,
    rolled_back: bool,
    persist_mode: str,
) -> None:
    from app.providers.video_gen import (
        VideoCancellationOutcome,
        VideoCancellationResult,
        VideoGenerationCancelled,
    )
    from app.services import video_remote_cancellation
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id=f"job-cancel-persist-{rolled_back}-{persist_mode}",
        reservation_id=f"reservation-cancel-persist-{rolled_back}-{persist_mode}",
    )
    db = _AsyncSessionAdapter(raw)
    provider_task_id = f"provider-cancel-persist-{rolled_back}-{persist_mode}"
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )

    class Provider:
        async def generate(self, request, _on_progress=None):
            saved = raw.get(VideoGenerationJob, request.job_id)
            assert saved is not None
            saved.status = "canceled"
            saved.progress_stage = "canceled"
            saved.lease_owner = None
            saved.lease_token = None
            saved.lease_until = None
            saved.output_json, saved.metrics = (
                video_remote_cancellation.merge_pending_provider_cancellation(
                    saved.output_json,
                    saved.metrics,
                    provider_task_id,
                )
            )
            raw.commit()
            raise VideoGenerationCancelled(
                VideoCancellationResult(
                    provider_task_id=provider_task_id,
                    outcome=VideoCancellationOutcome.FAILED,
                    detail="remote cancellation unavailable",
                )
            )

    persist_effect = (
        RuntimeError("cancellation persistence unavailable")
        if persist_mode == "error"
        else None
    )
    if rolled_back:
        persist = AsyncMock(
            return_value=False,
            side_effect=persist_effect,
        )
        monkeypatch.setattr(
            video_remote_cancellation,
            "persist_provider_cancellation_result",
            persist,
        )
    else:
        monkeypatch.setattr(worker, "_best_effort_rollback", AsyncMock(return_value=False))
        persist = AsyncMock(
            return_value=False,
            side_effect=persist_effect,
        )
        monkeypatch.setattr(
            worker,
            "_persist_video_cancellation_result_independently",
            persist,
        )

    cleanup = AsyncMock(side_effect=AssertionError("持久化失败后不得清理产物"))
    release = AsyncMock()
    mark_failed_once = AsyncMock()
    mark_failed = AsyncMock()
    monkeypatch.setattr(worker, "_cleanup_or_schedule_video_attempt_outputs", cleanup)
    monkeypatch.setattr(worker, "_credits_release", release)
    monkeypatch.setattr(worker, "_mark_failed_and_release_once", mark_failed_once)
    monkeypatch.setattr(worker, "_mark_failed_and_release", mark_failed)

    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="cancellation-worker",
        provider=Provider(),
        extract_last_frame=AsyncMock(),
    )

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert saved.status == "canceled"
    assert (saved.output_json or {})["provider_task_id"] == provider_task_id
    assert (saved.output_json or {})["provider_cancellation"] == {
        "provider_task_id": provider_task_id,
        "outcome": "pending",
        "detail": None,
        "retryable": True,
        "attempts": 0,
    }
    assert reservation is not None
    assert reservation.status == "reserved"
    persist.assert_awaited_once()
    cleanup.assert_not_awaited()
    release.assert_not_awaited()
    mark_failed_once.assert_not_awaited()
    mark_failed.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancellation_persistence_failure_backfills_missing_pending_debt(
    monkeypatch,
) -> None:
    from app.providers.video_gen import (
        VideoCancellationOutcome,
        VideoCancellationResult,
        VideoGenerationCancelled,
    )
    from app.services import video_remote_cancellation
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-cancel-backfill-pending",
        reservation_id="reservation-cancel-backfill-pending",
    )
    db = _AsyncSessionAdapter(raw)
    provider_task_id = "provider-cancel-backfill-pending"

    class Provider:
        async def generate(self, request, _on_progress=None):
            saved = raw.get(VideoGenerationJob, request.job_id)
            assert saved is not None
            saved.status = "canceled"
            saved.progress_stage = "canceled"
            saved.lease_owner = None
            saved.lease_token = None
            saved.lease_until = None
            saved.output_json = {"provider_task_id": provider_task_id}
            saved.metrics = {"provider_task_id": provider_task_id}
            raw.commit()
            raise VideoGenerationCancelled(
                VideoCancellationResult(
                    provider_task_id=provider_task_id,
                    outcome=VideoCancellationOutcome.FAILED,
                )
            )

    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )
    monkeypatch.setattr(
        video_remote_cancellation,
        "persist_provider_cancellation_result",
        AsyncMock(return_value=False),
    )
    cleanup = AsyncMock(side_effect=AssertionError("补建 pending 前后均不得清理产物"))
    monkeypatch.setattr(worker, "_cleanup_or_schedule_video_attempt_outputs", cleanup)

    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="cancellation-worker",
        provider=Provider(),
        extract_last_frame=AsyncMock(),
    )

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert (saved.output_json or {})["provider_cancellation"] == {
        "provider_task_id": provider_task_id,
        "outcome": "pending",
        "detail": None,
        "retryable": True,
        "attempts": 0,
    }
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancellation_persistence_soft_limit_becomes_id_recovery(
    monkeypatch,
) -> None:
    from app.providers.video_gen import (
        VideoCancellationOutcome,
        VideoCancellationResult,
        VideoGenerationCancelled,
    )
    from app.services import video_remote_cancellation
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-cancel-persist-soft-limit",
        reservation_id="reservation-cancel-persist-soft-limit",
    )
    db = _AsyncSessionAdapter(raw)
    provider_task_id = "provider-cancel-persist-soft-limit"
    timeout_error = SoftTimeLimitExceeded()

    class Provider:
        def __init__(self) -> None:
            self.generate_calls = 0
            self.cancel = AsyncMock(side_effect=AssertionError("不得再次取消 Provider 任务"))

        async def generate(self, request, _on_progress=None):
            self.generate_calls += 1
            saved = raw.get(VideoGenerationJob, request.job_id)
            assert saved is not None
            saved.status = "canceled"
            saved.progress_stage = "canceled"
            saved.lease_owner = None
            saved.lease_token = None
            saved.lease_until = None
            raw.commit()
            raise VideoGenerationCancelled(
                VideoCancellationResult(
                    provider_task_id=provider_task_id,
                    outcome=VideoCancellationOutcome.FAILED,
                )
            )

    provider = Provider()
    persist = AsyncMock(side_effect=timeout_error)
    recovery = AsyncMock(side_effect=AssertionError("持久化 soft limit 后应直接交给入口恢复"))
    cleanup = AsyncMock(side_effect=AssertionError("恢复事实未落盘前不得清理产物"))
    monkeypatch.setattr(
        video_remote_cancellation,
        "persist_provider_cancellation_result",
        persist,
    )
    monkeypatch.setattr(
        worker,
        "_ensure_video_cancellation_recovery_independently",
        recovery,
    )
    monkeypatch.setattr(worker, "_cleanup_or_schedule_video_attempt_outputs", cleanup)

    with pytest.raises(worker.VideoCancellationRecoveryUnavailable) as exc_info:
        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="cancellation-worker",
            provider=provider,
            extract_last_frame=AsyncMock(),
        )

    assert exc_info.value.provider_task_id == provider_task_id
    assert exc_info.value.__cause__ is timeout_error
    assert provider.generate_calls == 1
    provider.cancel.assert_not_awaited()
    persist.assert_awaited_once()
    recovery.assert_not_awaited()
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_cancellation_recovery_soft_limit_becomes_id_recovery(
    monkeypatch,
) -> None:
    from app.providers.video_gen import (
        VideoCancellationOutcome,
        VideoCancellationResult,
        VideoGenerationCancelled,
    )
    from app.services import video_remote_cancellation
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-cancel-pending-recovery-soft-limit",
        reservation_id="reservation-cancel-pending-recovery-soft-limit",
    )
    db = _AsyncSessionAdapter(raw)
    provider_task_id = "provider-cancel-pending-recovery-soft-limit"
    timeout_error = SoftTimeLimitExceeded()

    class Provider:
        def __init__(self) -> None:
            self.generate_calls = 0
            self.cancel = AsyncMock(side_effect=AssertionError("不得再次取消 Provider 任务"))

        async def generate(self, request, _on_progress=None):
            self.generate_calls += 1
            saved = raw.get(VideoGenerationJob, request.job_id)
            assert saved is not None
            saved.status = "canceled"
            saved.progress_stage = "canceled"
            saved.lease_owner = None
            saved.lease_token = None
            saved.lease_until = None
            raw.commit()
            raise VideoGenerationCancelled(
                VideoCancellationResult(
                    provider_task_id=provider_task_id,
                    outcome=VideoCancellationOutcome.FAILED,
                )
            )

    provider = Provider()
    persist = AsyncMock(return_value=False)
    recovery = AsyncMock(side_effect=timeout_error)
    cleanup = AsyncMock(side_effect=AssertionError("恢复事实未落盘前不得清理产物"))
    monkeypatch.setattr(
        video_remote_cancellation,
        "persist_provider_cancellation_result",
        persist,
    )
    monkeypatch.setattr(
        worker,
        "_ensure_video_cancellation_recovery_independently",
        recovery,
    )
    monkeypatch.setattr(worker, "_cleanup_or_schedule_video_attempt_outputs", cleanup)

    with pytest.raises(worker.VideoCancellationRecoveryUnavailable) as exc_info:
        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="cancellation-worker",
            provider=provider,
            extract_last_frame=AsyncMock(),
        )

    assert exc_info.value.provider_task_id == provider_task_id
    assert exc_info.value.__cause__ is timeout_error
    assert provider.generate_calls == 1
    provider.cancel.assert_not_awaited()
    persist.assert_awaited_once()
    recovery.assert_awaited_once_with(job.job_id, provider_task_id)
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancellation_without_fact_or_recovery_channel_raises_retryable_error(
    monkeypatch,
) -> None:
    from app.providers.video_gen import (
        VideoCancellationOutcome,
        VideoCancellationResult,
        VideoGenerationCancelled,
    )
    from app.services import video_remote_cancellation
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-cancel-recovery-unavailable",
        reservation_id="reservation-cancel-recovery-unavailable",
    )
    db = _AsyncSessionAdapter(raw)
    provider_task_id = "provider-cancel-recovery-unavailable"

    class Provider:
        async def generate(self, request, _on_progress=None):
            saved = raw.get(VideoGenerationJob, request.job_id)
            assert saved is not None
            saved.status = "canceled"
            saved.lease_owner = None
            saved.lease_token = None
            saved.lease_until = None
            saved.output_json = {}
            saved.metrics = {}
            raw.commit()
            raise VideoGenerationCancelled(
                VideoCancellationResult(
                    provider_task_id=provider_task_id,
                    outcome=VideoCancellationOutcome.FAILED,
                )
            )

    monkeypatch.setattr(
        video_remote_cancellation,
        "persist_provider_cancellation_result",
        AsyncMock(return_value=False),
    )
    recovery = AsyncMock(return_value=False)
    monkeypatch.setattr(
        worker,
        "_ensure_video_cancellation_recovery_independently",
        recovery,
        raising=False,
    )
    cleanup = AsyncMock()
    monkeypatch.setattr(worker, "_cleanup_or_schedule_video_attempt_outputs", cleanup)

    with pytest.raises(
        worker.VideoCancellationRecoveryUnavailable,
        match="video_cancellation_recovery_unavailable",
    ) as exc_info:
        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="cancellation-worker",
            provider=Provider(),
            extract_last_frame=AsyncMock(),
        )

    assert exc_info.value.provider_task_id == provider_task_id
    recovery.assert_awaited_once_with(job.job_id, provider_task_id)
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_retried_canceled_job_repairs_missing_provider_cancellation_debt(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-cancel-retry-repairs-debt",
        reservation_id="reservation-cancel-retry-repairs-debt",
    )
    provider_task_id = "provider-cancel-retry-repairs-debt"
    job.status = "canceled"
    job.lease_owner = None
    job.lease_token = None
    job.lease_until = None
    job.output_json = {"provider_task_id": provider_task_id}
    job.metrics = {"provider_task_id": provider_task_id}
    raw.commit()
    db = _AsyncSessionAdapter(raw)
    provider = SimpleNamespace(generate=AsyncMock())

    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="retry-worker",
        provider=provider,
        extract_last_frame=AsyncMock(),
    )

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert (saved.output_json or {})["provider_cancellation"] == {
        "provider_task_id": provider_task_id,
        "outcome": "pending",
        "detail": None,
        "retryable": True,
        "attempts": 0,
    }
    provider.generate.assert_not_awaited()


@pytest.mark.asyncio
async def test_claim_refreshes_stale_identity_before_cas_update() -> None:
    from app.crud.video import claim_video_job

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-claim-stale-identity",
        reservation_id="reservation-claim-stale-identity",
    )
    job.status = "processing"
    job.lease_owner = "old-worker"
    job.lease_token = "old-lease-token"
    job.lease_until = datetime.now(UTC) + timedelta(minutes=5)
    raw.commit()
    stale = raw.get(VideoGenerationJob, job.job_id)
    assert stale is not None
    assert stale.status == "processing"

    with Session(engine, expire_on_commit=False) as other:
        other.execute(
            update(VideoGenerationJob)
            .where(VideoGenerationJob.job_id == job.job_id)
            .values(
                status="starting",
                lease_owner=None,
                lease_token=None,
                lease_until=None,
            )
        )
        other.commit()

    assert stale.status == "processing"
    db = _AsyncSessionAdapter(raw)
    claim = await claim_video_job(
        db,
        job.job_id,
        owner="fresh-worker",
        allow_stale_processing=False,
    )
    assert claim is not None
    await db.commit()

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert saved.status == "processing"
    assert saved.lease_owner == "fresh-worker"
    assert saved.lease_token == claim.lease_token


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_scheduled", [True, False])
async def test_settle_failure_requires_recoverable_attempt_cleanup_before_release(
    monkeypatch,
    cleanup_scheduled: bool,
) -> None:
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id=f"job-settle-cleanup-{cleanup_scheduled}",
        reservation_id=f"reservation-settle-cleanup-{cleanup_scheduled}",
    )
    db = _AsyncSessionAdapter(raw)
    generated: dict[str, str] = {}

    class Provider:
        async def generate(self, request, _on_progress=None):
            attempt_id = request.metadata["storage_attempt_id"]
            video_key = f"generate_images/video/{request.job_id}-{attempt_id}.mp4"
            generated.update(
                attempt_id=attempt_id,
                video_key=video_key,
                provider_task_id="provider-settle-cleanup",
            )
            return SimpleNamespace(
                video_storage_key=video_key,
                provider_job_id=generated["provider_task_id"],
                metrics={},
            )

    async def failing_settle(*_args, **_kwargs) -> None:
        raise RuntimeError("settle unavailable")

    schedule_cleanup = AsyncMock(return_value=cleanup_scheduled)
    monkeypatch.setattr(worker, "_credits_settle", failing_settle)
    monkeypatch.setattr(worker, "_delete_generated_output", AsyncMock(return_value=False))
    monkeypatch.setattr(worker, "_schedule_video_attempt_cleanup", schedule_cleanup)
    monkeypatch.setattr(
        worker,
        "_authoritative_video_attempt_succeeded",
        AsyncMock(return_value=False),
    )

    if cleanup_scheduled:
        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="settle-worker",
            provider=Provider(),
            extract_last_frame=AsyncMock(),
        )
    else:
        with pytest.raises(RuntimeError, match="video_attempt_cleanup_unavailable"):
            await worker.run_video_generation_once(
                db,
                job.job_id,
                owner="settle-worker",
                provider=Provider(),
                extract_last_frame=AsyncMock(),
            )

    expected_cleanup = {
        "job_id": job.job_id,
        "script_id": job.script_id,
        "shot_group_id": job.shot_group_id,
        "storage_attempt_id": generated["attempt_id"],
        "video_storage_key": generated["video_key"],
        "last_frame_storage_key": None,
        "provider_task_id": generated["provider_task_id"],
    }
    expected_schedule_count = 1 if cleanup_scheduled else 2
    assert [call.kwargs for call in schedule_cleanup.await_args_list] == [
        expected_cleanup
    ] * expected_schedule_count
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert reservation is not None
    if cleanup_scheduled:
        assert saved.status == "failed"
        assert saved.error_type == "financial_cleanup"
        assert saved.lease_token is None
        assert reservation.status == "released"
    else:
        assert saved.status == "processing"
        assert saved.lease_token is not None
        assert reservation.status == "reserved"


@pytest.mark.asyncio
async def test_worker_does_not_trigger_last_frame_failure_path(
    monkeypatch,
) -> None:
    from app.providers import storage as storage_module
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-last-frame-double-failure",
        reservation_id="reservation-last-frame-double-failure",
    )
    db = _AsyncSessionAdapter(raw)
    generated: dict[str, str] = {}
    uploaded_frames: list[str] = []

    class Provider:
        async def generate(self, request, _on_progress=None):
            attempt_id = request.metadata["storage_attempt_id"]
            video_key = f"generate_images/video/{request.job_id}-{attempt_id}.mp4"
            generated.update(
                attempt_id=attempt_id,
                video_key=video_key,
                provider_task_id="provider-last-frame-double-failure",
            )
            return SimpleNamespace(
                video_storage_key=video_key,
                provider_job_id=generated["provider_task_id"],
                metrics={},
            )

    class Storage:
        async def put(self, key: str, _content: bytes) -> None:
            uploaded_frames.append(key)

    class MissingShotGroupDB:
        async def execute(self, _statement):
            return SimpleNamespace(scalar_one_or_none=lambda: None)

    def fake_subprocess_run(command, **_kwargs):
        if command[0] == "ffprobe":
            return SimpleNamespace(stdout="1.0")
        Path(command[-1]).write_bytes(b"last-frame")
        return SimpleNamespace(stdout="")

    storage = Storage()
    real_update_last_frame = worker._update_last_frame

    async def update_last_frame_with_missing_reference(
        _db,
        unit_id: str,
        video_storage_key: str,
        storage_attempt_id: str,
    ) -> str:
        return await real_update_last_frame(
            MissingShotGroupDB(),
            unit_id,
            video_storage_key,
            storage_attempt_id,
        )

    persist_cleanup = AsyncMock(return_value=False)
    publish_cleanup = Mock(side_effect=ConnectionError("broker unavailable"))
    release = AsyncMock()
    monkeypatch.setattr(storage_module, "get_storage", lambda: storage)
    monkeypatch.setattr(worker, "_download_storage_file", AsyncMock())
    monkeypatch.setattr(worker.subprocess, "run", fake_subprocess_run)
    monkeypatch.setattr(worker, "_update_last_frame", update_last_frame_with_missing_reference)
    monkeypatch.setattr(worker, "_delete_generated_output", AsyncMock(return_value=False))
    monkeypatch.setattr(worker, "_credits_settle", AsyncMock())
    monkeypatch.setattr(worker, "_credits_release", release)
    monkeypatch.setattr(
        worker,
        "_authoritative_video_attempt_succeeded",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        worker,
        "_authoritative_last_frame_referenced",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(worker, "_persist_video_attempt_cleanup_debt", persist_cleanup)
    monkeypatch.setattr(
        worker.reconcile_video_attempt_outputs_task,
        "apply_async",
        publish_cleanup,
    )

    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="last-frame-worker",
        provider=Provider(),
    )

    assert uploaded_frames == []
    persist_cleanup.assert_not_awaited()
    publish_cleanup.assert_not_called()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert saved.status == "succeeded"
    assert saved.lease_token is None
    assert reservation is not None
    assert reservation.status == "reserved"
    release.assert_not_awaited()
