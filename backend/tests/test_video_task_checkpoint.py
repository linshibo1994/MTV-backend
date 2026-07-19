"""视频远端任务 ID 强制 checkpoint 契约测试。"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import signal
import threading
import time
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded, WorkerLostError
from celery.exceptions import Reject, Retry
from celery.worker.request import Request
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models.video_generation_job import VideoGenerationJob
from app.providers.video_gen import (
    VideoGenerationRequest,
    VideoTaskCheckpointConflict,
    VideoTaskCheckpointUnavailable,
)
from tests.test_video_generation import (
    _add_worker_job,
    _AsyncSessionAdapter,
    _make_db,
    _seed,
)


class _SQLiteAsyncSessionFactory:
    def __init__(self, engine) -> None:
        self.engine = engine

    def __call__(self):
        engine = self.engine

        class _Context:
            async def __aenter__(self):
                self.raw = Session(engine, expire_on_commit=False)
                return _AsyncSessionAdapter(self.raw)

            async def __aexit__(self, _exc_type, _exc, _tb):
                self.raw.close()

        return _Context()


class _StopAfterCheckpoint(RuntimeError):
    pass


def test_dedicated_checkpoint_task_uses_worker_loss_redelivery_semantics() -> None:
    from app.workers import video_generation as worker

    task = worker.persist_video_task_checkpoint_task
    assert task.acks_late is True
    assert task.reject_on_worker_lost is True
    assert task.acks_on_failure_or_timeout is False


@pytest.mark.parametrize("failure", [WorkerLostError(), TimeLimitExceeded()])
def test_checkpoint_request_requeues_worker_loss_and_hard_timeout(
    monkeypatch,
    failure: BaseException,
) -> None:
    from celery.worker import request as request_module

    from app.workers import video_generation as worker

    reject = Mock()
    acknowledge = Mock()
    fake_request = SimpleNamespace(
        task=worker.persist_video_task_checkpoint_task,
        reject=reject,
        acknowledge=acknowledge,
        id="checkpoint-request-id",
        _context={},
        store_errors=False,
        send_event=Mock(),
    )
    monkeypatch.setattr(request_module, "task_ready", Mock())
    monkeypatch.setattr(
        worker.persist_video_task_checkpoint_task.backend,
        "mark_as_failure",
        Mock(),
    )

    Request.on_failure(
        fake_request,
        SimpleNamespace(exception=failure),
        send_failed_event=False,
        return_ok=True,
    )

    reject.assert_called_once_with(requeue=True)
    acknowledge.assert_not_called()


def _request(
    on_task_created,
    *,
    provider_task_id: str | None = None,
    before_submit=None,
    on_submission_uncertain=None,
) -> VideoGenerationRequest:
    metadata = (
        {"provider_task_id": provider_task_id}
        if provider_task_id is not None
        else {}
    )
    return VideoGenerationRequest(
        prompt="镜头提示词",
        reference_images=[],
        reference_image_names=[],
        reference_audios=[],
        reference_audio_names=[],
        model="video-model",
        job_id="job-task-checkpoint",
        metadata=metadata,
        cancel_requested=AsyncMock(return_value=False),
        on_task_created=on_task_created,
        before_submit=before_submit,
        on_submission_uncertain=on_submission_uncertain,
    )


def _ordered_checkpoint(events: list[tuple[str, str]]):
    async def checkpoint(provider_task_id: str) -> None:
        events.append(("checkpoint_started", provider_task_id))
        await asyncio.sleep(0)
        events.append(("checkpoint_finished", provider_task_id))

    return checkpoint


def _ordered_submit_intent(events: list[tuple[str, str]], provider: str):
    async def before_submit() -> None:
        events.append(("submit_intent", provider))

    return before_submit


@pytest.mark.asyncio
async def test_submission_boundary_classifies_timeout_and_explicit_rejection() -> None:
    from app.providers.video_gen import (
        VideoGenerationSubmissionUncertain,
        VideoGenerationSubmitRejected,
        execute_video_submission,
    )

    events: list[str] = []

    async def before_submit() -> None:
        events.append("intent")

    async def on_uncertain() -> None:
        events.append("uncertain")

    async def timeout_submit():
        events.append("remote")
        raise TimeoutError("response lost")

    with pytest.raises(VideoGenerationSubmissionUncertain):
        await execute_video_submission(
            timeout_submit,
            before_submit=before_submit,
            on_submission_uncertain=on_uncertain,
        )
    assert events == ["intent", "remote", "uncertain"]

    events.clear()
    response = SimpleNamespace(status_code=400)

    class ExplicitRejection(RuntimeError):
        status_code = 400

    async def rejected_submit():
        events.append("remote")
        error = ExplicitRejection("bad request")
        error.response = response
        raise error

    with pytest.raises(VideoGenerationSubmitRejected):
        await execute_video_submission(
            rejected_submit,
            before_submit=before_submit,
            on_submission_uncertain=on_uncertain,
        )
    assert events == ["intent", "remote"]


def test_openai_video_client_disables_sdk_submit_retries(monkeypatch) -> None:
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    captured: dict[str, object] = {}
    sentinel = object()

    def fake_async_openai(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr("openai.AsyncOpenAI", fake_async_openai)
    monkeypatch.setattr(OpenAIVideoGenProvider, "_api_key", staticmethod(lambda: "test-key"))

    provider = OpenAIVideoGenProvider(provider_key="openai")

    assert provider._build_client() is sentinel
    assert captured["max_retries"] == 0


class _ExplicitVideoSubmitRejection(RuntimeError):
    status_code = 400


def _build_close_failing_provider(
    monkeypatch,
    *,
    provider_key: str,
    failure_kind: str,
    provider_task_id: str,
):
    from app.providers.video_gen import VideoGenerationRemoteTerminalError

    close_error = RuntimeError(
        "close_failed Bearer private-close-secret https://private.example/video"
    )
    close = AsyncMock(side_effect=close_error)

    if provider_key == "openai":
        from app.providers.video_gen_openai import OpenAIVideoGenProvider

        class Videos:
            async def create(self, **_kwargs):
                if failure_kind != "submit_rejected":
                    raise AssertionError("恢复远端终态不得重复提交")
                raise _ExplicitVideoSubmitRejection("explicit_rejection")

            async def retrieve(self, video_id: str):
                assert video_id == provider_task_id
                return SimpleNamespace(status="failed")

        provider = OpenAIVideoGenProvider(provider_key=provider_key)
        monkeypatch.setattr(provider, "_check_keys", lambda: None)
        monkeypatch.setattr(
            provider,
            "_build_client",
            lambda: SimpleNamespace(videos=Videos(), close=close),
        )
        return provider, close

    if provider_key == "gemini":
        from app.providers.video_gen_google import GoogleVideoGenProvider

        generate_videos = AsyncMock(
            side_effect=_ExplicitVideoSubmitRejection("explicit_rejection")
        )
        client = SimpleNamespace(
            aio=SimpleNamespace(
                models=SimpleNamespace(generate_videos=generate_videos),
                aclose=close,
            )
        )
        provider = GoogleVideoGenProvider(provider_key=provider_key)
        monkeypatch.setattr(provider, "_build_client", lambda: client)
        monkeypatch.setattr(
            provider,
            "_load_input_media",
            AsyncMock(return_value=(None, [])),
        )
        monkeypatch.setattr(
            provider,
            "_build_config",
            Mock(return_value=SimpleNamespace()),
        )
        if failure_kind == "remote_terminal":
            monkeypatch.setattr(
                provider,
                "_poll",
                AsyncMock(
                    side_effect=VideoGenerationRemoteTerminalError(
                        provider_task_id=provider_task_id,
                        terminal_status="failed",
                    )
                ),
            )
        return provider, close

    if provider_key == "bailian":
        from app.providers import video_gen_bailian
        from app.providers.video_gen_bailian import BailianVideoGenProvider

        class HttpClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, _exc_type, _exc, _tb):
                await self.aclose()

            aclose = close

            async def post(self, *_args, **_kwargs):
                if failure_kind != "submit_rejected":
                    raise AssertionError("恢复远端终态不得重复提交")
                return SimpleNamespace(status_code=400)

        provider = BailianVideoGenProvider(provider_key=provider_key)
        monkeypatch.setattr(provider, "_check_keys", lambda: None)
        monkeypatch.setattr(provider, "_base_url", lambda: "https://bailian.example")
        monkeypatch.setattr(provider, "_api_key", lambda: "test-key")
        monkeypatch.setattr(provider, "_build_body", AsyncMock(return_value={}))
        monkeypatch.setattr(
            video_gen_bailian.httpx,
            "AsyncClient",
            lambda **_kwargs: HttpClient(),
        )
        if failure_kind == "remote_terminal":
            monkeypatch.setattr(
                provider,
                "_poll",
                AsyncMock(
                    side_effect=VideoGenerationRemoteTerminalError(
                        provider_task_id=provider_task_id,
                        terminal_status="failed",
                    )
                ),
            )
        return provider, close

    raise AssertionError(f"未知测试 Provider: {provider_key}")


@pytest.mark.parametrize("provider_key", ["openai", "gemini", "bailian"])
@pytest.mark.parametrize("failure_kind", ["remote_terminal", "submit_rejected"])
@pytest.mark.asyncio
async def test_provider_close_failure_preserves_typed_worker_outcome(
    monkeypatch,
    caplog,
    provider_key: str,
    failure_kind: str,
) -> None:
    from app.models.credit_reservation import CreditReservation
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id=f"job-{provider_key}-{failure_kind}-close-failure",
        reservation_id=f"reservation-{provider_key}-{failure_kind}-close-failure",
    )
    provider_task_id = (
        f"operations/{provider_key}-close-terminal"
        if provider_key == "gemini"
        else f"{provider_key}-close-terminal"
    )
    job.provider_key = provider_key
    if failure_kind == "remote_terminal":
        job.output_json = {"provider_task_id": provider_task_id}
        job.metrics = {"provider_task_id": provider_task_id}
    raw.commit()
    db = _AsyncSessionAdapter(raw)
    provider, close = _build_close_failing_provider(
        monkeypatch,
        provider_key=provider_key,
        failure_kind=failure_kind,
        provider_task_id=provider_task_id,
    )
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )
    monkeypatch.setattr(
        worker,
        "_resolve_execution_first_frame",
        AsyncMock(return_value=None),
    )
    caplog.set_level(logging.WARNING, logger="app.providers.video_gen")

    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="close-failure-worker",
        provider=provider,
        extract_last_frame=AsyncMock(),
    )

    close.assert_awaited_once()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert reservation is not None
    assert saved.status == "failed"
    assert saved.error_type == (
        "provider_remote_terminal"
        if failure_kind == "remote_terminal"
        else "provider_submit_rejected"
    )
    assert saved.retryable is False
    assert reservation.status == "released"
    assert "private-close-secret" not in caplog.text
    assert "private.example" not in caplog.text
    assert "RuntimeError" in caplog.text


@pytest.mark.parametrize(
    "primary_kind",
    ["video_cancelled", "task_cancelled", "soft_limit", "hard_limit"],
)
@pytest.mark.asyncio
async def test_openai_close_failure_preserves_control_flow_exception(
    monkeypatch,
    primary_kind: str,
) -> None:
    from app.providers.video_gen import (
        VideoCancellationOutcome,
        VideoCancellationResult,
        VideoGenerationCancelled,
    )
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    if primary_kind == "video_cancelled":
        primary_error: BaseException = VideoGenerationCancelled(
            VideoCancellationResult(
                provider_task_id="openai-control-flow",
                outcome=VideoCancellationOutcome.CANCELED,
            )
        )
    elif primary_kind == "task_cancelled":
        primary_error = asyncio.CancelledError("task_cancelled")
    elif primary_kind == "soft_limit":
        primary_error = SoftTimeLimitExceeded()
    else:
        primary_error = TimeLimitExceeded()

    close = AsyncMock(side_effect=RuntimeError("close_failed private-close-secret"))
    provider = OpenAIVideoGenProvider()
    monkeypatch.setattr(provider, "_check_keys", lambda: None)
    monkeypatch.setattr(
        provider,
        "_build_client",
        lambda: SimpleNamespace(videos=SimpleNamespace(), close=close),
    )
    monkeypatch.setattr(
        provider,
        "_poll_and_download_to_file",
        AsyncMock(side_effect=primary_error),
    )

    with pytest.raises(type(primary_error)) as exc_info:
        await provider.generate(
            _request(
                AsyncMock(),
                provider_task_id="openai-control-flow",
            )
        )

    assert exc_info.value is primary_error
    close.assert_awaited_once()


@pytest.mark.asyncio
async def test_openai_hanging_close_is_bounded_without_overriding_primary(
    monkeypatch,
) -> None:
    from app.providers import video_gen
    from app.providers.video_gen import VideoGenerationRemoteTerminalError
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    primary_error = VideoGenerationRemoteTerminalError(
        provider_task_id="openai-bounded-close",
        terminal_status="failed",
    )
    close_stopped = asyncio.Event()

    async def hanging_close() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            close_stopped.set()

    provider = OpenAIVideoGenProvider()
    monkeypatch.setattr(provider, "_check_keys", lambda: None)
    monkeypatch.setattr(
        provider,
        "_build_client",
        lambda: SimpleNamespace(
            videos=SimpleNamespace(),
            close=hanging_close,
        ),
    )
    monkeypatch.setattr(
        provider,
        "_poll_and_download_to_file",
        AsyncMock(side_effect=primary_error),
    )
    monkeypatch.setattr(video_gen, "_VIDEO_CLIENT_CLOSE_TIMEOUT", 0.01, raising=False)

    with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
        await asyncio.wait_for(
            provider.generate(
                _request(
                    AsyncMock(),
                    provider_task_id="openai-bounded-close",
                )
            ),
            timeout=0.2,
        )

    assert exc_info.value is primary_error
    assert close_stopped.is_set()


@pytest.mark.asyncio
async def test_openai_close_that_swallows_cancellation_respects_hard_deadline(
    monkeypatch,
) -> None:
    from app.providers import video_gen
    from app.providers.video_gen import VideoGenerationRemoteTerminalError
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    primary_error = VideoGenerationRemoteTerminalError(
        provider_task_id="openai-stubborn-close",
        terminal_status="failed",
    )
    close_cancelled = asyncio.Event()
    close_finished = asyncio.Event()
    release_close = asyncio.Event()

    async def stubborn_close() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            close_cancelled.set()
            current = asyncio.current_task()
            assert current is not None
            while current.cancelling():
                current.uncancel()
            await release_close.wait()
        finally:
            close_finished.set()

    provider = OpenAIVideoGenProvider()
    monkeypatch.setattr(provider, "_check_keys", lambda: None)
    monkeypatch.setattr(
        provider,
        "_build_client",
        lambda: SimpleNamespace(
            videos=SimpleNamespace(),
            close=stubborn_close,
        ),
    )
    monkeypatch.setattr(
        provider,
        "_poll_and_download_to_file",
        AsyncMock(side_effect=primary_error),
    )
    monkeypatch.setattr(video_gen, "_VIDEO_CLIENT_CLOSE_TIMEOUT", 0.01, raising=False)
    asyncio.get_running_loop().call_later(0.15, release_close.set)

    started_at = time.monotonic()
    with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
        await provider.generate(
            _request(
                AsyncMock(),
                provider_task_id="openai-stubborn-close",
            )
        )
    elapsed = time.monotonic() - started_at

    assert exc_info.value is primary_error
    assert close_cancelled.is_set()
    assert elapsed < 0.08
    assert len(video_gen._VIDEO_CLIENT_CLOSE_ORPHANS) == 1

    second_primary_error = VideoGenerationRemoteTerminalError(
        provider_task_id="openai-second-close",
        terminal_status="failed",
    )
    second_close = AsyncMock()
    with pytest.raises(VideoGenerationRemoteTerminalError) as second_exc_info:
        async with video_gen.managed_video_client(
            object(),
            second_close,
            provider_key="openai",
        ):
            raise second_primary_error
    assert second_exc_info.value is second_primary_error
    second_close.assert_not_called()
    assert len(video_gen._VIDEO_CLIENT_CLOSE_ORPHANS) == 1

    await asyncio.wait_for(close_finished.wait(), timeout=0.3)
    await asyncio.sleep(0)
    assert video_gen._VIDEO_CLIENT_CLOSE_ORPHANS == set()


def test_managed_video_client_reserves_one_cross_loop_close_slot_atomically(
    monkeypatch,
) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from app.providers import video_gen
    from app.providers.video_gen import (
        VideoGenerationRemoteTerminalError,
        managed_video_client,
    )

    concurrency = 24
    enter_close = threading.Barrier(concurrency)
    release_close = threading.Event()
    all_contexts_returned = threading.Event()
    counter_lock = threading.Lock()
    close_started = 0
    contexts_returned = 0

    async def invoke(index: int) -> None:
        nonlocal close_started, contexts_returned

        async def stubborn_close() -> None:
            nonlocal close_started
            with counter_lock:
                close_started += 1
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                current = asyncio.current_task()
                assert current is not None
                while not release_close.is_set():
                    while current.cancelling():
                        current.uncancel()
                    try:
                        await asyncio.sleep(0.005)
                    except asyncio.CancelledError:
                        continue

        primary_error = VideoGenerationRemoteTerminalError(
            provider_task_id=f"cross-loop-close-{index}",
            terminal_status="failed",
        )
        with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
            async with managed_video_client(
                object(),
                stubborn_close,
                provider_key="openai",
            ):
                enter_close.wait(timeout=5)
                raise primary_error
        assert exc_info.value is primary_error
        with counter_lock:
            contexts_returned += 1
            if contexts_returned == concurrency:
                all_contexts_returned.set()
        while not release_close.is_set():
            await asyncio.sleep(0.005)

    def run_in_thread(index: int) -> None:
        asyncio.run(invoke(index))

    monkeypatch.setattr(video_gen, "_VIDEO_CLIENT_CLOSE_TIMEOUT", 0.02, raising=False)
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_in_thread, index) for index in range(concurrency)]
            try:
                assert all_contexts_returned.wait(timeout=5)
                with counter_lock:
                    assert close_started == 1
                assert len(video_gen._VIDEO_CLIENT_CLOSE_ORPHANS) == 1
            finally:
                release_close.set()
            for future in futures:
                future.result(timeout=2)
    finally:
        release_close.set()

    deadline = time.monotonic() + 1
    while video_gen._VIDEO_CLIENT_CLOSE_ORPHANS and time.monotonic() < deadline:
        time.sleep(0.005)
    assert video_gen._VIDEO_CLIENT_CLOSE_ORPHANS == set()

    close_probe = AsyncMock()

    async def run_probe() -> None:
        async with managed_video_client(
            object(),
            close_probe,
            provider_key="openai",
        ):
            pass

    asyncio.run(run_probe())
    close_probe.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_managed_video_client_propagates_close_failure_without_primary_error() -> None:
    from app.providers.video_gen import managed_video_client

    close_error = RuntimeError("close_failed")
    close = AsyncMock(side_effect=close_error)

    with pytest.raises(RuntimeError) as exc_info:
        async with managed_video_client(
            object(),
            close,
            provider_key="openai",
        ):
            pass

    assert exc_info.value is close_error
    close.assert_awaited_once()


@pytest.mark.asyncio
async def test_video_output_delete_failure_log_does_not_expose_storage_key(
    monkeypatch,
    caplog,
) -> None:
    from app.workers import video_generation as worker

    storage_key = "generate_images/video/private.mp4?token=storage-secret"
    digest = hashlib.sha256(storage_key.encode("utf-8")).hexdigest()
    compensation_db = SimpleNamespace(commit=AsyncMock())

    class CompensationContext:
        async def __aenter__(self):
            return compensation_db

        async def __aexit__(self, _exc_type, _exc, _tb):
            return None

    enqueue_delete = AsyncMock()
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        lambda: CompensationContext(),
    )
    monkeypatch.setattr(
        "app.services.media_storage_jobs.enqueue_delete",
        enqueue_delete,
    )
    storage = SimpleNamespace(delete=AsyncMock(side_effect=RuntimeError("delete_failed")))
    caplog.set_level(logging.WARNING, logger="app.workers.video_generation")

    assert await worker._delete_generated_output(storage_key, storage=storage) is True

    assert storage_key not in caplog.text
    assert "storage-secret" not in caplog.text
    assert digest[:16] in caplog.text
    enqueue_delete.assert_awaited_once_with(
        compensation_db,
        idempotency_key=f"video-delete:{digest}",
        source_key=storage_key,
    )


@pytest.mark.parametrize(
    ("output_json", "metrics"),
    [
        ({"provider_submission": "invalid"}, {"provider_submission": "invalid"}),
        (
            {"provider_submission": {"state": "unknown"}},
            {"provider_submission": {"state": "unknown"}},
        ),
        (
            {
                "provider_submission": {
                    "state": "submit_intent",
                    "provider_key": "modelsai",
                    "attempt": 1,
                }
            },
            {},
        ),
    ],
)
def test_malformed_submission_authority_is_fail_closed(output_json, metrics) -> None:
    from app.services.video_remote_cancellation import provider_submission_is_unresolved

    assert provider_submission_is_unresolved(output_json, metrics) is True


@pytest.mark.asyncio
async def test_submission_state_cannot_regress_from_uncertain_to_intent() -> None:
    from app.crud.video import claim_video_job
    from app.services.video_remote_cancellation import persist_provider_submission_state

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-submission-regression",
        reservation_id="reservation-submission-regression",
    )
    db = _AsyncSessionAdapter(raw)
    claim = await claim_video_job(db, job.job_id, owner="submission-worker")
    assert claim is not None
    await db.commit()
    assert await persist_provider_submission_state(
        db,
        job.job_id,
        claim.lease_token,
        state="submission_uncertain",
        provider_key="modelsai",
        attempt=1,
    )
    await db.commit()

    with pytest.raises(VideoTaskCheckpointConflict, match="submission_state_regression"):
        await persist_provider_submission_state(
            db,
            job.job_id,
            claim.lease_token,
            state="submit_intent",
            provider_key="modelsai",
            attempt=1,
        )


def _stop_at_cancellation_probe(events: list[tuple[str, str]]):
    async def stop(_cancel_requested, provider_task_id: str, _cancel_remote) -> None:
        events.append(("cancellation_probe", provider_task_id))
        raise _StopAfterCheckpoint

    return stop


@pytest.mark.asyncio
async def test_openai_awaits_task_checkpoint_before_post_submit_cancellation(
    monkeypatch,
) -> None:
    from app.providers import video_gen_openai
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    events: list[tuple[str, str]] = []
    client = SimpleNamespace(
        videos=SimpleNamespace(
            create=AsyncMock(return_value=SimpleNamespace(id="openai-task-1"))
        ),
        close=AsyncMock(),
    )
    provider = OpenAIVideoGenProvider()
    monkeypatch.setattr(provider, "_check_keys", lambda: None)
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr(provider, "_load_input_reference", AsyncMock(return_value=None))
    monkeypatch.setattr(
        video_gen_openai,
        "raise_if_video_cancel_requested_before_submit",
        AsyncMock(),
    )
    monkeypatch.setattr(
        video_gen_openai,
        "raise_if_video_cancel_requested",
        _stop_at_cancellation_probe(events),
    )

    with pytest.raises(_StopAfterCheckpoint):
        await provider.generate(
            _request(
                _ordered_checkpoint(events),
                before_submit=_ordered_submit_intent(events, "openai"),
            )
        )

    assert events == [
        ("submit_intent", "openai"),
        ("checkpoint_started", "openai-task-1"),
        ("checkpoint_finished", "openai-task-1"),
        ("cancellation_probe", "openai-task-1"),
    ]
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_openai_resume_awaits_checkpoint_before_cancellation(monkeypatch) -> None:
    from app.providers import video_gen_openai
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    events: list[tuple[str, str]] = []
    client = SimpleNamespace(
        videos=SimpleNamespace(
            create=AsyncMock(side_effect=AssertionError("恢复任务不得重复提交"))
        ),
        close=AsyncMock(),
    )
    provider = OpenAIVideoGenProvider()
    monkeypatch.setattr(provider, "_check_keys", lambda: None)
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr(
        video_gen_openai,
        "raise_if_video_cancel_requested",
        _stop_at_cancellation_probe(events),
    )

    with pytest.raises(_StopAfterCheckpoint):
        await provider.generate(
            _request(
                _ordered_checkpoint(events),
                provider_task_id="openai-resume-task",
            )
        )

    assert events == [
        ("checkpoint_started", "openai-resume-task"),
        ("checkpoint_finished", "openai-resume-task"),
        ("cancellation_probe", "openai-resume-task"),
    ]
    client.videos.create.assert_not_awaited()
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_bailian_resume_checkpoints_before_creating_http_client(
    monkeypatch,
) -> None:
    from app.providers import video_gen_bailian
    from app.providers.video_gen_bailian import BailianVideoGenProvider

    events: list[tuple[str, str]] = []

    class HttpClient:
        async def aclose(self) -> None:
            return None

        async def post(self, *_args, **_kwargs):
            raise AssertionError("恢复任务不得重复提交")

    provider = BailianVideoGenProvider()
    monkeypatch.setattr(provider, "_check_keys", lambda: None)
    monkeypatch.setattr(provider, "_base_url", lambda: "https://bailian.example")
    monkeypatch.setattr(provider, "_api_key", lambda: "test-key")
    def create_http_client(**_kwargs):
        events.append(("http_client_created", "bailian-resume-task"))
        return HttpClient()

    monkeypatch.setattr(video_gen_bailian.httpx, "AsyncClient", create_http_client)
    monkeypatch.setattr(
        video_gen_bailian,
        "raise_if_video_cancel_requested",
        _stop_at_cancellation_probe(events),
    )

    with pytest.raises(_StopAfterCheckpoint):
        await provider.generate(
            _request(
                _ordered_checkpoint(events),
                provider_task_id="bailian-resume-task",
            )
        )

    assert events == [
        ("checkpoint_started", "bailian-resume-task"),
        ("checkpoint_finished", "bailian-resume-task"),
        ("http_client_created", "bailian-resume-task"),
        ("cancellation_probe", "bailian-resume-task"),
    ]


@pytest.mark.asyncio
async def test_volcengine_resume_checkpoints_before_media_preflight(
    monkeypatch,
) -> None:
    from app.providers import video_gen_volcengine
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    events: list[tuple[str, str]] = []
    provider = VolcengineVideoGenProvider()
    monkeypatch.setattr(provider, "_check_keys", lambda: None)

    async def preflight(*_args) -> None:
        raise AssertionError("恢复已有远端任务不得访问参考媒体")

    monkeypatch.setattr(provider, "_preflight_reference_sizes", preflight)
    monkeypatch.setattr(
        video_gen_volcengine,
        "raise_if_video_cancel_requested",
        _stop_at_cancellation_probe(events),
    )
    monkeypatch.setattr(
        provider,
        "_build_client",
        lambda **_kwargs: SimpleNamespace(
            content_generation=SimpleNamespace(tasks=SimpleNamespace()),
            close=AsyncMock(),
        ),
    )

    with pytest.raises(_StopAfterCheckpoint):
        await provider.generate(
            _request(
                _ordered_checkpoint(events),
                provider_task_id="volcengine-resume-task",
            )
        )

    assert events == [
        ("checkpoint_started", "volcengine-resume-task"),
        ("checkpoint_finished", "volcengine-resume-task"),
        ("cancellation_probe", "volcengine-resume-task"),
    ]


@pytest.mark.asyncio
async def test_google_awaits_task_checkpoint_before_post_submit_cancellation(
    monkeypatch,
) -> None:
    from app.providers import video_gen_google
    from app.providers.video_gen_google import GoogleVideoGenProvider

    events: list[tuple[str, str]] = []
    operation = SimpleNamespace(name="operations/google-task-1")
    client = SimpleNamespace(
        aio=SimpleNamespace(
            models=SimpleNamespace(generate_videos=AsyncMock(return_value=operation)),
            aclose=AsyncMock(),
        )
    )
    provider = GoogleVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr(provider, "_load_input_media", AsyncMock(return_value=(None, [])))
    monkeypatch.setattr(provider, "_build_config", Mock(return_value=SimpleNamespace()))
    monkeypatch.setattr(
        video_gen_google,
        "raise_if_video_cancel_requested_before_submit",
        AsyncMock(),
    )
    monkeypatch.setattr(
        video_gen_google,
        "raise_if_video_cancel_requested",
        _stop_at_cancellation_probe(events),
    )

    with pytest.raises(_StopAfterCheckpoint):
        await provider.generate(
            _request(
                _ordered_checkpoint(events),
                before_submit=_ordered_submit_intent(events, "google"),
            )
        )

    assert events == [
        ("submit_intent", "google"),
        ("checkpoint_started", "operations/google-task-1"),
        ("checkpoint_finished", "operations/google-task-1"),
        ("cancellation_probe", "operations/google-task-1"),
    ]
    client.aio.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_google_resume_awaits_checkpoint_before_cancellation(monkeypatch) -> None:
    from app.providers import video_gen_google
    from app.providers.video_gen_google import GoogleVideoGenProvider

    events: list[tuple[str, str]] = []
    generate_videos = AsyncMock(side_effect=AssertionError("恢复任务不得重复提交"))
    client = SimpleNamespace(
        aio=SimpleNamespace(
            models=SimpleNamespace(generate_videos=generate_videos),
            aclose=AsyncMock(),
        )
    )
    provider = GoogleVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr(
        video_gen_google,
        "raise_if_video_cancel_requested",
        _stop_at_cancellation_probe(events),
    )

    with pytest.raises(_StopAfterCheckpoint):
        await provider.generate(
            _request(
                _ordered_checkpoint(events),
                provider_task_id="operations/google-resume-task",
            )
        )

    assert events == [
        ("checkpoint_started", "operations/google-resume-task"),
        ("checkpoint_finished", "operations/google-resume-task"),
        ("cancellation_probe", "operations/google-resume-task"),
    ]
    generate_videos.assert_not_awaited()
    client.aio.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_bailian_awaits_task_checkpoint_before_post_submit_cancellation(
    monkeypatch,
) -> None:
    from app.providers import video_gen_bailian
    from app.providers.video_gen_bailian import BailianVideoGenProvider

    events: list[tuple[str, str]] = []

    class HttpClient:
        async def aclose(self) -> None:
            return None

        async def post(self, *_args, **_kwargs):
            return SimpleNamespace(
                status_code=200,
                json=lambda: {"output": {"task_id": "bailian-task-1"}},
            )

    provider = BailianVideoGenProvider()
    monkeypatch.setattr(provider, "_check_keys", lambda: None)
    monkeypatch.setattr(provider, "_base_url", lambda: "https://bailian.example")
    monkeypatch.setattr(provider, "_api_key", lambda: "test-key")
    monkeypatch.setattr(provider, "_build_body", AsyncMock(return_value={}))
    monkeypatch.setattr(video_gen_bailian.httpx, "AsyncClient", lambda **_kwargs: HttpClient())
    monkeypatch.setattr(
        video_gen_bailian,
        "raise_if_video_cancel_requested_before_submit",
        AsyncMock(),
    )
    monkeypatch.setattr(
        video_gen_bailian,
        "raise_if_video_cancel_requested",
        _stop_at_cancellation_probe(events),
    )

    with pytest.raises(_StopAfterCheckpoint):
        await provider.generate(
            _request(
                _ordered_checkpoint(events),
                before_submit=_ordered_submit_intent(events, "bailian"),
            )
        )

    assert events == [
        ("submit_intent", "bailian"),
        ("checkpoint_started", "bailian-task-1"),
        ("checkpoint_finished", "bailian-task-1"),
        ("cancellation_probe", "bailian-task-1"),
    ]


@pytest.mark.asyncio
async def test_volcengine_awaits_task_checkpoint_before_post_submit_cancellation(
    monkeypatch,
) -> None:
    from app.providers import video_gen_volcengine
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    events: list[tuple[str, str]] = []

    class Tasks:
        create = AsyncMock(return_value=SimpleNamespace(id="volcengine-task-1"))
        get = AsyncMock(side_effect=AssertionError("checkpoint 完成前不得轮询"))

    client = SimpleNamespace(
        content_generation=SimpleNamespace(tasks=Tasks()),
        close=AsyncMock(),
    )
    provider = VolcengineVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr(
        video_gen_volcengine,
        "raise_if_video_cancel_requested_before_submit",
        AsyncMock(),
    )
    monkeypatch.setattr(
        video_gen_volcengine,
        "raise_if_video_cancel_requested",
        _stop_at_cancellation_probe(events),
    )

    with pytest.raises(_StopAfterCheckpoint):
        await provider._submit_and_poll(
            "video-model",
            "镜头提示词",
            [],
            [],
            None,
            Mock(),
            job_id="job-task-checkpoint",
            on_task_created=_ordered_checkpoint(events),
            before_submit=_ordered_submit_intent(events, "volcengine"),
        )

    assert events == [
        ("submit_intent", "volcengine"),
        ("checkpoint_started", "volcengine-task-1"),
        ("checkpoint_finished", "volcengine-task-1"),
        ("cancellation_probe", "volcengine-task-1"),
    ]
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_processing_checkpoint_persists_id_with_lease_and_preserves_json() -> None:
    from app.crud.video import claim_video_job
    from app.services import video_remote_cancellation

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-processing-task-checkpoint",
        reservation_id="reservation-processing-task-checkpoint",
    )
    job.output_json = {"existing_output": "keep"}
    job.metrics = {"existing_metric": 7}
    raw.commit()
    db = _AsyncSessionAdapter(raw)
    claim = await claim_video_job(
        db,
        job.job_id,
        owner="checkpoint-worker",
        lease_seconds=1800,
    )
    assert claim is not None
    await db.commit()

    assert await video_remote_cancellation.persist_video_task_checkpoint(
        db,
        job.job_id,
        "provider-checkpoint-1",
        claim.lease_token,
    )
    await db.commit()

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert saved.status == "processing"
    assert saved.lease_token == claim.lease_token
    assert saved.output_json == {
        "existing_output": "keep",
        "provider_task_id": "provider-checkpoint-1",
    }
    assert saved.metrics == {
        "existing_metric": 7,
        "provider_task_id": "provider-checkpoint-1",
    }


@pytest.mark.asyncio
async def test_checkpoint_replay_is_idempotent_and_conflicting_id_is_fail_closed() -> None:
    from app.crud.video import claim_video_job
    from app.services import video_remote_cancellation

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-idempotent-task-checkpoint",
        reservation_id="reservation-idempotent-task-checkpoint",
    )
    db = _AsyncSessionAdapter(raw)
    claim = await claim_video_job(db, job.job_id, owner="checkpoint-worker")
    assert claim is not None
    await db.commit()

    assert await video_remote_cancellation.persist_video_task_checkpoint(
        db,
        job.job_id,
        "provider-authoritative",
        claim.lease_token,
    )
    await db.commit()
    raw.expire_all()
    persisted = raw.get(VideoGenerationJob, job.job_id)
    assert persisted is not None
    checkpoint_version = persisted.version

    assert await video_remote_cancellation.persist_video_task_checkpoint(
        db,
        job.job_id,
        "provider-authoritative",
        claim.lease_token,
    )
    await db.commit()
    raw.expire_all()
    replayed = raw.get(VideoGenerationJob, job.job_id)
    assert replayed is not None
    assert replayed.version == checkpoint_version
    before_output = deepcopy(replayed.output_json)
    before_metrics = deepcopy(replayed.metrics)

    with pytest.raises(VideoTaskCheckpointConflict):
        await video_remote_cancellation.persist_video_task_checkpoint(
            db,
            job.job_id,
            "provider-conflicting",
            claim.lease_token,
        )
    await db.rollback()
    raw.expire_all()
    conflicted = raw.get(VideoGenerationJob, job.job_id)
    assert conflicted is not None
    assert conflicted.output_json == before_output
    assert conflicted.metrics == before_metrics
    assert conflicted.version == checkpoint_version


@pytest.mark.asyncio
async def test_delayed_checkpoint_from_old_lease_cannot_write_new_processing_attempt() -> None:
    from app.crud.video import claim_video_job
    from app.services import video_remote_cancellation

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-delayed-old-lease-checkpoint",
        reservation_id="reservation-delayed-old-lease-checkpoint",
    )
    db = _AsyncSessionAdapter(raw)
    old_claim = await claim_video_job(db, job.job_id, owner="old-worker")
    assert old_claim is not None
    await db.commit()

    current = raw.get(VideoGenerationJob, job.job_id)
    assert current is not None
    current.status = "starting"
    current.lease_owner = None
    current.lease_token = None
    current.lease_until = None
    raw.commit()
    new_claim = await claim_video_job(db, job.job_id, owner="new-worker")
    assert new_claim is not None
    assert new_claim.lease_token != old_claim.lease_token
    await db.commit()

    with pytest.raises(VideoTaskCheckpointConflict):
        await video_remote_cancellation.persist_video_task_checkpoint(
            db,
            job.job_id,
            "provider-from-old-lease",
            old_claim.lease_token,
        )
    await db.rollback()

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert saved.status == "processing"
    assert saved.lease_token == new_claim.lease_token
    assert "provider_task_id" not in (saved.output_json or {})
    assert "provider_task_id" not in (saved.metrics or {})


@pytest.mark.asyncio
async def test_canceled_checkpoint_rejects_old_lease_and_accepts_canceling_lease(
    monkeypatch,
) -> None:
    from app.crud.video import claim_video_job
    from app.services import video_generation as video_service
    from app.services import video_remote_cancellation

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-canceled-checkpoint-generation",
        reservation_id="reservation-canceled-checkpoint-generation",
    )
    db = _AsyncSessionAdapter(raw)
    old_claim = await claim_video_job(db, job.job_id, owner="old-worker")
    assert old_claim is not None
    await db.commit()

    current = raw.get(VideoGenerationJob, job.job_id)
    assert current is not None
    current.status = "starting"
    current.lease_owner = None
    current.lease_token = None
    current.lease_until = None
    raw.commit()
    new_claim = await claim_video_job(db, job.job_id, owner="new-worker")
    assert new_claim is not None
    assert new_claim.lease_token != old_claim.lease_token
    await db.commit()

    monkeypatch.setattr(video_service, "_credits_reserve", None)
    monkeypatch.setattr(video_service, "cancel_outbox_task", AsyncMock())
    canceled = await video_service.cancel_video_job(
        db,
        script_id=job.script_id,
        job_id=job.job_id,
        actor_user_id=seeded.user.user_id,
    )
    assert canceled.status == "canceled"

    with pytest.raises(VideoTaskCheckpointConflict):
        await video_remote_cancellation.persist_video_task_checkpoint(
            db,
            job.job_id,
            "provider-from-old-lease",
            old_claim.lease_token,
        )
    await db.rollback()
    raw.expire_all()
    before_current_checkpoint = raw.get(VideoGenerationJob, job.job_id)
    assert before_current_checkpoint is not None
    assert "provider_task_id" not in (before_current_checkpoint.output_json or {})
    assert "provider_task_id" not in (before_current_checkpoint.metrics or {})
    assert "provider_cancellation" not in (before_current_checkpoint.output_json or {})
    assert "provider_cancellation" not in (before_current_checkpoint.metrics or {})

    assert await video_remote_cancellation.persist_video_task_checkpoint(
        db,
        job.job_id,
        "provider-from-new-lease",
        new_claim.lease_token,
    )
    await db.commit()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    checkpoint_version = saved.version
    lease_authority = (saved.output_json or {})["provider_task_checkpoint_lease"]
    assert lease_authority
    assert (saved.metrics or {})["provider_task_checkpoint_lease"] == lease_authority

    assert await video_remote_cancellation.persist_video_task_checkpoint(
        db,
        job.job_id,
        "provider-from-new-lease",
        new_claim.lease_token,
    )
    await db.commit()
    raw.expire_all()
    replayed = raw.get(VideoGenerationJob, job.job_id)
    assert replayed is not None
    assert replayed.version == checkpoint_version

    with pytest.raises(VideoTaskCheckpointConflict):
        await video_remote_cancellation.persist_video_task_checkpoint(
            db,
            job.job_id,
            "provider-conflicting-new-lease",
            new_claim.lease_token,
        )
    await db.rollback()

    from app.providers.video_gen import (
        VideoCancellationOutcome,
        VideoCancellationResult,
    )
    from app.workers import video_generation as worker

    cancel = AsyncMock(
        return_value=VideoCancellationResult(
            provider_task_id="provider-from-new-lease",
            outcome=VideoCancellationOutcome.CANCELED,
        )
    )
    monkeypatch.setattr(
        worker,
        "get_video_provider",
        lambda _provider_key: SimpleNamespace(cancel=cancel),
    )
    assert await worker.reconcile_video_remote_cancellations(db) == 1
    cancel.assert_awaited_once_with("provider-from-new-lease")


@pytest.mark.asyncio
async def test_cancel_without_active_lease_rejects_late_checkpoint(monkeypatch) -> None:
    from app.services import video_generation as video_service
    from app.services import video_remote_cancellation

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-canceled-without-active-lease",
        reservation_id="reservation-canceled-without-active-lease",
    )
    db = _AsyncSessionAdapter(raw)
    monkeypatch.setattr(video_service, "_credits_reserve", None)
    monkeypatch.setattr(video_service, "cancel_outbox_task", AsyncMock())

    canceled = await video_service.cancel_video_job(
        db,
        script_id=job.script_id,
        job_id=job.job_id,
        actor_user_id=seeded.user.user_id,
    )
    assert canceled.status == "canceled"
    assert (canceled.output_json or {})["provider_task_checkpoint_lease"] == "none"
    assert (canceled.metrics or {})["provider_task_checkpoint_lease"] == "none"

    with pytest.raises(VideoTaskCheckpointConflict):
        await video_remote_cancellation.persist_video_task_checkpoint(
            db,
            job.job_id,
            "provider-after-starting-cancel",
            "unowned-lease",
        )
    await db.rollback()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert "provider_task_id" not in (saved.output_json or {})
    assert "provider_cancellation" not in (saved.output_json or {})


@pytest.mark.asyncio
async def test_checkpoint_canceled_race_backfills_pending_cancellation_debt() -> None:
    from app.crud.video import claim_video_job
    from app.services import video_remote_cancellation

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-canceled-task-checkpoint",
        reservation_id="reservation-canceled-task-checkpoint",
    )
    db = _AsyncSessionAdapter(raw)
    claim = await claim_video_job(db, job.job_id, owner="checkpoint-worker")
    assert claim is not None
    await db.commit()
    job = raw.get(VideoGenerationJob, job.job_id)
    assert job is not None
    job.output_json, job.metrics = (
        video_remote_cancellation.merge_video_task_checkpoint_lease_authority(
            job.output_json,
            job.metrics,
            claim.lease_token,
        )
    )
    job.status = "canceled"
    job.lease_owner = None
    job.lease_token = None
    job.lease_until = None
    job.finished_at = datetime.now(UTC)
    raw.commit()

    assert await video_remote_cancellation.persist_video_task_checkpoint(
        db,
        job.job_id,
        "provider-canceled-race",
        claim.lease_token,
    )
    await db.commit()

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    expected_debt = {
        "provider_task_id": "provider-canceled-race",
        "outcome": "pending",
        "detail": None,
        "retryable": True,
        "attempts": 0,
    }
    assert (saved.output_json or {})["provider_task_id"] == "provider-canceled-race"
    assert (saved.metrics or {})["provider_task_id"] == "provider-canceled-race"
    assert (saved.output_json or {})["provider_cancellation"] == expected_debt
    assert (saved.metrics or {})["provider_cancellation"] == expected_debt


@pytest.mark.asyncio
async def test_complete_legacy_canceled_checkpoint_replay_is_read_only() -> None:
    from app.services import video_remote_cancellation

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-legacy-canceled-checkpoint",
        reservation_id="reservation-legacy-canceled-checkpoint",
    )
    provider_task_id = "provider-legacy-canceled-checkpoint"
    debt = video_remote_cancellation.pending_provider_cancellation(provider_task_id)
    job.status = "canceled"
    job.output_json = {
        "provider_task_id": provider_task_id,
        "provider_cancellation": dict(debt),
    }
    job.metrics = {
        "provider_task_id": provider_task_id,
        "provider_cancellation": dict(debt),
    }
    raw.commit()
    db = _AsyncSessionAdapter(raw)
    version = job.version

    assert await video_remote_cancellation.persist_video_task_checkpoint(
        db,
        job.job_id,
        provider_task_id,
        "legacy-lease-not-recorded",
    )
    await db.commit()
    raw.expire_all()
    replayed = raw.get(VideoGenerationJob, job.job_id)
    assert replayed is not None
    assert replayed.version == version
    assert "provider_task_checkpoint_lease" not in (replayed.output_json or {})


@pytest.mark.asyncio
async def test_worker_checkpoint_uses_broker_when_independent_database_fails(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    persist = AsyncMock(side_effect=ConnectionError("database unavailable"))
    publish = Mock(return_value=None)
    monkeypatch.setattr(worker, "_persist_video_task_checkpoint_independently", persist)
    monkeypatch.setattr(worker, "_publish_video_task_checkpoint", publish)

    await worker._ensure_video_task_checkpoint_durable(
        "job-broker-fallback",
        "provider-broker-fallback",
        "lease-broker-fallback",
    )

    persist.assert_awaited_once_with(
        "job-broker-fallback",
        "provider-broker-fallback",
        "lease-broker-fallback",
    )
    publish.assert_called_once_with(
        job_id="job-broker-fallback",
        provider_task_id="provider-broker-fallback",
        lease_token="lease-broker-fallback",
    )


@pytest.mark.asyncio
async def test_worker_checkpoint_turns_soft_limit_into_id_bearing_recovery(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    timeout_error = SoftTimeLimitExceeded()
    persist = AsyncMock(side_effect=[ConnectionError("database unavailable"), timeout_error])
    publish = Mock(side_effect=ConnectionError("broker unavailable"))
    sleep = AsyncMock()
    monkeypatch.setattr(worker, "_persist_video_task_checkpoint_independently", persist)
    monkeypatch.setattr(worker, "_publish_video_task_checkpoint", publish)
    monkeypatch.setattr(worker.asyncio, "sleep", sleep)

    with pytest.raises(VideoTaskCheckpointUnavailable) as exc_info:
        await worker._ensure_video_task_checkpoint_durable(
            "job-soft-checkpoint",
            "provider-soft-checkpoint",
            "lease-soft-checkpoint",
        )

    assert exc_info.value.provider_task_id == "provider-soft-checkpoint"
    assert exc_info.value.__cause__ is timeout_error
    assert persist.await_count == 2
    assert publish.call_count == 2
    sleep.assert_awaited_once_with(worker._TASK_CHECKPOINT_CHANNEL_RETRY_SECONDS)


@pytest.mark.asyncio
async def test_worker_checkpoint_propagates_hard_limit_unchanged(monkeypatch) -> None:
    from app.workers import video_generation as worker

    timeout_error = TimeLimitExceeded()
    monkeypatch.setattr(
        worker,
        "_persist_video_task_checkpoint_independently",
        AsyncMock(side_effect=timeout_error),
    )
    publish = Mock()
    monkeypatch.setattr(worker, "_publish_video_task_checkpoint", publish)

    with pytest.raises(TimeLimitExceeded) as exc_info:
        await worker._ensure_video_task_checkpoint_durable(
            "job-hard-checkpoint",
            "provider-hard-checkpoint",
            "lease-hard-checkpoint",
        )

    assert exc_info.value is timeout_error
    publish.assert_not_called()


def test_checkpoint_recovery_uses_isolated_database_when_runtime_is_stopping(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    isolated_persist = Mock(return_value=True)

    def stopping_runtime(coroutine):
        coroutine.close()
        raise worker.WorkerAsyncRuntimeStopping("worker async runtime is stopping")

    monkeypatch.setattr(worker, "run_worker_async", stopping_runtime)
    monkeypatch.setattr(
        worker,
        "_persist_video_task_checkpoint_in_isolated_runtime",
        isolated_persist,
        raising=False,
    )
    monkeypatch.setattr(
        worker,
        "sleep",
        Mock(side_effect=AssertionError("runtime 已停止时不得进入无效退避循环")),
    )

    result = worker._recover_video_task_checkpoint_until_durable(
        job_id="job-isolated-checkpoint-recovery",
        provider_task_id="provider-isolated-checkpoint-recovery",
        lease_token="lease-isolated-checkpoint-recovery",
    )

    assert result == {
        "job_id": "job-isolated-checkpoint-recovery",
        "provider_task_id": "provider-isolated-checkpoint-recovery",
        "status": "checkpointed",
    }
    isolated_persist.assert_called_once_with(
        "job-isolated-checkpoint-recovery",
        "provider-isolated-checkpoint-recovery",
        "lease-isolated-checkpoint-recovery",
    )


@pytest.mark.asyncio
async def test_isolated_checkpoint_engine_uses_null_pool_and_is_disposed(
    monkeypatch,
) -> None:
    from sqlalchemy.ext import asyncio as sqlalchemy_asyncio
    from sqlalchemy.pool import NullPool

    from app.services import video_remote_cancellation
    from app.workers import video_generation as worker

    checkpoint_db = SimpleNamespace(
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )

    class SessionContext:
        async def __aenter__(self):
            return checkpoint_db

        async def __aexit__(self, _exc_type, _exc, _tb):
            return None

    isolated_engine = SimpleNamespace(dispose=AsyncMock())
    create_engine = Mock(return_value=isolated_engine)
    sessionmaker = Mock(return_value=lambda: SessionContext())
    persist = AsyncMock(return_value=True)
    monkeypatch.setattr(sqlalchemy_asyncio, "create_async_engine", create_engine)
    monkeypatch.setattr(sqlalchemy_asyncio, "async_sessionmaker", sessionmaker)
    monkeypatch.setattr(
        video_remote_cancellation,
        "persist_video_task_checkpoint",
        persist,
    )

    assert await worker._persist_video_task_checkpoint_with_isolated_engine(
        "job-null-pool-checkpoint",
        "provider-null-pool-checkpoint",
        "lease-null-pool-checkpoint",
    )

    assert create_engine.call_args.kwargs["poolclass"] is NullPool
    persist.assert_awaited_once_with(
        checkpoint_db,
        "job-null-pool-checkpoint",
        "provider-null-pool-checkpoint",
        "lease-null-pool-checkpoint",
    )
    checkpoint_db.commit.assert_awaited_once()
    checkpoint_db.rollback.assert_not_awaited()
    isolated_engine.dispose.assert_awaited_once()


@pytest.mark.skipif(
    not hasattr(signal, "SIGALRM"),
    reason="当前平台不支持同步 SIGALRM 注入",
)
@pytest.mark.parametrize(
    "recovery_channel",
    ["broker", "database", "isolated_database"],
)
def test_sync_soft_limit_recovers_remote_id_after_checkpoint_coroutine_is_canceled(
    monkeypatch,
    tmp_path,
    recovery_channel: str,
) -> None:
    from app.providers import video_gen as provider_contract
    from app.workers import async_runtime as async_runtime_module
    from app.workers import video_generation as worker
    from app.workers.async_runtime import WorkerAsyncRuntime
    from tests import test_video_generation as video_fixtures

    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'checkpoint.db'}")
    for table in (
        video_fixtures.User.__table__,
        video_fixtures.Script.__table__,
        video_fixtures.Episode.__table__,
        video_fixtures.ShotGroup.__table__,
        video_fixtures.Shot.__table__,
        video_fixtures.Subject.__table__,
        video_fixtures.AssetEntry.__table__,
        video_fixtures.CreditAccount.__table__,
        video_fixtures.CreditReservation.__table__,
        video_fixtures.CreditLog.__table__,
        video_fixtures.VideoProviderConfig.__table__,
        video_fixtures.VideoGenerationJob.__table__,
        video_fixtures.VideoGenerationIdempotencyRequest.__table__,
        video_fixtures.TaskDispatchOutbox.__table__,
    ):
        table.create(engine)
    raw = Session(engine, expire_on_commit=False)
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-real-soft-checkpoint-holder",
        reservation_id="reservation-real-soft-checkpoint-holder",
    )
    provider_task_id = "provider-real-soft-checkpoint-holder"
    checkpoint_started = threading.Event()
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    persist_calls = 0
    provider_calls = 0

    class Provider:
        async def generate(self, request, _on_progress=None):
            nonlocal provider_calls
            provider_calls += 1
            assert request.on_task_created is not None
            try:
                await request.on_task_created(provider_task_id)
                raise AssertionError("checkpoint 恢复前 Provider 不得继续")
            finally:
                if recovery_channel in {"broker", "isolated_database"}:
                    cleanup_started.set()
                    current = asyncio.current_task()
                    assert current is not None
                    while current.cancelling():
                        current.uncancel()
                    while not release_cleanup.is_set():
                        try:
                            await asyncio.sleep(0.01)
                        except asyncio.CancelledError:
                            while current.cancelling():
                                current.uncancel()

    publish = Mock(
        side_effect=(
            ConnectionError("checkpoint broker unavailable")
            if recovery_channel in {"database", "isolated_database"}
            else None
        )
    )
    runtime = WorkerAsyncRuntime()
    original_persist = worker._persist_video_task_checkpoint_independently
    isolated_persist_calls = 0

    async def blocked_then_recover(
        recover_job_id: str,
        recover_provider_task_id: str,
        recover_lease_token: str,
    ) -> bool:
        nonlocal persist_calls
        persist_calls += 1
        if persist_calls == 1:
            checkpoint_started.set()
            await asyncio.Event().wait()
        if recovery_channel == "database":
            return await original_persist(
                recover_job_id,
                recover_provider_task_id,
                recover_lease_token,
            )
        raise ConnectionError("checkpoint database unavailable")

    def persist_in_isolated_runtime(
        recover_job_id: str,
        recover_provider_task_id: str,
        recover_lease_token: str,
    ) -> bool:
        nonlocal isolated_persist_calls
        isolated_persist_calls += 1
        return asyncio.run(
            original_persist(
                recover_job_id,
                recover_provider_task_id,
                recover_lease_token,
            )
        )

    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )
    monkeypatch.setattr(provider_contract, "get_video_provider", lambda _key: Provider())
    monkeypatch.setattr(
        worker,
        "_persist_video_task_checkpoint_independently",
        blocked_then_recover,
    )
    monkeypatch.setattr(worker, "_publish_video_task_checkpoint", publish)
    monkeypatch.setattr(
        worker,
        "_persist_video_task_checkpoint_in_isolated_runtime",
        persist_in_isolated_runtime,
    )
    monkeypatch.setattr(async_runtime_module, "_runtime", runtime)

    def raise_soft_limit(_signum: int, _frame: object) -> None:
        raise SoftTimeLimitExceeded()

    def interrupt_after_checkpoint_started() -> None:
        if checkpoint_started.wait(timeout=2):
            os.kill(os.getpid(), signal.SIGALRM)

    previous_handler = signal.signal(signal.SIGALRM, raise_soft_limit)
    interrupt = threading.Thread(target=interrupt_after_checkpoint_started, daemon=True)
    interrupt.start()
    persisted_provider_task_id = None
    started_at = time.monotonic()
    try:
        with pytest.raises(SoftTimeLimitExceeded):
            worker.run_video_generation.run(job.job_id)
        elapsed = time.monotonic() - started_at
        with Session(engine, expire_on_commit=False) as verify:
            saved = verify.get(VideoGenerationJob, job.job_id)
            assert saved is not None
            persisted_provider_task_id = (saved.output_json or {}).get(
                "provider_task_id"
            )
    finally:
        signal.signal(signal.SIGALRM, previous_handler)
        release_cleanup.set()
        interrupt.join(timeout=3)
        runtime.shutdown()
        raw.close()
        engine.dispose()

    assert checkpoint_started.is_set()
    assert provider_calls == 1
    assert persist_calls == (2 if recovery_channel == "database" else 1)
    assert isolated_persist_calls == (1 if recovery_channel == "isolated_database" else 0)
    publish.assert_called_once()
    assert publish.call_args.kwargs["job_id"] == job.job_id
    assert publish.call_args.kwargs["provider_task_id"] == provider_task_id
    assert publish.call_args.kwargs["lease_token"]
    if recovery_channel == "broker":
        assert elapsed < 0.8
        assert cleanup_started.is_set()
        assert persisted_provider_task_id is None
    else:
        if recovery_channel == "isolated_database":
            assert elapsed < 0.8
            assert cleanup_started.is_set()
        assert persisted_provider_task_id == provider_task_id


@pytest.mark.skipif(
    not hasattr(signal, "SIGALRM"),
    reason="当前平台不支持同步 SIGALRM 注入",
)
def test_sync_soft_limit_does_not_repeat_already_durable_checkpoint(monkeypatch) -> None:
    from app.workers import async_runtime as async_runtime_module
    from app.workers import video_generation as worker
    from app.workers.async_runtime import WorkerAsyncRuntime

    checkpoint_durable = threading.Event()
    runtime = WorkerAsyncRuntime()
    recover = Mock(side_effect=AssertionError("已 durable 不得重复 checkpoint"))
    publish = Mock(side_effect=AssertionError("已 durable 不得重复发布 checkpoint"))

    async def hold_after_durable(
        _job_id: str,
        *,
        provider_task_id=None,
        checkpoint_holder=None,
    ) -> dict:
        assert provider_task_id is None
        assert checkpoint_holder is not None
        checkpoint_holder.observe("provider-already-durable", "lease-already-durable")
        checkpoint_holder.mark_durable(
            "provider-already-durable",
            "lease-already-durable",
        )
        checkpoint_durable.set()
        await asyncio.Event().wait()
        return {}

    monkeypatch.setattr(worker, "_run_video_generation_async", hold_after_durable)
    monkeypatch.setattr(worker, "_recover_video_task_checkpoint_until_durable", recover)
    monkeypatch.setattr(worker, "_publish_video_task_checkpoint", publish)
    monkeypatch.setattr(async_runtime_module, "_runtime", runtime)

    def raise_soft_limit(_signum: int, _frame: object) -> None:
        raise SoftTimeLimitExceeded()

    def interrupt_after_durable() -> None:
        if checkpoint_durable.wait(timeout=2):
            os.kill(os.getpid(), signal.SIGALRM)

    previous_handler = signal.signal(signal.SIGALRM, raise_soft_limit)
    interrupt = threading.Thread(target=interrupt_after_durable, daemon=True)
    interrupt.start()
    try:
        with pytest.raises(SoftTimeLimitExceeded):
            worker.run_video_generation.run("job-already-durable")
    finally:
        signal.signal(signal.SIGALRM, previous_handler)
        interrupt.join(timeout=3)
        runtime.shutdown()

    assert checkpoint_durable.is_set()
    recover.assert_not_called()
    publish.assert_not_called()


@pytest.mark.asyncio
async def test_stale_reconciler_leaves_conflicting_provider_authority_untouched(
    monkeypatch,
) -> None:
    from app.crud import video as video_crud
    from app.services import task_dispatch
    from app.workers import video_generation as worker

    job = SimpleNamespace(
        job_id="job-stale-checkpoint-conflict",
        script_id="script-stale-checkpoint-conflict",
        episode_id="episode-stale-checkpoint-conflict",
        shot_group_id="group-stale-checkpoint-conflict",
        input_version=1,
        attempt=1,
        status="processing",
        lease_owner="old-worker",
        lease_token="old-lease-token",
        lease_until=datetime.now(UTC),
        output_json={"provider_task_id": "provider-output"},
        metrics={"provider_task_id": "provider-metrics"},
        dispatch_id=None,
    )

    class DB:
        def __init__(self) -> None:
            self.commit_count = 0

        async def commit(self) -> None:
            self.commit_count += 1

    db = DB()
    monkeypatch.setattr(
        worker,
        "reconcile_video_attempt_cleanup_debts",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        video_crud,
        "list_stale_processing_jobs",
        AsyncMock(return_value=[job]),
    )
    enqueue = AsyncMock()
    monkeypatch.setattr(task_dispatch, "enqueue_task", enqueue)

    assert await worker.reconcile_stale_video_jobs(db) == []
    assert job.status == "processing"
    assert job.lease_owner == "old-worker"
    assert job.lease_token == "old-lease-token"
    assert job.output_json == {"provider_task_id": "provider-output"}
    enqueue.assert_not_awaited()
    assert db.commit_count == 1


@pytest.mark.asyncio
async def test_delayed_stale_generation_ids_become_orphans_without_overwriting_new_id(
    monkeypatch,
) -> None:
    from app.crud import video as video_crud
    from app.services import video_remote_cancellation
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-stale-orphan-checkpoint",
        reservation_id="reservation-stale-orphan-checkpoint",
    )
    old_lease_token = "lease-stale-orphan-a"
    job.status = "processing"
    job.lease_owner = "worker-a"
    job.lease_token = old_lease_token
    job.lease_until = datetime.now(UTC) - timedelta(minutes=1)
    raw.commit()
    db = _AsyncSessionAdapter(raw)
    monkeypatch.setattr(
        worker,
        "reconcile_video_attempt_cleanup_debts",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )

    assert await worker.reconcile_stale_video_jobs(db) == [job.job_id]
    generation = video_remote_cancellation.video_task_lease_generation(
        old_lease_token
    )

    claim_b = await video_crud.claim_video_job(
        db,
        job.job_id,
        owner="worker-b",
        allow_stale_processing=False,
    )
    assert claim_b is not None
    await db.commit()
    assert await video_remote_cancellation.persist_video_task_checkpoint(
        db,
        job.job_id,
        "provider-authoritative-b",
        claim_b.lease_token,
    )
    await db.commit()

    for provider_task_id in ("provider-orphan-a1", "provider-orphan-a2"):
        assert await worker._run_video_task_checkpoint_async(
            job.job_id,
            provider_task_id,
            old_lease_token,
        ) == {
            "job_id": job.job_id,
            "provider_task_id": provider_task_id,
            "status": "persisted",
        }

    raw.expire_all()
    before_replay = raw.get(VideoGenerationJob, job.job_id)
    assert before_replay is not None
    version_before_replay = before_replay.version
    assert await worker._run_video_task_checkpoint_async(
        job.job_id,
        "provider-orphan-a1",
        old_lease_token,
    ) == {
        "job_id": job.job_id,
        "provider_task_id": "provider-orphan-a1",
        "status": "persisted",
    }

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert saved.version == version_before_replay
    assert (saved.output_json or {})["provider_task_id"] == "provider-authoritative-b"
    assert (saved.metrics or {})["provider_task_id"] == "provider-authoritative-b"
    output_state = (saved.output_json or {})["orphan_provider_cancellations"]
    metrics_state = (saved.metrics or {})["orphan_provider_cancellations"]
    assert output_state == metrics_state
    assert output_state["pending"] is True
    task_records = output_state["generations"][generation]["tasks"]
    assert {record["provider_task_id"] for record in task_records.values()} == {
        "provider-orphan-a1",
        "provider-orphan-a2",
    }
    assert all(record["outcome"] == "pending" for record in task_records.values())
    from app.models.credit_reservation import CreditReservation

    reservation = raw.get(CreditReservation, job.reservation_id)
    assert reservation is not None
    assert reservation.status == "reserved"


@pytest.mark.asyncio
async def test_orphan_checkpoint_debt_is_bounded_and_never_stores_raw_lease() -> None:
    from app.providers.video_gen import VideoTaskCheckpointUnavailable
    from app.services import video_remote_cancellation

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-bounded-orphan-checkpoint",
        reservation_id="reservation-bounded-orphan-checkpoint",
    )
    old_lease_token = "raw-lease-must-not-be-persisted"
    job.output_json, job.metrics = (
        video_remote_cancellation.merge_reclaimed_video_task_generation(
            job.output_json,
            job.metrics,
            old_lease_token,
        )
    )
    raw.commit()
    db = _AsyncSessionAdapter(raw)

    for index in range(8):
        assert await video_remote_cancellation.persist_video_task_checkpoint(
            db,
            job.job_id,
            f"provider-bounded-orphan-{index}",
            old_lease_token,
        )
        await db.commit()

    with pytest.raises(VideoTaskCheckpointUnavailable):
        await video_remote_cancellation.persist_video_task_checkpoint(
            db,
            job.job_id,
            "provider-bounded-orphan-overflow",
            old_lease_token,
        )
    await db.rollback()

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    state = (saved.output_json or {})["orphan_provider_cancellations"]
    generation = video_remote_cancellation.video_task_lease_generation(old_lease_token)
    assert len(state["generations"][generation]["tasks"]) == 8
    assert old_lease_token not in repr(saved.output_json)
    assert old_lease_token not in repr(saved.metrics)

    output_json: dict = {}
    metrics: dict = {}
    for index in range(16):
        output_json, metrics = (
            video_remote_cancellation.merge_reclaimed_video_task_generation(
                output_json,
                metrics,
                f"bounded-generation-{index}",
            )
        )
    with pytest.raises(
        video_remote_cancellation.OrphanProviderCancellationCapacityExceeded
    ):
        video_remote_cancellation.merge_reclaimed_video_task_generation(
            output_json,
            metrics,
            "bounded-generation-overflow",
        )


@pytest.mark.asyncio
async def test_reconciler_resolves_orphan_and_current_cancellation_debts_independently(
    monkeypatch,
) -> None:
    from app.providers.video_gen import VideoCancellationOutcome, VideoCancellationResult
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-orphan-and-current-cancellation",
        reservation_id="reservation-orphan-and-current-cancellation",
    )
    generation = f"sha256:{hashlib.sha256(b'old-lease-a').hexdigest()}"
    orphan_task_key = f"sha256:{hashlib.sha256(b'provider-orphan-a').hexdigest()}"
    current_record = {
        "provider_task_id": "provider-authoritative-b",
        "outcome": "pending",
        "detail": None,
        "retryable": True,
        "attempts": 0,
    }
    orphan_state = {
        "pending": True,
        "generations": {
            generation: {
                "tasks": {
                    orphan_task_key: {
                        "provider_task_id": "provider-orphan-a",
                        "outcome": "pending",
                        "detail": None,
                        "retryable": True,
                        "attempts": 0,
                    }
                }
            }
        },
    }
    job.status = "canceled"
    job.lease_owner = None
    job.lease_token = None
    job.lease_until = None
    job.output_json = {
        "provider_task_id": "provider-authoritative-b",
        "provider_cancellation": deepcopy(current_record),
        "orphan_provider_cancellations": deepcopy(orphan_state),
    }
    job.metrics = deepcopy(job.output_json)
    raw.commit()
    db = _AsyncSessionAdapter(raw)

    async def cancel(provider_task_id: str) -> VideoCancellationResult:
        return VideoCancellationResult(
            provider_task_id=provider_task_id,
            outcome=VideoCancellationOutcome.CANCELED,
        )

    cancel_mock = AsyncMock(side_effect=cancel)
    monkeypatch.setattr(
        worker,
        "get_video_provider",
        lambda _provider_key: SimpleNamespace(cancel=cancel_mock),
    )

    assert await worker.reconcile_video_remote_cancellations(db) == 2
    assert await worker.reconcile_video_remote_cancellations(db) == 0
    assert cancel_mock.await_count == 2
    assert {call.args[0] for call in cancel_mock.await_args_list} == {
        "provider-orphan-a",
        "provider-authoritative-b",
    }

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert (saved.output_json or {})["provider_task_id"] == "provider-authoritative-b"
    assert (saved.metrics or {})["provider_task_id"] == "provider-authoritative-b"
    assert (saved.output_json or {})["provider_cancellation"] == {
        "provider_task_id": "provider-authoritative-b",
        "outcome": "canceled",
        "detail": None,
        "retryable": False,
        "attempts": 1,
    }
    expected_orphan_state = {
        "pending": False,
        "generations": {
            generation: {
                "tasks": {
                    orphan_task_key: {
                        "provider_task_id": "provider-orphan-a",
                        "outcome": "canceled",
                        "detail": None,
                        "retryable": False,
                        "attempts": 1,
                    }
                }
            }
        },
    }
    assert (saved.output_json or {})["orphan_provider_cancellations"] == expected_orphan_state
    assert (saved.metrics or {})["orphan_provider_cancellations"] == expected_orphan_state


@pytest.mark.asyncio
async def test_run_once_checkpoint_unavailable_keeps_processing_without_cancel_or_release(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-run-once-checkpoint-unavailable",
        reservation_id="reservation-run-once-checkpoint-unavailable",
    )
    db = _AsyncSessionAdapter(raw)
    provider_task_id = "provider-run-once-checkpoint-unavailable"

    class Provider:
        cancel = AsyncMock()

        async def generate(self, request, _on_progress=None):
            assert request.on_task_created is not None
            await request.on_task_created(provider_task_id)
            raise AssertionError("检查点不可用后不得继续 Provider 流程")

    checkpoint_error = VideoTaskCheckpointUnavailable(
        "video_task_checkpoint_unavailable",
        provider_task_id=provider_task_id,
    )
    monkeypatch.setattr(
        worker,
        "_ensure_video_task_checkpoint_durable",
        AsyncMock(side_effect=checkpoint_error),
    )
    release = AsyncMock()
    monkeypatch.setattr(worker, "_credits_release", release)
    provider = Provider()

    with pytest.raises(worker.VideoTaskCheckpointRecoveryUnavailable) as exc_info:
        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="checkpoint-worker",
            provider=provider,
            extract_last_frame=AsyncMock(),
        )

    assert exc_info.value.provider_task_id == provider_task_id
    assert exc_info.value.lease_token
    provider.cancel.assert_not_awaited()
    release.assert_not_awaited()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert saved.status == "processing"
    assert saved.lease_token == exc_info.value.lease_token


@pytest.mark.asyncio
async def test_run_once_checkpoint_conflict_is_fail_closed(monkeypatch) -> None:
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-run-once-checkpoint-conflict",
        reservation_id="reservation-run-once-checkpoint-conflict",
    )
    db = _AsyncSessionAdapter(raw)
    provider_task_id = "provider-run-once-checkpoint-conflict"

    class Provider:
        cancel = AsyncMock()

        async def generate(self, request, _on_progress=None):
            assert request.on_task_created is not None
            await request.on_task_created(provider_task_id)
            raise AssertionError("检查点冲突后不得继续 Provider 流程")

    monkeypatch.setattr(
        worker,
        "_ensure_video_task_checkpoint_durable",
        AsyncMock(
            side_effect=VideoTaskCheckpointConflict(
                "video_task_checkpoint_conflict",
                provider_task_id=provider_task_id,
            )
        ),
    )
    release = AsyncMock()
    monkeypatch.setattr(worker, "_credits_release", release)
    provider = Provider()

    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="checkpoint-worker",
        provider=provider,
        extract_last_frame=AsyncMock(),
    )

    provider.cancel.assert_not_awaited()
    release.assert_not_awaited()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert saved.status == "processing"
    assert saved.lease_token


@pytest.mark.asyncio
async def test_confirmed_remote_terminal_fails_and_releases_without_cancel_retry(
    monkeypatch,
) -> None:
    from app.models.credit_reservation import CreditReservation
    from app.providers.video_gen import VideoGenerationRemoteTerminalError
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-confirmed-remote-terminal",
        reservation_id="reservation-confirmed-remote-terminal",
    )
    db = _AsyncSessionAdapter(raw)
    provider_task_id = "provider-confirmed-remote-terminal"

    class Provider:
        cancel = AsyncMock(side_effect=AssertionError("明确远端终态不得再次取消"))

        async def generate(self, request, _on_progress=None):
            assert request.on_task_created is not None
            await request.on_task_created(provider_task_id)
            raise VideoGenerationRemoteTerminalError(
                provider_task_id=provider_task_id,
                terminal_status="failed",
            )

    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )
    provider = Provider()

    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="remote-terminal-worker",
        provider=provider,
        extract_last_frame=AsyncMock(),
    )

    provider.cancel.assert_not_awaited()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert reservation is not None
    assert saved.status == "failed"
    assert saved.error_type == "provider_remote_terminal"
    assert saved.retryable is False
    assert saved.lease_token is None
    assert reservation.status == "released"
    evidence = {
        "provider_task_id": provider_task_id,
        "status": "failed",
    }
    assert (saved.output_json or {})["provider_remote_terminal"] == evidence
    assert (saved.metrics or {})["provider_remote_terminal"] == evidence


@pytest.mark.asyncio
async def test_broker_only_checkpoint_race_keeps_processing_on_provider_error(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    _, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-broker-only-provider-error",
        reservation_id="reservation-broker-only-provider-error",
    )
    db = _AsyncSessionAdapter(raw)
    provider_task_id = "provider-broker-only-provider-error"

    class Provider:
        cancel = AsyncMock()

        async def generate(self, request, _on_progress=None):
            assert request.on_task_created is not None
            await request.on_task_created(provider_task_id)
            raise RuntimeError("provider polling failed before checkpoint message persisted")

    # 模拟独立 DB 失败、专用 broker 发布成功：callback 可返回，但 DB 暂时还没有 ID。
    monkeypatch.setattr(
        worker,
        "_ensure_video_task_checkpoint_durable",
        AsyncMock(return_value=None),
    )
    release = AsyncMock()
    monkeypatch.setattr(worker, "_credits_release", release)
    provider = Provider()

    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="checkpoint-worker",
        provider=provider,
        extract_last_frame=AsyncMock(),
    )

    provider.cancel.assert_not_awaited()
    release.assert_not_awaited()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert saved.status == "processing"
    assert saved.lease_token
    assert "provider_task_id" not in (saved.output_json or {})
    assert "provider_task_id" not in (saved.metrics or {})


@pytest.mark.asyncio
async def test_submission_uncertain_is_durable_and_keeps_credit_frozen(
    monkeypatch,
) -> None:
    from app.models.credit_reservation import CreditReservation
    from app.providers.video_gen import VideoGenerationSubmissionUncertain
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-submission-uncertain",
        reservation_id="reservation-submission-uncertain",
    )
    db = _AsyncSessionAdapter(raw)
    events: list[str] = []

    class Provider:
        provider_key = "modelsai"
        cancel = AsyncMock(side_effect=AssertionError("未知提交不得尝试无 ID 取消"))

        async def generate(self, request, _on_progress=None):
            assert request.before_submit is not None
            assert request.on_submission_uncertain is not None
            await request.before_submit()
            events.append("remote_submit_started")
            await request.on_submission_uncertain()
            raise VideoGenerationSubmissionUncertain()

    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )
    release = AsyncMock(side_effect=AssertionError("提交未知不得释放冻结积分"))
    monkeypatch.setattr(worker, "_credits_release", release)

    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="submission-worker",
        provider=Provider(),
        extract_last_frame=AsyncMock(),
    )

    assert events == ["remote_submit_started"]
    release.assert_not_awaited()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert reservation is not None
    assert saved.status == "processing"
    assert saved.lease_token
    assert reservation.status == "reserved"
    evidence = {
        "state": "submission_uncertain",
        "provider_key": "modelsai",
        "attempt": 1,
    }
    assert (saved.output_json or {})["provider_submission"] == evidence
    assert (saved.metrics or {})["provider_submission"] == evidence


@pytest.mark.asyncio
async def test_submit_intent_alone_keeps_processing_after_untyped_provider_error(
    monkeypatch,
) -> None:
    from app.models.credit_reservation import CreditReservation
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-submit-intent-only",
        reservation_id="reservation-submit-intent-only",
    )
    db = _AsyncSessionAdapter(raw)

    class Provider:
        provider_key = "modelsai"
        cancel = AsyncMock()

        async def generate(self, request, _on_progress=None):
            assert request.before_submit is not None
            await request.before_submit()
            raise RuntimeError("response_lost_before_typed_conversion")

    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )
    release = AsyncMock(side_effect=AssertionError("提交意图未决不得释放冻结积分"))
    monkeypatch.setattr(worker, "_credits_release", release)

    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="submission-worker",
        provider=Provider(),
        extract_last_frame=AsyncMock(),
    )

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert reservation is not None
    assert saved.status == "processing"
    assert reservation.status == "reserved"
    assert (saved.output_json or {})["provider_submission"]["state"] == "submit_intent"


@pytest.mark.asyncio
async def test_submit_rejected_fails_and_releases_credit(monkeypatch) -> None:
    from app.models.credit_reservation import CreditReservation
    from app.providers.video_gen import VideoGenerationSubmitRejected
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-submit-rejected",
        reservation_id="reservation-submit-rejected",
    )
    db = _AsyncSessionAdapter(raw)

    class Provider:
        provider_key = "modelsai"
        cancel = AsyncMock()

        async def generate(self, request, _on_progress=None):
            assert request.before_submit is not None
            await request.before_submit()
            raise VideoGenerationSubmitRejected()

    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )

    await worker.run_video_generation_once(
        db,
        job.job_id,
        owner="submission-worker",
        provider=Provider(),
        extract_last_frame=AsyncMock(),
    )

    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    reservation = raw.get(CreditReservation, job.reservation_id)
    assert saved is not None
    assert reservation is not None
    assert saved.status == "failed"
    assert saved.error_type == "provider_submit_rejected"
    assert saved.retryable is False
    assert reservation.status == "released"
    assert (saved.output_json or {})["provider_submission"] == {
        "state": "submit_rejected",
        "provider_key": "modelsai",
        "attempt": 1,
    }


@pytest.mark.asyncio
async def test_dedicated_checkpoint_message_replay_is_idempotent(monkeypatch) -> None:
    from app.crud.video import claim_video_job
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-dedicated-checkpoint-replay",
        reservation_id="reservation-dedicated-checkpoint-replay",
    )
    db = _AsyncSessionAdapter(raw)
    claim = await claim_video_job(db, job.job_id, owner="checkpoint-worker")
    assert claim is not None
    await db.commit()
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )

    first = await worker._run_video_task_checkpoint_async(
        job.job_id,
        "provider-dedicated-checkpoint",
        claim.lease_token,
    )
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    checkpoint_version = saved.version
    second = await worker._run_video_task_checkpoint_async(
        job.job_id,
        "provider-dedicated-checkpoint",
        claim.lease_token,
    )

    assert first == second == {
        "job_id": job.job_id,
        "provider_task_id": "provider-dedicated-checkpoint",
        "status": "persisted",
    }
    raw.expire_all()
    replayed = raw.get(VideoGenerationJob, job.job_id)
    assert replayed is not None
    assert replayed.version == checkpoint_version
    assert (replayed.output_json or {})["provider_task_id"] == "provider-dedicated-checkpoint"
    assert (replayed.metrics or {})["provider_task_id"] == "provider-dedicated-checkpoint"


def test_dedicated_checkpoint_task_retries_original_payload_on_database_error(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    database_error = ConnectionError("database unavailable")
    retry = Mock(wraps=worker.persist_video_task_checkpoint_task.retry)

    def fail_database(awaitable):
        awaitable.close()
        raise database_error

    monkeypatch.setattr(worker, "run_worker_async", Mock(side_effect=fail_database))
    monkeypatch.setattr(worker.persist_video_task_checkpoint_task, "retry", retry)
    worker.persist_video_task_checkpoint_task.push_request(
        called_directly=False,
        is_eager=False,
        retries=0,
    )
    try:
        with pytest.raises(Retry) as exc_info:
            worker.persist_video_task_checkpoint_task.run(
                "job-dedicated-checkpoint-retry",
                "provider-dedicated-checkpoint-retry",
                "lease-dedicated-checkpoint-retry",
            )
    finally:
        worker.persist_video_task_checkpoint_task.pop_request()

    assert exc_info.value.exc is database_error
    retry.assert_called_once_with(
        exc=database_error,
        countdown=worker._TASK_CHECKPOINT_CHANNEL_RETRY_SECONDS,
        kwargs={
            "job_id": "job-dedicated-checkpoint-retry",
            "provider_task_id": "provider-dedicated-checkpoint-retry",
            "lease_token": "lease-dedicated-checkpoint-retry",
        },
    )


def test_dedicated_checkpoint_retry_publish_failure_requeues_original_message(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    database_error = ConnectionError("database unavailable")
    retry_signature = Mock()
    retry_signature.apply_async.side_effect = ConnectionError("broker unavailable")
    signature_from_request = Mock(return_value=retry_signature)

    def fail_database(awaitable):
        awaitable.close()
        raise database_error

    monkeypatch.setattr(worker, "run_worker_async", Mock(side_effect=fail_database))
    monkeypatch.setattr(
        worker.persist_video_task_checkpoint_task,
        "signature_from_request",
        signature_from_request,
    )
    worker.persist_video_task_checkpoint_task.push_request(
        called_directly=False,
        is_eager=False,
        retries=0,
    )
    try:
        with pytest.raises(Reject) as exc_info:
            worker.persist_video_task_checkpoint_task.run(
                "job-dedicated-checkpoint-broker-failure",
                "provider-dedicated-checkpoint-broker-failure",
                "lease-dedicated-checkpoint-broker-failure",
            )
    finally:
        worker.persist_video_task_checkpoint_task.pop_request()

    assert exc_info.value.requeue is True
    retry_signature.apply_async.assert_called_once_with()


@pytest.mark.asyncio
async def test_volcengine_worker_persists_checkpoint_before_soft_limit_probe(
    monkeypatch,
) -> None:
    from app.providers import video_gen_volcengine
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider
    from app.workers import video_generation as worker

    engine, raw = _make_db()
    seeded = _seed(raw)
    job = _add_worker_job(
        raw,
        seeded,
        job_id="job-volcengine-worker-checkpoint",
        reservation_id="reservation-volcengine-worker-checkpoint",
    )
    job.provider_key = "volcengine"
    raw.commit()
    db = _AsyncSessionAdapter(raw)
    tasks = SimpleNamespace(
        create=AsyncMock(return_value=SimpleNamespace(id="volcengine-worker-task")),
        get=AsyncMock(side_effect=AssertionError("soft limit 前不得轮询")),
        delete=AsyncMock(side_effect=AssertionError("soft limit 不得触发远端取消")),
    )
    client = SimpleNamespace(
        content_generation=SimpleNamespace(tasks=tasks),
        close=AsyncMock(),
    )
    provider = VolcengineVideoGenProvider(provider_key="volcengine")
    monkeypatch.setattr(provider, "_check_keys", lambda: None)
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)
    monkeypatch.setattr(
        worker,
        "_resolve_execution_first_frame",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "app.core.database.async_session_factory",
        _SQLiteAsyncSessionFactory(engine),
    )
    monkeypatch.setattr(
        video_gen_volcengine,
        "raise_if_video_cancel_requested_before_submit",
        AsyncMock(),
    )

    async def soft_limit_after_checkpoint(
        _cancel_requested,
        provider_task_id: str,
        _cancel_remote,
    ) -> None:
        raw.expire_all()
        saved = raw.get(VideoGenerationJob, job.job_id)
        assert saved is not None
        assert (saved.output_json or {})["provider_task_id"] == provider_task_id
        assert (saved.metrics or {})["provider_task_id"] == provider_task_id
        raise SoftTimeLimitExceeded()

    monkeypatch.setattr(
        video_gen_volcengine,
        "raise_if_video_cancel_requested",
        soft_limit_after_checkpoint,
    )

    with pytest.raises(SoftTimeLimitExceeded):
        await worker.run_video_generation_once(
            db,
            job.job_id,
            owner="checkpoint-worker",
            provider=provider,
            extract_last_frame=AsyncMock(),
        )

    tasks.create.assert_awaited_once()
    tasks.get.assert_not_awaited()
    tasks.delete.assert_not_awaited()
    client.close.assert_awaited_once()
    raw.expire_all()
    saved = raw.get(VideoGenerationJob, job.job_id)
    assert saved is not None
    assert saved.status == "processing"
    assert (saved.output_json or {})["provider_task_id"] == "volcengine-worker-task"
    assert (saved.metrics or {})["provider_task_id"] == "volcengine-worker-task"
