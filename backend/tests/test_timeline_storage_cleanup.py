"""时间线 attempt 对象删除补偿回归测试。"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest


class _CompensationDb:
    def __init__(self, *, commit_error: Exception | None = None) -> None:
        self.commit_error = commit_error
        self.commit_count = 0
        self.pending: dict[str, dict] = {}
        self.committed: dict[str, dict] = {}

    def stage(self, payload: dict) -> SimpleNamespace:
        idempotency_key = payload["idempotency_key"]
        if idempotency_key not in self.committed:
            self.pending[idempotency_key] = dict(payload)
        return SimpleNamespace(**payload)

    async def commit(self) -> None:
        self.commit_count += 1
        if self.commit_error is not None and self.commit_count > 1:
            self.pending.clear()
            raise self.commit_error
        self.committed.update(self.pending)
        self.pending.clear()

    async def rollback(self) -> None:
        self.pending.clear()


class _CompensationSessionFactory:
    def __init__(self, db: _CompensationDb) -> None:
        self.db = db
        self.enter_count = 0

    def __call__(self):
        return self

    async def __aenter__(self):
        self.enter_count += 1
        return self.db

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None:
            await self.db.rollback()
        return None


class _BoundaryFailure:
    def __init__(self, boundary: str, error: BaseException) -> None:
        self.boundary = boundary
        self.error = error
        self.triggered = False

    def raise_once(self, boundary: str) -> None:
        if self.triggered or self.boundary != boundary:
            return
        self.triggered = True
        raise self.error


class _BoundaryDb:
    def __init__(self, failure: _BoundaryFailure) -> None:
        self.failure = failure
        self.commit_count = 0

    async def commit(self) -> None:
        self.failure.raise_once("commit")
        self.commit_count += 1


class _BoundarySessionFactory:
    def __init__(self, failure: _BoundaryFailure) -> None:
        self.failure = failure
        self.db = _BoundaryDb(failure)
        self.call_count = 0
        self.enter_count = 0
        self.exit_count = 0

    def __call__(self):
        self.call_count += 1
        self.failure.raise_once("factory")
        return self

    async def __aenter__(self):
        self.enter_count += 1
        self.failure.raise_once("enter")
        return self.db

    async def __aexit__(self, exc_type, exc, tb):
        self.exit_count += 1
        self.failure.raise_once("exit")
        return None


async def _run_cleanup_path(
    monkeypatch,
    tmp_path: Path,
    *,
    failure_point: str,
    compensation_db: _CompensationDb,
) -> dict:
    from app.services import media_storage_jobs
    from app.workers import timeline_render as worker

    token = "lease-token-attempt-cleanup"
    base_key = "generate_outputs/script-1/episode-1/render-cleanup.mp4"
    job = SimpleNamespace(
        input_json={"segments": [{"storage_key": "generate_images/video/source.mp4"}]},
        output_storage_key=base_key,
        attempt=3,
    )
    original_error = RuntimeError(f"{failure_point} original failure")
    deleted: list[str] = []
    activated: list[tuple[str, ...]] = []
    finish_calls: list[dict] = []
    promote_calls: list[tuple[str, str]] = []

    async def load_claimed(*_args, **_kwargs):
        return job, token

    async def active(*_args, **_kwargs):
        return "active"

    async def heartbeat(*_args, **_kwargs):
        return True

    async def has_current_lease(*_args, **_kwargs):
        return failure_point != "lost_lease"

    async def finish(*_args, **kwargs):
        finish_calls.append(kwargs)
        return failure_point != "finish_false"

    async def enqueue_delete(_db, **kwargs):
        return _db.stage(kwargs)

    async def activate_delete_intents(intent_keys: tuple[str, ...]) -> bool:
        activated.append(intent_keys)
        return set(intent_keys) <= set(compensation_db.committed)

    async def authoritative(*_args, **_kwargs):
        return False

    class Storage:
        async def iter_chunks(self, _key: str, **_kwargs):
            yield b"source-video"

        async def put_file(self, key: str, _path: str) -> str:
            if failure_point == "upload":
                raise original_error
            if failure_point == "upload_return_base":
                return base_key
            return key

        async def promote(self, source_key: str, final_key: str) -> str:
            promote_calls.append((source_key, final_key))
            if failure_point == "promote":
                raise original_error
            if failure_point == "promote_return_base":
                return base_key
            return final_key

        async def delete(self, key: str) -> None:
            deleted.append(key)
            raise RuntimeError("对象存储删除失败")

    async def trim(
        _src: str,
        _start: float,
        _end: float | None,
        output: str,
        **_kwargs,
    ) -> None:
        Path(output).write_bytes(b"trimmed")

    async def concat(_paths: list[str], output: str, **_kwargs) -> None:
        Path(output).write_bytes(b"final-video")

    monkeypatch.setattr(worker, "async_session_factory", _CompensationSessionFactory(compensation_db))
    monkeypatch.setattr(worker, "_load_claimed_job", load_claimed)
    monkeypatch.setattr(worker, "_runtime_state", active)
    monkeypatch.setattr(worker, "_heartbeat", heartbeat)
    monkeypatch.setattr(worker, "_has_current_lease", has_current_lease)
    monkeypatch.setattr(worker, "_finish", finish)
    monkeypatch.setattr(worker, "_activate_delete_intents", activate_delete_intents)
    monkeypatch.setattr(worker, "_authoritative_attempt_succeeded", authoritative)
    monkeypatch.setattr(worker, "get_storage", lambda: Storage())
    monkeypatch.setattr(worker, "_trim_segment", trim)
    monkeypatch.setattr(worker, "_concat_segments", concat)
    monkeypatch.setattr(media_storage_jobs, "enqueue_delete", enqueue_delete)

    result = await worker.run_render_job(
        "render-cleanup",
        owner="worker-a",
        temp_root=tmp_path,
    )

    attempt_id = worker._storage_attempt_id(token)
    attempt_key = worker._attempt_output_key(base_key, job.attempt, attempt_id)
    temp_key = f"{attempt_key}.tmp"
    expected_keys = [temp_key, attempt_key]
    assert deleted == expected_keys
    committed = list(compensation_db.committed.values())
    assert [item["source_key"] for item in committed] == expected_keys
    assert base_key not in deleted
    assert all(base_key != item["source_key"] for item in committed)
    assert all(f".attempt-{job.attempt}-{attempt_id}" in key for key in expected_keys)
    assert compensation_db.commit_count >= 1
    assert not any(tmp_path.iterdir())
    return {
        "result": result,
        "error": original_error,
        "finish_calls": finish_calls,
        "enqueued": committed,
        "activated": activated,
        "promote_calls": promote_calls,
    }


async def test_delete_storage_keys_persists_stable_job_for_normalized_attempt_key(
    monkeypatch,
) -> None:
    from app.services import media_storage_jobs
    from app.workers import timeline_render as worker

    normalized_key = (
        "generate_outputs/script-1/episode-1/"
        "render.attempt-2-a1b2c3d4e5f60708.mp4.tmp"
    )
    raw_key = f"/{normalized_key}"
    deleted: list[str] = []
    enqueued: list[dict[str, str]] = []
    db = _CompensationDb()

    class Storage:
        async def delete(self, key: str) -> None:
            deleted.append(key)
            raise RuntimeError("对象存储删除失败")

    async def enqueue_delete(_db, **kwargs):
        enqueued.append(kwargs)
        return SimpleNamespace()

    monkeypatch.setattr(worker, "async_session_factory", _CompensationSessionFactory(db))
    monkeypatch.setattr(media_storage_jobs, "enqueue_delete", enqueue_delete)

    await worker._delete_storage_keys(Storage(), raw_key, normalized_key)

    digest = hashlib.sha256(normalized_key.encode("utf-8")).hexdigest()
    assert deleted == [normalized_key]
    assert len(enqueued) == 1
    assert enqueued[0]["idempotency_key"] == f"timeline-render-delete:{digest}"
    assert enqueued[0]["source_key"] == normalized_key
    assert enqueued[0]["available_at"] is not None
    assert db.commit_count == 1


@pytest.mark.parametrize("failure_point", ["lost_lease", "upload", "promote"])
async def test_render_cleanup_failure_persists_only_attempt_delete_jobs(
    monkeypatch,
    tmp_path,
    failure_point: str,
) -> None:
    outcome = await _run_cleanup_path(
        monkeypatch,
        tmp_path,
        failure_point=failure_point,
        compensation_db=_CompensationDb(),
    )

    if failure_point == "lost_lease":
        assert outcome["result"]["status"] == "skipped"
        assert outcome["finish_calls"] == []
    else:
        assert outcome["result"]["status"] == "failed"
        assert outcome["finish_calls"][0]["status"] == "failed"
        assert outcome["finish_calls"][0]["error"] is outcome["error"]
    for item in outcome["enqueued"]:
        digest = hashlib.sha256(item["source_key"].encode("utf-8")).hexdigest()
        assert item["idempotency_key"] == f"timeline-render-delete:{digest}"


async def test_cleanup_compensation_commit_failure_preserves_original_render_error(
    monkeypatch,
    tmp_path,
) -> None:
    db_error = RuntimeError("补偿数据库提交失败")
    db = _CompensationDb(commit_error=db_error)
    outcome = await _run_cleanup_path(
        monkeypatch,
        tmp_path,
        failure_point="upload",
        compensation_db=db,
    )

    assert outcome["result"]["status"] == "failed"
    assert outcome["finish_calls"][0]["status"] == "failed"
    assert outcome["finish_calls"][0]["error"] is outcome["error"]
    assert len(outcome["enqueued"]) == 2
    assert all(item["available_at"] is not None for item in outcome["enqueued"])
    assert outcome["activated"]
    assert db.pending == {}


@pytest.mark.parametrize("failure_point", ["upload_return_base", "promote_return_base"])
async def test_storage_adapter_returning_shared_base_key_fails_closed(
    monkeypatch,
    tmp_path,
    failure_point: str,
) -> None:
    outcome = await _run_cleanup_path(
        monkeypatch,
        tmp_path,
        failure_point=failure_point,
        compensation_db=_CompensationDb(),
    )

    assert outcome["result"]["status"] == "failed"
    assert outcome["finish_calls"][0]["status"] == "failed"
    if failure_point == "upload_return_base":
        assert outcome["promote_calls"] == []


async def test_finish_cas_failure_compensates_only_precomputed_attempt_keys(
    monkeypatch,
    tmp_path,
) -> None:
    outcome = await _run_cleanup_path(
        monkeypatch,
        tmp_path,
        failure_point="finish_false",
        compensation_db=_CompensationDb(),
    )

    assert outcome["result"]["status"] == "skipped"
    assert outcome["finish_calls"][0]["status"] == "succeeded"
    assert len(outcome["enqueued"]) == 2


@pytest.mark.parametrize(
    "boundary",
    ["delete", "factory", "enter", "enqueue", "commit", "exit"],
)
async def test_cleanup_component_cancelled_error_retries_persistent_compensation(
    monkeypatch,
    boundary: str,
) -> None:
    from app.services import media_storage_jobs
    from app.workers import timeline_render as worker

    key = "generate_outputs/script-1/render.attempt-1-cancel.mp4.tmp"
    failure = _BoundaryFailure(boundary, asyncio.CancelledError())
    factory = _BoundarySessionFactory(failure)
    enqueued: list[dict[str, str]] = []

    class Storage:
        async def delete(self, _key: str) -> None:
            failure.raise_once("delete")
            raise RuntimeError("对象删除失败")

    async def enqueue_delete(_db, **kwargs):
        failure.raise_once("enqueue")
        enqueued.append(kwargs)
        return SimpleNamespace()

    monkeypatch.setattr(worker, "async_session_factory", factory)
    monkeypatch.setattr(media_storage_jobs, "enqueue_delete", enqueue_delete)

    await worker._delete_storage_keys(Storage(), key)

    assert failure.triggered is True
    assert factory.db.commit_count >= 1
    assert enqueued
    assert {item["source_key"] for item in enqueued} == {key}
    assert len({item["idempotency_key"] for item in enqueued}) == 1


@pytest.mark.parametrize("boundary", ["factory", "enter", "enqueue", "commit", "exit"])
async def test_cleanup_persistence_runtime_failure_is_isolated(
    monkeypatch,
    boundary: str,
) -> None:
    from app.services import media_storage_jobs
    from app.workers import timeline_render as worker

    key = "generate_outputs/script-1/render.attempt-1-runtime.mp4.tmp"
    failure = _BoundaryFailure(boundary, RuntimeError("补偿持久化失败"))
    factory = _BoundarySessionFactory(failure)

    class Storage:
        async def delete(self, _key: str) -> None:
            raise RuntimeError("对象删除失败")

    async def enqueue_delete(_db, **_kwargs):
        failure.raise_once("enqueue")
        return SimpleNamespace()

    monkeypatch.setattr(worker, "async_session_factory", factory)
    monkeypatch.setattr(media_storage_jobs, "enqueue_delete", enqueue_delete)

    await worker._delete_storage_keys(Storage(), key)

    assert failure.triggered is True


async def test_cleanup_compensation_log_omits_sql_parameters_and_storage_key(
    monkeypatch,
    caplog,
) -> None:
    from app.workers import timeline_render as worker

    storage_key = "generate_outputs/private/raw-secret-attempt.mp4.tmp"
    digest = hashlib.sha256(storage_key.encode("utf-8")).hexdigest()

    async def fail_with_sensitive_message(*_args, **_kwargs) -> None:
        raise RuntimeError(f"INSERT parameters=({storage_key!r}, 'secret-token')")

    monkeypatch.setattr(worker, "_persist_delete_compensation", fail_with_sensitive_message)
    caplog.set_level(logging.ERROR, logger=worker.__name__)

    await worker._enqueue_delete_compensation(storage_key, digest)

    assert "RuntimeError" in caplog.text
    assert digest in caplog.text
    assert storage_key not in caplog.text
    assert "secret-token" not in caplog.text
    assert "parameters=" not in caplog.text


@pytest.mark.parametrize("cancel_phase", ["delete", "enqueue"])
@pytest.mark.parametrize("cancel_count", [2, 5])
async def test_caller_cancellation_waits_for_cleanup_before_returning(
    monkeypatch,
    cancel_phase: str,
    cancel_count: int,
) -> None:
    from app.services import media_storage_jobs
    from app.workers import timeline_render as worker

    key = "generate_outputs/script-1/render.attempt-1-shield.mp4.tmp"
    started = asyncio.Event()
    release = asyncio.Event()
    db = _CompensationDb()
    enqueued: list[dict[str, str]] = []

    class Storage:
        async def delete(self, _key: str) -> None:
            if cancel_phase == "delete":
                started.set()
                await release.wait()
            raise RuntimeError("对象删除失败")

    async def enqueue_delete(_db, **kwargs):
        if cancel_phase == "enqueue":
            started.set()
            await release.wait()
        enqueued.append(kwargs)
        return SimpleNamespace()

    monkeypatch.setattr(worker, "async_session_factory", _CompensationSessionFactory(db))
    monkeypatch.setattr(media_storage_jobs, "enqueue_delete", enqueue_delete)

    cleanup = asyncio.create_task(worker._delete_storage_keys(Storage(), key))
    await started.wait()
    for _ in range(cancel_count):
        cleanup.cancel()
    release.set()
    await cleanup

    assert cleanup.cancelled() is False
    assert cleanup.cancelling() == 0
    assert enqueued and enqueued[0]["source_key"] == key
    assert db.commit_count == 1


@pytest.mark.parametrize("io_fails", [False, True], ids=["success", "failure"])
@pytest.mark.parametrize("cancel_phase", ["upload", "promote"])
async def test_storage_io_cancellation_waits_then_cleans_attempt_objects(
    monkeypatch,
    tmp_path,
    cancel_phase: str,
    io_fails: bool,
) -> None:
    from app.workers import timeline_render as worker

    token = "lease-token-storage-cancel"
    job = SimpleNamespace(
        input_json={"segments": [{"storage_key": "generate_images/video/source.mp4"}]},
        output_storage_key="generate_outputs/script-1/render-cancel.mp4",
        attempt=2,
    )
    started = asyncio.Event()
    release = asyncio.Event()
    inner_cancelled = False
    staged_keys: tuple[str, ...] = ()
    activated: list[tuple[str, ...]] = []
    deleted: list[str] = []
    stored = {"generate_images/video/source.mp4": b"source"}

    async def load_claimed(*_args, **_kwargs):
        return job, token

    async def active(*_args, **_kwargs):
        return "active"

    async def heartbeat(*_args, **_kwargs):
        return True

    async def has_current_lease(*_args, **_kwargs):
        return True

    async def finish(*_args, **_kwargs):
        raise AssertionError("调用方已取消时不得写入失败终态")

    async def stage_delete_intents(*keys: str) -> tuple[str, ...]:
        nonlocal staged_keys
        staged_keys = tuple(keys)
        return tuple(worker._delete_intent_key(key) for key in keys)

    async def activate_delete_intents(intent_keys: tuple[str, ...]) -> bool:
        activated.append(intent_keys)
        return True

    async def authoritative(*_args, **_kwargs):
        return False

    class Storage:
        async def iter_chunks(self, _key: str, **_kwargs):
            yield stored["generate_images/video/source.mp4"]

        async def _finish_io(self) -> None:
            nonlocal inner_cancelled
            started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                inner_cancelled = True
                await release.wait()
                raise

        async def put_file(self, key: str, path: str) -> str:
            if cancel_phase == "upload":
                await self._finish_io()
            stored[key] = Path(path).read_bytes()
            if cancel_phase == "upload" and io_fails:
                raise RuntimeError("upload failed after apply")
            return key

        async def promote(self, source_key: str, final_key: str) -> str:
            if cancel_phase == "promote":
                await self._finish_io()
            stored[final_key] = stored[source_key]
            if cancel_phase == "promote" and io_fails:
                raise RuntimeError("promote failed after apply")
            return final_key

        async def delete(self, key: str) -> None:
            deleted.append(key)
            stored.pop(key, None)

    async def trim(
        _src: str,
        _start: float,
        _end: float | None,
        output: str,
        **_kwargs,
    ) -> None:
        Path(output).write_bytes(b"trimmed")

    async def concat(_paths: list[str], output: str, **_kwargs) -> None:
        Path(output).write_bytes(b"final")

    monkeypatch.setattr(worker, "_load_claimed_job", load_claimed)
    monkeypatch.setattr(worker, "_runtime_state", active)
    monkeypatch.setattr(worker, "_heartbeat", heartbeat)
    monkeypatch.setattr(worker, "_has_current_lease", has_current_lease)
    monkeypatch.setattr(worker, "_finish", finish)
    monkeypatch.setattr(worker, "_stage_delete_intents", stage_delete_intents)
    monkeypatch.setattr(worker, "_activate_delete_intents", activate_delete_intents, raising=False)
    monkeypatch.setattr(worker, "_authoritative_attempt_succeeded", authoritative)
    monkeypatch.setattr(worker, "get_storage", lambda: Storage())
    monkeypatch.setattr(worker, "_trim_segment", trim)
    monkeypatch.setattr(worker, "_concat_segments", concat)

    render = asyncio.create_task(
        worker.run_render_job("render-cancel", owner="worker-a", temp_root=tmp_path)
    )
    await started.wait()
    render.cancel()
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await render

    assert inner_cancelled is False
    assert activated == [tuple(worker._delete_intent_key(key) for key in staged_keys)]
    assert set(deleted) == set(staged_keys)
    assert set(stored) == {"generate_images/video/source.mp4"}
    assert not any(tmp_path.iterdir())
