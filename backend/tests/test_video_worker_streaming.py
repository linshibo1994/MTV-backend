"""视频 worker 大对象流式 I/O 行为测试。"""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest


class _ScalarResult:
    def __init__(self, value) -> None:
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class _FakeDB:
    def __init__(self, shot_group) -> None:
        self.shot_group = shot_group

    async def execute(self, _statement):
        return _ScalarResult(self.shot_group)


def _install_fake_ffmpeg(monkeypatch, worker, tmp_path: Path, expected_video: bytes) -> list[str]:
    real_named_temporary_file = worker.tempfile.NamedTemporaryFile
    temp_paths: list[str] = []

    def named_temporary_file(**kwargs):
        handle = real_named_temporary_file(dir=tmp_path, **kwargs)
        temp_paths.append(handle.name)
        return handle

    def fake_run(command, **_kwargs):
        if command[0] == "ffprobe":
            assert Path(command[-1]).read_bytes() == expected_video
            return SimpleNamespace(stdout="1.25")
        assert command[0] == "ffmpeg"
        temp_paths.append(command[-1])
        Path(command[-1]).write_bytes(b"last-frame")
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(worker.tempfile, "NamedTemporaryFile", named_temporary_file)
    monkeypatch.setattr(worker.subprocess, "run", fake_run)
    return temp_paths


async def test_last_frame_streams_video_and_keeps_small_image_put(monkeypatch, tmp_path) -> None:
    from app.workers import video_generation as worker

    iter_calls: list[tuple[str, int, int, int | None]] = []
    puts: list[tuple[str, bytes]] = []

    class Storage:
        async def get(self, _key: str) -> bytes:
            raise AssertionError("大视频末帧提取不得调用 get(bytes)")

        async def iter_chunks(
            self,
            key: str,
            *,
            chunk_size: int,
            start: int,
            end: int | None,
        ):
            iter_calls.append((key, chunk_size, start, end))
            yield b"video-"
            yield b"chunks"

        async def put(self, key: str, data: bytes) -> str:
            puts.append((key, data))
            return key

        async def delete(self, _key: str) -> None:
            return None

    storage = Storage()
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    temp_paths = _install_fake_ffmpeg(
        monkeypatch,
        worker,
        tmp_path,
        b"video-chunks",
    )
    shot_group = SimpleNamespace(reference_frames={})
    attempt_id = "0123456789abcdef"

    result = await worker._update_last_frame(
        _FakeDB(shot_group),
        "group:stream-group",
        "generate_images/video/source.mp4",
        attempt_id,
    )

    assert result == (
        "/generate_images/video/frames/"
        f"stream-group_{attempt_id}_last.png"
    )
    assert iter_calls[0][0] == "generate_images/video/source.mp4"
    assert iter_calls[0][2:] == (0, None)
    assert puts == [(result, b"last-frame")]
    assert shot_group.reference_frames == {"last_frame": result}
    assert temp_paths
    assert all(not Path(path).exists() for path in temp_paths)
    assert not any(tmp_path.iterdir())


async def test_last_frame_stream_failure_removes_partial_local_video(monkeypatch, tmp_path) -> None:
    from app.workers import video_generation as worker

    iterated = False

    class Storage:
        async def get(self, _key: str) -> bytes:
            raise AssertionError("大视频末帧提取不得调用 get(bytes)")

        async def iter_chunks(self, _key: str, **_kwargs):
            nonlocal iterated
            iterated = True
            yield b"partial-video"
            raise OSError("stream interrupted")

        async def put(self, _key: str, _data: bytes) -> str:
            raise AssertionError("下载失败后不得上传末帧")

    real_named_temporary_file = worker.tempfile.NamedTemporaryFile

    def named_temporary_file(**kwargs):
        return real_named_temporary_file(dir=tmp_path, **kwargs)

    monkeypatch.setattr(worker.tempfile, "NamedTemporaryFile", named_temporary_file)
    monkeypatch.setattr(
        worker.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("下载失败后不得启动 ffmpeg")
        ),
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: Storage())

    with pytest.raises(RuntimeError, match="frame_extraction_failed"):
        await worker._update_last_frame(
            _FakeDB(SimpleNamespace(reference_frames={})),
            "group:stream-failure",
            "generate_images/video/source.mp4",
            "fedcba9876543210",
        )

    assert iterated is True
    assert not any(tmp_path.iterdir())


@pytest.mark.parametrize(
    ("reported_size", "chunks", "message"),
    [
        (6, [b"unused"], "文件过大"),
        (4, [b"123", b"456"], "文件过大"),
        (None, [], "为空"),
    ],
)
async def test_video_stream_enforces_size_and_empty_limits(
    monkeypatch,
    tmp_path,
    reported_size: int | None,
    chunks: list[bytes],
    message: str,
) -> None:
    from app.core.config import settings
    from app.workers import video_generation as worker

    iterated = False

    class Storage:
        async def get(self, _key: str) -> bytes:
            raise AssertionError("存在流式接口时不得 fallback")

        async def get_size(self, _key: str) -> int:
            if reported_size is None:
                raise AssertionError("该场景不应调用 get_size")
            return reported_size

        async def iter_chunks(self, _key: str, **_kwargs):
            nonlocal iterated
            iterated = True
            for chunk in chunks:
                yield chunk

    storage = Storage()
    if reported_size is None:
        storage.get_size = None  # type: ignore[method-assign]
    local_path = tmp_path / "source.mp4"
    monkeypatch.setattr(settings, "max_media_read_bytes", 5)

    with pytest.raises(ValueError, match=message):
        await worker._download_storage_file(
            storage,
            "generate_images/video/source.mp4",
            str(local_path),
        )

    if reported_size == 6:
        assert iterated is False
    assert not local_path.exists()


async def test_last_frame_rejects_oversized_png_before_read_and_upload(
    monkeypatch,
    tmp_path,
) -> None:
    from app.workers import video_generation as worker

    puts: list[tuple[str, bytes]] = []

    class Storage:
        async def iter_chunks(self, _key: str, **_kwargs):
            yield b"video"

        async def put(self, key: str, data: bytes) -> str:
            puts.append((key, data))
            return key

        async def delete(self, _key: str) -> None:
            return None

    real_named_temporary_file = worker.tempfile.NamedTemporaryFile

    def named_temporary_file(**kwargs):
        return real_named_temporary_file(dir=tmp_path, **kwargs)

    def fake_run(command, **_kwargs):
        if command[0] == "ffprobe":
            return SimpleNamespace(stdout="1.0")
        Path(command[-1]).write_bytes(b"123456")
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(worker.tempfile, "NamedTemporaryFile", named_temporary_file)
    monkeypatch.setattr(worker.subprocess, "run", fake_run)
    monkeypatch.setattr(worker, "_MAX_LAST_FRAME_BYTES", 5, raising=False)
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: Storage())

    with pytest.raises(RuntimeError, match="frame_extraction_failed"):
        await worker._update_last_frame(
            _FakeDB(SimpleNamespace(reference_frames={})),
            "group:oversized-frame",
            "generate_images/video/source.mp4",
            "89abcdef01234567",
        )

    assert puts == []
    assert not any(tmp_path.iterdir())


async def test_last_frame_keeps_legacy_get_fallback(monkeypatch, tmp_path) -> None:
    from app.workers import video_generation as worker

    gets: list[str] = []
    puts: list[tuple[str, bytes]] = []

    class LegacyStorage:
        async def get(self, key: str) -> bytes:
            gets.append(key)
            return b"legacy-video"

        async def put(self, key: str, data: bytes) -> str:
            puts.append((key, data))
            return key

        async def delete(self, _key: str) -> None:
            return None

    storage = LegacyStorage()
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    _install_fake_ffmpeg(monkeypatch, worker, tmp_path, b"legacy-video")
    shot_group = SimpleNamespace(reference_frames={})

    result = await worker._update_last_frame(
        _FakeDB(shot_group),
        "group:legacy-group",
        "generate_images/video/legacy.mp4",
        "0011223344556677",
    )

    assert gets == ["generate_images/video/legacy.mp4"]
    assert puts == [(result, b"last-frame")]
    assert not any(tmp_path.iterdir())


@pytest.mark.parametrize("failure_stage", ["query", "update"])
@pytest.mark.parametrize("authoritative", [False, True, None])
async def test_last_frame_db_failure_reconciles_uploaded_attempt_object(
    monkeypatch,
    tmp_path,
    failure_stage: str,
    authoritative: bool | None,
) -> None:
    from app.workers import video_generation as worker

    puts: list[str] = []
    deletes: list[str] = []

    class Storage:
        async def iter_chunks(self, _key: str, **_kwargs):
            yield b"video"

        async def put(self, key: str, _data: bytes) -> str:
            puts.append(key)
            return key

        async def delete(self, key: str) -> None:
            deletes.append(key)

    class BrokenShotGroup:
        @property
        def reference_frames(self) -> dict:
            return {}

        @reference_frames.setter
        def reference_frames(self, _value: dict) -> None:
            raise RuntimeError("shot group update failed")

    class DB:
        async def execute(self, _statement):
            if failure_stage == "query":
                raise ConnectionError("shot group query failed")
            return _ScalarResult(BrokenShotGroup())

    storage = Storage()
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    _install_fake_ffmpeg(monkeypatch, worker, tmp_path, b"video")
    authoritative_read = AsyncMock(return_value=authoritative)
    schedule_cleanup = AsyncMock(return_value=True)
    monkeypatch.setattr(
        worker,
        "_authoritative_last_frame_referenced",
        authoritative_read,
        raising=False,
    )
    monkeypatch.setattr(
        worker,
        "_schedule_video_attempt_cleanup",
        schedule_cleanup,
        raising=False,
    )
    attempt_id = "0123456789abcdef"
    frame_key = (
        "/generate_images/video/frames/"
        f"db-failure_{attempt_id}_last.png"
    )

    with pytest.raises(RuntimeError, match="frame_extraction_failed"):
        await worker._update_last_frame(
            DB(),
            "group:db-failure",
            "generate_images/video/source.mp4",
            attempt_id,
        )

    assert puts == [frame_key]
    authoritative_read.assert_awaited_once_with("db-failure", frame_key)
    if authoritative is False:
        assert deletes == [frame_key]
        schedule_cleanup.assert_not_called()
    elif authoritative is True:
        assert deletes == []
        schedule_cleanup.assert_not_called()
    else:
        assert deletes == []
        schedule_cleanup.assert_awaited_once_with(
            job_id=None,
            script_id=None,
            shot_group_id="db-failure",
            storage_attempt_id=attempt_id,
            video_storage_key=None,
            last_frame_storage_key=frame_key,
        )
    assert not any(tmp_path.iterdir())


class _RunDB:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


class _RollbackFailureDB(_RunDB):
    async def rollback(self) -> None:
        self.rollbacks += 1
        raise ConnectionError("rollback response lost")


class _CommitAndRollbackFailureDB(_RollbackFailureDB):
    async def commit(self) -> None:
        self.commits += 1
        if self.commits == 2:
            raise ConnectionError("commit outcome unavailable")


def _video_job(*, provider_task_id: str | None = None):
    return SimpleNamespace(
        job_id="job-attempt",
        episode_id="episode-1",
        input_version=1,
        input_json={"prompt": "镜头", "model": "test-model"},
        output_json=(
            {"provider_task_id": provider_task_id}
            if provider_task_id
            else {}
        ),
        reservation_id=None,
        owner_user_id=None,
        provider_key="fake",
        unit_id="group:attempt-group",
    )


def _install_video_run_fakes(monkeypatch, worker, *, lease_token: str, heartbeat: bool):
    from app.crud import video as video_crud

    job = _video_job(provider_task_id="resume-provider-task")
    finished: list[dict] = []

    async def claim(*_args, **_kwargs):
        return SimpleNamespace(lease_token=lease_token)

    async def heartbeat_job(*_args, **_kwargs):
        return heartbeat

    async def finish(*_args, **kwargs):
        finished.append(kwargs)
        return True

    async def get_job(*_args, **_kwargs):
        return job

    async def current(*_args, **_kwargs):
        return True

    async def first_frame(*_args, **_kwargs):
        return None

    async def fail_release(*_args, **_kwargs):
        return None

    monkeypatch.setattr(video_crud, "claim_video_job", claim)
    monkeypatch.setattr(video_crud, "heartbeat_video_job", heartbeat_job)
    monkeypatch.setattr(video_crud, "finish_video_job", finish)
    monkeypatch.setattr(worker, "_get_video_job", get_job)
    monkeypatch.setattr(worker, "_input_version_is_current", current)
    monkeypatch.setattr(worker, "_resolve_execution_first_frame", first_frame)
    monkeypatch.setattr(worker, "_fail_video_with_release", fail_release)
    monkeypatch.setattr(worker, "_credits_settle", None)
    return job, finished


async def test_video_request_uses_irreversible_storage_attempt_id(monkeypatch) -> None:
    from app.workers import video_generation as worker

    lease_token = "secret-lease-token-must-not-leak"
    expected_attempt_id = hashlib.sha256(lease_token.encode("utf-8")).hexdigest()[:16]
    _, finished = _install_video_run_fakes(
        monkeypatch,
        worker,
        lease_token=lease_token,
        heartbeat=True,
    )
    requests = []

    class Provider:
        async def generate(self, request, _on_progress=None):
            requests.append(request)
            return SimpleNamespace(
                video_storage_key=(
                    "generate_images/video/"
                    f"job-attempt.{request.metadata['storage_attempt_id']}.mp4"
                ),
                provider_job_id="provider-task-new",
                metrics={},
            )

    async def extract_frame(_db, _unit_id, _video_key, token):
        assert token == lease_token
        return f"/generate_images/video/frames/frame_{expected_attempt_id}.png"

    await worker.run_video_generation_once(
        _RunDB(),
        "job-attempt",
        owner="worker-a",
        provider=Provider(),
        extract_last_frame=extract_frame,
    )

    assert len(requests) == 1
    assert requests[0].metadata == {
        "provider_task_id": "resume-provider-task",
        "storage_attempt_id": expected_attempt_id,
    }
    assert lease_token not in repr(requests[0].metadata)
    assert finished
    assert expected_attempt_id in finished[0]["output_json"]["video_storage_key"]


@pytest.mark.parametrize("attempt_scoped", [True, False])
async def test_lost_video_lease_only_deletes_own_attempt_output(
    monkeypatch,
    attempt_scoped: bool,
) -> None:
    from app.workers import video_generation as worker

    lease_token = "lost-lease-token"
    expected_attempt_id = hashlib.sha256(lease_token.encode("utf-8")).hexdigest()[:16]
    _install_video_run_fakes(
        monkeypatch,
        worker,
        lease_token=lease_token,
        heartbeat=False,
    )
    deleted: list[str] = []

    class Provider:
        async def generate(self, request, _on_progress=None):
            attempt_id = request.metadata.get("storage_attempt_id", "missing")
            storage_key = (
                f"generate_images/video/job-attempt.{attempt_id}.mp4"
                if attempt_scoped
                else "generate_images/video/job-attempt.other-worker.mp4"
            )
            return SimpleNamespace(
                video_storage_key=storage_key,
                provider_job_id=None,
                metrics={},
            )

    class Storage:
        async def delete(self, key: str) -> None:
            deleted.append(key)

    await worker.run_video_generation_once(
        _RunDB(),
        "job-attempt",
        owner="worker-a",
        provider=Provider(),
        extract_last_frame=lambda *_args: None,
        storage=Storage(),
    )

    if attempt_scoped:
        assert deleted == [
            f"generate_images/video/job-attempt.{expected_attempt_id}.mp4"
        ]
    else:
        assert deleted == []


def _attempt_outputs_provider(job_id: str):
    class Provider:
        async def generate(self, request, _on_progress=None):
            attempt_id = request.metadata["storage_attempt_id"]
            return SimpleNamespace(
                video_storage_key=(
                    f"generate_images/video/{job_id}.{attempt_id}.mp4"
                ),
                provider_job_id=None,
                metrics={},
            )

    return Provider()


async def test_commit_uncertainty_still_reconciles_after_rollback_failure(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    lease_token = "commit-rollback-failure"
    attempt_id = hashlib.sha256(lease_token.encode("utf-8")).hexdigest()[:16]
    _install_video_run_fakes(
        monkeypatch,
        worker,
        lease_token=lease_token,
        heartbeat=True,
    )
    authoritative_read = AsyncMock(return_value=False)
    failure_cleanup = AsyncMock()
    deleted: list[str] = []

    class Storage:
        async def delete(self, key: str) -> None:
            deleted.append(key)

    async def extract_frame(*_args) -> str:
        return f"generate_images/video/frames/job-attempt.{attempt_id}.png"

    monkeypatch.setattr(
        worker,
        "_authoritative_video_attempt_succeeded",
        authoritative_read,
    )
    monkeypatch.setattr(
        worker,
        "_authoritative_last_frame_referenced",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(worker, "_mark_failed_and_release", failure_cleanup)

    await worker.run_video_generation_once(
        _CommitAndRollbackFailureDB(),
        "job-attempt",
        owner="worker-a",
        provider=_attempt_outputs_provider("job-attempt"),
        extract_last_frame=extract_frame,
        storage=Storage(),
    )

    video_key = f"generate_images/video/job-attempt.{attempt_id}.mp4"
    frame_key = f"generate_images/video/frames/job-attempt.{attempt_id}.png"
    authoritative_read.assert_awaited_once_with("job-attempt", video_key)
    assert deleted == [video_key, frame_key]
    failure_cleanup.assert_awaited_once_with(
        "job-attempt",
        lease_token,
        "provider_or_storage_error",
    )


async def test_finish_false_still_reconciles_after_rollback_failure(monkeypatch) -> None:
    from app.crud import video as video_crud
    from app.workers import video_generation as worker

    lease_token = "finish-false-rollback-failure"
    attempt_id = hashlib.sha256(lease_token.encode("utf-8")).hexdigest()[:16]
    _install_video_run_fakes(
        monkeypatch,
        worker,
        lease_token=lease_token,
        heartbeat=True,
    )
    monkeypatch.setattr(video_crud, "finish_video_job", AsyncMock(return_value=False))
    authoritative_read = AsyncMock(return_value=False)
    monkeypatch.setattr(
        worker,
        "_authoritative_video_attempt_succeeded",
        authoritative_read,
    )
    monkeypatch.setattr(
        worker,
        "_authoritative_last_frame_referenced",
        AsyncMock(return_value=False),
    )
    deleted: list[str] = []

    class Storage:
        async def delete(self, key: str) -> None:
            deleted.append(key)

    async def extract_frame(*_args) -> str:
        return f"generate_images/video/frames/job-attempt.{attempt_id}.png"

    await worker.run_video_generation_once(
        _RollbackFailureDB(),
        "job-attempt",
        owner="worker-a",
        provider=_attempt_outputs_provider("job-attempt"),
        extract_last_frame=extract_frame,
        storage=Storage(),
    )

    video_key = f"generate_images/video/job-attempt.{attempt_id}.mp4"
    frame_key = f"generate_images/video/frames/job-attempt.{attempt_id}.png"
    authoritative_read.assert_awaited_once_with("job-attempt", video_key)
    assert deleted == [video_key, frame_key]


@pytest.mark.parametrize("authoritative", [False, True, None])
async def test_cancellation_reconciles_outputs_after_rollback_failure(
    monkeypatch,
    authoritative: bool | None,
) -> None:
    from app.crud import video as video_crud
    from app.providers.video_gen import (
        VideoCancellationOutcome,
        VideoCancellationResult,
        VideoGenerationCancelled,
    )
    from app.workers import video_generation as worker

    lease_token = "cancel-rollback-failure"
    attempt_id = hashlib.sha256(lease_token.encode("utf-8")).hexdigest()[:16]
    _install_video_run_fakes(
        monkeypatch,
        worker,
        lease_token=lease_token,
        heartbeat=True,
    )
    cancellation = VideoGenerationCancelled(
        VideoCancellationResult(
            provider_task_id="provider-canceled",
            outcome=VideoCancellationOutcome.CANCELED,
        )
    )
    monkeypatch.setattr(
        video_crud,
        "finish_video_job",
        AsyncMock(side_effect=cancellation),
    )
    authoritative_read = AsyncMock(return_value=authoritative)
    authoritative_frame = AsyncMock(return_value=False)
    schedule_cleanup = AsyncMock(return_value=True)
    persist_cancellation = AsyncMock(return_value=True)
    monkeypatch.setattr(
        worker,
        "_authoritative_video_attempt_succeeded",
        authoritative_read,
    )
    monkeypatch.setattr(
        worker,
        "_authoritative_last_frame_referenced",
        authoritative_frame,
    )
    monkeypatch.setattr(
        worker,
        "_schedule_video_attempt_cleanup",
        schedule_cleanup,
        raising=False,
    )
    monkeypatch.setattr(
        worker,
        "_persist_video_cancellation_result_independently",
        persist_cancellation,
    )
    deleted: list[str] = []

    class Storage:
        async def delete(self, key: str) -> None:
            deleted.append(key)

    async def extract_frame(*_args) -> str:
        return f"generate_images/video/frames/job-attempt.{attempt_id}.png"

    await worker.run_video_generation_once(
        _RollbackFailureDB(),
        "job-attempt",
        owner="worker-a",
        provider=_attempt_outputs_provider("job-attempt"),
        extract_last_frame=extract_frame,
        storage=Storage(),
    )

    video_key = f"generate_images/video/job-attempt.{attempt_id}.mp4"
    frame_key = f"generate_images/video/frames/job-attempt.{attempt_id}.png"
    authoritative_read.assert_awaited_once_with("job-attempt", video_key)
    if authoritative is False:
        assert deleted == [video_key, frame_key]
        authoritative_frame.assert_awaited_once_with("attempt-group", frame_key)
        schedule_cleanup.assert_not_called()
    elif authoritative is True:
        assert deleted == []
        schedule_cleanup.assert_not_called()
    else:
        assert deleted == []
        schedule_cleanup.assert_awaited_once_with(
            job_id="job-attempt",
            script_id=None,
            shot_group_id="attempt-group",
            storage_attempt_id=attempt_id,
            video_storage_key=video_key,
            last_frame_storage_key=frame_key,
            provider_task_id="provider-canceled",
        )
    persist_cancellation.assert_awaited_once()


async def test_deferred_attempt_reconciliation_only_deletes_non_authoritative_keys(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    attempt_id = "0123456789abcdef"
    video_key = f"generate_images/video/job.{attempt_id}.mp4"
    frame_key = f"generate_images/video/frames/job.{attempt_id}.png"
    authoritative_video = AsyncMock(return_value=False)
    authoritative_frame = AsyncMock(return_value=False)
    monkeypatch.setattr(
        worker,
        "_authoritative_video_attempt_succeeded",
        authoritative_video,
    )
    monkeypatch.setattr(
        worker,
        "_authoritative_last_frame_referenced",
        authoritative_frame,
        raising=False,
    )
    deleted: list[str] = []

    class Storage:
        async def delete(self, key: str) -> None:
            deleted.append(key)

    result = await worker._reconcile_video_attempt_outputs(
        job_id="job-attempt",
        shot_group_id="attempt-group",
        storage_attempt_id=attempt_id,
        video_storage_key=video_key,
        last_frame_storage_key=frame_key,
        storage=Storage(),
    )

    assert result is False
    assert deleted == [video_key, frame_key]
    authoritative_video.assert_awaited_once_with("job-attempt", video_key)
    authoritative_frame.assert_awaited_once_with("attempt-group", frame_key)


async def test_missing_video_job_preserves_authoritative_last_frame(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    attempt_id = "0123456789abcdef"
    video_key = f"generate_images/video/job.{attempt_id}.mp4"
    frame_key = f"generate_images/video/frames/job.{attempt_id}.png"
    monkeypatch.setattr(
        worker,
        "_authoritative_video_attempt_succeeded",
        AsyncMock(return_value=False),
    )
    authoritative_frame = AsyncMock(return_value=True)
    monkeypatch.setattr(
        worker,
        "_authoritative_last_frame_referenced",
        authoritative_frame,
    )
    deleted: list[str] = []

    class Storage:
        async def delete(self, key: str) -> None:
            deleted.append(key)

    result = await worker._reconcile_video_attempt_outputs(
        job_id="deleted-job",
        shot_group_id="attempt-group",
        storage_attempt_id=attempt_id,
        video_storage_key=video_key,
        last_frame_storage_key=frame_key,
        storage=Storage(),
    )

    assert result is False
    assert deleted == [video_key]
    authoritative_frame.assert_awaited_once_with("attempt-group", frame_key)


async def test_unknown_attempt_authority_publishes_durable_reconciliation(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    publish = Mock()
    monkeypatch.setattr(
        worker.reconcile_video_attempt_outputs_task,
        "apply_async",
        publish,
    )

    persist = AsyncMock(return_value=True)
    monkeypatch.setattr(worker, "_persist_video_attempt_cleanup_debt", persist)

    assert await worker._schedule_video_attempt_cleanup(
        job_id="job-attempt",
        script_id="script-attempt",
        shot_group_id="attempt-group",
        storage_attempt_id="0123456789abcdef",
        video_storage_key="generate_images/video/job.0123456789abcdef.mp4",
        last_frame_storage_key=(
            "generate_images/video/frames/job.0123456789abcdef.png"
        ),
    ) is True

    persist.assert_awaited_once_with(
        job_id="job-attempt",
        script_id="script-attempt",
        shot_group_id="attempt-group",
        storage_attempt_id="0123456789abcdef",
        video_storage_key="generate_images/video/job.0123456789abcdef.mp4",
        last_frame_storage_key=(
            "generate_images/video/frames/job.0123456789abcdef.png"
        ),
    )
    publish.assert_called_once_with(
        kwargs={
            "job_id": "job-attempt",
            "shot_group_id": "attempt-group",
            "storage_attempt_id": "0123456789abcdef",
            "video_storage_key": (
                "generate_images/video/job.0123456789abcdef.mp4"
            ),
            "last_frame_storage_key": (
                "generate_images/video/frames/job.0123456789abcdef.png"
            ),
        },
        countdown=worker._ATTEMPT_CLEANUP_RETRY_SECONDS,
    )
    assert worker.reconcile_video_attempt_outputs_task.max_retries is None
    assert worker.reconcile_video_attempt_outputs_task.acks_late is True
    assert worker.reconcile_video_attempt_outputs_task.app.conf.task_default_delivery_mode == 2


async def test_attempt_reconciliation_publish_failure_keeps_database_debt(
    monkeypatch,
) -> None:
    from app.workers import video_generation as worker

    publish = Mock(side_effect=ConnectionError("redis unavailable"))
    persist = AsyncMock(return_value=True)
    monkeypatch.setattr(
        worker.reconcile_video_attempt_outputs_task,
        "apply_async",
        publish,
    )
    monkeypatch.setattr(worker, "_persist_video_attempt_cleanup_debt", persist)

    assert await worker._schedule_video_attempt_cleanup(
        job_id="job-attempt",
        script_id="script-attempt",
        shot_group_id="attempt-group",
        storage_attempt_id="0123456789abcdef",
        video_storage_key="generate_images/video/job.0123456789abcdef.mp4",
        last_frame_storage_key=(
            "generate_images/video/frames/job.0123456789abcdef.png"
        ),
    ) is True

    publish.assert_called_once()
    persist.assert_awaited_once_with(
        job_id="job-attempt",
        script_id="script-attempt",
        shot_group_id="attempt-group",
        storage_attempt_id="0123456789abcdef",
        video_storage_key="generate_images/video/job.0123456789abcdef.mp4",
        last_frame_storage_key=(
            "generate_images/video/frames/job.0123456789abcdef.png"
        ),
    )


async def test_last_frame_unknown_authority_is_republished_with_job_debt_context(
    monkeypatch,
    tmp_path,
) -> None:
    from app.workers import video_generation as worker

    lease_token = "frame-unknown-authority"
    attempt_id = hashlib.sha256(lease_token.encode("utf-8")).hexdigest()[:16]
    _install_video_run_fakes(
        monkeypatch,
        worker,
        lease_token=lease_token,
        heartbeat=True,
    )

    class Storage:
        async def iter_chunks(self, _key: str, **_kwargs):
            yield b"video"

        async def put(self, key: str, _data: bytes) -> str:
            return key

        async def delete(self, _key: str) -> None:
            return None

    storage = Storage()
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    _install_fake_ffmpeg(monkeypatch, worker, tmp_path, b"video")
    monkeypatch.setattr(
        worker,
        "_authoritative_last_frame_referenced",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        worker,
        "_authoritative_video_attempt_succeeded",
        AsyncMock(return_value=None),
    )
    schedule_cleanup = AsyncMock(return_value=True)
    monkeypatch.setattr(worker, "_schedule_video_attempt_cleanup", schedule_cleanup)

    await worker.run_video_generation_once(
        _RollbackFailureDB(),
        "job-attempt",
        owner="worker-a",
        provider=_attempt_outputs_provider("job-attempt"),
        storage=storage,
    )

    frame_key = (
        "/generate_images/video/frames/"
        f"attempt-group_{attempt_id}_last.png"
    )
    # 内层末帧、外层视频与外层末帧分别判权，避免任一对象借用另一对象的结论。
    assert schedule_cleanup.await_count == 3
    assert schedule_cleanup.await_args_list[-1].kwargs == {
        "job_id": "job-attempt",
        "script_id": None,
        "shot_group_id": "attempt-group",
        "storage_attempt_id": attempt_id,
        "video_storage_key": None,
        "last_frame_storage_key": frame_key,
        "provider_task_id": "resume-provider-task",
    }


async def test_outer_frame_failure_never_deletes_authoritative_frame(
    monkeypatch,
    tmp_path,
) -> None:
    from app.workers import video_generation as worker

    lease_token = "authoritative-frame"
    attempt_id = hashlib.sha256(lease_token.encode("utf-8")).hexdigest()[:16]
    _install_video_run_fakes(
        monkeypatch,
        worker,
        lease_token=lease_token,
        heartbeat=True,
    )
    deleted: list[str] = []

    class Storage:
        async def iter_chunks(self, _key: str, **_kwargs):
            yield b"video"

        async def put(self, key: str, _data: bytes) -> str:
            return key

        async def delete(self, key: str) -> None:
            deleted.append(key)

    storage = Storage()
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    _install_fake_ffmpeg(monkeypatch, worker, tmp_path, b"video")
    frame_authority = AsyncMock(return_value=True)
    monkeypatch.setattr(
        worker,
        "_authoritative_last_frame_referenced",
        frame_authority,
    )
    schedule_cleanup = AsyncMock(return_value=True)
    monkeypatch.setattr(worker, "_schedule_video_attempt_cleanup", schedule_cleanup)

    await worker.run_video_generation_once(
        _RunDB(),
        "job-attempt",
        owner="worker-a",
        provider=_attempt_outputs_provider("job-attempt"),
        storage=storage,
    )

    video_key = f"generate_images/video/job-attempt.{attempt_id}.mp4"
    frame_key = (
        "/generate_images/video/frames/"
        f"attempt-group_{attempt_id}_last.png"
    )
    assert deleted == [video_key]
    frame_authority.assert_awaited_once_with("attempt-group", frame_key)
    schedule_cleanup.assert_not_awaited()
