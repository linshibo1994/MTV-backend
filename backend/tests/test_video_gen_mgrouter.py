"""MGRouter AICC Seedance 视频供应商协议测试。"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.core.config import Settings
from app.providers.registry import ProviderConfigurationError
from app.providers.video_gen import (
    VideoCancellationOutcome,
    VideoGenerationRemoteTerminalError,
    VideoGenerationRequest,
    VideoGenerationSubmissionUncertain,
    VideoGenerationSubmitRejected,
)
from app.providers.video_gen_mgrouter import MgrouterVideoGenProvider


def _settings(**kwargs) -> Settings:
    values = {
        "video_gen_provider": "mgrouter",
        "mgrouter_api_key": "test-mgrouter-key",
        "mgrouter_resolution": "480p",
        "mgrouter_ratio": "16:9",
        "mgrouter_generate_audio": False,
        "mgrouter_watermark": False,
    }
    values.update(kwargs)
    return Settings(_env_file=None, **values)  # type: ignore[call-arg]


def _request(
    *,
    duration_seconds: int | None = 4,
    first_frame: str | None = None,
    reference_images: list[dict] | None = None,
    reference_audios: list[dict] | None = None,
    metadata: dict | None = None,
    on_task_created=None,
    before_submit=None,
    on_submission_uncertain=None,
    cancel_requested=None,
) -> VideoGenerationRequest:
    return VideoGenerationRequest(
        prompt="雨后的竹林，镜头缓慢向前移动",
        reference_images=reference_images or [],
        reference_image_names=[],
        reference_audios=reference_audios or [],
        reference_audio_names=[],
        model="AICC-Doubao-Seedance-2.0",
        job_id="job-mgrouter",
        first_frame=first_frame,
        duration_seconds=duration_seconds,
        metadata=metadata or {},
        on_task_created=on_task_created,
        before_submit=before_submit,
        on_submission_uncertain=on_submission_uncertain,
        cancel_requested=cancel_requested,
    )


class _Response:
    def __init__(
        self,
        status_code: int,
        payload: object,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        if isinstance(self._payload, BaseException):
            raise self._payload
        return self._payload


class _HttpClient:
    def __init__(
        self,
        *,
        post_responses: list[_Response] | None = None,
        get_responses: list[_Response] | None = None,
        delete_responses: list[_Response] | None = None,
        events: list[str] | None = None,
    ) -> None:
        self.post_responses = list(post_responses or [])
        self.get_responses = list(get_responses or [])
        self.delete_responses = list(delete_responses or [])
        self.events = events
        self.posts: list[dict] = []
        self.gets: list[dict] = []
        self.deletes: list[dict] = []
        self.closed = False

    async def post(self, url, *, headers, json):
        if self.events is not None:
            self.events.append("post")
        self.posts.append({"url": url, "headers": dict(headers), "json": json})
        if not self.post_responses:
            raise AssertionError("测试未配置 POST 响应")
        return self.post_responses.pop(0)

    async def get(self, url, *, headers):
        self.gets.append({"url": url, "headers": dict(headers)})
        if not self.get_responses:
            raise AssertionError("测试未配置 GET 响应")
        return self.get_responses.pop(0)

    async def delete(self, url, *, headers):
        self.deletes.append({"url": url, "headers": dict(headers)})
        if not self.delete_responses:
            raise AssertionError("测试未配置 DELETE 响应")
        return self.delete_responses.pop(0)

    async def aclose(self) -> None:
        self.closed = True


class _AtomicStorage:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.deleted: list[str] = []
        self.upload_paths: list[str] = []

    async def put(self, _key: str, _data: bytes) -> str:
        raise AssertionError("视频输出禁止使用 storage.put(bytes)")

    async def put_file(self, key: str, path: str) -> str:
        self.upload_paths.append(path)
        self.objects[key] = Path(path).read_bytes()
        return key

    async def promote(self, source_key: str, final_key: str) -> str:
        self.objects[final_key] = self.objects.pop(source_key)
        return final_key

    async def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.objects.pop(key, None)

    async def exists(self, key: str) -> bool:
        return key in self.objects


def _patch_settings(monkeypatch, configured: Settings | None = None) -> Settings:
    from app.providers import video_gen_mgrouter

    configured = configured or _settings(mgrouter_base_url="https://mgrouter.example")
    monkeypatch.setattr(video_gen_mgrouter, "settings", configured)
    return configured


@pytest.mark.asyncio
async def test_mgrouter_config_urls_and_minimum_cost_body(monkeypatch) -> None:
    from app.providers import video_gen_mgrouter

    configured = _settings()
    monkeypatch.setattr(video_gen_mgrouter, "settings", configured)
    provider = MgrouterVideoGenProvider()

    body, sensitive_values = await provider._build_request_body(
        _request(duration_seconds=4),
        "AICC-Doubao-Seedance-2.0",
    )

    assert provider._api_key() == "test-mgrouter-key"
    assert provider._tasks_url() == "https://mgrouter.com/api/v3/contents/generations/tasks"
    assert provider._task_url("task/with space") == (
        "https://mgrouter.com/api/v3/contents/generations/tasks/task%2Fwith%20space"
    )
    assert provider._download_url("task/with space") == (
        "https://mgrouter.com/v1/videos/task%2Fwith%20space/content"
    )
    assert body == {
        "model": "AICC-Doubao-Seedance-2.0",
        "content": [{"type": "text", "text": "雨后的竹林，镜头缓慢向前移动"}],
        "duration": 4,
        "resolution": "480p",
        "ratio": "16:9",
        "generate_audio": False,
        "watermark": False,
    }
    assert sensitive_values == ("雨后的竹林，镜头缓慢向前移动",)


def test_mgrouter_duration_maps_three_seconds_to_four_and_rejects_other_invalid_values() -> None:
    assert MgrouterVideoGenProvider._duration(3) == 4
    assert MgrouterVideoGenProvider._duration(4) == 4
    assert MgrouterVideoGenProvider._duration(15) == 15

    for invalid in (0, 2, 16):
        with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
            MgrouterVideoGenProvider._duration(invalid)
        assert exc_info.value.diagnostics == {"code": "DurationOutOfRange"}


@pytest.mark.asyncio
async def test_mgrouter_first_frame_rejects_other_reference_images(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    provider = MgrouterVideoGenProvider()
    provider._encode_storage_image = AsyncMock(return_value="data:image/png;base64,Zmlyc3Q=")

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await provider._build_request_body(
            _request(
                first_frame="frames/first.png",
                reference_images=[
                    {"storage_key": "frames/first.png", "mime_type": "image/png"},
                    {"storage_key": "references/other.webp", "mime_type": "image/webp"},
                ],
            ),
            "AICC-Doubao-Seedance-2.0",
        )

    assert exc_info.value.diagnostics == {"code": "reference_images_limit_exceeded"}
    provider._encode_storage_image.assert_not_awaited()


@pytest.mark.asyncio
async def test_mgrouter_first_frame_rejects_reference_audio(monkeypatch) -> None:
    _patch_settings(monkeypatch)

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await MgrouterVideoGenProvider()._build_request_body(
            _request(
                first_frame="frames/first.png",
                reference_audios=[{"storage_key": "references/voice.mp3"}],
            ),
            "AICC-Doubao-Seedance-2.0",
        )

    assert exc_info.value.diagnostics == {"code": "reference_audios_limit_exceeded"}


@pytest.mark.asyncio
async def test_mgrouter_reference_audio_requires_normal_reference_image(monkeypatch) -> None:
    _patch_settings(monkeypatch)

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await MgrouterVideoGenProvider()._build_request_body(
            _request(reference_audios=[{"storage_key": "references/voice.mp3"}]),
            "AICC-Doubao-Seedance-2.0",
        )

    assert exc_info.value.diagnostics == {
        "code": "reference_audio_requires_reference_image"
    }


@pytest.mark.asyncio
async def test_mgrouter_reference_image_prefers_external_read_url(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    storage = SimpleNamespace(
        external_read_url=AsyncMock(
            return_value="https://storage.example/images/character.png?signature=test"
        )
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)

    body, sensitive_values = await MgrouterVideoGenProvider()._build_request_body(
        _request(
            reference_images=[
                {"storage_key": "references/character.png", "mime_type": "image/png"}
            ]
        ),
        "AICC-Doubao-Seedance-2.0",
    )

    image_url = "https://storage.example/images/character.png?signature=test"
    assert body["content"][1] == {
        "type": "image_url",
        "image_url": {"url": image_url},
        "role": "reference_image",
    }
    assert image_url in sensitive_values
    storage.external_read_url.assert_awaited_once_with(
        "references/character.png",
        expires_seconds=900,
    )


@pytest.mark.asyncio
async def test_mgrouter_reference_image_falls_back_to_data_url_without_external_url(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    storage = SimpleNamespace(
        external_read_url=AsyncMock(return_value=None),
        get_size=AsyncMock(return_value=3),
        get=AsyncMock(return_value=b"png"),
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)

    image_url = await MgrouterVideoGenProvider()._encode_storage_image(
        "references/character.png",
        "image/png",
    )

    assert image_url == "data:image/png;base64,cG5n"
    storage.external_read_url.assert_awaited_once_with(
        "references/character.png",
        expires_seconds=900,
    )
    storage.get_size.assert_awaited_once_with("references/character.png")
    storage.get.assert_awaited_once_with("references/character.png")


@pytest.mark.asyncio
async def test_mgrouter_reference_audio_uses_external_read_url(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    storage = SimpleNamespace(
        external_read_url=AsyncMock(
            return_value="https://storage.example/audio/voice.wav?signature=test"
        )
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    provider = MgrouterVideoGenProvider()
    provider._encode_storage_image = AsyncMock(return_value="data:image/png;base64,aW1hZ2U=")

    body, sensitive_values = await provider._build_request_body(
        _request(
            reference_images=[
                {"storage_key": "references/character.png", "mime_type": "image/png"}
            ],
            reference_audios=[
                {"storage_key": "references/voice.wav", "mime_type": "audio/wav"}
            ],
        ),
        "AICC-Doubao-Seedance-2.0",
    )

    storage.external_read_url.assert_awaited_once_with(
        "references/voice.wav",
        expires_seconds=900,
    )
    assert body["content"][-1] == {
        "type": "audio_url",
        "audio_url": {
            "url": "https://storage.example/audio/voice.wav?signature=test"
        },
        "role": "reference_audio",
    }
    assert "https://storage.example/audio/voice.wav?signature=test" in sensitive_values


@pytest.mark.asyncio
async def test_mgrouter_reference_audio_rejects_storage_without_external_url(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    storage = SimpleNamespace(external_read_url=AsyncMock(return_value=None))
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    provider = MgrouterVideoGenProvider()
    provider._encode_storage_image = AsyncMock(return_value="data:image/png;base64,aW1hZ2U=")

    with pytest.raises(ProviderConfigurationError, match="无法.*外部 HTTPS URL"):
        await provider._build_request_body(
            _request(
                reference_images=[
                    {"storage_key": "references/character.png", "mime_type": "image/png"}
                ],
                reference_audios=[{"storage_key": "references/voice.wav"}],
            ),
            "AICC-Doubao-Seedance-2.0",
        )


@pytest.mark.asyncio
async def test_mgrouter_submit_4xx_is_explicit_rejection_with_safe_diagnostics(
    monkeypatch,
) -> None:
    _patch_settings(monkeypatch)
    private_message = "不得泄露的提示词和签名 URL"
    client = _HttpClient(
        post_responses=[
            _Response(
                400,
                {
                    "code": "InvalidParameter",
                    "param": "duration",
                    "request_id": "request-safe-1",
                    "message": private_message,
                },
            )
        ]
    )
    provider = MgrouterVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)
    before_submit = AsyncMock()
    on_uncertain = AsyncMock()

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await provider.generate(
            _request(
                before_submit=before_submit,
                on_submission_uncertain=on_uncertain,
            )
        )

    assert exc_info.value.diagnostics == {
        "status_code": 400,
        "code": "InvalidParameter",
        "param": "duration",
        "request_id": "request-safe-1",
    }
    assert private_message not in repr(exc_info.value.diagnostics)
    before_submit.assert_awaited_once()
    on_uncertain.assert_not_awaited()
    assert client.closed is True


@pytest.mark.asyncio
async def test_mgrouter_submit_4xx_keeps_nested_safe_diagnostics_and_header_request_id(
    monkeypatch,
) -> None:
    _patch_settings(monkeypatch)
    client = _HttpClient(
        post_responses=[
            _Response(
                400,
                {
                    "error": {
                        "code": "RequestTooLarge",
                        "param": "content",
                        "message": "不得持久化的供应商错误正文",
                    },
                    "detail": {"type": "invalid_request"},
                },
                headers={
                    "content-type": "application/problem+json; charset=utf-8",
                    "x-request-id": "request-header-1",
                },
            )
        ]
    )
    provider = MgrouterVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await provider.generate(_request())

    assert exc_info.value.diagnostics == {
        "status_code": 400,
        "content_type": "application/problem+json",
        "code": "RequestTooLarge",
        "param": "content",
        "type": "invalid_request",
        "request_id": "request-header-1",
    }
    assert "不得持久化的供应商错误正文" not in repr(exc_info.value.diagnostics)
    assert client.closed is True


@pytest.mark.asyncio
async def test_mgrouter_submit_4xx_keeps_safe_non_json_metadata(
    monkeypatch,
) -> None:
    _patch_settings(monkeypatch)
    client = _HttpClient(
        post_responses=[
            _Response(
                400,
                ValueError("not json"),
                headers={
                    "content-type": "text/html; charset=utf-8",
                    "request-id": "request-header-2",
                },
            )
        ]
    )
    provider = MgrouterVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await provider.generate(_request())

    assert exc_info.value.diagnostics == {
        "status_code": 400,
        "content_type": "text/html",
        "request_id": "request-header-2",
    }
    assert client.closed is True


@pytest.mark.asyncio
async def test_mgrouter_submit_4xx_redacts_reflected_sensitive_value(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    client = _HttpClient(
        post_responses=[
            _Response(
                400,
                {"error": {"code": "test-mgrouter-key", "param": "content"}},
                headers={"content-type": "application/json"},
            )
        ]
    )
    provider = MgrouterVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await provider.generate(_request())

    assert exc_info.value.diagnostics == {
        "status_code": 400,
        "content_type": "application/json",
        "param": "content",
    }
    assert "test-mgrouter-key" not in repr(exc_info.value.diagnostics)
    assert client.closed is True


@pytest.mark.parametrize(
    ("response", "case_id"),
    [
        pytest.param(_Response(500, {"message": "upstream failed"}), "http-500", id="http-500"),
        pytest.param(_Response(200, {}), "missing-id", id="missing-id"),
    ],
)
@pytest.mark.asyncio
async def test_mgrouter_submit_unresolved_response_is_submission_uncertain(
    monkeypatch,
    response: _Response,
    case_id: str,
) -> None:
    _patch_settings(monkeypatch)
    client = _HttpClient(post_responses=[response])
    provider = MgrouterVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)
    before_submit = AsyncMock()
    on_uncertain = AsyncMock()

    with pytest.raises(VideoGenerationSubmissionUncertain):
        await provider.generate(
            _request(
                before_submit=before_submit,
                on_submission_uncertain=on_uncertain,
            )
        )

    assert case_id
    before_submit.assert_awaited_once()
    on_uncertain.assert_awaited_once()
    assert client.closed is True


class _StopAfterPoll(RuntimeError):
    pass


@pytest.mark.asyncio
async def test_mgrouter_checkpoints_before_post_submit_cancellation_and_poll(
    monkeypatch,
) -> None:
    from app.providers import video_gen_mgrouter

    _patch_settings(monkeypatch)
    events: list[str] = []
    client = _HttpClient(
        post_responses=[_Response(200, {"id": "task-new"})],
        events=events,
    )
    provider = MgrouterVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)
    monkeypatch.setattr(
        video_gen_mgrouter,
        "raise_if_video_cancel_requested_before_submit",
        AsyncMock(),
    )

    async def before_submit() -> None:
        events.append("submit_intent")

    async def checkpoint(task_id: str) -> None:
        assert task_id == "task-new"
        events.append("checkpoint")

    async def cancel_requested() -> bool:
        events.append("cancel_probe")
        return False

    async def poll(*_args, **_kwargs):
        events.append("poll")
        raise _StopAfterPoll

    monkeypatch.setattr(provider, "_poll", poll)

    with pytest.raises(_StopAfterPoll):
        await provider.generate(
            _request(
                on_task_created=checkpoint,
                before_submit=before_submit,
                cancel_requested=cancel_requested,
            )
        )

    assert events == ["submit_intent", "post", "checkpoint", "cancel_probe", "poll"]
    assert client.closed is True


@pytest.mark.asyncio
async def test_mgrouter_resumes_existing_task_without_rebuilding_or_posting(
    monkeypatch,
) -> None:
    _patch_settings(monkeypatch)
    events: list[str] = []
    client = _HttpClient(events=events)
    provider = MgrouterVideoGenProvider()

    def build_client(**_kwargs):
        events.append("client")
        return client

    monkeypatch.setattr(provider, "_build_client", build_client)
    build_body = AsyncMock(side_effect=AssertionError("恢复任务不得重新读取素材"))
    monkeypatch.setattr(provider, "_build_request_body", build_body)

    async def checkpoint(task_id: str) -> None:
        assert task_id == "task-existing"
        events.append("checkpoint")

    async def cancel_requested() -> bool:
        events.append("cancel_probe")
        return False

    async def poll(*_args, **_kwargs):
        events.append("poll")
        raise _StopAfterPoll

    monkeypatch.setattr(provider, "_poll", poll)

    with pytest.raises(_StopAfterPoll):
        await provider.generate(
            _request(
                metadata={"provider_task_id": "task-existing"},
                on_task_created=checkpoint,
                cancel_requested=cancel_requested,
            )
        )

    assert events == ["checkpoint", "client", "cancel_probe", "poll"]
    assert client.posts == []
    build_body.assert_not_awaited()
    assert client.closed is True


@pytest.mark.asyncio
async def test_mgrouter_poll_transitions_from_queued_to_succeeded(monkeypatch) -> None:
    from app.providers import video_gen_mgrouter

    _patch_settings(monkeypatch)
    client = _HttpClient(
        get_responses=[
            _Response(200, {"id": "task-poll", "status": "queued"}),
            _Response(
                200,
                {
                    "id": "task-poll",
                    "status": "succeeded",
                    "content": {"video_url": "https://upstream.example/video.mp4"},
                    "usage": {"completion_tokens": 12, "total_tokens": 12},
                },
            ),
        ]
    )
    sleep = AsyncMock()
    monkeypatch.setattr(video_gen_mgrouter.asyncio, "sleep", sleep)
    notifications: list[tuple] = []

    result = await MgrouterVideoGenProvider()._poll(
        client,
        "task-poll",
        lambda *args: notifications.append(args),
    )

    assert result["status"] == "succeeded"
    assert len(client.gets) == 2
    assert {call["url"] for call in client.gets} == {
        "https://mgrouter.example/api/v3/contents/generations/tasks/task-poll"
    }
    sleep.assert_awaited_once()
    assert notifications[0] == ("polling", "视频生成中，轮询任务状态", 20, "task-poll")


@pytest.mark.parametrize(
    ("status", "expected_terminal"),
    [
        ("failed", "failed"),
        ("cancelled", "canceled"),
    ],
)
@pytest.mark.asyncio
async def test_mgrouter_poll_maps_confirmed_remote_terminal_statuses(
    monkeypatch,
    status: str,
    expected_terminal: str,
) -> None:
    _patch_settings(monkeypatch)
    client = _HttpClient(
        get_responses=[_Response(200, {"id": "task-terminal", "status": status})]
    )

    with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
        await MgrouterVideoGenProvider()._poll(
            client,
            "task-terminal",
            lambda *_args: None,
        )

    assert exc_info.value.provider_task_id == "task-terminal"
    assert exc_info.value.terminal_status == expected_terminal


@pytest.mark.asyncio
async def test_mgrouter_poll_unknown_status_is_not_confirmed_terminal(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    client = _HttpClient(
        get_responses=[_Response(200, {"id": "task-unknown", "status": "paused"})]
    )

    with pytest.raises(RuntimeError, match="mgrouter_video_task_status_unknown"):
        await MgrouterVideoGenProvider()._poll(
            client,
            "task-unknown",
            lambda *_args: None,
        )


@pytest.mark.asyncio
async def test_mgrouter_download_uses_authenticated_fixed_proxy_and_safe_origin(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from app.providers import _url_safety

    _patch_settings(monkeypatch)
    captured: dict = {}
    cancellation_probe = AsyncMock()

    async def download(url, path, **kwargs) -> int:
        captured.update({"url": url, "path": path, **kwargs})
        kwargs["url_validator"](url)
        await kwargs["cancellation_probe"]()
        Path(path).write_bytes(b"video")
        return 5

    monkeypatch.setattr(_url_safety, "safe_download_to_file", download)
    destination = tmp_path / "video.mp4"

    written = await MgrouterVideoGenProvider()._download_to_file(
        "task/with space",
        destination,
        cancellation_probe=cancellation_probe,
    )

    assert written == 5
    assert captured["url"] == (
        "https://mgrouter.example/v1/videos/task%2Fwith%20space/content"
    )
    assert captured["extra_headers"] == {
        "Authorization": "Bearer test-mgrouter-key"
    }
    assert captured["max_bytes"] == 512 * 1024 * 1024
    cancellation_probe.assert_awaited_once()
    assert destination.read_bytes() == b"video"

    provider = MgrouterVideoGenProvider()
    provider._validate_download_origin(
        "https://mgrouter.example/v1/videos/task/content"
    )
    for unsafe_url in (
        "http://mgrouter.example/v1/videos/task/content",
        "https://attacker.example/v1/videos/task/content",
        "https://user:password@mgrouter.example/v1/videos/task/content",
        "https://mgrouter.example:8443/v1/videos/task/content",
    ):
        with pytest.raises(RuntimeError, match="provider_download_url_invalid"):
            provider._validate_download_origin(unsafe_url)


@pytest.mark.parametrize(
    ("status_code", "expected_outcome"),
    [
        (200, VideoCancellationOutcome.CANCELED),
        (204, VideoCancellationOutcome.CANCELED),
        (404, VideoCancellationOutcome.CANCELED),
        (409, VideoCancellationOutcome.FAILED),
        (500, VideoCancellationOutcome.FAILED),
    ],
)
@pytest.mark.asyncio
async def test_mgrouter_cancel_maps_http_statuses(
    monkeypatch,
    status_code: int,
    expected_outcome: VideoCancellationOutcome,
) -> None:
    _patch_settings(monkeypatch)
    client = _HttpClient(delete_responses=[_Response(status_code, {})])
    provider = MgrouterVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)

    result = await provider.cancel("task/cancel")

    assert result.provider_task_id == "task/cancel"
    assert result.outcome is expected_outcome
    assert client.deletes == [
        {
            "url": (
                "https://mgrouter.example/api/v3/contents/generations/tasks/task%2Fcancel"
            ),
            "headers": {
                "Authorization": "Bearer test-mgrouter-key",
                "Content-Type": "application/json",
            },
        }
    ]
    assert client.closed is True


@pytest.mark.asyncio
async def test_mgrouter_generate_resumes_downloads_and_atomically_stores_result(
    monkeypatch,
) -> None:
    _patch_settings(monkeypatch)
    client = _HttpClient(
        get_responses=[
            _Response(
                200,
                {
                    "id": "task-atomic",
                    "status": "succeeded",
                    "content": {"video_url": "https://upstream.example/video.mp4"},
                    "usage": {"completion_tokens": 9, "total_tokens": 10},
                },
            )
        ]
    )
    storage = _AtomicStorage()
    provider = MgrouterVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    downloaded_paths: list[str] = []

    async def download(_task_id: str, path: str | Path, **_kwargs) -> int:
        downloaded_paths.append(str(path))
        Path(path).write_bytes(b"video-bytes")
        return len(b"video-bytes")

    monkeypatch.setattr(provider, "_download_to_file", download)
    checkpoint = AsyncMock()

    result = await provider.generate(
        _request(
            metadata={
                "provider_task_id": "task-atomic",
                "storage_attempt_id": "0123456789abcdef",
            },
            on_task_created=checkpoint,
        )
    )

    assert result.video_storage_key == (
        "generate_images/video/job-mgrouter-0123456789abcdef.mp4"
    )
    assert result.provider_job_id == "task-atomic"
    assert result.metrics == {
        "provider_key": "mgrouter",
        "task_id": "task-atomic",
        "mgrouter_status": "succeeded",
        "usage": {"completion_tokens": 9, "total_tokens": 10},
    }
    assert storage.objects == {result.video_storage_key: b"video-bytes"}
    assert not any(".upload-" in key for key in storage.objects)
    assert downloaded_paths and not Path(downloaded_paths[0]).exists()
    checkpoint.assert_awaited_once_with("task-atomic")
    assert client.posts == []
    assert client.closed is True
