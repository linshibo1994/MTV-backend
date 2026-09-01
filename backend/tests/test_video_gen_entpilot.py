"""EntPilot Seedance 2.0/2.5 视频供应商协议测试。"""

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
from app.providers.video_gen_entpilot import (
    EntPilotVideoGenProvider,
    EntpilotVideoGenProvider,
)


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "video_gen_provider": "entpilot",
        "entpilot_api_key": "test-entpilot-key",
        "entpilot_base_url": "https://ka.ent-pilot.com",
        "entpilot_video_model": "seedance-2.0-720p",
        "entpilot_ratio": "16:9",
        "entpilot_generate_audio": True,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)  # type: ignore[call-arg]


def _request(
    *,
    model: str = "seedance-2.0-720p",
    duration_seconds: int | None = 10,
    reference_images: list[dict] | None = None,
    reference_videos: list[dict] | None = None,
    reference_audios: list[dict] | None = None,
    first_frame: str | None = None,
    metadata: dict | None = None,
    **callbacks: object,
) -> VideoGenerationRequest:
    return VideoGenerationRequest(
        prompt="电影感城市夜景，镜头缓慢向前推进。",
        reference_images=reference_images or [],
        reference_image_names=[],
        reference_videos=reference_videos or [],
        reference_video_names=[],
        reference_audios=reference_audios or [],
        reference_audio_names=[],
        model=model,
        job_id="job-entpilot",
        first_frame=first_frame,
        duration_seconds=duration_seconds,
        metadata=metadata or {},
        **callbacks,
    )  # type: ignore[arg-type]


class _Response:
    def __init__(self, status_code: int, payload: object, headers: dict[str, str] | None = None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self) -> object:
        if isinstance(self._payload, BaseException):
            raise self._payload
        return self._payload


class _Client:
    def __init__(self, *, post: list[_Response] | None = None, get: list[_Response] | None = None):
        self.post_responses = list(post or [])
        self.get_responses = list(get or [])
        self.posts: list[dict] = []
        self.gets: list[dict] = []
        self.closed = False

    async def post(self, url: str, *, headers: dict[str, str], json: dict) -> _Response:
        self.posts.append({"url": url, "headers": headers, "json": json})
        return self.post_responses.pop(0)

    async def get(self, url: str, *, headers: dict[str, str]) -> _Response:
        self.gets.append({"url": url, "headers": headers})
        return self.get_responses.pop(0)

    async def aclose(self) -> None:
        self.closed = True


def _patch_settings(monkeypatch, **overrides: object) -> Settings:
    from app.providers import video_gen_entpilot

    configured = _settings(**overrides)
    monkeypatch.setattr(video_gen_entpilot, "settings", configured)
    return configured


def test_entpilot_factory_and_cancel_contract(monkeypatch) -> None:
    from app.providers.video_gen import get_video_provider

    _patch_settings(monkeypatch)
    provider = get_video_provider("ent-pilot")
    assert isinstance(provider, EntpilotVideoGenProvider)
    assert provider.provider_key == "entpilot"


def test_entpilot_provider_brand_spelling_alias_is_stable() -> None:
    """品牌大小写拼写与历史 Python 符号应指向同一个实现。"""

    assert EntPilotVideoGenProvider is EntpilotVideoGenProvider


@pytest.mark.asyncio
async def test_cancel_is_explicitly_unsupported_and_never_calls_delete(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    provider = EntpilotVideoGenProvider()
    provider._build_client = AsyncMock(side_effect=AssertionError("不得创建取消客户端"))

    result = await provider.cancel("task-1")

    assert result.provider_task_id == "task-1"
    assert result.outcome is VideoCancellationOutcome.UNSUPPORTED
    assert result.detail


@pytest.mark.asyncio
async def test_build_body_matches_sd20_document(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    provider = EntpilotVideoGenProvider()

    body, sensitive = await provider._build_request_body(
        _request(model="seedance-2.0-720p", duration_seconds=10),
        "seedance-2.0-720p",
    )

    assert body == {
        "model": "seedance-2.0-720p",
        "duration": 10,
        "generate_audio": True,
        "ratio": "16:9",
        "resolution": "720p",
        "prompt": "电影感城市夜景，镜头缓慢向前推进。",
        "content": [
            {"type": "text", "text": "电影感城市夜景，镜头缓慢向前推进。"},
        ],
    }
    assert sensitive == ("电影感城市夜景，镜头缓慢向前推进。",)


@pytest.mark.asyncio
async def test_build_body_matches_sd25_and_allows_audio_only_reference(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    storage = SimpleNamespace(
        external_read_url=AsyncMock(return_value="https://cdn.example/audio.mp3")
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    provider = EntpilotVideoGenProvider()

    body, _ = await provider._build_request_body(
        _request(
            model="seedance-2.5-720p",
            reference_audios=[{"storage_key": "ref/audio.mp3"}],
        ),
        "seedance-2.5-720p",
    )

    assert body["ratio"] == "16:9"
    assert body["duration"] == 10
    assert body["content"][-1] == {
        "type": "audio_url",
        "role": "reference_audio",
        "audio_url": {"url": "https://cdn.example/audio.mp3"},
    }


@pytest.mark.asyncio
async def test_sd25_edit_body_enforces_special_parameters(monkeypatch) -> None:
    """SD 2.5 edit 必须使用参考视频、adaptive 比例和 duration=-1。"""

    _patch_settings(monkeypatch, entpilot_ratio="16:9")
    storage = SimpleNamespace(
        external_read_url=AsyncMock(return_value="https://cdn.example/input.mp4")
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)

    body, _ = await EntpilotVideoGenProvider()._build_request_body(
        _request(
            model="seedance-2.5-720p",
            duration_seconds=-1,
            reference_videos=[
                {"storage_key": "refs/input.mp4", "duration_seconds": 12}
            ],
            metadata={
                "omni_reference_task_type": "edit",
                "ratio": "adaptive",
            },
        ),
        "seedance-2.5-720p",
    )

    assert body["duration"] == -1
    assert body["ratio"] == "adaptive"
    assert body["resolution"] == "720p"
    assert body["omni_reference_task_type"] == "edit"
    assert body["content"][-1] == {
        "type": "video_url",
        "role": "reference_video",
        "video_url": {"url": "https://cdn.example/input.mp4"},
    }


@pytest.mark.asyncio
async def test_sd25_extend_body_enforces_adaptive_ratio(monkeypatch) -> None:
    """SD 2.5 extend 使用有效延长时长，并要求 adaptive 比例。"""

    _patch_settings(monkeypatch)
    storage = SimpleNamespace(
        external_read_url=AsyncMock(return_value="https://cdn.example/input.mp4")
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)

    body, _ = await EntpilotVideoGenProvider()._build_request_body(
        _request(
            model="seedance-2.5-480p",
            duration_seconds=4,
            reference_videos=[{"storage_key": "refs/input.mp4"}],
            metadata={
                "omni_reference_task_type": "extend",
                "ratio": "adaptive",
            },
        ),
        "seedance-2.5-480p",
    )

    assert body["duration"] == 4
    assert body["ratio"] == "adaptive"
    assert body["omni_reference_task_type"] == "extend"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("task_type", "request_kwargs", "expected_code"),
    [
        (
            "edit",
            {"duration_seconds": -1, "metadata": {"ratio": "adaptive"}},
            "EditRequiresReferenceVideo",
        ),
        (
            "extend",
            {
                "duration_seconds": 4,
                "reference_videos":[{"storage_key": "refs/input.mp4"}],
                "metadata": {"ratio": "16:9"},
            },
            "ExtendRatioMustBeAdaptive",
        ),
        (
            "edit",
            {
                "duration_seconds": 10,
                "reference_videos":[{"storage_key": "refs/input.mp4"}],
                "metadata": {"ratio": "adaptive"},
            },
            "EditDurationMustBeMinusOne",
        ),
        (
            "extend",
            {
                "duration_seconds": 31,
                "reference_videos":[{"storage_key": "refs/input.mp4"}],
                "metadata": {"ratio": "adaptive"},
            },
            "ExtendDurationOutOfRange",
        ),
    ],
)
async def test_sd25_special_task_rejects_invalid_constraints(
    monkeypatch,
    task_type: str,
    request_kwargs: dict,
    expected_code: str,
) -> None:
    _patch_settings(monkeypatch)
    task_metadata = request_kwargs.get("metadata", {})
    body_kwargs = {key: value for key, value in request_kwargs.items() if key != "metadata"}

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await EntpilotVideoGenProvider()._build_request_body(
            _request(
                model="seedance-2.5-720p",
                metadata={
                    "omni_reference_task_type": task_type,
                    **task_metadata,
                },
                **body_kwargs,
            ),
            "seedance-2.5-720p",
        )

    assert exc_info.value.diagnostics == {"code": expected_code}


@pytest.mark.asyncio
async def test_sd25_edit_rejects_reference_video_duration_out_of_range(monkeypatch) -> None:
    _patch_settings(monkeypatch)

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await EntpilotVideoGenProvider()._build_request_body(
            _request(
                model="seedance-2.5-720p",
                duration_seconds=-1,
                reference_videos=[
                    {"storage_key": "refs/input.mp4", "duration_seconds": 3}
                ],
                metadata={
                    "omni_reference_task_type": "edit",
                    "ratio": "adaptive",
                },
            ),
            "seedance-2.5-720p",
        )

    assert exc_info.value.diagnostics == {
        "code": "EditReferenceVideoDurationOutOfRange"
    }


@pytest.mark.asyncio
async def test_first_frame_for_sd25_forces_auto_and_is_exclusive(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    provider = EntpilotVideoGenProvider()
    provider._encode_storage_image = AsyncMock(
        return_value="data:image/png;base64,Zmlyc3Q="
    )

    body, _ = await provider._build_request_body(
        _request(
            model="seedance-2.5-480p",
            first_frame="frames/first.png",
            reference_images=[{"storage_key": "frames/first.png", "mime_type": "image/png"}],
        ),
        "seedance-2.5-480p",
    )

    assert body["ratio"] == "auto"
    assert body["content"][-1] == {
        "type": "image_url",
        "role": "first_frame",
        "image_url": {"url": "data:image/png;base64,Zmlyc3Q="},
    }


@pytest.mark.asyncio
async def test_first_frame_rejects_mixed_reference_media(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await EntpilotVideoGenProvider()._build_request_body(
            _request(
                first_frame="frames/first.png",
                reference_images=[
                    {"storage_key": "frames/first.png", "mime_type": "image/png"},
                    {"storage_key": "frames/other.png", "mime_type": "image/png"},
                ],
            ),
            "seedance-2.0-720p",
        )
    assert exc_info.value.diagnostics == {"code": "reference_images_limit_exceeded"}


@pytest.mark.asyncio
@pytest.mark.parametrize("model", ["seedance-2.0-720p", "seedance-2.5-720p"])
async def test_regular_tasks_reject_auto_ratio(monkeypatch, model: str) -> None:
    """文档中的 auto 仅属于 2.5 首帧模式，普通任务必须显式画幅。"""

    _patch_settings(monkeypatch, entpilot_ratio="auto")
    with pytest.raises(ProviderConfigurationError, match="ENTPILOT_RATIO"):
        await EntpilotVideoGenProvider()._build_request_body(
            _request(model=model),
            model,
        )


def test_documented_image_mime_types_are_normalized() -> None:
    for value, expected in (
        ("image/jpg", "image/jpeg"),
        ("image/x-ms-bmp", "image/bmp"),
        ("image/tif", "image/tiff"),
        ("image/gif", "image/gif"),
    ):
        assert EntpilotVideoGenProvider._normalize_image_mime_type(value, "ref.bin") == expected


@pytest.mark.parametrize("model", [
    "seedance-2.0-1080p",
    "seedance-2.0-480p",
    "seedance-2.0-4k",
    "seedance-2.0-720p",
    "seedance-2.0-fast-480p",
    "seedance-2.0-fast-720p",
    "seedance-2.5-1080p",
    "seedance-2.5-480p",
    "seedance-2.5-720p",
])
def test_all_documented_models_are_accepted(model: str) -> None:
    family = EntpilotVideoGenProvider._model_family(model)
    assert family in {"2.0", "2.5"}


def test_model_and_duration_validation() -> None:
    with pytest.raises(VideoGenerationSubmitRejected, match="video_generation_submit_rejected"):
        EntpilotVideoGenProvider._model_family("legacy-seedance-model")
    for unsupported_model in (
        "seedance-2.0-fast-1080p",
        "seedance-2.0-fast-4k",
        "seedance-2.5-fast-480p",
        "seedance-2.5-4k",
    ):
        with pytest.raises(VideoGenerationSubmitRejected):
            EntpilotVideoGenProvider._model_family(unsupported_model)
    assert EntpilotVideoGenProvider._duration(3, model_family="2.0") == 4
    assert EntpilotVideoGenProvider._duration(1, model_family="2.0") == 4
    assert EntpilotVideoGenProvider._duration(16, model_family="2.0") == 15
    assert EntpilotVideoGenProvider._duration(30, model_family="2.5") == 30
    with pytest.raises(VideoGenerationSubmitRejected):
        EntpilotVideoGenProvider._duration(31, model_family="2.5")


@pytest.mark.asyncio
async def test_generate_posts_polls_and_downloads_result(monkeypatch, tmp_path: Path) -> None:
    from app.providers import video_gen_entpilot

    _patch_settings(monkeypatch, entpilot_base_url="https://entpilot.example")
    client = _Client(
        post=[_Response(202, {"id": "task-123", "status": "queued"})],
        get=[
            _Response(200, {"id": "task-123", "status": "processing", "progress": 25}),
            _Response(200, {
                "id": "task-123",
                "status": "completed",
                "video_url": "https://cdn.example/video.mp4",
                "usage": {"completion_tokens": 4, "total_tokens": 5},
            }),
        ],
    )
    provider = EntpilotVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)
    monkeypatch.setattr(video_gen_entpilot, "_POLL_INTERVAL", 0)

    async def download(_url: str, path: str | Path, **_kwargs: object) -> int:
        Path(path).write_bytes(b"video")
        return 5

    monkeypatch.setattr(provider, "_download_to_file", download)
    monkeypatch.setattr(provider, "_store_video_file", AsyncMock(return_value="video/job.mp4"))

    created = AsyncMock()
    result = await provider.generate(_request(on_task_created=created))

    assert result.video_storage_key == "video/job.mp4"
    assert result.provider_job_id == "task-123"
    assert result.metrics["entpilot_status"] == "completed"
    assert result.metrics["usage"] == {"completion_tokens": 4, "total_tokens": 5}
    assert client.posts[0]["url"] == "https://entpilot.example/v1/videos"
    assert client.gets[-1]["url"] == "https://entpilot.example/v1/videos/task-123"
    assert client.posts[0]["headers"]["Authorization"] == "Bearer test-entpilot-key"
    assert client.closed is True
    created.assert_awaited_once_with("task-123")


@pytest.mark.asyncio
async def test_generate_handles_nested_result_and_submission_rejection(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    client = _Client(
        post=[_Response(400, {"error": {"code": "InvalidParameter", "param": "duration"}})]
    )
    provider = EntpilotVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)
    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await provider.generate(_request())
    assert exc_info.value.diagnostics["status_code"] == 400
    assert exc_info.value.diagnostics["code"] == "InvalidParameter"
    assert client.closed is True


@pytest.mark.asyncio
async def test_poll_accepts_documented_in_process_and_nested_video_url(monkeypatch) -> None:
    from app.providers import video_gen_entpilot

    _patch_settings(monkeypatch, entpilot_base_url="https://entpilot.example")
    client = _Client(
        get=[
            _Response(200, {"data": {"status": "in_process"}}),
            _Response(200, {"data": {"status": "completed", "result_url": "https://cdn.example/a.mp4"}}),
        ]
    )
    monkeypatch.setattr(video_gen_entpilot, "_POLL_INTERVAL", 0)
    result = await EntpilotVideoGenProvider()._poll(client, "task/nested", lambda *_args: None)
    assert result["data"]["status"] == "completed"
    assert client.gets[0]["url"].endswith("task%2Fnested")


def test_extract_video_url_prefers_nested_absolute_url_over_relative_placeholder() -> None:
    """文档示例的顶层占位地址不能遮蔽 data 中的真实下载地址。"""

    payload = {
        "status": "completed",
        "video_url": "xxxxx/video.mp4",
        "result_url": "xxxxx/video.mp4",
        "data": {
            "video_url": "https://cdn.example/video.mp4",
        },
    }

    assert EntpilotVideoGenProvider._extract_video_url(payload) == "https://cdn.example/video.mp4"


def test_extract_video_url_rejects_relative_only_candidates() -> None:
    assert EntpilotVideoGenProvider._extract_video_url({"video_url": "xxxxx/video.mp4"}) == ""


def test_extract_video_url_does_not_use_input_content_media() -> None:
    payload = {
        "status": "completed",
        "content": [
            {
                "type": "image_url",
                "image_url": {"url": "https://cdn.example/reference.jpg"},
            }
        ],
    }

    assert EntpilotVideoGenProvider._extract_video_url(payload) == ""


@pytest.mark.asyncio
async def test_poll_reports_failed_terminal_status(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    client = _Client(get=[_Response(200, {"status": "failed"})])
    with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
        await EntpilotVideoGenProvider()._poll(client, "task-failed", lambda *_args: None)
    assert exc_info.value.provider_task_id == "task-failed"
    assert exc_info.value.terminal_status == "failed"


@pytest.mark.parametrize(
    ("value", "expected"),
    [("https://ka.ent-pilot.com", "https://ka.ent-pilot.com"),
     ("https://ka.ent-pilot.com/", "https://ka.ent-pilot.com")],
)
def test_api_urls_are_rooted_and_task_ids_are_quoted(monkeypatch, value: str, expected: str) -> None:
    _patch_settings(monkeypatch, entpilot_base_url=value)
    provider = EntpilotVideoGenProvider()
    assert provider._videos_url() == f"{expected}/v1/videos"
    assert provider._task_url("task/with space") == f"{expected}/v1/videos/task%2Fwith%20space"


def test_api_origin_rejects_non_https_or_non_root(monkeypatch) -> None:
    from app.providers import video_gen_entpilot

    for value in ("http://entpilot.example", "https://entpilot.example/api/v3", "https://u:p@entpilot.example"):
        monkeypatch.setattr(
            video_gen_entpilot,
            "settings",
            SimpleNamespace(
                video_gen_base_url="",
                entpilot_base_url=value,
                is_explicitly_configured=lambda _name: False,
            ),
        )
        with pytest.raises(ProviderConfigurationError):
            EntpilotVideoGenProvider._api_origin()


@pytest.mark.asyncio
async def test_submit_4xx_redacts_sensitive_diagnostics_and_keeps_callback_order(monkeypatch) -> None:
    """明确拒绝只暴露安全字段，不误触发提交不确定回调。"""

    _patch_settings(monkeypatch)
    secret_message = "不得持久化的提示词和签名 URL"
    client = _Client(
        post=[
            _Response(
                400,
                {
                    "error": {
                        "code": "InvalidParameter",
                        "param": "content",
                        "message": secret_message,
                    }
                },
                headers={
                    "content-type": "application/problem+json; charset=utf-8",
                    "x-request-id": "request-safe-1",
                },
            )
        ]
    )
    provider = EntpilotVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)
    before_submit = AsyncMock()
    on_uncertain = AsyncMock()

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await provider.generate(
            _request(before_submit=before_submit, on_submission_uncertain=on_uncertain)
        )

    assert exc_info.value.diagnostics == {
        "status_code": 400,
        "content_type": "application/problem+json",
        "code": "InvalidParameter",
        "param": "content",
        "request_id": "request-safe-1",
    }
    assert secret_message not in repr(exc_info.value.diagnostics)
    before_submit.assert_awaited_once()
    on_uncertain.assert_not_awaited()
    assert client.closed is True


@pytest.mark.parametrize(
    "response",
    [
        pytest.param(_Response(500, {"message": "upstream failure"}), id="http-500"),
        pytest.param(_Response(200, {"status": "queued"}), id="missing-task-id"),
    ],
)
@pytest.mark.asyncio
async def test_unresolved_submission_calls_uncertain_callback_after_submit(
    monkeypatch, response: _Response
) -> None:
    _patch_settings(monkeypatch)
    client = _Client(post=[response])
    provider = EntpilotVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)
    events: list[str] = []

    async def before_submit() -> None:
        events.append("intent")

    async def on_uncertain() -> None:
        events.append("uncertain")

    with pytest.raises(VideoGenerationSubmissionUncertain):
        await provider.generate(
            _request(before_submit=before_submit, on_submission_uncertain=on_uncertain)
        )

    assert events == ["intent", "uncertain"]
    assert client.closed is True


@pytest.mark.asyncio
async def test_resume_existing_task_does_not_post_or_rebuild_body(monkeypatch) -> None:
    _patch_settings(monkeypatch, entpilot_base_url="https://entpilot.example")
    client = _Client(
        get=[
            _Response(
                200,
                {"task_id": "task-existing", "status": "completed", "url": "https://cdn.example/a.mp4"},
            )
        ]
    )
    provider = EntpilotVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda **_kwargs: client)
    body_builder = AsyncMock(side_effect=AssertionError("恢复任务不得重建请求体"))
    monkeypatch.setattr(provider, "_build_request_body", body_builder)
    monkeypatch.setattr(provider, "_download_to_file", AsyncMock(return_value=1))
    monkeypatch.setattr(provider, "_store_video_file", AsyncMock(return_value="video/existing.mp4"))
    checkpoint = AsyncMock()

    result = await provider.generate(
        _request(
            metadata={"provider_task_id": "task-existing"},
            on_task_created=checkpoint,
        )
    )

    assert result.provider_job_id == "task-existing"
    assert client.posts == []
    body_builder.assert_not_awaited()
    checkpoint.assert_awaited_once_with("task-existing")
    assert client.closed is True


@pytest.mark.asyncio
async def test_reference_image_prefers_external_url_and_preserves_subject_type(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    storage = SimpleNamespace(
        external_read_url=AsyncMock(return_value="https://cdn.example/reference.png?sig=ok")
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)

    body, sensitive = await EntpilotVideoGenProvider()._build_request_body(
        _request(
            reference_images=[
                {
                    "storage_key": "refs/character.png",
                    "mime_type": "image/png",
                    "subject_type": "person",
                }
            ]
        ),
        "seedance-2.0-720p",
    )

    assert body["content"][1] == {
        "type": "image_url",
        "role": "reference_image",
        "subject_type": "person",
        "image_url": {"url": "https://cdn.example/reference.png?sig=ok"},
    }
    assert "https://cdn.example/reference.png?sig=ok" in sensitive
    storage.external_read_url.assert_awaited_once_with("refs/character.png", expires_seconds=900)


@pytest.mark.asyncio
async def test_reference_image_falls_back_to_data_url_when_external_url_unavailable(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    storage = SimpleNamespace(
        external_read_url=AsyncMock(return_value=None),
        get_size=AsyncMock(return_value=4),
        get=AsyncMock(return_value=b"test"),
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)

    body, _ = await EntpilotVideoGenProvider()._build_request_body(
        _request(
            reference_images=[{"storage_key": "refs/character.png", "mime_type": "image/png"}]
        ),
        "seedance-2.0-720p",
    )

    assert body["content"][1]["image_url"]["url"] == "data:image/png;base64,dGVzdA=="
    storage.get_size.assert_awaited_once_with("refs/character.png")
    storage.get.assert_awaited_once_with("refs/character.png")


@pytest.mark.asyncio
async def test_reference_audio_uses_external_url_without_requiring_image(monkeypatch) -> None:
    _patch_settings(monkeypatch)
    storage = SimpleNamespace(
        external_read_url=AsyncMock(return_value="https://cdn.example/reference.mp3")
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)

    body, _ = await EntpilotVideoGenProvider()._build_request_body(
        _request(reference_audios=[{"storage_key": "refs/voice.mp3"}]),
        "seedance-2.0-720p",
    )

    assert body["content"][-1] == {
        "type": "audio_url",
        "role": "reference_audio",
        "audio_url": {"url": "https://cdn.example/reference.mp3"},
    }


@pytest.mark.asyncio
async def test_download_uses_safe_streaming_limits_and_rejects_unsafe_origin(monkeypatch, tmp_path: Path) -> None:
    from app.providers import _url_safety

    _patch_settings(monkeypatch)
    captured: dict[str, object] = {}
    cancellation_probe = AsyncMock()

    async def safe_download(url, path, **kwargs) -> int:
        captured.update({"url": url, "path": path, **kwargs})
        await kwargs["cancellation_probe"]()
        Path(path).write_bytes(b"video")
        return 5

    monkeypatch.setattr(_url_safety, "safe_download_to_file", safe_download)
    destination = tmp_path / "video.mp4"
    written = await EntpilotVideoGenProvider()._download_to_file(
        "https://cdn.example/video.mp4", destination, cancellation_probe=cancellation_probe
    )

    assert written == 5
    assert captured["url"] == "https://cdn.example/video.mp4"
    assert captured["max_bytes"] == 512 * 1024 * 1024
    assert captured["timeout"] == 120.0
    cancellation_probe.assert_awaited_once()
    assert destination.read_bytes() == b"video"
    with pytest.raises(ProviderConfigurationError):
        EntpilotVideoGenProvider._validate_external_reference_url("http://cdn.example/video.mp4")


@pytest.mark.asyncio
async def test_store_video_file_uses_atomic_file_promotion(monkeypatch, tmp_path: Path) -> None:
    from app.providers import streaming_io

    _patch_settings(monkeypatch)
    path = tmp_path / "video.mp4"
    path.write_bytes(b"video")
    storage = SimpleNamespace()
    atomic_put = AsyncMock(return_value="generate_images/video/job.mp4")
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    monkeypatch.setattr(streaming_io, "atomic_put_file", atomic_put)

    result = await EntpilotVideoGenProvider()._store_video_file(
        path, "task-1", "job", {"storage_attempt_id": "0123456789abcdef"}
    )

    assert result == "generate_images/video/job.mp4"
    atomic_put.assert_awaited_once()
    kwargs = atomic_put.await_args.kwargs
    assert kwargs["attempt_id"] == "0123456789abcdef"
    assert kwargs["preserve_existing_final"] is False
