"""阿里百炼视频模型协议适配测试。"""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.providers.video_gen import VideoGenerationRequest, VideoGenerationSubmitRejected
from app.providers.video_gen_bailian import BailianVideoGenProvider


def _request(
    *,
    model: str,
    duration_seconds: int | None = 10,
    reference_audios: list[dict] | None = None,
    reference_images: list[dict] | None = None,
    reference_videos: list[dict] | None = None,
    first_frame: str | None = None,
) -> VideoGenerationRequest:
    return VideoGenerationRequest(
        prompt="一名角色缓慢转身，看向镜头外。",
        reference_images=(
            [
                {
                    "storage_key": "references/first.webp",
                    "mime_type": "image/webp",
                }
            ]
            if reference_images is None
            else reference_images
        ),
        reference_image_names=["角色"],
        reference_audios=reference_audios or [],
        reference_audio_names=[],
        reference_videos=reference_videos or [],
        reference_video_names=[],
        model=model,
        job_id="bailian-protocol-test",
        first_frame=first_frame,
        duration_seconds=duration_seconds,
    )


@pytest.mark.asyncio
async def test_wan26_i2v_flash_uses_legacy_img_url_and_capabilities() -> None:
    provider = BailianVideoGenProvider()
    provider._encode_single = AsyncMock(return_value="data:image/webp;base64,aW1hZ2U=")

    body = await provider._build_body(
        _request(model="wan2.6-i2v-flash"),
        "wan2.6-i2v-flash",
        lambda *_args: None,
    )

    assert body == {
        "model": "wan2.6-i2v-flash",
        "input": {
            "prompt": "一名角色缓慢转身，看向镜头外。",
            "img_url": "data:image/webp;base64,aW1hZ2U=",
        },
        "parameters": {
            "prompt_extend": True,
            "watermark": False,
            "shot_type": "multi",
            "duration": 10,
        },
    }
    provider._encode_single.assert_awaited_once_with("references/first.webp", "image/webp")
    assert provider._api_root("https://dashscope.aliyuncs.com") == "https://dashscope.aliyuncs.com/api/v1"
    assert provider._api_root("https://workspace.cn-beijing.maas.aliyuncs.com/api/v1") == (
        "https://workspace.cn-beijing.maas.aliyuncs.com/api/v1"
    )


@pytest.mark.asyncio
async def test_wan26_i2v_flash_posts_to_configured_video_api_root(monkeypatch) -> None:
    from app.providers import video_gen_bailian

    calls: list[dict] = []

    class HttpClient:
        async def aclose(self) -> None:
            return None

        async def post(self, url, *, headers, json):
            calls.append({"url": url, "headers": headers, "json": json})
            return SimpleNamespace(
                status_code=200,
                json=lambda: {"output": {"task_id": "bailian-task-1"}},
            )

    provider = BailianVideoGenProvider()
    provider._check_keys = lambda: None
    provider._base_url = lambda: "https://dashscope.aliyuncs.com"
    provider._api_key = lambda: "test-key"
    provider._encode_single = AsyncMock(return_value="data:image/webp;base64,aW1hZ2U=")
    provider._poll = AsyncMock(return_value="https://result.example/video.mp4")
    provider._download_to_file = AsyncMock(return_value=1)
    provider._store_video_file = AsyncMock(return_value="video/output.mp4")
    monkeypatch.setattr(video_gen_bailian.httpx, "AsyncClient", lambda **_kwargs: HttpClient())

    result = await provider.generate(_request(model="wan2.6-i2v-flash"))

    assert result.video_storage_key == "video/output.mp4"
    assert calls == [
        {
            "url": "https://dashscope.aliyuncs.com/api/v1/services/aigc/video-generation/video-synthesis",
            "headers": {
                "Authorization": "Bearer test-key",
                "Content-Type": "application/json",
                "X-DashScope-Async": "enable",
            },
            "json": {
                "model": "wan2.6-i2v-flash",
                "input": {
                    "prompt": "一名角色缓慢转身，看向镜头外。",
                    "img_url": "data:image/webp;base64,aW1hZ2U=",
                },
                "parameters": {
                    "prompt_extend": True,
                    "watermark": False,
                    "shot_type": "multi",
                    "duration": 10,
                },
            },
        }
    ]


@pytest.mark.asyncio
async def test_wan26_i2v_flash_rejects_unsupported_duration_before_submit() -> None:
    provider = BailianVideoGenProvider()

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await provider._build_body(
            _request(model="wan2.6-i2v-flash", duration_seconds=12),
            "wan2.6-i2v-flash",
            lambda *_args: None,
        )
    assert exc_info.value.diagnostics == {"code": "Wan26DurationMustBe5Or10"}


@pytest.mark.asyncio
async def test_i2v_rejects_missing_input_image_before_submit() -> None:
    provider = BailianVideoGenProvider()
    provider._check_keys = lambda: None

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await provider.generate(
            _request(model="wanx2.1-kf2v", reference_images=[]),
        )

    assert exc_info.value.diagnostics == {"code": "reference_input_image_required"}


@pytest.mark.asyncio
async def test_i2v_accepts_first_frame_as_required_input_image(monkeypatch) -> None:
    provider = BailianVideoGenProvider()
    provider._check_keys = lambda: None
    provider._build_body = AsyncMock(return_value={"model": "wanx2.1-kf2v", "input": {}})
    provider._poll = AsyncMock(return_value="https://result.example/video.mp4")
    provider._download_to_file = AsyncMock(return_value=1)
    provider._store_video_file = AsyncMock(return_value="video/output.mp4")

    class HttpClient:
        async def aclose(self) -> None:
            return None

        async def post(self, *_args, **_kwargs):
            return SimpleNamespace(
                status_code=200,
                json=lambda: {"output": {"task_id": "bailian-task-1"}},
            )

    from app.providers import video_gen_bailian

    monkeypatch.setattr(video_gen_bailian.httpx, "AsyncClient", lambda **_kwargs: HttpClient())
    result = await provider.generate(
        _request(
            model="wanx2.1-kf2v",
            reference_images=[],
            first_frame="frames/previous-tail.png",
        ),
    )

    assert result.video_storage_key == "video/output.mp4"


@pytest.mark.asyncio
async def test_wan27_i2v_uses_media_protocol_with_driving_audio() -> None:
    provider = BailianVideoGenProvider()
    provider._encode_single = AsyncMock(
        side_effect=[
            "data:image/webp;base64,aW1hZ2U=",
            "data:audio/wav;base64,YXVkaW8=",
        ]
    )

    body = await provider._build_body(
        _request(
            model="wan2.7-i2v-2026-04-25",
            duration_seconds=12,
            reference_audios=[
                {"storage_key": "references/voice.wav", "mime_type": "audio/wav"}
            ],
        ),
        "wan2.7-i2v-2026-04-25",
        lambda *_args: None,
    )

    assert body["input"] == {
        "prompt": "一名角色缓慢转身，看向镜头外。",
        "media": [
            {"type": "first_frame", "url": "data:image/webp;base64,aW1hZ2U="},
            {"type": "driving_audio", "url": "data:audio/wav;base64,YXVkaW8="},
        ],
    }
    assert body["parameters"] == {
        "prompt_extend": True,
        "watermark": False,
        "duration": 12,
    }
    assert "img_url" not in body["input"]


@pytest.mark.asyncio
async def test_wan30_pure_text_generation_omits_media() -> None:
    provider = BailianVideoGenProvider()

    body = await provider._build_body(
        _request(model="wan3.0-video-prime", reference_images=[]),
        "wan3.0-video-prime",
        lambda *_args: None,
    )

    # wan3.0 的 prompt 与 media 必填其一，纯文生无需强制传图。
    assert body == {
        "model": "wan3.0-video-prime",
        "input": {"prompt": "一名角色缓慢转身，看向镜头外。"},
        "parameters": {
            "prompt_extend": True,
            "watermark": False,
            "duration": 10,
        },
    }


@pytest.mark.asyncio
async def test_wan30_first_frame_uses_media_protocol() -> None:
    provider = BailianVideoGenProvider()
    provider._encode_single = AsyncMock(return_value="data:image/webp;base64,aW1hZ2U=")

    body = await provider._build_body(
        _request(
            model="wan3.0-video-prime",
            reference_images=[],
            first_frame="frames/previous-tail.png",
        ),
        "wan3.0-video-prime",
        lambda *_args: None,
    )

    assert body["input"] == {
        "prompt": "一名角色缓慢转身，看向镜头外。",
        "media": [
            {"type": "first_frame", "url": "data:image/webp;base64,aW1hZ2U="},
        ],
    }
    assert body["parameters"] == {
        "prompt_extend": True,
        "watermark": False,
        "duration": 10,
    }
    provider._encode_single.assert_awaited_once_with("frames/previous-tail.png", "image/png")


@pytest.mark.asyncio
async def test_wan30_rejects_reference_media_when_explicit_first_frame_is_present() -> None:
    provider = BailianVideoGenProvider()
    provider._encode_single = AsyncMock(return_value="data:image/png;base64,aW1hZ2U=")

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await provider._build_body(
            _request(
                model="wan3.0-video-prime",
                reference_images=[],
                first_frame="frames/previous-tail.png",
                reference_audios=[
                    {"storage_key": "references/voice.wav", "mime_type": "audio/wav"}
                ],
            ),
            "wan3.0-video-prime",
            lambda *_args: None,
        )

    assert exc_info.value.diagnostics == {"code": "wan30_media_mode_conflict"}


@pytest.mark.asyncio
async def test_wan30_generate_rejects_reference_media_with_explicit_first_frame() -> None:
    provider = BailianVideoGenProvider()
    provider._check_keys = lambda: None

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await provider.generate(
            _request(
                model="wan3.0-video-prime",
                reference_images=[],
                first_frame="frames/previous-tail.png",
                reference_audios=[
                    {"storage_key": "references/voice.wav", "mime_type": "audio/wav"}
                ],
            )
        )

    assert exc_info.value.diagnostics == {"code": "wan30_media_mode_conflict"}


@pytest.mark.asyncio
async def test_wan30_audio_without_first_frame_submits_reference_audio() -> None:
    provider = BailianVideoGenProvider()
    provider._encode_single = AsyncMock(return_value="data:audio/wav;base64,YXVkaW8=")
    provider._external_reference_url = AsyncMock(
        return_value="https://storage.example/reference/voice.wav?signature=audio"
    )

    body = await provider._build_body(
        _request(
            model="wan3.0-video-prime",
            reference_images=[],
            reference_audios=[
                {"storage_key": "references/voice.wav", "mime_type": "audio/wav"}
            ],
        ),
        "wan3.0-video-prime",
        lambda *_args: None,
    )

    assert body["input"]["media"] == [
        {
            "type": "reference_audio",
            "url": "https://storage.example/reference/voice.wav?signature=audio",
        },
    ]
    provider._external_reference_url.assert_awaited_once_with("references/voice.wav")


@pytest.mark.asyncio
async def test_wan30_serializes_all_reference_media_in_stable_order() -> None:
    provider = BailianVideoGenProvider()
    provider._encode_single = AsyncMock(
        side_effect=[
            "data:image/webp;base64,aW1hZ2Ux",
            "data:image/png;base64,aW1hZ2Uy",
        ]
    )
    provider._external_reference_url = AsyncMock(
        side_effect=[
            "https://storage.example/reference/motion.mp4?signature=video-1",
            "https://storage.example/reference/style.mov?signature=video-2",
            "https://storage.example/reference/dialogue.wav?signature=audio-1",
            "https://storage.example/reference/music.mp3?signature=audio-2",
        ]
    )

    body = await provider._build_body(
        _request(
            model="wan3.0-video-prime",
            reference_images=[
                {"storage_key": "references/character.webp", "mime_type": "image/webp"},
                {"storage_key": "references/prop.png", "mime_type": "image/png"},
            ],
            reference_videos=[
                {"storage_key": "references/motion.mp4", "mime_type": "video/mp4"},
                {"storage_key": "references/style.mov", "mime_type": "video/quicktime"},
            ],
            reference_audios=[
                {"storage_key": "references/dialogue.wav", "mime_type": "audio/wav"},
                {"storage_key": "references/music.mp3", "mime_type": "audio/mpeg"},
            ],
        ),
        "wan3.0-video-prime",
        lambda *_args: None,
    )

    assert body["input"]["media"] == [
        {"type": "reference_image", "url": "data:image/webp;base64,aW1hZ2Ux"},
        {"type": "reference_image", "url": "data:image/png;base64,aW1hZ2Uy"},
        {
            "type": "reference_video",
            "url": "https://storage.example/reference/motion.mp4?signature=video-1",
        },
        {
            "type": "reference_video",
            "url": "https://storage.example/reference/style.mov?signature=video-2",
        },
        {
            "type": "reference_audio",
            "url": "https://storage.example/reference/dialogue.wav?signature=audio-1",
        },
        {
            "type": "reference_audio",
            "url": "https://storage.example/reference/music.mp3?signature=audio-2",
        },
    ]
    assert provider._encode_single.await_args_list == [
        (("references/character.webp", "image/webp"),),
        (("references/prop.png", "image/png"),),
    ]
    assert provider._external_reference_url.await_args_list == [
        (("references/motion.mp4",),),
        (("references/style.mov",),),
        (("references/dialogue.wav",),),
        (("references/music.mp3",),),
    ]


@pytest.mark.asyncio
async def test_wan30_submit_allows_reference_videos(monkeypatch) -> None:
    from app.providers import video_gen_bailian

    class HttpClient:
        async def aclose(self) -> None:
            return None

        async def post(self, *_args, **_kwargs):
            return SimpleNamespace(
                status_code=200,
                json=lambda: {"output": {"task_id": "bailian-task-1"}},
            )

    provider = BailianVideoGenProvider()
    provider._check_keys = lambda: None
    provider._build_body = AsyncMock(return_value={"model": "wan3.0-video-prime", "input": {}})
    provider._poll = AsyncMock(return_value="https://result.example/video.mp4")
    provider._download_to_file = AsyncMock(return_value=1)
    provider._store_video_file = AsyncMock(return_value="video/output.mp4")
    monkeypatch.setattr(video_gen_bailian.httpx, "AsyncClient", lambda **_kwargs: HttpClient())

    result = await provider.generate(
        _request(
            model="wan3.0-video-prime",
            reference_images=[],
            reference_videos=[{"storage_key": "references/motion.mp4", "mime_type": "video/mp4"}],
        )
    )

    assert result.video_storage_key == "video/output.mp4"
    provider._build_body.assert_awaited_once()


@pytest.mark.asyncio
async def test_wan30_truncates_prompt_to_20000_characters() -> None:
    provider = BailianVideoGenProvider()

    request = replace(
        _request(model="wan3.0-video-prime", reference_images=[]),
        prompt="图" * 20001,
    )
    body = await provider._build_body(
        request,
        "wan3.0-video-prime",
        lambda *_args: None,
    )

    assert body["input"]["prompt"] == "图" * 20000


@pytest.mark.asyncio
async def test_happyhorse_i2v_uses_first_frame_media_and_its_capabilities() -> None:
    provider = BailianVideoGenProvider()
    provider._encode_single = AsyncMock(return_value="data:image/webp;base64,aW1hZ2U=")

    body = await provider._build_body(
        _request(model="happyhorse-1.0-i2v", duration_seconds=10),
        "happyhorse-1.0-i2v",
        lambda *_args: None,
    )

    assert body == {
        "model": "happyhorse-1.0-i2v",
        "input": {
            "prompt": "一名角色缓慢转身，看向镜头外。",
            "media": [
                {
                    "type": "first_frame",
                    "url": "data:image/webp;base64,aW1hZ2U=",
                }
            ],
        },
        "parameters": {
            "resolution": "1080P",
            "watermark": False,
            "duration": 10,
        },
    }
    provider._encode_single.assert_awaited_once_with("references/first.webp", "image/webp")
    assert "ratio" not in body["parameters"]
    assert "prompt_extend" not in body["parameters"]


@pytest.mark.asyncio
async def test_happyhorse_i2v_validates_duration_and_prompt_limit() -> None:
    provider = BailianVideoGenProvider()
    provider._encode_single = AsyncMock(return_value="data:image/png;base64,aW1hZ2U=")

    with pytest.raises(VideoGenerationSubmitRejected) as exc_info:
        await provider._build_body(
            _request(model="happyhorse-1.0-i2v", duration_seconds=16),
            "happyhorse-1.0-i2v",
            lambda *_args: None,
        )
    assert exc_info.value.diagnostics == {"code": "HappyHorseDurationMustBe3To15"}
    provider._encode_single.assert_not_awaited()

    request = replace(
        _request(model="happyhorse-1.0-i2v"),
        prompt="图" * 2501,
    )
    body = await provider._build_body(
        request,
        "happyhorse-1.0-i2v",
        lambda *_args: None,
    )
    assert body["input"]["prompt"] == "图" * 2500


def test_bailian_submit_rejection_exposes_only_safe_diagnostics() -> None:
    private_message = "提示词或 data:image/png;base64,private 不应返回"
    response = SimpleNamespace(
        status_code=400,
        json=lambda: {
            "code": "InvalidParameter",
            "request_id": "request-safe-123",
            "message": private_message,
            "input": {"img_url": "data:image/png;base64,private"},
        },
    )

    rejection = VideoGenerationSubmitRejected(
        diagnostics=BailianVideoGenProvider._submit_rejection_diagnostics(response)
    )

    assert rejection.diagnostics == {
        "status_code": 400,
        "code": "InvalidParameter",
        "request_id": "request-safe-123",
    }
    assert private_message not in repr(rejection.diagnostics)


def test_bailian_video_strips_openai_compatible_path_from_explicit_base_url(monkeypatch) -> None:
    from app.providers import video_gen_bailian

    monkeypatch.setattr(
        video_gen_bailian,
        "settings",
        SimpleNamespace(
            is_explicitly_configured=lambda field_name: field_name == "video_gen_base_url",
            video_gen_base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
            dashscope_base_url="https://dashscope.aliyuncs.com",
        ),
    )

    assert BailianVideoGenProvider._base_url() == "https://dashscope.aliyuncs.com"


@pytest.mark.asyncio
async def test_download_failure_is_wrapped_as_generation_download_error(monkeypatch, tmp_path) -> None:
    """远端成功后的下载异常必须包装为专用类型，供 Worker 跳过取消确认门禁。"""
    from app.providers import _url_safety
    from app.providers.video_gen import VideoGenerationDownloadError

    provider = BailianVideoGenProvider()
    sentinel = RuntimeError("provider_download_url_invalid")

    async def failing_download(*_args, **_kwargs):
        raise sentinel

    monkeypatch.setattr(_url_safety, "safe_download_to_file", failing_download)

    with pytest.raises(VideoGenerationDownloadError) as exc_info:
        await provider._download_to_file(
            "https://dashscope-a717.oss-accelerate.aliyuncs.com/video.mp4",
            tmp_path / "video.mp4",
        )

    assert exc_info.value.reason == "provider_download_url_invalid"
    assert exc_info.value.__cause__ is sentinel


@pytest.mark.asyncio
async def test_download_time_limit_signals_are_not_wrapped(monkeypatch, tmp_path) -> None:
    """Celery 时间限制信号必须原样透传，不允许被包装吞掉。"""
    from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded

    from app.providers import _url_safety
    from app.providers.video_gen import VideoGenerationDownloadError

    provider = BailianVideoGenProvider()

    for signal in (SoftTimeLimitExceeded(), TimeLimitExceeded()):
        async def signal_download(*_args, _signal=signal, **_kwargs):
            raise _signal

        monkeypatch.setattr(_url_safety, "safe_download_to_file", signal_download)

        with pytest.raises((SoftTimeLimitExceeded, TimeLimitExceeded)) as exc_info:
            await provider._download_to_file(
                "https://dashscope-a717.oss-accelerate.aliyuncs.com/video.mp4",
                tmp_path / "video.mp4",
            )
        assert not isinstance(exc_info.value, VideoGenerationDownloadError)


@pytest.mark.asyncio
async def test_download_success_returns_byte_count(monkeypatch, tmp_path) -> None:
    """正常下载不受包装影响，返回字节数。"""
    from app.providers import _url_safety

    provider = BailianVideoGenProvider()

    async def ok_download(*_args, **_kwargs):
        return 12345

    monkeypatch.setattr(_url_safety, "safe_download_to_file", ok_download)

    assert (
        await provider._download_to_file(
            "https://dashscope-a717.oss-accelerate.aliyuncs.com/video.mp4",
            tmp_path / "video.mp4",
        )
        == 12345
    )


@pytest.mark.asyncio
async def test_store_failure_is_wrapped_as_generation_download_error(monkeypatch, tmp_path) -> None:
    """远端成功后的存储转存异常同样必须包装为专用类型（下载/转存一体防护）。"""
    from app.providers import streaming_io
    from app.providers.video_gen import VideoGenerationDownloadError

    provider = BailianVideoGenProvider()
    sentinel = OSError("disk full")

    async def failing_store(*_args, **_kwargs):
        raise sentinel

    monkeypatch.setattr(streaming_io, "atomic_put_file", failing_store)

    with pytest.raises(VideoGenerationDownloadError) as exc_info:
        await provider._store_video_file(
            tmp_path / "video.mp4",
            "task-id",
            "job-id",
            {"attempt": 1},
        )

    assert exc_info.value.reason.startswith("store_failed:")
    assert exc_info.value.__cause__ is sentinel
