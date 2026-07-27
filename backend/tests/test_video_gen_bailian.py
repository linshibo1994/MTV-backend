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
) -> VideoGenerationRequest:
    return VideoGenerationRequest(
        prompt="一名角色缓慢转身，看向镜头外。",
        reference_images=[
            {
                "storage_key": "references/first.webp",
                "mime_type": "image/webp",
            }
        ],
        reference_image_names=["角色"],
        reference_audios=reference_audios or [],
        reference_audio_names=[],
        model=model,
        job_id="bailian-protocol-test",
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
