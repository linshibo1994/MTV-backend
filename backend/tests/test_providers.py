"""统一多模态 Provider 配置、能力注册与厂商路由测试。"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.core.config import Settings


def _settings(**kwargs: Any) -> Settings:
    """构造不读取真实 .env 的确定性测试配置。"""
    return Settings(_env_file=None, **kwargs)  # type: ignore[call-arg]


def test_registry_normalizes_vendor_aliases_and_rejects_wrong_capability() -> None:
    from app.providers.registry import (
        UnsupportedCapabilityError,
        UnsupportedProviderError,
        normalize_provider,
        require_capability,
    )

    assert normalize_provider("Claude-Code") == "anthropic"
    assert normalize_provider("dashscope") == "bailian"
    assert normalize_provider("veo") == "gemini"
    assert normalize_provider("custom") == "openai_compatible"
    assert require_capability("deepseek", "chat") == "deepseek"
    with pytest.raises(UnsupportedCapabilityError, match="不支持 image"):
        require_capability("deepseek", "image")
    with pytest.raises(UnsupportedProviderError, match="unknown-vendor"):
        normalize_provider("unknown-vendor")


def test_video_capability_uses_generic_config_before_vendor_fallbacks() -> None:
    configured = _settings(
        video_provider_selection_mode="env",
        video_gen_provider="gemini",
        video_gen_api_key="capability-key",
        video_gen_base_url="https://gateway.example/v1",
        video_gen_model="veo-custom",
        google_api_key="legacy-key",
        google_video_model="veo-legacy",
    )

    assert configured.video_gen_api_key == "capability-key"
    assert configured.video_provider_selection_mode == "env"
    assert configured.video_gen_base_url == "https://gateway.example/v1"
    assert configured.video_gen_model == "veo-custom"


def test_video_provider_selection_keeps_database_mode_for_legacy_deployments() -> None:
    assert _settings().video_provider_selection_mode == "database"


@pytest.mark.parametrize(
    "base_url",
    [
        "http://provider.example/v1",
        "https://user:password@provider.example/v1",
        "https://provider.example/v1?api_key=leak",
    ],
)
def test_production_rejects_unsafe_provider_base_urls(base_url: str) -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="TEXT_LLM_BASE_URL 配置不安全"):
        _settings(
            app_env="prod",
            session_secret="MTV_9qL2-vR7!cX4@pN8#sK5$wH1%zF6&dB3",
            text_llm_base_url=base_url,
        )


def test_development_allows_local_http_provider_gateway() -> None:
    configured = _settings(text_llm_base_url="http://127.0.0.1:11434/v1/")

    assert configured.text_llm_base_url == "http://127.0.0.1:11434/v1"


def test_video_capability_falls_back_to_selected_vendor_legacy_config() -> None:
    configured = _settings(
        video_gen_provider="bailian",
        dashscope_api_key="dashscope-key",
        dashscope_base_url="https://dashscope.example",
        dashscope_video_model="wan-custom",
    )

    assert configured.video_gen_api_key == "dashscope-key"
    assert configured.video_gen_base_url == "https://dashscope.example"
    assert configured.video_gen_model == "wan-custom"


def test_image_factory_explicit_env_provider_wins_over_model_alias(monkeypatch) -> None:
    from app.core import config
    from app.providers.image_gen import get_image_provider
    from app.providers.image_gen_openai import OpenAIImageGenProvider

    configured = _settings(image_gen_provider="openai")
    monkeypatch.setattr(config, "settings", configured)

    assert isinstance(get_image_provider("wan2.1-t2i-plus"), OpenAIImageGenProvider)


def test_image_factory_keeps_legacy_model_alias_without_explicit_provider(monkeypatch) -> None:
    from app.core import config
    from app.providers.image_gen import get_image_provider
    from app.providers.image_gen_bailian import BailianImageGenProvider

    configured = _settings()
    monkeypatch.setattr(config, "settings", configured)

    assert isinstance(get_image_provider("wan2.1-t2i-plus"), BailianImageGenProvider)


def test_llm_factory_supports_anthropic_and_custom_openai_compatible(monkeypatch) -> None:
    from app.providers import _build_llm, llm_openai_compat
    from app.providers.llm_anthropic import AnthropicLLMProvider
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    monkeypatch.setattr(
        llm_openai_compat,
        "AsyncOpenAI",
        lambda **kwargs: SimpleNamespace(config=kwargs),
    )

    anthropic = _build_llm("claude-code", "key", "claude-model", "", "chat")
    custom = _build_llm(
        "openai_compatible",
        "key",
        "vendor-model",
        "https://vendor.example/v1",
        "text",
    )

    assert isinstance(anthropic, AnthropicLLMProvider)
    assert isinstance(custom, OpenAICompatLLMProvider)
    assert custom._base_url == "https://vendor.example/v1"


@pytest.mark.asyncio
async def test_openai_compatible_chat_preserves_multi_turn_messages() -> None:
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    create = AsyncMock(
        return_value=SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="最终回答"))])
    )
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "vendor-model"
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    result = await provider.chat(
        [
            {"role": "user", "content": "问题"},
            {"role": "assistant", "content": "中间回答"},
            {"role": "user", "content": "追问"},
        ],
        system="系统指令",
        temperature=0.2,
        max_tokens=512,
    )

    assert result == "最终回答"
    create.assert_awaited_once_with(
        model="vendor-model",
        messages=[
            {"role": "system", "content": "系统指令"},
            {"role": "user", "content": "问题"},
            {"role": "assistant", "content": "中间回答"},
            {"role": "user", "content": "追问"},
        ],
        temperature=0.2,
        max_tokens=512,
    )


@pytest.mark.asyncio
async def test_anthropic_chat_moves_system_messages_to_top_level(monkeypatch) -> None:
    from app.providers.llm_anthropic import AnthropicLLMProvider

    provider = AnthropicLLMProvider(api_key="key", model="claude-model")
    create = AsyncMock(return_value={"content": [{"type": "text", "text": "Claude 回答"}]})
    monkeypatch.setattr(provider, "_create", create)

    result = await provider.chat(
        [
            {"role": "system", "content": "消息内系统指令"},
            {"role": "user", "content": "问题"},
        ],
        system="顶层系统指令",
    )

    assert result == "Claude 回答"
    create.assert_awaited_once_with(
        system="顶层系统指令\n\n消息内系统指令",
        messages=[{"role": "user", "content": "问题"}],
        max_tokens=None,
        temperature=None,
    )


def test_video_factory_routes_gemini_and_rejects_unknown_vendor(monkeypatch) -> None:
    from app.core import config
    from app.providers.registry import UnsupportedProviderError
    from app.providers.video_gen import get_video_provider
    from app.providers.video_gen_google import GoogleVideoGenProvider

    monkeypatch.setattr(config, "settings", _settings())

    assert isinstance(get_video_provider("veo"), GoogleVideoGenProvider)
    with pytest.raises(UnsupportedProviderError, match="unknown-vendor"):
        get_video_provider("unknown-vendor")


@pytest.mark.asyncio
async def test_google_image_provider_uses_capability_key_url_and_model(monkeypatch) -> None:
    from app.providers import image_gen_google
    from app.providers.image_gen_google import GoogleImageGenProvider

    configured = _settings(
        image_gen_provider="gemini",
        image_gen_api_key="image-key",
        image_gen_base_url="https://gemini-gateway.example",
        image_gen_model="gemini-image-custom",
    )
    monkeypatch.setattr(image_gen_google, "settings", configured)
    calls: dict = {}

    class Models:
        async def generate_content(self, **kwargs):
            calls["generate"] = kwargs
            inline = SimpleNamespace(data=b"image-bytes")
            part = SimpleNamespace(inline_data=inline)
            return SimpleNamespace(candidates=[SimpleNamespace(content=SimpleNamespace(parts=[part]))])

    class Aio:
        models = Models()
        aclose = AsyncMock()

    class Client:
        def __init__(self, **kwargs):
            calls["client"] = kwargs
            self.aio = Aio()

    monkeypatch.setattr(image_gen_google.genai, "Client", Client)

    result = await GoogleImageGenProvider().generate("prompt", "", "2k", "16:9")

    assert result[0].data == b"image-bytes"
    assert calls["client"]["api_key"] == "image-key"
    assert calls["client"]["http_options"].base_url == "https://gemini-gateway.example"
    assert calls["generate"]["model"] == "gemini-image-custom"


@pytest.mark.asyncio
async def test_bailian_qwen_image_2_uses_sync_multimodal_api(monkeypatch) -> None:
    from app.providers import image_gen_bailian
    from app.providers.image_gen import ImageResult
    from app.providers.image_gen_bailian import BailianImageGenProvider

    configured = _settings(
        image_gen_provider="bailian",
        image_gen_api_key="image-key",
        image_gen_base_url="https://dashscope.example",
        image_gen_model="qwen-image-2.0",
    )
    monkeypatch.setattr(image_gen_bailian, "settings", configured)
    calls: dict = {}

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "output": {
                    "choices": [
                        {
                            "message": {
                                "content": [
                                    {"text": "已生成"},
                                    {"image": "https://oss.aliyuncs.com/result.png"},
                                ]
                            }
                        }
                    ]
                }
            }

    class Client:
        def __init__(self, **kwargs):
            calls["client"] = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, *, headers, json):
            calls["post"] = {"url": url, "headers": headers, "json": json}
            return Response()

    monkeypatch.setattr(image_gen_bailian.httpx, "AsyncClient", Client)
    provider = BailianImageGenProvider()
    download = AsyncMock(return_value=ImageResult(data=b"image", mime_type="image/png"))
    monkeypatch.setattr(provider, "_download", download)

    result = await provider.generate("一只纸飞机", "ignored", "2k", "1:1")

    assert result[0].data == b"image"
    assert calls["post"]["url"].endswith("/api/v1/services/aigc/multimodal-generation/generation")
    assert "X-DashScope-Async" not in calls["post"]["headers"]
    assert calls["post"]["json"]["input"]["messages"][0]["content"] == [{"text": "一只纸飞机"}]
    assert calls["post"]["json"]["parameters"]["size"] == "1024*1024"
    download.assert_awaited_once_with(
        "https://oss.aliyuncs.com/result.png",
        base_url="https://dashscope.example",
    )


@pytest.mark.asyncio
async def test_openai_image_provider_uses_capability_config_and_closes_client(monkeypatch) -> None:
    import base64

    from app.providers import image_gen_openai
    from app.providers.image_gen_openai import OpenAIImageGenProvider

    configured = _settings(
        image_gen_provider="openai",
        image_gen_api_key="image-key",
        image_gen_base_url="https://openai-gateway.example/v1",
    )
    monkeypatch.setattr(image_gen_openai, "settings", configured)
    calls: dict = {}

    class Client:
        def __init__(self, **kwargs):
            calls["client"] = kwargs
            self.images = SimpleNamespace(generate=AsyncMock(side_effect=self._generate))
            self.close = AsyncMock()

        async def _generate(self, **kwargs):
            calls["generate"] = kwargs
            encoded = base64.b64encode(b"openai-image").decode("ascii")
            return SimpleNamespace(data=[SimpleNamespace(b64_json=encoded, url=None)])

    client_holder: dict = {}

    def _client_factory(**kwargs):
        client = Client(**kwargs)
        client_holder["client"] = client
        return client

    monkeypatch.setattr(image_gen_openai, "AsyncOpenAI", _client_factory)

    result = await OpenAIImageGenProvider().generate("prompt", "ignored", "2k", "16:9")

    assert result[0].data == b"openai-image"
    assert calls["client"] == {
        "api_key": "image-key",
        "base_url": "https://openai-gateway.example/v1",
    }
    assert calls["generate"]["model"] == "gpt-image-2"
    client_holder["client"].close.assert_awaited_once()


@pytest.mark.asyncio
async def test_google_video_provider_submits_polls_and_stores(monkeypatch) -> None:
    from app.providers import video_gen_google
    from app.providers.video_gen import VideoGenerationRequest
    from app.providers.video_gen_google import GoogleVideoGenProvider

    configured = _settings(
        video_gen_provider="gemini",
        video_gen_api_key="video-key",
        video_gen_model="veo-custom",
    )
    monkeypatch.setattr(video_gen_google, "settings", configured)
    calls: dict = {}
    generated_video = SimpleNamespace(video=SimpleNamespace(video_bytes=b"video-bytes"))
    operation = SimpleNamespace(
        name="operations/veo-1",
        done=True,
        error=None,
        result=SimpleNamespace(generated_videos=[generated_video]),
        response=None,
    )

    class Models:
        async def generate_videos(self, **kwargs):
            calls["generate"] = kwargs
            return operation

    class Aio:
        models = Models()
        operations = SimpleNamespace(get=AsyncMock())
        files = SimpleNamespace(download=AsyncMock())
        aclose = AsyncMock()

    client = SimpleNamespace(aio=Aio())
    storage = SimpleNamespace(put=AsyncMock())
    provider = GoogleVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    request = VideoGenerationRequest(
        prompt="镜头提示词",
        reference_images=[],
        reference_image_names=[],
        reference_audios=[],
        reference_audio_names=[],
        model="",
        job_id="job-veo",
        duration_seconds=8,
        metadata={"aspect": "16:9"},
    )

    result = await provider.generate(request)

    assert calls["generate"]["model"] == "veo-custom"
    assert calls["generate"]["source"].prompt == "镜头提示词"
    assert calls["generate"]["config"].duration_seconds == 8
    assert calls["generate"]["config"].aspect_ratio == "16:9"
    storage.put.assert_awaited_once_with("generate_images/video/job-veo.mp4", b"video-bytes")
    assert result.provider_job_id == "operations/veo-1"
    client.aio.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_google_video_maps_reference_images_and_rejects_reference_audio(
    monkeypatch,
) -> None:
    from app.providers.registry import UnsupportedCapabilityError
    from app.providers.video_gen import VideoGenerationRequest
    from app.providers.video_gen_google import GoogleVideoGenProvider

    storage = SimpleNamespace(
        get=AsyncMock(
            side_effect=lambda key: {
                "frames/first.png": b"first",
                "refs/character.png": b"character",
                "refs/scene.jpg": b"scene",
            }[key]
        )
    )
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    request = VideoGenerationRequest(
        prompt="镜头提示词",
        reference_images=[
            {"storage_key": "refs/character.png", "mime_type": "image/png"},
            {"storage_key": "refs/scene.jpg", "mime_type": "image/jpeg"},
        ],
        reference_image_names=[],
        reference_audios=[],
        reference_audio_names=[],
        model="veo-model",
        job_id="job-veo-refs",
        first_frame="frames/first.png",
    )

    input_image, references = await GoogleVideoGenProvider._load_input_media(request)

    assert input_image is not None and input_image.image_bytes == b"first"
    assert [item.image.image_bytes for item in references if item.image] == [
        b"character",
        b"scene",
    ]

    audio_request = VideoGenerationRequest(
        prompt="镜头提示词",
        reference_images=[],
        reference_image_names=[],
        reference_audios=[{"storage_key": "audio/voice.mp3"}],
        reference_audio_names=[],
        model="veo-model",
        job_id="job-veo-audio",
    )
    with pytest.raises(UnsupportedCapabilityError, match="不支持参考音频"):
        await GoogleVideoGenProvider._load_input_media(audio_request)


@pytest.mark.asyncio
async def test_google_video_resumes_polls_and_downloads_uri(monkeypatch) -> None:
    from app.providers import video_gen_google
    from app.providers.video_gen import VideoGenerationRequest
    from app.providers.video_gen_google import GoogleVideoGenProvider

    configured = _settings(
        video_gen_provider="gemini",
        video_gen_api_key="video-key",
        video_gen_model="veo-custom",
    )
    monkeypatch.setattr(video_gen_google, "settings", configured)
    video = SimpleNamespace(video_bytes=None, uri="https://files.example/video.mp4")
    completed = SimpleNamespace(
        name="operations/veo-resume",
        done=True,
        error=None,
        result=SimpleNamespace(generated_videos=[SimpleNamespace(video=video)]),
        response=None,
    )
    operations = SimpleNamespace(get=AsyncMock(return_value=completed))
    files = SimpleNamespace(download=AsyncMock(return_value=b"downloaded-video"))

    class Models:
        async def generate_videos(self, **kwargs):
            raise AssertionError("恢复任务不得重复提交")

    aio = SimpleNamespace(
        models=Models(),
        operations=operations,
        files=files,
        aclose=AsyncMock(),
    )
    client = SimpleNamespace(aio=aio)
    storage = SimpleNamespace(put=AsyncMock())
    provider = GoogleVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    monkeypatch.setattr(video_gen_google.asyncio, "sleep", AsyncMock())
    request = VideoGenerationRequest(
        prompt="镜头提示词",
        reference_images=[],
        reference_image_names=[],
        reference_audios=[],
        reference_audio_names=[],
        model="veo-custom",
        job_id="job-veo-resume",
        metadata={"provider_task_id": "operations/veo-resume"},
    )

    result = await provider.generate(request)

    operations.get.assert_awaited_once()
    files.download.assert_awaited_once_with(file=video)
    storage.put.assert_awaited_once_with("generate_images/video/job-veo-resume.mp4", b"downloaded-video")
    assert result.provider_job_id == "operations/veo-resume"


@pytest.mark.asyncio
async def test_google_video_operation_error_is_reported() -> None:
    from app.providers.video_gen_google import GoogleVideoGenProvider

    operation = SimpleNamespace(
        name="operations/veo-failed",
        done=True,
        error={"code": 13},
    )
    with pytest.raises(RuntimeError, match="Gemini Veo 生成失败"):
        await GoogleVideoGenProvider()._poll(
            SimpleNamespace(),
            operation,
            lambda *_args: None,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_video_service_rejects_gemini_before_database_migration(monkeypatch) -> None:
    from app.providers.registry import ProviderConfigurationError
    from app.services import video_generation

    configured = _settings(
        video_provider_selection_mode="env",
        video_gen_provider="gemini",
        video_gen_api_key="video-key",
    )
    monkeypatch.setattr(video_generation, "settings", configured)

    with pytest.raises(ProviderConfigurationError, match="Alembic 迁移"):
        await video_generation.create_video_job(
            SimpleNamespace(),  # type: ignore[arg-type]
            script_id="script-1",
            episode_id="episode-1",
            shot_group_id="group-1",
            user_id="user-1",
            idempotency_key="idem-1",
        )
