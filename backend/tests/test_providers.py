"""统一多模态 Provider 配置、能力注册与厂商路由测试。"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.core.config import Settings


def _settings(**kwargs: Any) -> Settings:
    """构造不读取真实 .env 的确定性测试配置。"""
    return Settings(_env_file=None, **kwargs)  # type: ignore[call-arg]


class _AtomicFileStorage:
    """测试视频输出仅允许文件上传与原子提升。"""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.deleted: list[str] = []

    async def put(self, _key: str, _data: bytes) -> str:
        raise AssertionError("视频输出禁止使用 storage.put(bytes)")

    async def put_file(self, key: str, path: str) -> str:
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
    assert normalize_provider("AICC") == "mgrouter"
    assert normalize_provider("mg-router") == "mgrouter"
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


def test_mgrouter_video_capability_uses_dedicated_fallbacks() -> None:
    configured = _settings(
        video_gen_provider="aicc",
        mgrouter_api_key="mgrouter-key",
        mgrouter_base_url="https://gateway.example/api/v3/",
        mgrouter_video_model="AICC-Seedance-Test",
    )

    assert configured.video_gen_api_key == "mgrouter-key"
    assert configured.video_gen_base_url == "https://gateway.example/api/v3"
    assert configured.video_gen_model == "AICC-Seedance-Test"


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
@pytest.mark.parametrize(
    "response",
    [
        SimpleNamespace(choices=[]),
        SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content=None,
                        reasoning_content="不得返回或泄露的推理内容",
                    )
                )
            ]
        ),
        SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=["非字符串正文"]))]
        ),
        SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=" \n"))]
        ),
    ],
    ids=["empty-choices", "none-with-reasoning", "non-string", "blank"],
)
async def test_openai_compatible_chat_rejects_invalid_assistant_content(response) -> None:
    from app.providers.llm import LLMResponseContractError
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    create = AsyncMock(return_value=response)
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "vendor-model"
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    with pytest.raises(LLMResponseContractError) as exc_info:
        await provider.chat([{"role": "user", "content": "问题"}])

    assert str(exc_info.value) == "LLM 供应商返回了无效的 assistant 正文"
    assert "不得返回或泄露的推理内容" not in str(exc_info.value)
    assert "非字符串正文" not in str(exc_info.value)
    create.assert_awaited_once()


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


@pytest.mark.asyncio
async def test_anthropic_chat_concatenates_multiple_text_blocks_in_order(
    monkeypatch,
) -> None:
    from app.providers.llm_anthropic import AnthropicLLMProvider

    provider = AnthropicLLMProvider(api_key="key", model="claude-model")
    create = AsyncMock(
        return_value={
            "content": [
                {"type": "text", "text": "第一段"},
                {"type": "tool_use", "input": {"ignored": True}},
                {"type": "text", "text": "\n第二段"},
            ]
        }
    )
    monkeypatch.setattr(provider, "_create", create)

    result = await provider.chat([{"role": "user", "content": "问题"}])

    assert result == "第一段\n第二段"
    create.assert_awaited_once()


@pytest.mark.asyncio
async def test_anthropic_chat_rejects_malformed_text_mixed_with_valid_text(
    monkeypatch,
) -> None:
    from app.providers.llm import LLMResponseContractError
    from app.providers.llm_anthropic import AnthropicLLMProvider

    provider = AnthropicLLMProvider(api_key="key", model="claude-model")
    create = AsyncMock(
        return_value={
            "content": [
                {"type": "text", "text": "不可返回的部分正文"},
                {"type": "text", "text": None},
            ]
        }
    )
    monkeypatch.setattr(provider, "_create", create)

    with pytest.raises(LLMResponseContractError) as exc_info:
        await provider.chat([{"role": "user", "content": "问题"}])

    assert str(exc_info.value) == "LLM 供应商返回了无效的 assistant 正文"
    assert "不可返回的部分正文" not in str(exc_info.value)
    create.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        {"content": []},
        {"content": [{"type": "tool_use", "input": {"secret": "不得泄露"}}]},
        {"content": [{"type": "text", "text": None}]},
        {"content": [{"type": "text", "text": " \n"}]},
    ],
    ids=["empty-content", "no-text-block", "non-string-text", "blank-text"],
)
async def test_anthropic_chat_rejects_responses_without_valid_text(
    monkeypatch, response
) -> None:
    from app.providers.llm import LLMResponseContractError
    from app.providers.llm_anthropic import AnthropicLLMProvider

    provider = AnthropicLLMProvider(api_key="key", model="claude-model")
    create = AsyncMock(return_value=response)
    monkeypatch.setattr(provider, "_create", create)

    with pytest.raises(LLMResponseContractError) as exc_info:
        await provider.chat([{"role": "user", "content": "问题"}])

    assert str(exc_info.value) == "LLM 供应商返回了无效的 assistant 正文"
    assert "不得泄露" not in str(exc_info.value)
    create.assert_awaited_once()


def test_video_factory_routes_gemini_and_rejects_unknown_vendor(monkeypatch) -> None:
    from app.core import config
    from app.providers.registry import UnsupportedProviderError
    from app.providers.video_gen import get_video_provider
    from app.providers.video_gen_google import GoogleVideoGenProvider
    from app.providers.video_gen_mgrouter import MgrouterVideoGenProvider
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    monkeypatch.setattr(config, "settings", _settings())

    for alias in ("gemini", "google", "veo"):
        assert isinstance(get_video_provider(alias), GoogleVideoGenProvider)
    for alias in ("volcengine", "doubao", "jianyu", "modelsai"):
        assert isinstance(get_video_provider(alias), VolcengineVideoGenProvider)
    for alias in ("mgrouter", "mg-router", "aicc", "aicc-seedance"):
        assert isinstance(get_video_provider(alias), MgrouterVideoGenProvider)
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

    result = await GoogleImageGenProvider().generate(
        "prompt",
        "",
        "2k",
        "16:9",
        negative_prompt="photorealistic",
    )

    assert result[0].data == b"image-bytes"
    assert calls["client"]["api_key"] == "image-key"
    assert calls["client"]["http_options"].base_url == "https://gemini-gateway.example"
    assert calls["generate"]["model"] == "gemini-image-custom"
    assert calls["generate"]["contents"] == (
        "prompt\n\nDo not include in the generated image: photorealistic"
    )
    image_config = calls["generate"]["config"].image_config
    assert image_config.aspect_ratio == "16:9"
    assert image_config.image_size == "2K"


@pytest.mark.asyncio
async def test_google_image_provider_rejects_unsupported_image_size(monkeypatch) -> None:
    from app.providers import image_gen_google
    from app.providers.image_gen_google import GoogleImageGenProvider
    from app.providers.registry import ProviderConfigurationError

    monkeypatch.setattr(
        image_gen_google,
        "settings",
        _settings(image_gen_api_key="image-key"),
    )

    with pytest.raises(ProviderConfigurationError, match="1K、2K 或 4K"):
        await GoogleImageGenProvider().generate(
            "prompt",
            "banana-2",
            "8k",
            "16:9",
        )


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

    result = await provider.generate(
        "一只纸飞机",
        "ignored",
        "2k",
        "1:1",
        negative_prompt="写实摄影",
    )

    assert result[0].data == b"image"
    assert calls["post"]["url"].endswith("/api/v1/services/aigc/multimodal-generation/generation")
    assert "X-DashScope-Async" not in calls["post"]["headers"]
    assert calls["post"]["json"]["input"]["messages"][0]["content"] == [{"text": "一只纸飞机"}]
    assert calls["post"]["json"]["parameters"]["size"] == "1024*1024"
    assert calls["post"]["json"]["parameters"]["negative_prompt"] == "写实摄影"
    download.assert_awaited_once_with(
        "https://oss.aliyuncs.com/result.png",
        base_url="https://dashscope.example",
    )


@pytest.mark.asyncio
async def test_bailian_qwen_image_legacy_passes_negative_prompt(monkeypatch) -> None:
    from app.providers import image_gen_bailian
    from app.providers.image_gen import ImageResult
    from app.providers.image_gen_bailian import BailianImageGenProvider

    configured = _settings(
        image_gen_provider="bailian",
        image_gen_api_key="image-key",
        image_gen_base_url="https://dashscope.example",
        image_gen_model="qwen-image",
    )
    monkeypatch.setattr(image_gen_bailian, "settings", configured)
    calls: dict = {}

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {"output": {"task_id": "task-1"}}

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
    monkeypatch.setattr(
        provider,
        "_poll",
        AsyncMock(return_value=["https://oss.aliyuncs.com/result.png"]),
    )
    monkeypatch.setattr(
        provider,
        "_download",
        AsyncMock(return_value=ImageResult(data=b"image", mime_type="image/png")),
    )

    result = await provider.generate(
        "一只纸飞机",
        "ignored",
        "2k",
        "1:1",
        negative_prompt="写实摄影",
    )

    assert result[0].data == b"image"
    assert calls["post"]["url"].endswith("/api/v1/services/aigc/text2image/image-synthesis")
    assert calls["post"]["headers"]["X-DashScope-Async"] == "enable"
    assert calls["post"]["json"]["input"]["negative_prompt"] == "写实摄影"


@pytest.mark.asyncio
async def test_bailian_image_provider_retries_429_with_backoff(monkeypatch) -> None:
    from app.providers import image_gen_bailian
    from app.providers.image_gen import ImageResult
    from app.providers.image_gen_bailian import BailianImageGenProvider

    monkeypatch.setattr(
        image_gen_bailian,
        "settings",
        _settings(
            image_gen_provider="bailian",
            image_gen_api_key="image-key",
            image_gen_base_url="https://dashscope.example",
            image_gen_model="qwen-image-2.0",
        ),
    )

    class Response:
        def __init__(self, status_code: int, *, retry_after: str | None = None) -> None:
            self.status_code = status_code
            self.headers = {"Retry-After": retry_after} if retry_after else {}

        @staticmethod
        def json():
            return {
                "output": {
                    "choices": [
                        {
                            "message": {
                                "content": [{"image": "https://oss.aliyuncs.com/result.png"}]
                            }
                        }
                    ]
                }
            }

    responses = [Response(429, retry_after="0.25"), Response(429), Response(200)]
    post = AsyncMock(side_effect=responses)

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, *args, **kwargs):
            return await post(*args, **kwargs)

    sleep = AsyncMock()
    monkeypatch.setattr(image_gen_bailian.httpx, "AsyncClient", Client)
    monkeypatch.setattr(image_gen_bailian.asyncio, "sleep", sleep)
    monkeypatch.setattr(image_gen_bailian.random, "uniform", lambda *_args: 0.0)
    provider = BailianImageGenProvider()
    monkeypatch.setattr(
        provider,
        "_download",
        AsyncMock(return_value=ImageResult(data=b"image", mime_type="image/png")),
    )

    result = await provider.generate("一只纸飞机", "ignored", "2k", "1:1")

    assert result[0].data == b"image"
    assert post.await_count == 3
    assert [call.args[0] for call in sleep.await_args_list] == [0.25, 2.0]


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [400, 503])
async def test_bailian_image_provider_does_not_retry_other_errors(
    monkeypatch, status_code: int
) -> None:
    from app.providers import image_gen_bailian
    from app.providers.image_gen_bailian import BailianImageGenProvider

    monkeypatch.setattr(
        image_gen_bailian,
        "settings",
        _settings(
            image_gen_provider="bailian",
            image_gen_api_key="image-key",
            image_gen_base_url="https://dashscope.example",
            image_gen_model="qwen-image-2.0",
        ),
    )

    class Response:
        headers: dict[str, str] = {}

        def __init__(self) -> None:
            self.status_code = status_code

    post = AsyncMock(return_value=Response())

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, *args, **kwargs):
            return await post(*args, **kwargs)

    sleep = AsyncMock()
    monkeypatch.setattr(image_gen_bailian.httpx, "AsyncClient", Client)
    monkeypatch.setattr(image_gen_bailian.asyncio, "sleep", sleep)

    with pytest.raises(RuntimeError, match=f"status={status_code}"):
        await BailianImageGenProvider().generate("一只纸飞机", "ignored", "2k", "1:1")

    assert post.await_count == 1
    sleep.assert_not_awaited()


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

    result = await OpenAIImageGenProvider().generate(
        "prompt",
        "ignored",
        "2k",
        "16:9",
        negative_prompt="photorealistic",
    )

    assert result[0].data == b"openai-image"
    assert calls["client"] == {
        "api_key": "image-key",
        "base_url": "https://openai-gateway.example/v1",
    }
    assert calls["generate"]["model"] == "gpt-image-2"
    assert calls["generate"]["prompt"] == (
        "prompt\n\nDo not include in the generated image: photorealistic"
    )
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
    storage = _AtomicFileStorage()
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
    assert storage.objects == {"generate_images/video/job-veo.mp4": b"video-bytes"}
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
    video = SimpleNamespace(
        video_bytes=None,
        uri="https://generativelanguage.googleapis.com/v1beta/files/veo-resume",
    )
    completed = SimpleNamespace(
        name="operations/veo-resume",
        done=True,
        error=None,
        result=SimpleNamespace(generated_videos=[SimpleNamespace(video=video)]),
        response=None,
    )
    operations = SimpleNamespace(get=AsyncMock(return_value=completed))
    files = SimpleNamespace(
        download=AsyncMock(side_effect=AssertionError("禁止 eager files.download"))
    )

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
    storage = _AtomicFileStorage()
    provider = GoogleVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr("app.providers.storage.get_storage", lambda: storage)
    monkeypatch.setattr(video_gen_google.asyncio, "sleep", AsyncMock())
    download_calls: list[tuple[str, dict]] = []

    async def download(url: str, path: str | Path, **kwargs) -> int:
        download_calls.append((url, kwargs))
        kwargs["url_validator"](url)
        return Path(path).write_bytes(b"downloaded-video")

    monkeypatch.setattr(video_gen_google, "safe_download_to_file", download)
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
    files.download.assert_not_awaited()
    assert download_calls[0][0] == (
        "https://generativelanguage.googleapis.com/v1beta/files/veo-resume:download?alt=media"
    )
    assert download_calls[0][1]["extra_headers"] == {"x-goog-api-key": "video-key"}
    assert storage.objects == {
        "generate_images/video/job-veo-resume.mp4": b"downloaded-video"
    }
    assert result.provider_job_id == "operations/veo-resume"


@pytest.mark.asyncio
async def test_google_video_operation_error_is_reported() -> None:
    from app.providers.video_gen import VideoGenerationRemoteTerminalError
    from app.providers.video_gen_google import GoogleVideoGenProvider

    operation = SimpleNamespace(
        name="operations/veo-failed",
        done=True,
        error={"code": 13},
    )
    with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
        await GoogleVideoGenProvider()._poll(
            SimpleNamespace(),
            operation,
            lambda *_args: None,  # type: ignore[arg-type]
        )

    assert exc_info.value.provider_task_id == "operations/veo-failed"
    assert exc_info.value.terminal_status == "failed"


@pytest.mark.asyncio
async def test_openai_video_explicit_failed_status_is_typed_terminal_error(tmp_path) -> None:
    from app.providers.video_gen import VideoGenerationRemoteTerminalError
    from app.providers.video_gen_openai import OpenAIVideoGenProvider

    client = SimpleNamespace(
        videos=SimpleNamespace(
            retrieve=AsyncMock(return_value=SimpleNamespace(status="failed"))
        )
    )
    provider = OpenAIVideoGenProvider()
    with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
        await provider._poll_and_download_to_file(
            client,
            "openai-terminal-task",
            tmp_path / "video.mp4",
            lambda *_args: None,
        )

    assert exc_info.value.provider_task_id == "openai-terminal-task"
    assert exc_info.value.terminal_status == "failed"


@pytest.mark.asyncio
async def test_bailian_video_explicit_failed_status_is_typed_terminal_error() -> None:
    from app.providers.video_gen import VideoGenerationRemoteTerminalError
    from app.providers.video_gen_bailian import BailianVideoGenProvider

    response = SimpleNamespace(
        status_code=200,
        json=lambda: {"output": {"task_status": "FAILED"}},
    )
    http = SimpleNamespace(get=AsyncMock(return_value=response))
    with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
        await BailianVideoGenProvider()._poll(
            http,
            "https://dashscope.example",
            "redacted-test-key",
            "bailian-terminal-task",
            lambda *_args: None,
        )

    assert exc_info.value.provider_task_id == "bailian-terminal-task"
    assert exc_info.value.terminal_status == "failed"


@pytest.mark.asyncio
async def test_bailian_video_unknown_status_is_not_confirmed_terminal() -> None:
    from app.providers.video_gen_bailian import BailianVideoGenProvider

    response = SimpleNamespace(
        status_code=200,
        json=lambda: {"output": {"task_status": "UNKNOWN"}},
    )
    http = SimpleNamespace(get=AsyncMock(return_value=response))
    with pytest.raises(RuntimeError, match="status_unknown"):
        await BailianVideoGenProvider()._poll(
            http,
            "https://dashscope.example",
            "redacted-test-key",
            "bailian-unknown-task",
            lambda *_args: None,
        )


@pytest.mark.asyncio
async def test_volcengine_video_explicit_cancelled_status_is_typed_terminal_error(
    monkeypatch,
) -> None:
    from app.providers.video_gen import VideoGenerationRemoteTerminalError
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    tasks = SimpleNamespace(
        create=AsyncMock(side_effect=AssertionError("恢复任务不得重复提交")),
        get=AsyncMock(return_value=SimpleNamespace(status="cancelled")),
    )
    client = SimpleNamespace(content_generation=SimpleNamespace(tasks=tasks))
    provider = VolcengineVideoGenProvider()
    monkeypatch.setattr(provider, "_build_client", lambda: client)
    monkeypatch.setattr(provider, "_close_client_safely", AsyncMock())

    with pytest.raises(VideoGenerationRemoteTerminalError) as exc_info:
        await provider._submit_and_poll(
            "video-model",
            "prompt",
            [],
            [],
            None,
            lambda *_args: None,
            provider_task_id="volcengine-terminal-task",
        )

    assert exc_info.value.provider_task_id == "volcengine-terminal-task"
    assert exc_info.value.terminal_status == "canceled"
    tasks.create.assert_not_awaited()
