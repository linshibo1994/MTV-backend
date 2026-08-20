"""统一多模态 Provider 配置、能力注册与厂商路由测试。"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal
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
    assert normalize_provider("Neo-Link") == "neolink"
    assert normalize_provider("custom") == "openai_compatible"
    assert require_capability("deepseek", "chat") == "deepseek"
    with pytest.raises(UnsupportedCapabilityError, match="不支持 image"):
        require_capability("deepseek", "image")
    with pytest.raises(UnsupportedProviderError, match="unknown-vendor"):
        normalize_provider("unknown-vendor")


@pytest.mark.parametrize("provider", ["neolink", "neo-link", "neo_link"])
@pytest.mark.parametrize("capability", ["text", "chat"])
def test_neolink_registry_supports_both_aliases_for_text_and_chat(
    provider: str,
    capability: Literal["text", "chat"],
) -> None:
    from app.providers.registry import require_capability

    assert require_capability(provider, capability) == "neolink"


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


def test_neolink_video_capability_only_needs_dedicated_url_and_key() -> None:
    configured = _settings(
        video_gen_provider="neolink",
        neolink_api_key="neolink-key",
        neolink_base_url="https://neolink.com/api/",
    )

    assert configured.video_gen_api_key == "neolink-key"
    assert configured.video_gen_base_url == "https://neolink.com/api"
    assert configured.video_gen_model == "doubao-seedance-2-0-260128"


def test_neolink_image_capability_falls_back_to_dedicated_config() -> None:
    configured = _settings(
        image_gen_provider="neolink",
        neolink_api_key="neolink-key",
        neolink_base_url="https://neolink.com/api/",
    )

    assert configured.image_gen_api_key == "neolink-key"
    assert configured.image_gen_base_url == "https://neolink.com/api"
    assert configured.image_gen_model == "gp-im-2"


@pytest.mark.parametrize(
    ("model", "expected_provider"),
    [
        ("banana-2", "google"),
        ("gpt-image-2", "openai"),
        ("wan2.1-t2i-plus", "bailian"),
        ("qwen-image", "bailian"),
        ("qwen-image-2.0", "bailian"),
        ("gp-im-2", "neolink"),
    ],
)
def test_image_factory_recognized_model_alias_wins_over_explicit_env_provider(
    monkeypatch, model: str, expected_provider: str
) -> None:
    from app.core import config
    from app.providers.image_gen import get_image_provider
    from app.providers.image_gen_bailian import BailianImageGenProvider
    from app.providers.image_gen_google import GoogleImageGenProvider
    from app.providers.image_gen_neolink import NeolinkImageGenProvider
    from app.providers.image_gen_openai import OpenAIImageGenProvider

    configured = _settings(image_gen_provider="openai")
    monkeypatch.setattr(config, "settings", configured)

    expected_types = {
        "bailian": BailianImageGenProvider,
        "google": GoogleImageGenProvider,
        "neolink": NeolinkImageGenProvider,
        "openai": OpenAIImageGenProvider,
    }
    assert isinstance(get_image_provider(model), expected_types[expected_provider])


def test_image_factory_unknown_model_uses_explicit_env_provider(monkeypatch) -> None:
    from app.core import config
    from app.providers.image_gen import get_image_provider
    from app.providers.image_gen_google import GoogleImageGenProvider

    monkeypatch.setattr(config, "settings", _settings(image_gen_provider="gemini"))

    assert isinstance(get_image_provider("custom-image-model"), GoogleImageGenProvider)


def test_image_factory_keeps_legacy_model_alias_without_explicit_provider(monkeypatch) -> None:
    from app.core import config
    from app.providers.image_gen import get_image_provider
    from app.providers.image_gen_bailian import BailianImageGenProvider

    configured = _settings()
    monkeypatch.setattr(config, "settings", configured)

    assert isinstance(get_image_provider("wan2.1-t2i-plus"), BailianImageGenProvider)


@pytest.mark.parametrize(
    ("selected_provider", "vendor"),
    [
        ("google", "gemini"),
        ("gemini", "google"),
        ("dashscope", "bailian"),
        ("aliyun", "qwen"),
        ("neo-link", "neolink"),
    ],
)
def test_image_gen_provider_override_normalizes_vendor_aliases(
    selected_provider: str,
    vendor: str,
) -> None:
    from app.providers.image_gen import image_gen_provider_is_selected

    configured = _settings(image_gen_provider=selected_provider)

    assert image_gen_provider_is_selected(vendor, configuration=configured) is True


@pytest.mark.parametrize("selected_provider", ["", "unsupported-image-vendor"])
def test_image_gen_provider_override_ignores_empty_or_unknown_vendor(selected_provider: str) -> None:
    from app.providers.image_gen import image_gen_provider_is_selected

    configured = _settings(image_gen_provider=selected_provider)

    assert image_gen_provider_is_selected("bailian", configuration=configured) is False


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


def test_llm_factory_supports_neolink_alias_for_text_and_chat(monkeypatch) -> None:
    from app.providers import _build_llm, llm_openai_compat
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    monkeypatch.setattr(
        llm_openai_compat,
        "AsyncOpenAI",
        lambda **kwargs: SimpleNamespace(config=kwargs),
    )

    providers = [
        _build_llm(
            provider_name,
            "neolink-key",
            "qwen-plus",
            "https://neolink.example/api",
            capability,
        )
        for provider_name in ("neolink", "neo-link", "neo_link")
        for capability in ("text", "chat")
    ]

    assert all(isinstance(provider, OpenAICompatLLMProvider) for provider in providers)
    assert [provider._vendor for provider in providers] == ["neolink"] * 6
    assert [provider._base_url for provider in providers] == [
        "https://neolink.example/api/v1",
        "https://neolink.example/api/v1",
        "https://neolink.example/api/v1",
        "https://neolink.example/api/v1",
        "https://neolink.example/api/v1",
        "https://neolink.example/api/v1",
    ]


def test_neolink_settings_and_factory_use_default_openai_compatible_base_url(monkeypatch) -> None:
    from app.providers import _build_llm, llm_openai_compat
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    monkeypatch.setattr(
        llm_openai_compat,
        "AsyncOpenAI",
        lambda **kwargs: SimpleNamespace(config=kwargs),
    )
    configured = _settings(
        text_llm_provider="neolink",
        chat_llm_provider="neolink",
        neolink_api_key="neolink-key",
    )

    assert (configured.text_llm_api_key, configured.chat_llm_api_key) == (
        "neolink-key",
        "neolink-key",
    )
    assert (configured.text_llm_base_url, configured.chat_llm_base_url) == (
        "https://neolink.com/api",
        "https://neolink.com/api",
    )

    providers = [
        _build_llm(
            configured.text_llm_provider,
            configured.text_llm_api_key,
            configured.text_llm_model,
            configured.text_llm_base_url,
            "text",
        ),
        _build_llm(
            configured.chat_llm_provider,
            configured.chat_llm_api_key,
            configured.chat_llm_model,
            configured.chat_llm_base_url,
            "chat",
        ),
    ]

    assert all(isinstance(provider, OpenAICompatLLMProvider) for provider in providers)
    assert [provider._base_url for provider in providers] == [
        "https://neolink.com/api/v1",
        "https://neolink.com/api/v1",
    ]
    assert [provider._client.config["base_url"] for provider in providers] == [
        "https://neolink.com/api/v1",
        "https://neolink.com/api/v1",
    ]


@pytest.mark.parametrize(
    ("base_url", "expected_base_url"),
    [
        ("https://neolink.example/api", "https://neolink.example/api/v1"),
        ("https://neolink.example/api/v1/", "https://neolink.example/api/v1"),
    ],
)
def test_neolink_llm_normalizes_openai_compatible_base_url(
    monkeypatch,
    base_url: str,
    expected_base_url: str,
) -> None:
    from app.providers import llm_openai_compat
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    monkeypatch.setattr(
        llm_openai_compat,
        "AsyncOpenAI",
        lambda **kwargs: SimpleNamespace(config=kwargs),
    )

    provider = OpenAICompatLLMProvider(
        vendor="neolink",
        api_key="neolink-key",
        model="qwen-plus",
        base_url=base_url,
    )

    assert provider._base_url == expected_base_url
    assert provider._client.config["base_url"] == expected_base_url


def test_neolink_llm_uses_documented_default_openai_compatible_base_url(monkeypatch) -> None:
    from app.providers import llm_openai_compat
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    monkeypatch.setattr(
        llm_openai_compat,
        "AsyncOpenAI",
        lambda **kwargs: SimpleNamespace(config=kwargs),
    )

    provider = OpenAICompatLLMProvider(
        vendor="neolink",
        api_key="neolink-key",
        model="qwen-plus",
        base_url="",
    )

    assert provider._base_url == "https://neolink.com/api/v1"
    assert provider._client.config["base_url"] == "https://neolink.com/api/v1"


@pytest.mark.parametrize(
    "base_url",
    [
        "http://neolink.example/api",
        "https://user:password@neolink.example/api",
        "https://neolink.example/api?token=private",
        "https://neolink.example/api#fragment",
    ],
    ids=["http", "userinfo", "query", "fragment"],
)
def test_neolink_llm_rejects_unsafe_openai_compatible_base_url(base_url: str) -> None:
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider
    from app.providers.registry import ProviderConfigurationError

    with pytest.raises(ProviderConfigurationError, match="NEOLINK_BASE_URL"):
        OpenAICompatLLMProvider(
            vendor="neolink",
            api_key="neolink-key",
            model="qwen-plus",
            base_url=base_url,
        )


@pytest.mark.asyncio
async def test_openai_complete_json_uses_strict_json_schema_when_provided() -> None:
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    create = AsyncMock(
        return_value=SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"title": "分镜"}'))]
        )
    )
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "gpt-test"
    provider._vendor = "openai"
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )
    response_schema = {
        "type": "object",
        "properties": {"title": {"type": "string"}},
        "required": ["title"],
        "additionalProperties": False,
    }

    result = await provider.complete_json(
        "系统 JSON 指令",
        "用户输入",
        response_schema=response_schema,
    )

    assert result == {"title": "分镜"}
    create.assert_awaited_once_with(
        model="gpt-test",
        messages=[
            {"role": "system", "content": "系统 JSON 指令"},
            {"role": "user", "content": "用户输入"},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "structured_response",
                "strict": True,
                "schema": response_schema,
            },
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [400, 422])
async def test_openai_strict_schema_falls_back_once_for_schema_compatibility_status(
    status_code: int,
) -> None:
    from httpx import Request, Response
    from openai import BadRequestError, UnprocessableEntityError

    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    response_body = "STRICT_SCHEMA_PRIVATE_RESPONSE_BODY"
    error_type = BadRequestError if status_code == 400 else UnprocessableEntityError
    schema_error = error_type(
        response_body,
        response=Response(
            status_code,
            request=Request("POST", "https://provider.example/v1/chat/completions"),
            json={"error": {"message": response_body}},
        ),
        body={"error": {"message": response_body}},
    )
    create = AsyncMock(
        side_effect=[
            schema_error,
            SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content='{"title": "降级成功"}'))]
            ),
        ]
    )
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "gpt-test"
    provider._vendor = "openai"
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )
    response_schema = {"type": "object", "additionalProperties": False}

    result = await provider.complete_json(
        "系统 json 指令",
        "用户输入",
        response_schema=response_schema,
    )

    assert result == {"title": "降级成功"}
    assert create.await_count == 2
    first_kwargs = create.await_args_list[0].kwargs
    second_kwargs = create.await_args_list[1].kwargs
    assert first_kwargs["response_format"] == {
        "type": "json_schema",
        "json_schema": {
            "name": "structured_response",
            "strict": True,
            "schema": response_schema,
        },
    }
    assert second_kwargs["response_format"] == {"type": "json_object"}


@pytest.mark.asyncio
async def test_openai_strict_schema_fallback_runs_once_and_sanitizes_failure() -> None:
    from httpx import Request, Response
    from openai import BadRequestError, UnprocessableEntityError

    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    first_body = "FIRST_PRIVATE_RESPONSE_BODY"
    fallback_body = "FALLBACK_PRIVATE_RESPONSE_BODY"
    request = Request("POST", "https://provider.example/v1/chat/completions")
    create = AsyncMock(
        side_effect=[
            BadRequestError(
                first_body,
                response=Response(400, request=request, json={"error": {"message": first_body}}),
                body={"error": {"message": first_body}},
            ),
            UnprocessableEntityError(
                fallback_body,
                response=Response(422, request=request, json={"error": {"message": fallback_body}}),
                body={"error": {"message": fallback_body}},
            ),
        ]
    )
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "gpt-test"
    provider._vendor = "openai"
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    with pytest.raises(RuntimeError, match="status=422") as exc_info:
        await provider.complete_json(
            "系统 json 指令",
            "用户输入",
            response_schema={"type": "object"},
        )

    assert create.await_count == 2
    assert first_body not in str(exc_info.value)
    assert fallback_body not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


@pytest.mark.asyncio
async def test_openai_strict_schema_does_not_fallback_for_auth_error() -> None:
    from httpx import Request, Response
    from openai import AuthenticationError

    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    response_body = "AUTH_PRIVATE_RESPONSE_BODY"
    create = AsyncMock(
        side_effect=AuthenticationError(
            response_body,
            response=Response(
                401,
                request=Request("POST", "https://provider.example/v1/chat/completions"),
                json={"error": {"message": response_body}},
            ),
            body={"error": {"message": response_body}},
        )
    )
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "gpt-test"
    provider._vendor = "openai"
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    with pytest.raises(RuntimeError, match="status=401") as exc_info:
        await provider.complete_json(
            "系统 json 指令",
            "用户输入",
            response_schema={"type": "object"},
        )

    assert create.await_count == 1
    assert response_body not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("vendor", "response_schema"),
    [
        ("openai", None),
        ("openai_compatible", {"type": "object"}),
        ("deepseek", {"type": "object"}),
        ("bailian", {"type": "object"}),
    ],
)
async def test_openai_compatible_complete_json_keeps_json_object_for_compatible_endpoints(
    vendor: str,
    response_schema: dict | None,
) -> None:
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    create = AsyncMock(
        return_value=SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="{}"))]
        )
    )
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "vendor-model"
    provider._vendor = vendor
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    if response_schema is None:
        result = await provider.complete_json("系统 json 指令", "用户输入")
    else:
        result = await provider.complete_json(
            "系统 json 指令",
            "用户输入",
            response_schema=response_schema,
        )

    assert result == {}
    create.assert_awaited_once_with(
        model="vendor-model",
        messages=[
            {"role": "system", "content": "系统 json 指令"},
            {"role": "user", "content": "用户输入"},
        ],
        response_format={"type": "json_object"},
    )


@pytest.mark.asyncio
async def test_neolink_complete_json_uses_documented_json_object_response_format() -> None:
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    create = AsyncMock(
        return_value=SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"title": "NeoLink"}'))]
        )
    )
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "qwen-plus"
    provider._vendor = "neolink"
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    result = await provider.complete_json(
        "请输出 JSON",
        "生成标题",
        response_schema={"type": "object"},
    )

    assert result == {"title": "NeoLink"}
    create.assert_awaited_once_with(
        model="qwen-plus",
        messages=[
            {"role": "system", "content": "请输出 JSON"},
            {"role": "user", "content": "生成标题"},
        ],
        response_format={"type": "json_object"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        SimpleNamespace(choices=[]),
        SimpleNamespace(choices=[SimpleNamespace(reasoning_content="NEOLINK_PRIVATE_REASONING")]),
        SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content=None,
                        reasoning_content="NEOLINK_PRIVATE_REASONING",
                    )
                )
            ]
        ),
    ],
    ids=["empty-choices", "missing-message", "none-content"],
)
async def test_neolink_complete_json_rejects_invalid_assistant_content(response) -> None:
    from app.providers.llm import LLMResponseContractError
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    create = AsyncMock(return_value=response)
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "qwen-plus"
    provider._vendor = "neolink"
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    with pytest.raises(LLMResponseContractError) as exc_info:
        await provider.complete_json("请输出 JSON", "生成标题")

    assert str(exc_info.value) == "LLM 供应商返回了无效的 assistant 正文"
    assert "NEOLINK_PRIVATE_REASONING" not in str(exc_info.value)
    create.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("content", ["", " \n"], ids=["empty-string", "blank-content"])
async def test_neolink_complete_json_reports_invalid_json_for_empty_or_blank_content(
    content: str,
) -> None:
    from app.providers.llm import LLMJSONResponseError
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    create = AsyncMock(
        return_value=SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content=content,
                        reasoning_content="NEOLINK_PRIVATE_REASONING",
                    )
                )
            ]
        )
    )
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "qwen-plus"
    provider._vendor = "neolink"
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    with pytest.raises(LLMJSONResponseError) as exc_info:
        await provider.complete_json("请输出 JSON", "生成标题")

    assert "NEOLINK_PRIVATE_REASONING" not in str(exc_info.value)
    create.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("error_name", ["rate-limit", "timeout"])
async def test_neolink_complete_json_maps_transient_openai_errors_to_connection_error(
    error_name: str,
) -> None:
    from httpx import Request, Response
    from openai import APITimeoutError, RateLimitError

    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    request = Request("POST", "https://neolink.example/api/v1/chat/completions")
    error = (
        RateLimitError(
            "NEOLINK_PRIVATE_RATE_LIMIT_BODY",
            response=Response(429, request=request),
            body={"error": "NEOLINK_PRIVATE_RATE_LIMIT_BODY"},
        )
        if error_name == "rate-limit"
        else APITimeoutError(request)
    )
    create = AsyncMock(side_effect=error)
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "qwen-plus"
    provider._vendor = "neolink"
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    with pytest.raises(ConnectionError, match="LLM 供应商暂不可用") as exc_info:
        await provider.complete_json("请输出 JSON", "生成标题")

    assert "NEOLINK_PRIVATE_RATE_LIMIT_BODY" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    create.assert_awaited_once()


@pytest.mark.asyncio
async def test_neolink_complete_json_hides_auth_response_body() -> None:
    from httpx import Request, Response
    from openai import AuthenticationError

    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    response_body = "NEOLINK_PRIVATE_AUTH_RESPONSE_BODY"
    create = AsyncMock(
        side_effect=AuthenticationError(
            response_body,
            response=Response(
                401,
                request=Request("POST", "https://neolink.example/api/v1/chat/completions"),
                json={"error": {"message": response_body}},
            ),
            body={"error": {"message": response_body}},
        )
    )
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "qwen-plus"
    provider._vendor = "neolink"
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    with pytest.raises(RuntimeError, match="status=401") as exc_info:
        await provider.complete_json("请输出 JSON", "生成标题")

    assert response_body not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    create.assert_awaited_once()


@pytest.mark.asyncio
async def test_neolink_chat_hides_auth_response_body() -> None:
    from httpx import Request, Response
    from openai import AuthenticationError

    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    response_body = "NEOLINK_PRIVATE_CHAT_AUTH_RESPONSE_BODY"
    create = AsyncMock(
        side_effect=AuthenticationError(
            response_body,
            response=Response(
                401,
                request=Request("POST", "https://neolink.example/api/v1/chat/completions"),
                json={"error": {"message": response_body}},
            ),
            body={"error": {"message": response_body}},
        )
    )
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "qwen-plus"
    provider._vendor = "neolink"
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    with pytest.raises(RuntimeError, match="status=401") as exc_info:
        await provider.chat([{"role": "user", "content": "生成标题"}])

    assert response_body not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    create.assert_awaited_once()


@pytest.mark.asyncio
async def test_gemini_complete_json_omits_response_format_even_with_schema() -> None:
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    create = AsyncMock(
        return_value=SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="{}"))]
        )
    )
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "gemini-model"
    provider._vendor = "gemini"
    provider._client = SimpleNamespace(  # type: ignore[assignment]
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    result = await provider.complete_json(
        "系统 json 指令",
        "用户输入",
        response_schema={"type": "object"},
    )

    assert result == {}
    create.assert_awaited_once_with(
        model="gemini-model",
        messages=[
            {"role": "system", "content": "系统 json 指令"},
            {"role": "user", "content": "用户输入"},
        ],
    )


@pytest.mark.asyncio
async def test_anthropic_complete_json_passes_response_schema_to_emit_result_tool(
    monkeypatch,
) -> None:
    from app.providers.llm_anthropic import AnthropicLLMProvider

    provider = AnthropicLLMProvider(api_key="key", model="claude-model")
    create = AsyncMock(
        return_value={"content": [{"type": "tool_use", "name": "emit_result", "input": {}}]}
    )
    monkeypatch.setattr(provider, "_create", create)
    response_schema = {
        "type": "object",
        "properties": {"shots": {"type": "array"}},
        "required": ["shots"],
    }

    result = await provider.complete_json(
        "系统指令",
        "用户输入",
        response_schema=response_schema,
    )

    assert result == {}
    create.assert_awaited_once_with(
        system="系统指令",
        messages=[{"role": "user", "content": "用户输入"}],
        tools=[
            {
                "name": "emit_result",
                "description": "以 JSON 对象形式返回最终结果",
                "input_schema": response_schema,
            }
        ],
        tool_choice={"type": "tool", "name": "emit_result"},
    )


@pytest.mark.asyncio
async def test_anthropic_complete_json_keeps_wide_schema_for_two_argument_calls(
    monkeypatch,
) -> None:
    from app.providers.llm_anthropic import AnthropicLLMProvider

    provider = AnthropicLLMProvider(api_key="key", model="claude-model")
    create = AsyncMock(
        return_value={"content": [{"type": "tool_use", "name": "emit_result", "input": {}}]}
    )
    monkeypatch.setattr(provider, "_create", create)

    result = await provider.complete_json("系统指令", "用户输入")

    assert result == {}
    create.assert_awaited_once_with(
        system="系统指令",
        messages=[{"role": "user", "content": "用户输入"}],
        tools=[
            {
                "name": "emit_result",
                "description": "以 JSON 对象形式返回最终结果",
                "input_schema": {"type": "object", "additionalProperties": True},
            }
        ],
        tool_choice={"type": "tool", "name": "emit_result"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [400, 422])
async def test_anthropic_schema_falls_back_once_for_schema_compatibility_status(
    monkeypatch,
    status_code: int,
) -> None:
    from app.providers.llm_anthropic import AnthropicLLMProvider, _AnthropicHTTPStatusError

    provider = AnthropicLLMProvider(api_key="key", model="claude-model")
    create = AsyncMock(
        side_effect=[
            _AnthropicHTTPStatusError(status_code),
            {"content": [{"type": "tool_use", "name": "emit_result", "input": {}}]},
        ]
    )
    monkeypatch.setattr(provider, "_create", create)
    response_schema = {"type": "object", "additionalProperties": False}

    result = await provider.complete_json(
        "系统指令",
        "用户输入",
        response_schema=response_schema,
    )

    assert result == {}
    assert create.await_count == 2
    first_kwargs = create.await_args_list[0].kwargs
    second_kwargs = create.await_args_list[1].kwargs
    assert first_kwargs["tools"][0]["input_schema"] == response_schema
    assert second_kwargs["tools"][0]["input_schema"] == {
        "type": "object",
        "additionalProperties": True,
    }


@pytest.mark.asyncio
async def test_anthropic_schema_fallback_runs_once_when_fallback_fails(monkeypatch) -> None:
    from app.providers.llm_anthropic import AnthropicLLMProvider, _AnthropicHTTPStatusError

    provider = AnthropicLLMProvider(api_key="key", model="claude-model")
    create = AsyncMock(
        side_effect=[_AnthropicHTTPStatusError(400), _AnthropicHTTPStatusError(422)]
    )
    monkeypatch.setattr(provider, "_create", create)

    with pytest.raises(RuntimeError, match="status=422") as exc_info:
        await provider.complete_json(
            "系统指令",
            "用户输入",
            response_schema={"type": "object"},
        )

    assert create.await_count == 2
    assert exc_info.value.__cause__ is None


@pytest.mark.asyncio
async def test_anthropic_schema_does_not_fallback_for_auth_error(monkeypatch) -> None:
    from app.providers.llm_anthropic import AnthropicLLMProvider, _AnthropicHTTPStatusError

    provider = AnthropicLLMProvider(api_key="key", model="claude-model")
    create = AsyncMock(side_effect=_AnthropicHTTPStatusError(401))
    monkeypatch.setattr(provider, "_create", create)

    with pytest.raises(RuntimeError, match="status=401") as exc_info:
        await provider.complete_json(
            "系统指令",
            "用户输入",
            response_schema={"type": "object"},
        )

    assert create.await_count == 1
    assert exc_info.value.__cause__ is None


@pytest.mark.asyncio
async def test_anthropic_http_error_hides_response_body(monkeypatch) -> None:
    from app.providers import llm_anthropic
    from app.providers.llm_anthropic import AnthropicLLMProvider

    response_body = "ANTHROPIC_PRIVATE_RESPONSE_BODY"

    class FakeResponse:
        status_code = 401
        text = response_body

        def json(self) -> dict:
            return {"error": {"message": response_body}}

    class FakeAsyncClient:
        def __init__(self, *, timeout: float) -> None:
            self.timeout = timeout

        async def __aenter__(self) -> FakeAsyncClient:
            return self

        async def __aexit__(self, *_args) -> None:
            return None

        async def post(self, *_args, **_kwargs) -> FakeResponse:
            return FakeResponse()

    monkeypatch.setattr(llm_anthropic.httpx, "AsyncClient", FakeAsyncClient)
    provider = AnthropicLLMProvider(api_key="key", model="claude-model")

    with pytest.raises(RuntimeError, match="status=401") as exc_info:
        await provider._create(system="系统指令", messages=[{"role": "user", "content": "输入"}])

    assert response_body not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


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
async def test_neolink_chat_preserves_multi_turn_messages_and_max_tokens() -> None:
    from app.providers.llm_openai_compat import OpenAICompatLLMProvider

    create = AsyncMock(
        return_value=SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="最终回答"))])
    )
    provider = OpenAICompatLLMProvider.__new__(OpenAICompatLLMProvider)
    provider._model = "qwen-plus"
    provider._vendor = "neolink"
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
        max_tokens=512,
    )

    assert result == "最终回答"
    create.assert_awaited_once_with(
        model="qwen-plus",
        messages=[
            {"role": "system", "content": "系统指令"},
            {"role": "user", "content": "问题"},
            {"role": "assistant", "content": "中间回答"},
            {"role": "user", "content": "追问"},
        ],
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
    from app.providers.video_gen_neolink import NeolinkVideoGenProvider
    from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

    monkeypatch.setattr(config, "settings", _settings())

    for alias in ("gemini", "google", "veo"):
        assert isinstance(get_video_provider(alias), GoogleVideoGenProvider)
    for alias in ("volcengine", "doubao", "jianyu", "modelsai"):
        assert isinstance(get_video_provider(alias), VolcengineVideoGenProvider)
    for alias in ("mgrouter", "mg-router", "aicc", "aicc-seedance"):
        assert isinstance(get_video_provider(alias), MgrouterVideoGenProvider)
    for alias in ("neolink", "neo-link"):
        assert isinstance(get_video_provider(alias), NeolinkVideoGenProvider)
    with pytest.raises(UnsupportedProviderError, match="unknown-vendor"):
        get_video_provider("unknown-vendor")


@pytest.mark.parametrize("aspect", ["3:4", "2:3", "4:5", "9:16"])
def test_openai_image_size_maps_portrait_ratios_to_portrait_size(aspect: str) -> None:
    from app.providers.image_gen_openai import _map_size

    assert _map_size("2k", aspect) == "1024x1536"


@pytest.mark.parametrize(
    ("aspect", "expected"),
    [("1:1", "1024x1024"), ("16:9", "1536x1024"), ("4:3", "1536x1024")],
)
def test_openai_image_size_keeps_square_and_landscape_mappings(
    aspect: str,
    expected: str,
) -> None:
    from app.providers.image_gen_openai import _map_size

    assert _map_size("2k", aspect) == expected


@pytest.mark.parametrize(
    ("size", "aspect", "expected"),
    [
        ("1k", "16:9", "1792x1008"),
        ("1k", "9:16", "1008x1792"),
        ("1k", "1:1", "1024x1024"),
        ("1k", "4:3", "1344x1008"),
        ("1k", "3:4", "1008x1344"),
        ("1k", "3:2", "1536x1024"),
        ("1k", "2:3", "1024x1536"),
        ("2k", "16:9", "2816x1584"),
        ("2k", "9:16", "1584x2816"),
        ("2k", "1:1", "1536x1536"),
        ("2k", "4:3", "2048x1536"),
        ("2k", "3:4", "1536x2048"),
        ("2k", "3:2", "2304x1536"),
        ("2k", "2:3", "1536x2304"),
        ("4k", "16:9", "3840x2160"),
        ("4k", "9:16", "2160x3840"),
        ("4k", "1:1", "2160x2160"),
        ("4k", "4:3", "2880x2160"),
        ("4k", "3:4", "2160x2880"),
        ("4k", "3:2", "3216x2144"),
        ("4k", "2:3", "2144x3216"),
        ("1K", "AUTO", "1792x1008"),
    ],
)
def test_neolink_image_size_mapping_uses_documented_16_multiple_sizes(
    size: str,
    aspect: str,
    expected: str,
) -> None:
    from app.providers.image_gen_neolink import _map_size

    actual = _map_size("gp-im-2", size, aspect)

    assert actual == expected
    width, height = (int(part) for part in actual.split("x", 1))
    assert width % 16 == 0
    assert height % 16 == 0
    assert width <= height * 3
    assert height <= width * 3
    assert max(width, height) <= 3840
    assert min(width, height) <= 2160


def test_neolink_image_size_mapping_rejects_unsupported_gpt_image_tier() -> None:
    from app.providers.image_gen_neolink import _map_size
    from app.providers.registry import ProviderConfigurationError

    with pytest.raises(ProviderConfigurationError):
        _map_size("gpt-image-1", "2k", "16:9")


def test_neolink_image_size_mapping_rejects_unsupported_aspect() -> None:
    from app.providers.image_gen_neolink import _map_size
    from app.providers.registry import ProviderConfigurationError

    with pytest.raises(ProviderConfigurationError, match="支持"):
        _map_size("gp-im-2", "1k", "5:4")


def test_neolink_image_base_url_normalizes_openai_sdk_path() -> None:
    from app.providers.image_gen_neolink import _normalize_neolink_image_base_url

    assert (
        _normalize_neolink_image_base_url("https://neolink.com/api")
        == "https://neolink.com/api/v1"
    )


@pytest.mark.parametrize(
    "base_url",
    [
        "http://neolink.com/api",
        "https://neolink.com/api?api_key=leak",
        "https://neolink.com:8443/api",
        "https://user:password@neolink.com/api",
        "https://neolink.com/api#fragment",
    ],
)
def test_neolink_image_base_url_rejects_unsafe_values(base_url: str) -> None:
    from app.providers.image_gen_neolink import _normalize_neolink_image_base_url
    from app.providers.registry import ProviderConfigurationError

    with pytest.raises(ProviderConfigurationError):
        _normalize_neolink_image_base_url(base_url)


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


@pytest.mark.parametrize(
    ("model", "size", "aspect", "expected"),
    [
        ("qwen-image-2.0", "1k", "3:4", "864*1184"),
        ("qwen-image-2.0", "2k", "3:4", "1728*2368"),
        ("qwen-image-2.0", "2k", "16:9", "2688*1536"),
        ("qwen-image", "1k", "3:4", "1104*1472"),
    ],
)
def test_bailian_size_mapping_uses_effective_model_capabilities(
    model: str,
    size: str,
    aspect: str,
    expected: str,
) -> None:
    from app.providers.image_gen_bailian import _map_size

    assert _map_size(model, size, aspect) == expected


@pytest.mark.parametrize(
    ("model", "size", "message"),
    [
        ("qwen-image-2.0", "4k", "仅支持 1K 或 2K"),
        ("qwen-image", "2k", "仅支持固定预设尺寸"),
        ("qwen-image-2.0", "8k", "仅支持 1K、2K 或 4K"),
    ],
)
def test_bailian_size_mapping_rejects_unsupported_tiers(
    model: str,
    size: str,
    message: str,
) -> None:
    from app.providers.image_gen_bailian import _map_size
    from app.providers.registry import ProviderConfigurationError

    with pytest.raises(ProviderConfigurationError, match=message):
        _map_size(model, size, "3:4")


def test_bailian_prompt_extension_preserves_structured_constraints() -> None:
    from app.providers.image_gen_bailian import _should_extend_prompt

    assert _should_extend_prompt("一只纸飞机") is True
    assert (
        _should_extend_prompt("主体事实：一只纸飞机\n\n【风格渲染约束】赛博朋克风格")
        is False
    )
    assert _should_extend_prompt("【视觉资产设定提示词】\n【人物设定图构图硬约束 v2】") is False
    assert _should_extend_prompt("【人物设定图构图硬约束】存量角色图") is False


@pytest.mark.asyncio
async def test_bailian_provider_config_keeps_requested_model_without_explicit_model(monkeypatch) -> None:
    """仅配置厂商时，不能用 Settings 的回退模型覆盖画布任务模型。"""
    from app.providers import image_gen_bailian
    from app.providers.image_gen import ImageResult
    from app.providers.image_gen_bailian import BailianImageGenProvider

    configured = _settings(
        image_gen_provider="bailian",
        image_gen_api_key="image-key",
        image_gen_base_url="https://dashscope.example",
    )
    assert configured.image_gen_model == "wan2.1-t2i-plus"
    assert configured.is_explicitly_configured("image_gen_model") is False
    monkeypatch.setattr(image_gen_bailian, "settings", configured)

    class Client:
        def __init__(self, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    monkeypatch.setattr(image_gen_bailian.httpx, "AsyncClient", Client)
    provider = BailianImageGenProvider()
    sync_generate = AsyncMock(return_value=["https://oss.aliyuncs.com/result.png"])
    monkeypatch.setattr(provider, "_generate_sync_multimodal", sync_generate)
    monkeypatch.setattr(
        provider,
        "_download",
        AsyncMock(return_value=ImageResult(data=b"image", mime_type="image/png")),
    )

    result = await provider.generate("角色设定图", "qwen-image-2.0", "2k", "3:4")

    assert result[0].data == b"image"
    assert sync_generate.await_args.args[4:7] == ("qwen-image-2.0", "1728*2368", 1)


@pytest.mark.asyncio
async def test_bailian_image_provider_keeps_vendor_config_when_neolink_is_selected(
    monkeypatch,
) -> None:
    """模型别名路由到百炼时，不得使用 NeoLink 的通用图片配置。"""
    from app.providers import image_gen_bailian
    from app.providers.image_gen import ImageResult
    from app.providers.image_gen_bailian import BailianImageGenProvider

    configured = _settings(
        image_gen_provider="neolink",
        image_gen_api_key="neolink-image-key",
        image_gen_base_url="https://neolink.com/api",
        image_gen_model="gp-im-2",
        dashscope_api_key="dashscope-key",
        dashscope_base_url="https://dashscope.example",
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
                                "content": [{"image": "https://oss.aliyuncs.com/result.png"}]
                            }
                        }
                    ]
                }
            }

    class Client:
        def __init__(self, **kwargs) -> None:
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

    result = await provider.generate("角色设定图", "qwen-image-2.0", "2k", "1:1")

    assert result[0].data == b"image"
    assert calls["post"]["url"] == (
        "https://dashscope.example/api/v1/services/aigc/multimodal-generation/generation"
    )
    assert calls["post"]["headers"]["Authorization"] == "Bearer dashscope-key"
    assert "neolink-image-key" not in calls["post"]["headers"].values()
    assert calls["post"]["json"]["model"] == "qwen-image-2.0"
    download.assert_awaited_once_with(
        "https://oss.aliyuncs.com/result.png",
        base_url="https://dashscope.example",
    )


@pytest.mark.asyncio
async def test_bailian_image_provider_applies_generic_override_when_bailian_is_selected(
    monkeypatch,
) -> None:
    """百炼是生效厂商时，IMAGE_GEN_* 覆盖仍需正常生效。"""
    from app.providers import image_gen_bailian
    from app.providers.image_gen import ImageResult
    from app.providers.image_gen_bailian import BailianImageGenProvider

    configured = _settings(
        image_gen_provider="bailian",
        image_gen_api_key="bailian-image-key",
        image_gen_base_url="https://bailian-gateway.example",
        image_gen_model="qwen-image-2.0",
        dashscope_api_key="dashscope-fallback-key",
        dashscope_base_url="https://dashscope-fallback.example",
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
                                "content": [{"image": "https://oss.aliyuncs.com/result.png"}]
                            }
                        }
                    ]
                }
            }

    class Client:
        def __init__(self, **kwargs) -> None:
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

    await provider.generate("角色设定图", "wan2.1-t2i-plus", "2k", "1:1")

    assert calls["post"]["url"] == (
        "https://bailian-gateway.example/api/v1/services/aigc/multimodal-generation/generation"
    )
    assert calls["post"]["headers"]["Authorization"] == "Bearer bailian-image-key"
    assert calls["post"]["json"]["model"] == "qwen-image-2.0"
    download.assert_awaited_once_with(
        "https://oss.aliyuncs.com/result.png",
        base_url="https://bailian-gateway.example",
    )


@pytest.mark.asyncio
async def test_neolink_image_provider_applies_generic_override_when_neolink_is_selected(
    monkeypatch,
) -> None:
    """NeoLink 是生效厂商时，IMAGE_GEN_* 覆盖仍需正常生效。"""
    import base64

    from app.providers import image_gen_neolink
    from app.providers.image_gen_neolink import NeolinkImageGenProvider

    configured = _settings(
        image_gen_provider="neolink",
        image_gen_api_key="neolink-image-key",
        image_gen_base_url="https://neolink.com/api",
        image_gen_model="gp-im-2",
        neolink_api_key="neolink-fallback-key",
        neolink_base_url="https://neolink-fallback.example/api",
    )
    monkeypatch.setattr(image_gen_neolink, "settings", configured)
    calls: dict = {}

    class Client:
        def __init__(self, **kwargs) -> None:
            calls["client"] = kwargs
            self.images = SimpleNamespace(generate=AsyncMock(side_effect=self._generate))
            self.close = AsyncMock()

        async def _generate(self, **kwargs):
            calls["generate"] = kwargs
            encoded = base64.b64encode(b"neolink-image").decode("ascii")
            return SimpleNamespace(data=[SimpleNamespace(b64_json=encoded, url=None)])

    monkeypatch.setattr(image_gen_neolink, "AsyncOpenAI", Client)

    result = await NeolinkImageGenProvider().generate(
        prompt="prompt",
        model="qwen-image-2.0",
        size="1k",
        aspect="1:1",
    )

    assert result[0].data == b"neolink-image"
    assert calls["client"] == {
        "api_key": "neolink-image-key",
        "base_url": "https://neolink.com/api/v1",
    }
    assert calls["generate"]["model"] == "gp-im-2"


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

    styled_prompt = "主体事实：一只纸飞机\n\n【风格渲染约束】赛博朋克风格"
    result = await provider.generate(
        styled_prompt,
        "ignored",
        "2k",
        "1:1",
        negative_prompt="写实摄影",
    )

    assert result[0].data == b"image"
    assert calls["post"]["url"].endswith("/api/v1/services/aigc/multimodal-generation/generation")
    assert "X-DashScope-Async" not in calls["post"]["headers"]
    assert calls["post"]["json"]["input"]["messages"][0]["content"] == [
        {"text": styled_prompt}
    ]
    assert calls["post"]["json"]["parameters"]["size"] == "2048*2048"
    assert calls["post"]["json"]["parameters"]["prompt_extend"] is False
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

    styled_prompt = "主体事实：一只纸飞机\n\n【风格渲染约束】赛博朋克风格"
    result = await provider.generate(
        styled_prompt,
        "ignored",
        "1k",
        "1:1",
        negative_prompt="写实摄影",
    )

    assert result[0].data == b"image"
    assert calls["post"]["url"].endswith("/api/v1/services/aigc/text2image/image-synthesis")
    assert calls["post"]["headers"]["X-DashScope-Async"] == "enable"
    assert calls["post"]["json"]["input"]["prompt"] == styled_prompt
    assert calls["post"]["json"]["input"]["negative_prompt"] == "写实摄影"
    assert calls["post"]["json"]["parameters"]["size"] == "1328*1328"
    assert calls["post"]["json"]["parameters"]["prompt_extend"] is False


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
    assert calls["generate"]["model"] == "ignored"
    assert calls["generate"]["prompt"] == (
        "prompt\n\nDo not include in the generated image: photorealistic"
    )
    client_holder["client"].close.assert_awaited_once()


@pytest.mark.asyncio
async def test_neolink_image_provider_decodes_base64_and_closes_client(monkeypatch) -> None:
    import base64

    from app.providers import image_gen_neolink
    from app.providers.image_gen import ImageResult
    from app.providers.image_gen_neolink import NeolinkImageGenProvider

    monkeypatch.setattr(
        image_gen_neolink,
        "settings",
        _settings(
            image_gen_provider="neolink",
            image_gen_api_key="image-key",
            image_gen_base_url="https://neolink.com/api",
            image_gen_model="gp-im-2",
        ),
    )
    calls: dict = {}

    class Client:
        def __init__(self, **kwargs):
            calls["client"] = kwargs
            self.images = SimpleNamespace(generate=AsyncMock(side_effect=self._generate))
            self.close = AsyncMock()

        async def _generate(self, **kwargs):
            calls["generate"] = kwargs
            encoded = base64.b64encode(b"neolink-image").decode("ascii")
            return SimpleNamespace(data=[SimpleNamespace(b64_json=encoded, url=None)])

    client_holder: dict = {}

    def _client_factory(**kwargs):
        client = Client(**kwargs)
        client_holder["client"] = client
        return client

    monkeypatch.setattr(image_gen_neolink, "AsyncOpenAI", _client_factory)

    result = await NeolinkImageGenProvider().generate(
        prompt="prompt",
        model="ignored",
        size="1k",
        aspect="16:9",
        count=2,
        negative_prompt="photorealistic",
    )

    assert isinstance(result[0], ImageResult)
    assert result[0].data == b"neolink-image"
    assert calls["client"] == {
        "api_key": "image-key",
        "base_url": "https://neolink.com/api/v1",
    }
    assert calls["generate"] == {
        "model": "gp-im-2",
        "prompt": "prompt\n\nDo not include in the generated image: photorealistic",
        "size": "1792x1008",
        "n": 2,
    }
    client_holder["client"].close.assert_awaited_once()


@pytest.mark.asyncio
async def test_neolink_image_provider_rejects_invalid_base64_data(monkeypatch) -> None:
    from app.providers import image_gen_neolink
    from app.providers.image_gen_neolink import NeolinkImageGenProvider

    monkeypatch.setattr(
        image_gen_neolink,
        "settings",
        _settings(
            image_gen_provider="neolink",
            image_gen_api_key="image-key",
            image_gen_base_url="https://neolink.com/api",
            image_gen_model="gp-im-2",
        ),
    )

    class Client:
        def __init__(self, **kwargs):
            self.images = SimpleNamespace(
                generate=AsyncMock(
                    return_value=SimpleNamespace(
                        data=[SimpleNamespace(b64_json="%%%", url=None)]
                    )
                )
            )
            self.close = AsyncMock()

    monkeypatch.setattr(image_gen_neolink, "AsyncOpenAI", Client)

    with pytest.raises(RuntimeError, match="NeoLink 返回的图片 Base64 数据无效"):
        await NeolinkImageGenProvider().generate(
            prompt="prompt",
            model="ignored",
            size="1k",
            aspect="16:9",
        )


@pytest.mark.asyncio
async def test_neolink_image_provider_treats_model_not_found_as_configuration_error(monkeypatch) -> None:
    from httpx import Request, Response
    from openai import NotFoundError

    from app.providers import image_gen_neolink
    from app.providers.image_gen_neolink import NeolinkImageGenProvider
    from app.providers.registry import ProviderConfigurationError

    monkeypatch.setattr(
        image_gen_neolink,
        "settings",
        _settings(
            image_gen_provider="neolink",
            image_gen_api_key="image-key",
            image_gen_base_url="https://neolink.com/api",
            image_gen_model="gp-im-2",
        ),
    )
    body = {"error": {"code": "model_not_found", "type": "neolink_error"}}
    error = NotFoundError(
        "model not found",
        response=Response(
            404,
            request=Request("POST", "https://neolink.com/api/v1/images/generations"),
            json=body,
        ),
        body=body,
    )

    class Client:
        def __init__(self, **kwargs):
            self.images = SimpleNamespace(generate=AsyncMock(side_effect=error))
            self.close = AsyncMock()

    monkeypatch.setattr(image_gen_neolink, "AsyncOpenAI", Client)

    with pytest.raises(ProviderConfigurationError) as exc_info:
        await NeolinkImageGenProvider().generate(
            prompt="prompt",
            model="ignored",
            size="1k",
            aspect="16:9",
        )

    assert "gp-im-2" in str(exc_info.value)
    assert "当前 NeoLink API Key 分组未开通该图片模型渠道" in str(exc_info.value)


@pytest.mark.asyncio
async def test_neolink_image_provider_treats_model_not_found_type_as_configuration_error(
    monkeypatch,
) -> None:
    from httpx import Request, Response
    from openai import BadRequestError

    from app.providers import image_gen_neolink
    from app.providers.image_gen_neolink import NeolinkImageGenProvider
    from app.providers.registry import ProviderConfigurationError

    monkeypatch.setattr(
        image_gen_neolink,
        "settings",
        _settings(
            image_gen_provider="neolink",
            image_gen_api_key="image-key",
            image_gen_base_url="https://neolink.com/api",
            image_gen_model="gp-im-2",
        ),
    )
    body = {"error": {"type": "model_not_found"}}
    error = BadRequestError(
        "model not found",
        response=Response(
            400,
            request=Request("POST", "https://neolink.com/api/v1/images/generations"),
            json=body,
        ),
        body=body,
    )

    class Client:
        def __init__(self, **kwargs):
            self.images = SimpleNamespace(generate=AsyncMock(side_effect=error))
            self.close = AsyncMock()

    monkeypatch.setattr(image_gen_neolink, "AsyncOpenAI", Client)

    with pytest.raises(ProviderConfigurationError, match="gp-im-2"):
        await NeolinkImageGenProvider().generate(
            prompt="prompt",
            model="ignored",
            size="1k",
            aspect="16:9",
        )


@pytest.mark.asyncio
async def test_neolink_image_provider_maps_content_filter_to_safety_rejection(monkeypatch) -> None:
    from httpx import Request, Response
    from openai import BadRequestError

    from app.providers import image_gen_neolink
    from app.providers.image_gen import ImageSafetyRejectedError
    from app.providers.image_gen_neolink import NeolinkImageGenProvider

    monkeypatch.setattr(
        image_gen_neolink,
        "settings",
        _settings(
            image_gen_provider="neolink",
            image_gen_api_key="image-key",
            image_gen_base_url="https://neolink.com/api",
            image_gen_model="gp-im-2",
        ),
    )
    body = {"error": {"code": "content_filter", "type": "neolink_error"}}
    error = BadRequestError(
        "content filtered",
        response=Response(
            400,
            request=Request("POST", "https://neolink.com/api/v1/images/generations"),
            json=body,
        ),
        body=body,
    )

    class Client:
        def __init__(self, **kwargs):
            self.images = SimpleNamespace(generate=AsyncMock(side_effect=error))
            self.close = AsyncMock()

    monkeypatch.setattr(image_gen_neolink, "AsyncOpenAI", Client)

    with pytest.raises(ImageSafetyRejectedError) as exc_info:
        await NeolinkImageGenProvider().generate(
            prompt="prompt",
            model="ignored",
            size="1k",
            aspect="16:9",
        )

    assert exc_info.value.provider == "neolink"


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
