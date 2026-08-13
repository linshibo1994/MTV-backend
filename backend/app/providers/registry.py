"""模型供应商能力注册表。

这里仅描述“厂商是否支持某种能力”和稳定别名，不保存密钥，也不创建客户端。
工厂在实例化前统一校验，避免未知厂商静默回退到 OpenAI 或火山方舟。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal, cast

ProviderCapability = Literal["text", "chat", "image", "video"]
PromptSyntax = Literal["plain", "ark_content", "aicc_content"]
ProviderName = Literal[
    "openai",
    "openai_compatible",
    "anthropic",
    "gemini",
    "bailian",
    "deepseek",
    "volcengine",
    "neolink",
    "mgrouter",
]

_ALIASES: dict[str, ProviderName] = {
    "openai": "openai",
    "gpt": "openai",
    "sora": "openai",
    "openai-compatible": "openai_compatible",
    "openai-compatible-api": "openai_compatible",
    "custom": "openai_compatible",
    "anthropic": "anthropic",
    "claude": "anthropic",
    "claude-code": "anthropic",
    "google": "gemini",
    "gemini": "gemini",
    "veo": "gemini",
    "bailian": "bailian",
    "dashscope": "bailian",
    "aliyun": "bailian",
    "qwen": "bailian",
    "wanx": "bailian",
    "deepseek": "deepseek",
    "volcengine": "volcengine",
    "doubao": "volcengine",
    "seedance": "volcengine",
    "jianyu": "volcengine",
    "modelsai": "volcengine",
    "neolink": "neolink",
    "neo-link": "neolink",
    "mgrouter": "mgrouter",
    "mg-router": "mgrouter",
    "aicc": "mgrouter",
    "aicc-seedance": "mgrouter",
}

_CAPABILITIES: dict[ProviderCapability, frozenset[ProviderName]] = {
    "text": frozenset(
        {"openai", "openai_compatible", "anthropic", "gemini", "bailian", "deepseek"}
    ),
    "chat": frozenset(
        {"openai", "openai_compatible", "anthropic", "gemini", "bailian", "deepseek"}
    ),
    "image": frozenset({"openai", "gemini", "bailian"}),
    "video": frozenset(
        {"openai", "gemini", "bailian", "volcengine", "neolink", "mgrouter"}
    ),
}


class ProviderError(RuntimeError):
    """供应商适配层的稳定基础异常。"""


class UnsupportedProviderError(ProviderError):
    """厂商名称或别名未知。"""


class UnsupportedCapabilityError(ProviderError):
    """已知厂商不提供请求的能力。"""


class ProviderConfigurationError(ProviderError):
    """请求能力所需的环境变量配置不完整。"""


@dataclass(frozen=True)
class ProviderPromptCapability:
    """由 JSON 单一事实源加载的视频提示词能力。

    ``max_prompt_characters`` 按 Unicode 码点计数，不使用字节长度或英文单词数，
    因而适用于中文提示词。模型级覆盖只改变 JSON 明确声明的字段。
    """

    provider: ProviderName
    adapter_version: str
    prompt_syntax: PromptSyntax
    max_prompt_characters: int | None
    supports_motion_phrases: bool
    supports_character_reference: bool
    supports_reference_images: bool
    supports_reference_audio: bool
    supports_first_frame: bool


_PROMPT_CAPABILITY_PATH = Path(__file__).with_name("provider_prompt_capabilities.json")
_PROMPT_CAPABILITY_FIELDS = frozenset(
    {
        "prompt_syntax",
        "max_prompt_characters",
        "supports_motion_phrases",
        "supports_character_reference",
        "supports_reference_images",
        "supports_reference_audio",
        "supports_first_frame",
    }
)


def _required_prompt_bool(config: dict[str, object], field_name: str) -> bool:
    value = config.get(field_name)
    if type(value) is not bool:
        raise ProviderConfigurationError(f"提示词能力矩阵字段 {field_name} 必须是布尔值")
    return value


def _optional_prompt_limit(config: dict[str, object]) -> int | None:
    value = config.get("max_prompt_characters")
    if value is None:
        return None
    if type(value) is not int or value <= 0:
        raise ProviderConfigurationError("提示词能力矩阵 max_prompt_characters 必须是正整数或 null")
    return value


@lru_cache(maxsize=1)
def _load_prompt_capability_document() -> dict[str, object]:
    """读取并校验无密钥的版本化能力矩阵。"""
    try:
        document = json.loads(_PROMPT_CAPABILITY_PATH.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProviderConfigurationError("视频提示词能力矩阵无法读取") from exc
    if type(document) is not dict:
        raise ProviderConfigurationError("视频提示词能力矩阵根节点必须是对象")
    if document.get("schema_version") != 1:
        raise ProviderConfigurationError("视频提示词能力矩阵 schema_version 不受支持")
    adapter_version = document.get("adapter_version")
    if type(adapter_version) is not str or not adapter_version.strip():
        raise ProviderConfigurationError("视频提示词能力矩阵缺少 adapter_version")
    providers = document.get("providers")
    if type(providers) is not dict:
        raise ProviderConfigurationError("视频提示词能力矩阵缺少 providers")
    return document


def _base_prompt_capability(provider: ProviderName) -> ProviderPromptCapability:
    document = _load_prompt_capability_document()
    providers = cast(dict[str, object], document["providers"])
    raw_config = providers.get(provider)
    if type(raw_config) is not dict:
        raise ProviderConfigurationError(f"视频提示词能力矩阵未声明厂商 {provider}")
    config = cast(dict[str, object], raw_config)
    prompt_syntax = config.get("prompt_syntax")
    if prompt_syntax not in {"plain", "ark_content", "aicc_content"}:
        raise ProviderConfigurationError(f"厂商 {provider} 的 prompt_syntax 无效")
    return ProviderPromptCapability(
        provider=provider,
        adapter_version=cast(str, document["adapter_version"]),
        prompt_syntax=cast(PromptSyntax, prompt_syntax),
        max_prompt_characters=_optional_prompt_limit(config),
        supports_motion_phrases=_required_prompt_bool(config, "supports_motion_phrases"),
        supports_character_reference=_required_prompt_bool(config, "supports_character_reference"),
        supports_reference_images=_required_prompt_bool(config, "supports_reference_images"),
        supports_reference_audio=_required_prompt_bool(config, "supports_reference_audio"),
        supports_first_frame=_required_prompt_bool(config, "supports_first_frame"),
    )


def _apply_model_prompt_override(
    capability: ProviderPromptCapability,
    *,
    model: str | None,
) -> ProviderPromptCapability:
    if not model:
        return capability
    document = _load_prompt_capability_document()
    providers = cast(dict[str, object], document["providers"])
    config = cast(dict[str, object], providers[capability.provider])
    raw_overrides = config.get("model_overrides", [])
    if type(raw_overrides) is not list:
        raise ProviderConfigurationError(
            f"厂商 {capability.provider} 的 model_overrides 必须是数组"
        )
    normalized_model = model.strip().lower()
    for raw_override in raw_overrides:
        if type(raw_override) is not dict:
            raise ProviderConfigurationError("提示词能力矩阵的模型覆盖必须是对象")
        override = cast(dict[str, object], raw_override)
        exact_models = override.get("exact_models")
        if type(exact_models) is not list or not all(type(item) is str for item in exact_models):
            raise ProviderConfigurationError("提示词能力矩阵 exact_models 必须是字符串数组")
        if normalized_model not in {cast(str, item).strip().lower() for item in exact_models}:
            continue
        updates = {key: value for key, value in override.items() if key != "exact_models"}
        unknown_fields = set(updates) - _PROMPT_CAPABILITY_FIELDS
        if unknown_fields:
            raise ProviderConfigurationError(
                "提示词能力矩阵含未知模型覆盖字段: " + ", ".join(sorted(unknown_fields))
            )
        if "max_prompt_characters" in updates:
            updates["max_prompt_characters"] = _optional_prompt_limit(updates)
        for bool_field in _PROMPT_CAPABILITY_FIELDS - {
            "prompt_syntax",
            "max_prompt_characters",
        }:
            if bool_field in updates and type(updates[bool_field]) is not bool:
                raise ProviderConfigurationError(f"模型覆盖字段 {bool_field} 必须是布尔值")
        if "prompt_syntax" in updates and updates["prompt_syntax"] not in {
            "plain",
            "ark_content",
            "aicc_content",
        }:
            raise ProviderConfigurationError("模型覆盖字段 prompt_syntax 无效")
        return ProviderPromptCapability(
            provider=capability.provider,
            adapter_version=capability.adapter_version,
            prompt_syntax=cast(
                PromptSyntax,
                updates.get("prompt_syntax", capability.prompt_syntax),
            ),
            max_prompt_characters=cast(
                int | None,
                updates.get(
                    "max_prompt_characters",
                    capability.max_prompt_characters,
                ),
            ),
            supports_motion_phrases=cast(
                bool,
                updates.get(
                    "supports_motion_phrases",
                    capability.supports_motion_phrases,
                ),
            ),
            supports_character_reference=cast(
                bool,
                updates.get(
                    "supports_character_reference",
                    capability.supports_character_reference,
                ),
            ),
            supports_reference_images=cast(
                bool,
                updates.get(
                    "supports_reference_images",
                    capability.supports_reference_images,
                ),
            ),
            supports_reference_audio=cast(
                bool,
                updates.get(
                    "supports_reference_audio",
                    capability.supports_reference_audio,
                ),
            ),
            supports_first_frame=cast(
                bool,
                updates.get("supports_first_frame", capability.supports_first_frame),
            ),
        )
    return capability


def get_prompt_capability(
    provider: str,
    *,
    model: str | None = None,
) -> ProviderPromptCapability:
    """返回规范化厂商及可选模型对应的视频提示词能力。"""
    normalized = require_capability(provider, "video")
    return _apply_model_prompt_override(_base_prompt_capability(normalized), model=model)


def normalize_provider(provider: str) -> ProviderName:
    """把公开别名规范化为内部稳定厂商名。"""
    raw = (provider or "").strip().lower().replace("_", "-")
    normalized = _ALIASES.get(raw)
    if normalized is None:
        supported = ", ".join(sorted(set(_ALIASES.values())))
        raise UnsupportedProviderError(
            f"不支持的模型厂商: {provider or '<空>'}；可选厂商: {supported}"
        )
    return cast(ProviderName, normalized)


def require_capability(provider: str, capability: ProviderCapability) -> ProviderName:
    """校验厂商支持指定能力并返回规范化名称。"""
    normalized = normalize_provider(provider)
    if normalized not in _CAPABILITIES[capability]:
        raise UnsupportedCapabilityError(
            f"模型厂商 {normalized} 不支持 {capability} 能力"
        )
    return normalized


def supported_providers(capability: ProviderCapability) -> tuple[ProviderName, ...]:
    """返回能力支持的厂商，供文档、错误信息和测试复用。"""
    return tuple(sorted(_CAPABILITIES[capability]))
