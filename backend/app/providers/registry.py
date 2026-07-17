"""模型供应商能力注册表。

这里仅描述“厂商是否支持某种能力”和稳定别名，不保存密钥，也不创建客户端。
工厂在实例化前统一校验，避免未知厂商静默回退到 OpenAI 或火山方舟。
"""

from __future__ import annotations

from typing import Literal, cast

ProviderCapability = Literal["text", "chat", "image", "video"]
ProviderName = Literal[
    "openai",
    "openai_compatible",
    "anthropic",
    "gemini",
    "bailian",
    "deepseek",
    "volcengine",
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
}

_CAPABILITIES: dict[ProviderCapability, frozenset[ProviderName]] = {
    "text": frozenset(
        {"openai", "openai_compatible", "anthropic", "gemini", "bailian", "deepseek"}
    ),
    "chat": frozenset(
        {"openai", "openai_compatible", "anthropic", "gemini", "bailian", "deepseek"}
    ),
    "image": frozenset({"openai", "gemini", "bailian"}),
    "video": frozenset({"openai", "gemini", "bailian", "volcengine"}),
}


class ProviderError(RuntimeError):
    """供应商适配层的稳定基础异常。"""


class UnsupportedProviderError(ProviderError):
    """厂商名称或别名未知。"""


class UnsupportedCapabilityError(ProviderError):
    """已知厂商不提供请求的能力。"""


class ProviderConfigurationError(ProviderError):
    """请求能力所需的环境变量配置不完整。"""


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
