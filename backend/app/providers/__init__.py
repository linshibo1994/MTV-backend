"""请求级 LLM 供应商+模型动态路由与同厂商保底模型回退。

- get_llm_provider(provider, model)：未传 provider 时保持历史单例（服务端默认
  TEXT_LLM/CHAT_LLM 链）；传入时按 (规范化厂商, 模型) 动态构造并缓存实例，
  避免每次请求重建 AsyncOpenAI 客户端。
- resolve_provider_credentials(provider)：从各厂商专属 .env 字段解析凭证、
  基础地址与保底模型，缺 Key 抛 ProviderConfigurationError。
- build_llm_with_fallback(provider, model)：显式模型调用遇到"模型不存在/
  不可用"类 4xx 时，自动用同厂商保底模型重试一次；瞬态网络错误（连接/
  超时/限流）与 Schema 契约错误不触发换模型。
"""

from __future__ import annotations

import logging
import re
from collections import OrderedDict

from app.providers.llm import LLMProvider
from app.providers.registry import (
    ProviderCapability,
    ProviderConfigurationError,
    require_capability,
)

__all__ = [
    "get_llm_provider",
    "get_chat_llm_provider",
    "resolve_provider_credentials",
    "build_llm_with_fallback",
    "FallbackLLMProvider",
    "reset_llm_provider_cache",
    "LLMProvider",
]

logger = logging.getLogger(__name__)

_text_provider: LLMProvider | None = None
_chat_provider: LLMProvider | None = None

# 动态构造的 (厂商, 模型) 实例缓存；带条目上限，避免长驻进程无限增长。
_DYNAMIC_CACHE_MAX_ENTRIES = 32
_dynamic_provider_cache: OrderedDict[tuple[str, str, str, str], LLMProvider] = (
    OrderedDict()
)

# 触发"换保底模型重试"的 HTTP 状态码。百炼等厂商对不存在/无权限的模型
# 返回 403 而不是 404；400/422 中的 Schema 兼容问题已由具体 Provider 在
# 内部降级重试消化，到达包装层的 400/422 剩余场景按保守策略换模型重试
# 一次（误伤成本仅为同厂商保底模型多一次调用）。
_MODEL_FALLBACK_STATUS_CODES = frozenset({400, 403, 404, 422})

# 从异常消息中提取 "status=<code>" 形态状态码；llm_openai_compat 与
# llm_anthropic 对非瞬态 HTTP 错误统一抛 RuntimeError("... status=xxx")。
_STATUS_IN_MESSAGE_RE = re.compile(r"status=(\d{3})")

# openai SDK 异常与 Anthropic 适配层异常均可能携带 status_code 属性。
_OPENAI_STATUS_ERROR_NAMES = (
    "NotFoundError",
    "BadRequestError",
    "PermissionDeniedError",
    "UnprocessableEntityError",
)


def _build_llm(
    provider: str,
    api_key: str,
    model: str,
    base_url: str,
    capability: ProviderCapability = "text",
) -> LLMProvider:
    """按 provider 标识构造 LLM 实例。

    openai/deepseek/bailian/gemini/neolink 走 OpenAI 兼容实现；claude 走 Anthropic 原生实现。
    """
    key = require_capability(provider, capability)
    if key in ("openai", "openai_compatible", "deepseek", "bailian", "gemini", "neolink"):
        from app.providers.llm_openai_compat import OpenAICompatLLMProvider

        return OpenAICompatLLMProvider(  # type: ignore[return-value]
            vendor=key,
            api_key=api_key,
            model=model,
            base_url=base_url,
        )
    if key == "anthropic":
        from app.providers.llm_anthropic import AnthropicLLMProvider

        return AnthropicLLMProvider(  # type: ignore[return-value]
            api_key=api_key, model=model, base_url=base_url
        )
    raise AssertionError(f"遗漏的 LLM provider 实现: {key}")


def resolve_provider_credentials(
    provider: str,
    capability: ProviderCapability = "text",
) -> tuple[str, str, str]:
    """解析厂商专属凭证，返回 (api_key, base_url, fallback_model)。

    仅从各厂商独立 env 字段取值（DEEPSEEK_API_KEY、DASHSCOPE_API_KEY 等），
    不再依赖 TEXT_LLM_*/CHAT_LLM_* 能力级映射；缺 Key 时抛
    ProviderConfigurationError，由调用方转为用户可读错误。
    """
    from app.core.config import settings

    key = require_capability(provider, capability)
    if key == "openai":
        api_key, base_url, fallback_model = (
            settings.openai_api_key,
            "",
            settings.openai_llm_model,
        )
    elif key == "deepseek":
        api_key, base_url, fallback_model = (
            settings.deepseek_api_key,
            "https://api.deepseek.com",
            settings.deepseek_model,
        )
    elif key == "bailian":
        api_key, base_url, fallback_model = (
            settings.dashscope_api_key,
            f"{settings.dashscope_base_url.rstrip('/')}/compatible-mode/v1",
            "qwen-plus",
        )
    elif key == "anthropic":
        api_key, base_url, fallback_model = (
            settings.anthropic_api_key,
            settings.anthropic_base_url,
            settings.anthropic_model,
        )
    elif key == "gemini":
        api_key, base_url, fallback_model = (
            settings.gemini_api_key,
            "",
            settings.gemini_llm_model,
        )
    elif key == "neolink":
        api_key, base_url, fallback_model = (
            settings.neolink_api_key,
            settings.neolink_base_url,
            "qwen-turbo",
        )
    else:
        raise ProviderConfigurationError(f"厂商 {key} 不支持 LLM 能力")
    if not api_key.strip():
        raise ProviderConfigurationError(
            f"厂商 {provider} 未配置 API Key，无法按请求路由"
        )
    return api_key, base_url, fallback_model


def _dynamic_cache_key(
    vendor: str, model: str, api_key: str, base_url: str
) -> tuple[str, str, str, str]:
    """缓存键含凭证尾缀，避免换 Key 后复用旧客户端。"""
    return (vendor, model, base_url, api_key[-4:])


def _get_dynamic_llm_provider(
    provider: str,
    model: str | None,
    capability: ProviderCapability,
) -> LLMProvider:
    """按显式 (厂商, 模型) 构造/复用 LLM 实例。

    model 缺省时使用该厂商 .env 配置的保底模型；厂商专属 Key 缺失时抛
    ProviderConfigurationError（不回退到其他厂商，避免静默串台计费）。
    """
    api_key, base_url, fallback_model = resolve_provider_credentials(provider, capability)
    if not api_key:
        raise ProviderConfigurationError(
            f"厂商 {provider} 未配置 API Key，无法按请求路由"
        )
    vendor = require_capability(provider, capability)
    effective_model = (model or "").strip() or fallback_model
    if not effective_model:
        raise ProviderConfigurationError(
            f"厂商 {provider} 未配置保底模型，且请求未显式指定 model"
        )
    cache_key = _dynamic_cache_key(vendor, effective_model, api_key, base_url)
    cached = _dynamic_provider_cache.get(cache_key)
    if cached is not None:
        # LRU 语义：命中后移到队尾。
        _dynamic_provider_cache.move_to_end(cache_key)
        return cached
    instance = _build_llm(vendor, api_key, effective_model, base_url, capability)
    _dynamic_provider_cache[cache_key] = instance
    while len(_dynamic_provider_cache) > _DYNAMIC_CACHE_MAX_ENTRIES:
        _dynamic_provider_cache.popitem(last=False)
    return instance


def get_llm_provider(
    provider: str | None = None,
    model: str | None = None,
) -> LLMProvider:
    """返回文本 LLM 供应商；显式传入 (provider, model) 时按请求动态路由。

    未传 provider 时保持历史单例行为（settings.text_llm_provider 链路），
    pipeline split/extract/merge/enrich/parents 的服务端默认调用不受影响。
    """
    if provider:
        return _get_dynamic_llm_provider(provider, model, "text")
    global _text_provider
    if _text_provider is None:
        from app.core.config import settings

        _text_provider = _build_llm(
            settings.text_llm_provider,
            settings.text_llm_api_key,
            settings.text_llm_model,
            settings.text_llm_base_url,
            "text",
        )
    return _text_provider


def get_chat_llm_provider(
    provider: str | None = None,
    model: str | None = None,
) -> LLMProvider:
    """返回对话 LLM 供应商；显式传入 (provider, model) 时按请求动态路由。"""
    if provider:
        return _get_dynamic_llm_provider(provider, model, "chat")
    global _chat_provider
    if _chat_provider is None:
        from app.core.config import settings

        _chat_provider = _build_llm(
            settings.chat_llm_provider,
            settings.chat_llm_api_key,
            settings.chat_llm_model,
            settings.chat_llm_base_url,
            "chat",
        )
    return _chat_provider


def _model_unavailable_status(exc: BaseException) -> int | None:
    """判定异常是否属于"模型不存在/不可用"类 4xx，返回其状态码。

    openai SDK 的 APIStatusError 携带 status_code 属性；Anthropic 适配层
    的 _AnthropicHTTPStatusError 同样携带。两个实现都会把非瞬态 HTTP 错误
    包装成 RuntimeError("... status=xxx") 再上抛，因此消息回退解析是
    必需路径。ConnectionError（瞬态）返回 None，不触发模型回退。
    """
    if isinstance(exc, ConnectionError):
        # 瞬态网络/限流错误已由 Provider 归一化为 ConnectionError；换模型重试
        # 无法改善，且上层已有 Celery autoretry。
        return None
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int):
        return status_code if status_code in _MODEL_FALLBACK_STATUS_CODES else None
    match = _STATUS_IN_MESSAGE_RE.search(str(exc))
    if match is not None:
        code = int(match.group(1))
        return code if code in _MODEL_FALLBACK_STATUS_CODES else None
    return None


class FallbackLLMProvider:
    """同厂商显式模型 + 保底模型的包装器。

    primary 为显式请求模型，fallback 为同厂商保底模型（为 None 或与
    primary 同模型时不启用回退）。complete_json/chat 的"模型不可用类"
    4xx 错误用 fallback 重试一次并记录告警；其余异常原样上抛。
    """

    def __init__(self, primary: LLMProvider, fallback: LLMProvider | None) -> None:
        self._primary = primary
        self._fallback = fallback

    @property
    def fallback_provider(self) -> LLMProvider | None:
        """暴露保底实例，供测试与诊断读取。"""
        return self._fallback

    async def _with_fallback(self, operation_name: str, *args, **kwargs):
        operation = getattr(self._primary, operation_name)
        try:
            return await operation(*args, **kwargs)
        except Exception as exc:
            status_code = _model_unavailable_status(exc)
            if (
                status_code is None
                or self._fallback is None
                or self._fallback is self._primary
            ):
                raise
            logger.warning(
                "请求级模型不可用 status=%s，改用同厂商保底模型重试 error_type=%s",
                status_code,
                type(exc).__name__,
            )
            return await getattr(self._fallback, operation_name)(*args, **kwargs)

    async def complete_json(
        self,
        system: str,
        user: str,
        *,
        response_schema: dict | None = None,
    ) -> dict:
        """JSON 模式补全；模型不可用类错误时用保底模型重试一次。"""
        return await self._with_fallback(
            "complete_json",
            system,
            user,
            response_schema=response_schema,
        )

    async def chat(
        self,
        messages: list[dict],
        *,
        system: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        """多轮对话补全；模型不可用类错误时用保底模型重试一次。"""
        return await self._with_fallback(
            "chat",
            messages,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens,
        )


def build_llm_with_fallback(
    provider: str,
    model: str | None,
    capability: ProviderCapability = "text",
) -> LLMProvider:
    """构造带同厂商保底模型回退的 LLM 实例。

    model 为空或等于厂商保底模型时直接返回 primary（不再包一层），
    避免同一次调用做两次同模型请求。
    """
    primary = _get_dynamic_llm_provider(provider, model, capability)
    fallback_model = resolve_provider_credentials(provider, capability)[2]
    normalized_model = (model or "").strip()
    if not normalized_model or normalized_model == fallback_model:
        return primary
    if not fallback_model:
        return primary
    fallback = _get_dynamic_llm_provider(provider, fallback_model, capability)
    return FallbackLLMProvider(primary, fallback)


def reset_llm_provider_cache() -> None:
    """测试辅助：重置 LLM 单例与动态路由缓存（切换配置后强制重建）。"""
    global _text_provider, _chat_provider
    _text_provider = None
    _chat_provider = None
    _dynamic_provider_cache.clear()
