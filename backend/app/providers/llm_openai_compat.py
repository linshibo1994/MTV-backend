"""OpenAI 兼容 LLM 供应商实现 — 一个类覆盖多家厂商。

通过 base_url + api_key + model 驱动，复用 openai.AsyncOpenAI SDK：
- OpenAI:    https://api.openai.com/v1
- DeepSeek:  https://api.deepseek.com          (model: deepseek-v4-flash / deepseek-v4-pro)
- 阿里百炼:   https://dashscope.aliyuncs.com/compatible-mode/v1  (model: qwen-plus / qwen-max ...)
- Gemini:    https://generativelanguage.googleapis.com/v1beta/openai/  (model: gemini-2.5-flash)

complete_json 默认用 response_format={"type":"json_object"}；OpenAI 在调用方提供
JSON Schema 时改用 strict json_schema。部分厂商（百炼/DeepSeek）要求 prompt 含 "json"
关键词，故当 system 缺失该词时自动注入一行 JSON 约束，避免 400。
chat 为标准 chat.completions 多轮调用。

缺 key 时抛 RuntimeError，由调用方捕获后释放积分，不阻塞队列。
"""

from __future__ import annotations

import sys
from typing import Literal

from openai import (
    APIConnectionError,
    APITimeoutError,
    AsyncOpenAI,
    InternalServerError,
    RateLimitError,
)

from app.providers.llm import parse_json_object, require_non_empty_chat_content
from app.providers.registry import ProviderConfigurationError

LLMVendor = Literal["openai", "openai_compatible", "deepseek", "bailian", "gemini"]

# 各厂商 OpenAI 兼容端点默认 base_url（.env 未显式配置时使用）
_DEFAULT_BASE_URL: dict[str, str] = {
    "openai": "https://api.openai.com/v1",
    "deepseek": "https://api.deepseek.com",
    "bailian": "https://dashscope.aliyuncs.com/compatible-mode/v1",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/openai/",
}

# 强制注入的 JSON 约束前缀（仅当 system 不含 "json" 关键词时追加）
_JSON_GUARD = "请严格输出合法 JSON（不要包含 markdown 代码块标记或任何额外说明）。"
_STRICT_JSON_SCHEMA_NAME = "structured_response"
_SCHEMA_COMPATIBILITY_STATUS_CODES = frozenset({400, 422})


def _http_status_code(exc: BaseException) -> int | None:
    """兼容不同 OpenAI SDK/测试替身，只读取整数 HTTP 状态码。"""
    status_code = getattr(exc, "status_code", None)
    return status_code if isinstance(status_code, int) else None


def _is_transient_provider_error(exc: BaseException) -> bool:
    """兼容 SDK 模块被测试替身后的异常类，且不依赖异常正文。"""
    if isinstance(
        exc,
        (APIConnectionError, APITimeoutError, InternalServerError, RateLimitError),
    ):
        return True
    runtime_module = sys.modules.get("openai")
    if runtime_module is None:
        return False
    runtime_types = tuple(
        error_type
        for name in (
            "APIConnectionError",
            "APITimeoutError",
            "InternalServerError",
            "RateLimitError",
        )
        if isinstance((error_type := getattr(runtime_module, name, None)), type)
    )
    return bool(runtime_types) and isinstance(exc, runtime_types)


class OpenAICompatLLMProvider:
    """OpenAI 兼容端点的统一 LLM 实现。

    complete_json 默认使用 response_format=json_object；OpenAI 可按需使用 strict
    json_schema；chat 走标准多轮补全。
    """

    def __init__(
        self,
        *,
        vendor: LLMVendor,
        api_key: str,
        model: str,
        base_url: str = "",
    ) -> None:
        if not api_key:
            raise ProviderConfigurationError(
                f"{vendor} LLM API key 未配置，无法初始化 LLM 供应商"
            )
        self._vendor = vendor
        self._model = model
        self._base_url = base_url or _DEFAULT_BASE_URL.get(vendor, "")
        if not self._base_url:
            raise ProviderConfigurationError(f"{vendor} LLM base_url 未配置")
        self._client = AsyncOpenAI(
            api_key=api_key,
            base_url=self._base_url,
            timeout=300.0,
            max_retries=0,
        )

    async def complete_json(
        self,
        system: str,
        user: str,
        *,
        response_schema: dict | None = None,
    ) -> dict:
        """单次 JSON 模式补全；总计三次预算由上层 schema 校验器统一管理。

        Gemini 的 OpenAI 兼容层对 response_format 支持不稳（部分版本 400），
        故 vendor==gemini 时不传 response_format，改靠 _JSON_GUARD 提示词约束。
        其余兼容端点即使收到了 schema 也保持 json_object，避免将 OpenAI 专属的
        strict json_schema 下推到不兼容的供应商。OpenAI strict schema 仅在 400/422
        时降级一次到 json_object，其他状态码不改变原调用语义。
        """
        guarded_system = system if "json" in system.lower() else f"{system}\n\n{_JSON_GUARD}"
        kwargs: dict = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": guarded_system},
                {"role": "user", "content": user},
            ],
        }
        vendor = getattr(self, "_vendor", "openai")
        uses_strict_json_schema = vendor == "openai" and response_schema is not None
        if uses_strict_json_schema:
            kwargs["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": _STRICT_JSON_SCHEMA_NAME,
                    "strict": True,
                    "schema": response_schema,
                },
            }
        elif vendor != "gemini":
            kwargs["response_format"] = {"type": "json_object"}
        try:
            response = await self._client.chat.completions.create(**kwargs)
        except (
            APIConnectionError,
            APITimeoutError,
            InternalServerError,
            RateLimitError,
        ):
            raise ConnectionError("LLM 供应商暂不可用") from None
        except Exception as exc:
            if _is_transient_provider_error(exc):
                raise ConnectionError("LLM 供应商暂不可用") from None
            status_code = _http_status_code(exc)
            if status_code is None:
                raise
            if (
                uses_strict_json_schema
                and status_code in _SCHEMA_COMPATIBILITY_STATUS_CODES
            ):
                fallback_kwargs = dict(kwargs)
                fallback_kwargs["response_format"] = {"type": "json_object"}
                try:
                    response = await self._client.chat.completions.create(**fallback_kwargs)
                except (
                    APIConnectionError,
                    APITimeoutError,
                    InternalServerError,
                    RateLimitError,
                ):
                    raise ConnectionError("LLM 供应商暂不可用") from None
                except Exception as fallback_exc:
                    if _is_transient_provider_error(fallback_exc):
                        raise ConnectionError("LLM 供应商暂不可用") from None
                    fallback_status_code = _http_status_code(fallback_exc)
                    if fallback_status_code is None:
                        raise
                    raise RuntimeError(
                        f"LLM 供应商调用失败 status={fallback_status_code}"
                    ) from None
            else:
                raise RuntimeError(f"LLM 供应商调用失败 status={status_code}") from None
        content = response.choices[0].message.content or ""
        return parse_json_object(content, kind="text")

    async def chat(
        self,
        messages: list[dict],
        *,
        system: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        """多轮对话补全，返回 assistant 文本。"""
        full_messages: list[dict] = []
        if system:
            full_messages.append({"role": "system", "content": system})
        full_messages.extend(messages)
        kwargs: dict = {"model": self._model, "messages": full_messages}
        if temperature is not None:
            kwargs["temperature"] = temperature
        if max_tokens is not None:
            kwargs["max_tokens"] = max_tokens
        try:
            response = await self._client.chat.completions.create(**kwargs)
        except (
            APIConnectionError,
            APITimeoutError,
            InternalServerError,
            RateLimitError,
        ):
            raise ConnectionError("LLM 供应商暂不可用") from None
        except Exception as exc:
            if _is_transient_provider_error(exc):
                raise ConnectionError("LLM 供应商暂不可用") from None
            raise
        choices = getattr(response, "choices", None)
        if not choices:
            return require_non_empty_chat_content(None)
        message = getattr(choices[0], "message", None)
        content = getattr(message, "content", None)
        return require_non_empty_chat_content(content)


def build_openai_compat(
    *,
    vendor: LLMVendor,
    api_key: str,
    model: str,
    base_url: str = "",
) -> OpenAICompatLLMProvider:
    """工厂辅助：构造 OpenAI 兼容 LLM 实例。"""
    return OpenAICompatLLMProvider(
        vendor=vendor, api_key=api_key, model=model, base_url=base_url
    )
