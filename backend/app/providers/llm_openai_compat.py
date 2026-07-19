"""OpenAI 兼容 LLM 供应商实现 — 一个类覆盖多家厂商。

通过 base_url + api_key + model 驱动，复用 openai.AsyncOpenAI SDK：
- OpenAI:    https://api.openai.com/v1
- DeepSeek:  https://api.deepseek.com          (model: deepseek-v4-flash / deepseek-v4-pro)
- 阿里百炼:   https://dashscope.aliyuncs.com/compatible-mode/v1  (model: qwen-plus / qwen-max ...)
- Gemini:    https://generativelanguage.googleapis.com/v1beta/openai/  (model: gemini-2.5-flash)

complete_json 用 response_format={"type":"json_object"}；部分厂商（百炼/DeepSeek）要求
prompt 含 "json" 关键词，故当 system 缺失该词时自动注入一行 JSON 约束，避免 400。
chat 为标准 chat.completions 多轮调用。

缺 key 时抛 RuntimeError，由调用方捕获后释放积分，不阻塞队列。
"""

from __future__ import annotations

import json
from typing import Literal

from openai import (
    APIConnectionError,
    APITimeoutError,
    AsyncOpenAI,
    InternalServerError,
    RateLimitError,
)

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


class OpenAICompatLLMProvider:
    """OpenAI 兼容端点的统一 LLM 实现。

    complete_json 锁定 response_format=json_object；chat 走标准多轮补全。
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
        self._client = AsyncOpenAI(api_key=api_key, base_url=self._base_url)

    async def complete_json(self, system: str, user: str) -> dict:
        """单次 JSON 模式补全；总计三次预算由上层 schema 校验器统一管理。

        Gemini 的 OpenAI 兼容层对 response_format 支持不稳（部分版本 400），
        故 vendor==gemini 时不传 response_format，改靠 _JSON_GUARD 提示词约束。
        """
        guarded_system = system if "json" in system.lower() else f"{system}\n\n{_JSON_GUARD}"
        kwargs: dict = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": guarded_system},
                {"role": "user", "content": user},
            ],
        }
        if getattr(self, "_vendor", "openai") != "gemini":
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
        content = response.choices[0].message.content or "{}"
        # 去除部分厂商偶发的 markdown 代码块包裹
        stripped = content.strip()
        if stripped.startswith("```"):
            stripped = stripped.split("```", 2)
            stripped = stripped[1] if len(stripped) >= 2 else content
        try:
            parsed = json.loads(stripped)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}

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
        return response.choices[0].message.content or ""


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
