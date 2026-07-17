"""OpenAI LLM 供应商实现 — 已收敛到 OpenAI 兼容适配层。

本模块保留为向后兼容入口（历史代码可能直接 import OpenAILLMProvider）。
真实实现见 llm_openai_compat.OpenAICompatLLMProvider(vendor="openai")。
"""

from __future__ import annotations

from app.core.config import settings
from app.providers.llm_openai_compat import OpenAICompatLLMProvider


class OpenAILLMProvider(OpenAICompatLLMProvider):
    """OpenAI LLM 供应商（向后兼容别名）。

    复用 OpenAI 兼容实现，vendor=openai，从 settings.text_llm_* 读配置
    （为空时回退 openai_api_key / openai_llm_model）。
    """

    def __init__(self) -> None:
        super().__init__(
            vendor="openai",
            api_key=settings.text_llm_api_key or settings.openai_api_key,
            model=settings.text_llm_model or settings.openai_llm_model,
            base_url=settings.text_llm_base_url,
        )
