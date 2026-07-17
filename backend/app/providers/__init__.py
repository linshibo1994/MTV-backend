"""AI 供应商包 — 单例工厂。

按能力×厂商适配：
- LLM 文本（pipeline 结构化补全）：get_llm_provider() 按 settings.text_llm_provider 路由
- LLM 对话（多轮）：get_chat_llm_provider() 按 settings.chat_llm_provider 路由
- 图片/视频：见 image_gen.py / video_gen.py 工厂

Provider 客户端在本模块单例初始化，按需懒加载。
缺 key 时抛 RuntimeError，由调用方捕获处理。
"""

from __future__ import annotations

from typing import cast

from app.providers.llm import LLMProvider

__all__ = [
    "get_llm_provider",
    "get_chat_llm_provider",
    "LLMProvider",
]

_text_provider: LLMProvider | None = None
_chat_provider: LLMProvider | None = None


def _build_llm(provider: str, api_key: str, model: str, base_url: str) -> LLMProvider:
    """按 provider 标识构造 LLM 实例。

    openai/deepseek/bailian/gemini 走 OpenAI 兼容实现；claude 走 Anthropic 原生实现。
    """
    key = (provider or "").strip().lower()
    if key in ("openai", "deepseek", "bailian", "gemini"):
        from app.providers.llm_openai_compat import LLMVendor, OpenAICompatLLMProvider

        return OpenAICompatLLMProvider(  # type: ignore[return-value]
            vendor=cast(LLMVendor, key),
            api_key=api_key,
            model=model,
            base_url=base_url,
        )
    if key == "claude":
        from app.providers.llm_anthropic import AnthropicLLMProvider

        return AnthropicLLMProvider(  # type: ignore[return-value]
            api_key=api_key, model=model, base_url=base_url
        )
    raise RuntimeError(f"不支持的 LLM provider: {provider}")


def get_llm_provider() -> LLMProvider:
    """返回文本 LLM 供应商单例（懒加载，pipeline split/extract/merge/enrich/parents 用）。

    按 settings.text_llm_provider 路由；缺 key 时抛 RuntimeError。
    """
    global _text_provider
    if _text_provider is None:
        from app.core.config import settings

        _text_provider = _build_llm(
            settings.text_llm_provider,
            settings.text_llm_api_key,
            settings.text_llm_model,
            settings.text_llm_base_url,
        )
    return _text_provider


def get_chat_llm_provider() -> LLMProvider:
    """返回对话 LLM 供应商单例（懒加载，多轮对话/新闻播报用）。

    按 settings.chat_llm_provider 路由；缺 key 时抛 RuntimeError。
    """
    global _chat_provider
    if _chat_provider is None:
        from app.core.config import settings

        _chat_provider = _build_llm(
            settings.chat_llm_provider,
            settings.chat_llm_api_key,
            settings.chat_llm_model,
            settings.chat_llm_base_url,
        )
    return _chat_provider


def reset_llm_provider_cache() -> None:
    """测试辅助：重置 LLM 单例缓存（切换配置后强制重建）。"""
    global _text_provider, _chat_provider
    _text_provider = None
    _chat_provider = None
