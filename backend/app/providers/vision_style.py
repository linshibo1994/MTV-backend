"""参考图画风提取 Provider 抽象与 OpenAI 兼容实现。"""

from __future__ import annotations

import base64
from typing import Any, Protocol, cast, runtime_checkable

from openai import AsyncOpenAI

from app.core.config import settings
from app.providers.llm import parse_json_object
from app.providers.registry import ProviderConfigurationError, normalize_provider


@runtime_checkable
class VisionStyleProvider(Protocol):
    provider_name: str
    model_name: str

    async def extract_style(self, images: list[tuple[bytes, str]]) -> dict:
        """从已鉴权图片字节提取结构化画风；不得接收裸存储地址。"""
        ...


class OpenAICompatVisionStyleProvider:
    """使用 OpenAI 兼容多模态消息，支持 OpenAI/Gemini/百炼兼容端点。"""

    def __init__(
        self,
        *,
        provider_name: str,
        api_key: str,
        model: str,
        base_url: str,
    ) -> None:
        provider = normalize_provider(provider_name)
        if provider not in {"openai", "gemini", "bailian", "openai_compatible"}:
            raise ProviderConfigurationError("当前对话供应商不支持参考图画风提取")
        if not api_key or not model:
            raise ProviderConfigurationError("参考图画风提取 Provider 配置不完整")
        self.provider_name: str = provider
        self.model_name = model
        defaults = {
            "openai": "https://api.openai.com/v1",
            "gemini": "https://generativelanguage.googleapis.com/v1beta/openai/",
            "bailian": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        }
        resolved_url = base_url or defaults.get(provider, "")
        if not resolved_url:
            raise ProviderConfigurationError("参考图画风提取 Provider 缺少 base_url")
        self._client = AsyncOpenAI(api_key=api_key, base_url=resolved_url)

    async def extract_style(self, images: list[tuple[bytes, str]]) -> dict:
        content: list[dict] = [
            {
                "type": "text",
                "text": (
                    "综合这些参考图，只提取可迁移的视觉画风，不复述人物身份或剧情。"
                    "返回 JSON 对象：prompt_zh、prompt_en、negative_prompt、visual_style；"
                    "visual_style 只能是 live_action、anime 或 null。"
                ),
            }
        ]
        for data, mime_type in images:
            encoded = base64.b64encode(data).decode("ascii")
            content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:{mime_type};base64,{encoded}"},
                }
            )
        response = await self._client.chat.completions.create(
            model=self.model_name,
            messages=cast(Any, [{"role": "user", "content": content}]),
            response_format={"type": "json_object"},
            temperature=0.2,
        )
        raw = response.choices[0].message.content or ""
        return parse_json_object(raw, kind="vision_style")


def get_vision_style_provider() -> VisionStyleProvider:
    return OpenAICompatVisionStyleProvider(
        provider_name=settings.chat_llm_provider,
        api_key=settings.chat_llm_api_key,
        model=settings.chat_llm_model,
        base_url=settings.chat_llm_base_url,
    )
