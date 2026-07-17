"""文生图 Provider 抽象与工厂路由。

ImageGenProvider 协议统一文生图调用入口：
generate(prompt, model, size, aspect, count) -> list[ImageResult]。

get_image_provider(model) 路由策略（.env 主导 + model 别名向后兼容）：
  1. model 为已知别名 -> 对应厂商：
     - banana-2     -> Google Nano Banana (image_gen_google)
     - gpt-image-2  -> OpenAI GPT Image (image_gen_openai)
     - wan2.1-* / wanx-* / qwen-image* -> 阿里百炼通义万相 (image_gen_bailian)
  2. model 为未知别名/真实 id -> 按 settings.image_gen_provider 选厂商，model 作为真实 id
  3. 缺对应 API key 由具体实现抛 RuntimeError，worker 捕获后 release 积分。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class ImageResult:
    """Provider 返回的强类型图片结果。"""

    data: bytes
    mime_type: str


@runtime_checkable
class ImageGenProvider(Protocol):
    """文生图供应商协议 — 返回图片二进制数据列表。

    generate 一次请求可返回多张图片（count 张），供多张选一场景减少 API 往返。
    """

    async def generate(
        self, prompt: str, model: str, size: str, aspect: str, count: int = 1
    ) -> list[ImageResult]:
        """根据 prompt/尺寸/比例生成 count 张图片，返回图片结果列表。"""
        ...


def _provider_for_model(model: str) -> str | None:
    """按 model 别名识别厂商；无法识别返回 None（交由 .env provider 配置决定）。"""
    m = (model or "").strip().lower()
    if m == "banana-2" or m.startswith("gemini-") and "image" in m:
        return "google"
    if m.startswith("gpt-image"):
        return "openai"
    if m.startswith(("wan2.", "wanx", "qwen-image")):
        return "bailian"
    return None


def get_image_provider(
    model: str | None = None, *, provider: str | None = None
) -> ImageGenProvider:
    """按 model 别名优先、否则按 .env image_gen_provider 路由到供应商实现。

    缺少对应 API key 时由具体实现抛 RuntimeError，worker 捕获后 release 积分。
    """
    from app.core.config import settings

    # 1) 调用方显式 provider 优先。
    if provider:
        return _instantiate(provider)

    # 2) 仅当 provider 确实来自环境变量/显式 Settings 入参时由配置主导；
    # 类默认值不能冒充用户配置，否则会破坏旧 model 别名路由。
    configured = (settings.image_gen_provider or "").strip().lower()
    if configured and settings.is_explicitly_configured("image_gen_provider"):
        return _instantiate(configured)

    # 3) 未显式配置 provider 时保留旧 model 别名路由。
    if model:
        vendor = _provider_for_model(model)
        if vendor:
            return _instantiate(vendor)

    # 4) 最后使用 Settings 默认值，旧部署仍默认为 OpenAI。
    return _instantiate(configured or "openai")


def _instantiate(vendor: str) -> ImageGenProvider:
    from app.providers.registry import require_capability

    normalized = require_capability(vendor, "image")
    if normalized == "gemini":
        from app.providers.image_gen_google import GoogleImageGenProvider

        return GoogleImageGenProvider()  # type: ignore[return-value]
    if normalized == "bailian":
        from app.providers.image_gen_bailian import BailianImageGenProvider

        return BailianImageGenProvider()  # type: ignore[return-value]
    if normalized == "openai":
        from app.providers.image_gen_openai import OpenAIImageGenProvider

        return OpenAIImageGenProvider()  # type: ignore[return-value]
    raise AssertionError(f"遗漏的图片 provider 实现: {normalized}")


async def image_result_from_url(url: str) -> ImageResult:
    """默认禁用 URL 回退，避免 SSRF 风险。"""
    raise RuntimeError("Provider URL 回退已禁用")
