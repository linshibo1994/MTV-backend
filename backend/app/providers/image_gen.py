"""文生图 Provider 抽象与工厂路由。

ImageGenProvider 协议统一文生图调用入口：
generate(prompt, model, size, aspect, count, negative_prompt) -> list[ImageResult]。

get_image_provider(model) 路由策略（模型别名优先 + .env 未知模型回退）：
  1. 调用方显式传入 provider -> 对应厂商。
  2. model 为已知别名 -> 对应厂商：
     - banana-2     -> Google Nano Banana (image_gen_google)
     - gpt-image-2  -> OpenAI GPT Image (image_gen_openai)
     - gp-im-2      -> NeoLink 图片生成 (image_gen_neolink)
     - wan2.1-* / wanx-* / qwen-image* -> 阿里百炼通义万相 (image_gen_bailian)
  3. model 为未知别名/真实 id -> 按 settings.image_gen_provider 选厂商，model 作为真实 id。
  4. 缺对应 API key 由具体实现抛 RuntimeError，worker 捕获后 release 积分。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable

from app.providers.registry import ProviderError, UnsupportedProviderError, normalize_provider


class ImageSafetyCategory(StrEnum):
    """可跨供应商审计的最小内容安全分类。"""

    SAFETY = "safety"
    BLOCKLIST = "blocklist"
    PROHIBITED_CONTENT = "prohibited_content"
    IMAGE_SAFETY = "image_safety"
    PRIVACY = "privacy"


class ImageSafetyRejectedError(ProviderError):
    """供应商明确因内容安全拒绝，且没有可用图片结果。

    异常只保存规范化厂商、稳定分类和脱敏代码，不保存原始响应或用户提示词。
    """

    def __init__(
        self,
        *,
        provider: str,
        category: ImageSafetyCategory,
        code: str,
    ) -> None:
        normalized_provider = provider.strip().lower()
        normalized_code = _normalize_safety_token(code)
        if not normalized_provider or not normalized_code:
            raise ValueError("内容安全拒绝必须包含规范化厂商与稳定代码")
        self.provider = normalized_provider
        self.category = category
        self.code = normalized_code
        super().__init__(
            f"image_generation_safety_rejected:{normalized_provider}:{category.value}:{normalized_code}"
        )


class PromptSoftenLevel(StrEnum):
    """确定性的递进软化等级；Worker 后续可按顺序调度。"""

    GENTLE = "gentle"
    CONSERVATIVE = "conservative"
    SYMBOLIC = "symbolic"


@dataclass(frozen=True)
class SoftenedPrompt:
    prompt: str
    level: PromptSoftenLevel
    strategy_version: str
    changed: bool


_SOFTEN_STRATEGY_VERSION = "image-safety-soften.v1"
_PROTECTED_TOKEN_PATTERN = re.compile(r"\{\{[^{}]+\}\}|\{[^{}]+\}|<[^<>]+>")
_SOFTEN_REPLACEMENTS: dict[PromptSoftenLevel, tuple[tuple[str, str], ...]] = {
    PromptSoftenLevel.GENTLE: (
        ("鲜血淋漓", "克制的受伤痕迹"),
        ("血肉模糊", "含蓄的受伤表现"),
        ("大量鲜血", "少量非写实红色痕迹"),
        ("喷血", "以镜外反应表现受伤"),
        ("砍头", "镜外表现角色倒下"),
        ("断肢", "用遮挡表现严重受伤"),
        ("裸露", "穿着得体且完整遮挡"),
    ),
    PromptSoftenLevel.CONSERVATIVE: (
        ("鲜血", "非写实红色痕迹"),
        ("流血", "受伤后的克制反应"),
        ("尸体", "静止倒地的人物剪影"),
        ("杀死", "使其失去行动能力"),
        ("杀害", "使其退出冲突"),
        ("枪击", "画外冲突与声光反应"),
        ("刺穿", "被击中后的动作反应"),
        ("裸体", "穿着完整且得体"),
    ),
    PromptSoftenLevel.SYMBOLIC: (
        ("血", "象征性的红色光影"),
        ("死亡", "以空镜和人物反应暗示离场"),
        ("死去", "以空镜和人物反应暗示离场"),
        ("暴力", "非写实、无伤口细节的戏剧冲突"),
        ("攻击", "克制且非接触式的对峙"),
        ("搏斗", "剪影化、非接触式的动作对峙"),
        ("情色", "克制、得体且无裸露的情感表达"),
        ("性爱", "含蓄且无身体细节的亲密氛围"),
    ),
}
_SOFTEN_LEVEL_ORDER = (
    PromptSoftenLevel.GENTLE,
    PromptSoftenLevel.CONSERVATIVE,
    PromptSoftenLevel.SYMBOLIC,
)

_SAFETY_CODE_CATEGORY: dict[str, ImageSafetyCategory] = {
    "content_filter": ImageSafetyCategory.SAFETY,
    "content_policy_violation": ImageSafetyCategory.SAFETY,
    "safety": ImageSafetyCategory.SAFETY,
    "blocklist": ImageSafetyCategory.BLOCKLIST,
    "prohibited_content": ImageSafetyCategory.PROHIBITED_CONTENT,
    "image_prohibited_content": ImageSafetyCategory.PROHIBITED_CONTENT,
    "image_safety": ImageSafetyCategory.IMAGE_SAFETY,
    "data_inspection_failed": ImageSafetyCategory.SAFETY,
    "inappropriate_content": ImageSafetyCategory.SAFETY,
    "input_data_inspection_failed": ImageSafetyCategory.SAFETY,
    "output_data_inspection_failed": ImageSafetyCategory.SAFETY,
    "privacy_information": ImageSafetyCategory.PRIVACY,
}


def _normalize_safety_token(value: str) -> str:
    snake_case = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", value.strip())
    normalized = re.sub(r"[^a-z0-9._-]+", "_", snake_case.lower())
    return normalized.strip("_")[:128]


def classify_image_safety_code(value: object) -> tuple[ImageSafetyCategory, str] | None:
    """仅从稳定结构化代码识别内容安全拒绝，不读取自由错误消息。"""
    if not isinstance(value, str):
        return None
    normalized = _normalize_safety_token(value)
    if not normalized:
        return None
    normalized_parts = tuple(part for part in normalized.replace("-", "_").split(".") if part)
    for candidate, category in _SAFETY_CODE_CATEGORY.items():
        if candidate in normalized_parts or normalized == candidate:
            return category, normalized
    return None


def soften_prompt(
    prompt: str,
    *,
    level: PromptSoftenLevel | str,
) -> SoftenedPrompt:
    """纯确定性地软化已知高风险描写，并严格保护模板占位符。

    本函数不判断输入是否违规，也不发起 LLM/网络调用；只有捕获到明确的
    ``ImageSafetyRejectedError`` 后，Worker 才应按等级逐次调用。
    """
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError("待软化提示词不能为空")
    try:
        normalized_level = PromptSoftenLevel(level)
    except ValueError as exc:
        raise ValueError(f"不支持的软化等级: {level}") from exc
    protected_tokens = tuple(_PROTECTED_TOKEN_PATTERN.findall(prompt))
    selected_level_index = _SOFTEN_LEVEL_ORDER.index(normalized_level)

    def _soften_unprotected(match: re.Match[str]) -> str:
        text = match.group(0)
        for current_level in _SOFTEN_LEVEL_ORDER[: selected_level_index + 1]:
            for source, replacement in _SOFTEN_REPLACEMENTS[current_level]:
                text = text.replace(source, replacement)
        return text

    parts = _PROTECTED_TOKEN_PATTERN.split(prompt)
    softened_parts: list[str] = []
    token_iterator = iter(protected_tokens)
    for index, part in enumerate(parts):
        softened_parts.append(re.sub(r"[\s\S]+", _soften_unprotected, part))
        if index < len(parts) - 1:
            softened_parts.append(next(token_iterator))
    softened = "".join(softened_parts)
    if tuple(_PROTECTED_TOKEN_PATTERN.findall(softened)) != protected_tokens:
        raise ValueError("软化策略不得修改模板占位符")
    return SoftenedPrompt(
        prompt=softened,
        level=normalized_level,
        strategy_version=_SOFTEN_STRATEGY_VERSION,
        changed=softened != prompt,
    )


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
        self,
        prompt: str,
        model: str,
        size: str,
        aspect: str,
        count: int = 1,
        negative_prompt: str | None = None,
    ) -> list[ImageResult]:
        """根据正反向提示词、尺寸和比例生成 count 张图片。"""
        ...


def prompt_with_negative_constraints(prompt: str, negative_prompt: str | None) -> str:
    """为没有原生反向提示词参数的供应商构造等价文本约束。"""
    normalized = (negative_prompt or "").strip()
    if not normalized:
        return prompt
    return f"{prompt.rstrip()}\n\nDo not include in the generated image: {normalized}"


def _image_gen_configuration(configuration: object | None) -> object:
    """返回调用方传入的配置；默认延迟读取全局 Settings。"""
    if configuration is not None:
        return configuration
    from app.core.config import settings

    return settings


def image_gen_provider_is_selected(
    vendor: str,
    *,
    configuration: object | None = None,
) -> bool:
    """判断厂商是否为 IMAGE_GEN_PROVIDER 规范化后选定的图片生成厂商。

    空值、别名无法识别或非法厂商都视为不匹配，不能阻断实际图片生成调用。
    ``configuration`` 仅供 Provider 传入当前绑定的 Settings，避免测试替身绕过归属校验。
    """
    configured = _image_gen_configuration(configuration)
    try:
        selected_vendor = getattr(configured, "image_gen_provider")
        return normalize_provider(selected_vendor) == normalize_provider(vendor)
    except (AttributeError, TypeError, UnsupportedProviderError):
        return False


def image_gen_model_override_applies_to(
    vendor: str,
    *,
    configuration: object | None = None,
) -> bool:
    """仅在 IMAGE_GEN_MODEL 显式配置且厂商归属匹配时允许覆盖调用模型。

    这样历史节点显式指定其他厂商模型时，不会被当前部署的图片厂商默认模型覆盖。
    """
    configured = _image_gen_configuration(configuration)
    try:
        is_explicitly_configured = getattr(configured, "is_explicitly_configured")
        return bool(
            is_explicitly_configured("image_gen_model")
            and image_gen_provider_is_selected(vendor, configuration=configured)
        )
    except (AttributeError, TypeError):
        return False


def _provider_for_model(model: str) -> str | None:
    """按 model 别名识别厂商；无法识别返回 None（交由 .env provider 配置决定）。"""
    m = (model or "").strip().lower()
    if m == "banana-2" or m.startswith("gemini-") and "image" in m:
        return "google"
    if m.startswith("gpt-image"):
        return "openai"
    if m == "gp-im-2":
        return "neolink"
    if m.startswith(("wan2.", "wanx", "qwen-image")):
        return "bailian"
    return None


def get_image_provider(
    model: str | None = None, *, provider: str | None = None
) -> ImageGenProvider:
    """已知 model 别名优先；未知真实模型再按 .env provider 路由。

    缺少对应 API key 时由具体实现抛 RuntimeError，worker 捕获后 release 积分。
    """
    from app.core.config import settings

    # 1) 调用方显式 provider 优先。
    if provider:
        return _instantiate(provider)

    # 2) 已知模型别名代表产品层明确选择的供应商。即使部署默认 provider
    # 被设为 OpenAI/Gemini，也不能把 qwen-image 等百炼模型误发给其他厂商。
    if model:
        vendor = _provider_for_model(model)
        if vendor:
            return _instantiate(vendor)

    # 3) 未知真实模型才由显式环境配置决定供应商；类默认值仍作为最终回退。
    configured = (settings.image_gen_provider or "").strip().lower()
    if configured and settings.is_explicitly_configured("image_gen_provider"):
        return _instantiate(configured)

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
    if normalized == "neolink":
        from app.providers.image_gen_neolink import NeolinkImageGenProvider

        return NeolinkImageGenProvider()  # type: ignore[return-value]
    if normalized == "openai":
        from app.providers.image_gen_openai import OpenAIImageGenProvider

        return OpenAIImageGenProvider()  # type: ignore[return-value]
    raise AssertionError(f"遗漏的图片 provider 实现: {normalized}")


async def image_result_from_url(url: str) -> ImageResult:
    """默认禁用 URL 回退，避免 SSRF 风险。"""
    raise RuntimeError("Provider URL 回退已禁用")
