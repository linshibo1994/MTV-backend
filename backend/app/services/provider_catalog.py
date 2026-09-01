"""供应商目录服务 — 前端模型选择器的数据源。

静态推荐模型清单（含中文 label）与 .env 已配置的保底模型合并输出；
只暴露厂商 key、模型 ID 与能力标签，绝不返回密钥、base_url 等敏感配置。
已配置凭证的供应商标记 available: true（仅判断 Key 是否非空）。
"""

from __future__ import annotations

from typing import Literal, cast

from pydantic import BaseModel, Field

from app.providers.registry import ProviderCapability, ProviderError, normalize_provider

__all__ = ["ProviderCatalogOut", "ProviderCatalogEntry", "ProviderModelEntry", "build_provider_catalog"]


class ProviderModelEntry(BaseModel):
    """目录中的单个模型条目。"""

    id: str
    label: str
    capabilities: list[Literal["text", "chat", "image", "video"]] = Field(
        default_factory=list
    )
    is_default: bool = False


class ProviderCatalogEntry(BaseModel):
    """目录中的单个供应商条目。"""

    key: str
    label: str
    capabilities: list[Literal["text", "chat", "image", "video"]] = Field(
        default_factory=list
    )
    models: list[ProviderModelEntry] = Field(default_factory=list)
    default_model: str | None = None
    available: bool = False


class ProviderCatalogOut(BaseModel):
    """供应商目录响应；登录即可读，不包含任何密钥。"""

    providers: list[ProviderCatalogEntry] = Field(default_factory=list)


# 各厂商推荐模型清单；(模型 ID, 中文 label, 能力标签列表)。
# 清单只是前端下拉建议，不是可用性保证：模型可用性取决于厂商 Key 权限，
# 调用失败时由 FallbackLLMProvider 用保底模型兜底。
_RECOMMENDED_MODELS: dict[str, list[tuple[str, str, tuple[ProviderCapability, ...]]]] = {
    "bailian": [
        ("qwen-plus", "通义千问 Plus", ("text", "chat")),
        ("qwen-max", "通义千问 Max", ("text", "chat")),
        ("qwen-turbo", "通义千问 Turbo", ("text", "chat")),
        ("qwen3-max", "通义千问 3 Max", ("text", "chat")),
        ("qwen-vl-plus", "通义千问 VL Plus", ("text", "chat")),
        ("wan2.1-t2i-plus", "通义万相 2.1 文生图 Plus", ("image",)),
        ("qwen-image", "Qwen Image", ("image",)),
        ("qwen-image-2.0", "Qwen Image 2.0", ("image",)),
        ("qwen-image-3.0", "Qwen Image 3.0", ("image",)),
        ("qwen-image-3.0-pro", "Qwen Image 3.0 Pro", ("image",)),
        ("wanx2.1-t2v-turbo", "通义万相 2.1 文生视频 Turbo", ("video",)),
        ("wanx2.1-i2v-turbo", "通义万相 2.1 图生视频 Turbo", ("video",)),
        ("wan2.5-i2v-plus", "通义万相 2.5 图生视频 Plus", ("video",)),
        ("wan3.0-video-plus", "通义万相 3.0 视频 Plus", ("video",)),
    ],
    "deepseek": [
        ("deepseek-v4-flash", "DeepSeek V4 Flash", ("text", "chat")),
        ("deepseek-v4-pro", "DeepSeek V4 Pro", ("text", "chat")),
    ],
    "volcengine": [
        ("doubao-seedance-2-0-260128", "豆包 Seedance 2.0", ("video",)),
    ],
    "neolink": [
        ("gp-im-2", "NeoLink GP-IM-2", ("image",)),
        ("qwen-turbo", "通义千问 Turbo（NeoLink）", ("text", "chat")),
    ],
    "openai": [
        ("gpt-4o", "GPT-4o", ("text", "chat")),
        ("gpt-image-2", "GPT Image 2", ("image",)),
        ("sora-2", "Sora 2", ("video",)),
    ],
    "anthropic": [
        ("claude-sonnet-5", "Claude Sonnet 5", ("text", "chat")),
    ],
    "gemini": [
        ("gemini-2.5-flash", "Gemini 2.5 Flash", ("text", "chat")),
        ("gemini-3.1-flash-image", "Gemini 3.1 Flash Image", ("image",)),
        ("veo-3.1-generate-preview", "Veo 3.1", ("video",)),
    ],
    "entpilot": [
        ("seedance-2.0-1080p", "Seedance 2.0 1080p", ("video",)),
        ("seedance-2.0-480p", "Seedance 2.0 480p", ("video",)),
        ("seedance-2.0-4k", "Seedance 2.0 4K", ("video",)),
        ("seedance-2.0-720p", "Seedance 2.0 720p", ("video",)),
        ("seedance-2.0-fast-480p", "Seedance 2.0 Fast 480p", ("video",)),
        ("seedance-2.0-fast-720p", "Seedance 2.0 Fast 720p", ("video",)),
        ("seedance-2.5-1080p", "Seedance 2.5 1080p", ("video",)),
        ("seedance-2.5-480p", "Seedance 2.5 480p", ("video",)),
        ("seedance-2.5-720p", "Seedance 2.5 720p", ("video",)),
    ],
}

_ENTPILOT_VIDEO_MODELS = frozenset(
    model_id
    for model_id, _label, model_capabilities in _RECOMMENDED_MODELS["entpilot"]
    if "video" in model_capabilities
)

_PROVIDER_LABELS: dict[str, str] = {
    "bailian": "阿里百炼",
    "deepseek": "DeepSeek",
    "volcengine": "火山方舟",
    "neolink": "NeoLink",
    "openai": "OpenAI",
    "anthropic": "Anthropic",
    "gemini": "Google Gemini",
    "entpilot": "EntPilot Seedance",
}

# 各厂商凭证是否已配置（仅判断非空，绝不返回 Key 本身）。
_PROVIDER_CREDENTIAL_FIELDS: dict[str, tuple[str, ...]] = {
    "bailian": ("dashscope_api_key",),
    "deepseek": ("deepseek_api_key",),
    "volcengine": ("volcengine_api_key",),
    "neolink": ("neolink_api_key",),
    "openai": ("openai_api_key",),
    "anthropic": ("anthropic_api_key",),
    "gemini": ("gemini_api_key", "google_api_key"),
    "entpilot": ("entpilot_api_key",),
}

# 各厂商 .env 配置的保底模型字段（按能力分组）。能力级字段（TEXT_LLM_*
# /IMAGE_GEN_* 等）仅在能力 provider 与厂商匹配时才作为该厂商的默认。
_PROVIDER_DEFAULT_MODEL_FIELDS: dict[str, dict[str, tuple[str, ...]]] = {
    "bailian": {
        "text": ("text_llm_model",),
        "chat": ("chat_llm_model",),
        "image": ("image_gen_model",),
        "video": ("video_gen_model", "dashscope_video_model"),
    },
    "deepseek": {"text": ("deepseek_model",), "chat": ("deepseek_model",)},
    "volcengine": {"video": ("video_gen_model", "seedance_model")},
    "neolink": {
        "text": ("text_llm_model",),
        "chat": ("chat_llm_model",),
        "image": ("image_gen_model",),
        "video": ("video_gen_model", "seedance_model"),
    },
    "openai": {
        "text": ("openai_llm_model",),
        "chat": ("openai_llm_model",),
        "image": ("image_gen_model", "openai_image_model"),
        "video": ("video_gen_model", "openai_video_model"),
    },
    "anthropic": {
        "text": ("anthropic_model",),
        "chat": ("anthropic_model",),
    },
    "gemini": {
        "text": ("gemini_llm_model",),
        "chat": ("gemini_llm_model",),
        "image": ("image_gen_model", "google_image_model"),
        "video": ("video_gen_model", "google_video_model"),
    },
    "entpilot": {"video": ("video_gen_model", "entpilot_video_model")},
}

# 能力级字段所属的 provider 字段名：能力默认模型只在 provider 匹配时生效。
_CAPABILITY_PROVIDER_FIELDS = {
    "text_llm_model": "text_llm_provider",
    "chat_llm_model": "chat_llm_provider",
    "image_gen_model": "image_gen_provider",
    "video_gen_model": "video_gen_provider",
}

# 供应商能力矩阵（与 app.providers.registry._CAPABILITIES 保持一致的方向）。
_PROVIDER_CAPABILITIES: dict[str, tuple[ProviderCapability, ...]] = {
    "bailian": ("text", "chat", "image", "video"),
    "deepseek": ("text", "chat"),
    "volcengine": ("video",),
    "neolink": ("text", "chat", "image", "video"),
    "openai": ("text", "chat", "image", "video"),
    "anthropic": ("text", "chat"),
    "gemini": ("text", "chat", "image", "video"),
    "entpilot": ("video",),
}


def _provider_is_available(key: str) -> bool:
    """厂商任一凭证字段非空即视为已配置。"""
    from app.core.config import settings

    for field_name in _PROVIDER_CREDENTIAL_FIELDS.get(key, ()):
        value = getattr(settings, field_name, "")
        if str(value or "").strip():
            return True

    # 能力级通用 Key 只在明确路由到该供应商时生效，避免把其他厂商的
    # VIDEO_GEN_API_KEY 误报为 EntPilot 凭证。Settings 的回退赋值会将
    # 非显式字段标记为 fallback，因此这里只接受用户显式配置的通用 Key。
    if key == "entpilot":
        is_explicit = getattr(settings, "is_explicitly_configured", None)
        provider_raw = str(getattr(settings, "video_gen_provider", "") or "").strip()
        provider_key: str
        try:
            provider_key = normalize_provider(provider_raw)
        except ProviderError:
            provider_key = provider_raw.lower()
        if (
            provider_key == key
            and callable(is_explicit)
            and is_explicit("video_gen_api_key")
            and str(getattr(settings, "video_gen_api_key", "") or "").strip()
        ):
            return True
    return False


def _provider_default_models(key: str) -> dict[str, str]:
    """读取厂商在各能力下 .env 显式配置的保底模型。

    能力级字段（TEXT_LLM_MODEL 等）只在对应 *_PROVIDER 被 .env 显式
    配置为该厂商时才视为该厂商的默认，避免两类误标：provider 字段
    未配置时命中类默认值（如 volcengine），把其他厂商的 VIDEO_GEN_MODEL
    错标给该厂商。
    """
    from app.core.config import settings

    defaults: dict[str, str] = {}
    for capability, field_names in _PROVIDER_DEFAULT_MODEL_FIELDS.get(key, {}).items():
        for field_name in field_names:
            provider_field = _CAPABILITY_PROVIDER_FIELDS.get(field_name)
            if provider_field is not None:
                provider_explicit = settings.is_explicitly_configured(provider_field)
                if not provider_explicit and not settings.is_explicitly_configured(field_name):
                    # provider 与模型都未显式配置时不消费能力级字段，
                    # 否则类默认值（或其他厂商回退值）会被误标过来。
                    continue
                configured_provider_raw = str(
                    getattr(settings, provider_field, "")
                ).strip()
                configured_provider: str
                try:
                    configured_provider = normalize_provider(configured_provider_raw)
                except ProviderError:
                    # 未知配置仍交给运行时能力校验；目录构建不能因一个坏配置中断。
                    configured_provider = configured_provider_raw.lower()
                # provider 显式配置但指向其他厂商时同样不消费。
                if configured_provider != key:
                    continue
            value = getattr(settings, field_name, "").strip()
            if value:
                if (
                    key == "entpilot"
                    and capability == "video"
                    and value.lower() not in _ENTPILOT_VIDEO_MODELS
                ):
                    # EntPilot 只开放固定的九个 Seedance 模型；非法配置不能
                    # 被目录暴露为可选项，否则前端提交后必然被适配器拒绝。
                    continue
                defaults[capability] = value
                break
    return defaults


def build_provider_catalog() -> ProviderCatalogOut:
    """构建供应商目录：静态推荐模型 + .env 保底模型合并。"""
    entries: list[ProviderCatalogEntry] = []
    for key, capabilities in _PROVIDER_CAPABILITIES.items():
        default_models = _provider_default_models(key)
        models_by_id: dict[str, ProviderModelEntry] = {}
        for model_id, label, model_capabilities in _RECOMMENDED_MODELS.get(key, []):
            models_by_id[model_id] = ProviderModelEntry(
                id=model_id,
                label=label,
                capabilities=list(model_capabilities),
            )
        # .env 配置的保底模型加入目录并标注 is_default；与推荐清单重合时
        # 只更新标记，不重复条目。
        default_model: str | None = None
        for capability, model_id in sorted(default_models.items()):
            if not model_id:
                continue
            # 该映射由内部能力字段构造；在写入 Pydantic Literal 列表前显式收窄类型。
            typed_capability = cast(ProviderCapability, capability)
            if default_model is None:
                default_model = model_id
            existing = models_by_id.get(model_id)
            if existing is None:
                models_by_id[model_id] = ProviderModelEntry(
                    id=model_id,
                    label=model_id,
                    capabilities=[typed_capability],
                    is_default=True,
                )
            else:
                existing.is_default = True
                if typed_capability not in existing.capabilities:
                    existing.capabilities.append(typed_capability)
        # 首个 text/chat 保底作为 default_model 展示；纯媒体厂商取首个。
        if default_model is None:
            default_model = next(iter(sorted(default_models.values())), None)
        entries.append(
            ProviderCatalogEntry(
                key=key,
                label=_PROVIDER_LABELS.get(key, key),
                capabilities=list(capabilities),
                models=list(models_by_id.values()),
                default_model=default_model,
                available=_provider_is_available(key),
            )
        )
    return ProviderCatalogOut(providers=entries)
