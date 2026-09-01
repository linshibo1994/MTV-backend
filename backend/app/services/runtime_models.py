"""运行时模型配置解析。

所有面向调用方的模型信息都从这里取值，避免 Web 接口与实际视频创建在
``VIDEO_PROVIDER_SELECTION_MODE`` 下出现不同的供应商或模型。此模块仅返回
可公开的供应商和模型名称，绝不返回密钥、基础地址或配置完整性。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.crud import video as video_crud
from app.providers.registry import require_capability


@dataclass(frozen=True)
class RuntimeModel:
    """一项能力实际使用的规范化供应商和模型。"""

    provider: str
    model: str


@dataclass(frozen=True)
class RuntimeVideoModel(RuntimeModel):
    """视频能力的运行时配置。"""

    reference_image_audit_required: bool
    # 数据库模式保留原始 key，避免改变既有任务记录和供应商切换语义；provider
    # 始终是给前端和审核判定使用的规范化值。
    task_provider_key: str


_DATABASE_VIDEO_PROVIDER_KEYS = frozenset(
    {
        "jianyu",
        "modelsai",
        "volcengine",
        "doubao",
        "neolink",
        "bailian",
        "openai",
        "gemini",
        "entpilot",
    }
)
_ENTPILOT_VIDEO_MODELS = frozenset(
    {
        "seedance-2.0-1080p",
        "seedance-2.0-480p",
        "seedance-2.0-4k",
        "seedance-2.0-720p",
        "seedance-2.0-fast-480p",
        "seedance-2.0-fast-720p",
        "seedance-2.5-1080p",
        "seedance-2.5-480p",
        "seedance-2.5-720p",
    }
)


def _validate_video_model(provider: str, model: str) -> str:
    """在创建任务前校验供应商专属模型清单。"""

    normalized = str(model or "").strip()
    if provider == "entpilot" and normalized.lower() not in _ENTPILOT_VIDEO_MODELS:
        raise ValueError(f"EntPilot 不支持视频模型: {normalized or '<空>'}")
    return normalized


def _video_fallback_models(configured_settings: Any) -> dict[str, str]:
    """按规范化视频供应商返回专属环境保底模型。"""

    return {
        "volcengine": getattr(configured_settings, "seedance_model", ""),
        "neolink": getattr(configured_settings, "seedance_model", ""),
        "bailian": getattr(configured_settings, "dashscope_video_model", ""),
        "openai": getattr(configured_settings, "openai_video_model", ""),
        "gemini": getattr(configured_settings, "google_video_model", ""),
        "entpilot": getattr(
            configured_settings,
            "entpilot_video_model",
            "seedance-2.0-720p",
        ),
    }


def _configured_video_provider(configured_settings: Any) -> str | None:
    """返回 ``VIDEO_GEN_PROVIDER`` 的规范化值，配置无效时保持不可用。"""

    raw_provider = str(getattr(configured_settings, "video_gen_provider", "") or "").strip()
    if not raw_provider:
        return None
    try:
        return require_capability(raw_provider, "video")
    except Exception:  # noqa: BLE001 - 仅用于判断通用模型是否属于当前厂商
        return None


def _video_model_for_provider(configured_settings: Any, provider: str) -> str:
    """按目标供应商选择模型，避免复用其他供应商遗留的通用模型。"""

    # NeoLink 的历史契约始终使用 SEEDANCE_MODEL，即使 VIDEO_GEN_MODEL 已填写。
    if provider == "neolink":
        return str(
            getattr(configured_settings, "seedance_model", "")
            or getattr(configured_settings, "video_gen_model", "")
            or ""
        ).strip()

    generic_model = str(getattr(configured_settings, "video_gen_model", "") or "").strip()
    if generic_model and _configured_video_provider(configured_settings) == provider:
        # 旧部署可能保留 ``doubao-*`` 或其他未接入值，即使 provider 已切到
        # EntPilot；这里只复用适配器已接入的精确清单，避免任务提交后才失败。
        if provider != "entpilot" or generic_model.lower() in _ENTPILOT_VIDEO_MODELS:
            return generic_model

    fallback_models = _video_fallback_models(configured_settings)
    fallback = str(fallback_models.get(provider, "") or "").strip()
    if provider == "entpilot" and fallback.lower() not in _ENTPILOT_VIDEO_MODELS:
        # EntPilot 只接受公开的九个 Seedance 模型；配置遗留值时回退到
        # 可提交的默认模型，避免任务创建后才因白名单校验失败。
        return "seedance-2.0-720p"
    return fallback


def _task_provider_key(raw_provider: str, normalized_provider: str) -> str:
    """保留数据库允许的历史 key；未知别名统一成规范 key。"""

    if raw_provider in _DATABASE_VIDEO_PROVIDER_KEYS:
        return raw_provider
    return _STABLE_VIDEO_PROVIDER_KEYS.get(normalized_provider, normalized_provider)


def _default_video_runtime_model(configured_settings: Any) -> RuntimeVideoModel:
    """解析没有数据库 active 行时的服务端视频默认。"""

    raw_provider = str(getattr(configured_settings, "video_default_provider", "entpilot"))
    provider = require_capability(raw_provider, "video")
    model = _validate_video_model(
        provider,
        _video_model_for_provider(configured_settings, provider),
    )
    return RuntimeVideoModel(
        provider=provider,
        model=model,
        reference_image_audit_required=provider == "volcengine",
        task_provider_key=_task_provider_key(raw_provider, provider),
    )


def resolve_default_video_model(configured_settings: Any | None = None) -> RuntimeVideoModel:
    """返回没有数据库 active 配置时实际采用的视频供应商和模型。"""

    configured_settings = settings if configured_settings is None else configured_settings
    return _default_video_runtime_model(configured_settings)


def resolve_text_llm_model(configured_settings: Any | None = None) -> RuntimeModel:
    configured_settings = settings if configured_settings is None else configured_settings
    return RuntimeModel(
        provider=require_capability(configured_settings.text_llm_provider, "text"),
        model=configured_settings.text_llm_model,
    )


def resolve_chat_llm_model(configured_settings: Any | None = None) -> RuntimeModel:
    configured_settings = settings if configured_settings is None else configured_settings
    return RuntimeModel(
        provider=require_capability(configured_settings.chat_llm_provider, "chat"),
        model=configured_settings.chat_llm_model,
    )


def resolve_image_gen_model(configured_settings: Any | None = None) -> RuntimeModel:
    configured_settings = settings if configured_settings is None else configured_settings
    return RuntimeModel(
        provider=require_capability(configured_settings.image_gen_provider, "image"),
        model=configured_settings.image_gen_model,
    )


def resolve_env_video_model(configured_settings: Any | None = None) -> RuntimeVideoModel:
    """解析 ``env`` 模式创建视频任务会采用的配置。"""
    configured_settings = settings if configured_settings is None else configured_settings
    provider = require_capability(configured_settings.video_gen_provider, "video")
    model = _validate_video_model(
        provider,
        _video_model_for_provider(configured_settings, provider),
    )
    return RuntimeVideoModel(
        provider=provider,
        model=model,
        reference_image_audit_required=provider == "volcengine",
        task_provider_key=provider,
    )


async def resolve_video_model(
    db: AsyncSession, configured_settings: Any | None = None
) -> RuntimeVideoModel:
    """解析一次视频创建实际会选择的供应商和模型。

    database 模式严格复用 ``create_video_job`` 的 active-provider 查询及其无配置
    回退；因此前端展示和参考图审核门禁不会与实际任务漂移。
    """
    configured_settings = settings if configured_settings is None else configured_settings
    if configured_settings.video_provider_selection_mode == "env":
        return resolve_env_video_model(configured_settings)

    provider_cfg = await video_crud.get_active_provider(db, "all_reference")
    if provider_cfg is None:
        return _default_video_runtime_model(configured_settings)
    else:
        raw_provider = provider_cfg.provider_key
        model = provider_cfg.model
    provider = require_capability(raw_provider, "video")
    model = _validate_video_model(provider, model)
    return RuntimeVideoModel(
        provider=provider,
        model=model,
        reference_image_audit_required=provider == "volcengine",
        task_provider_key=_task_provider_key(raw_provider, provider),
    )


async def resolve_request_video_model(
    db: AsyncSession,
    configured_settings: Any | None = None,
    *,
    provider: str | None = None,
    model: str | None = None,
) -> RuntimeVideoModel:
    """解析带请求级显式覆盖的视频模型；优先级为显式请求 > 服务端默认。

    显式 provider 需通过 video 能力校验并规范化；model 缺省时回退该厂商
    的 .env 保底模型。task_provider_key 使用 ``get_video_provider`` 认识的
    稳定 key（与数据库 CHECK 约束对齐），保证任务行可持久化、worker 可路由。
    """
    normalized_provider = (provider or "").strip()
    normalized_model = (model or "").strip()
    if not normalized_provider and not normalized_model:
        return await resolve_video_model(db, configured_settings)
    if not normalized_provider:
        raise ValueError("请求级视频路由必须显式传入 provider")
    configured_settings = settings if configured_settings is None else configured_settings
    vendor = require_capability(normalized_provider, "video")
    # 请求未显式指定 model 时，只有通用模型声明的供应商就是目标供应商才可
    # 复用它；否则回退该供应商专属字段，避免 EntPilot 收到旧 Doubao 模型名。
    effective_model = _validate_video_model(
        vendor,
        normalized_model or _video_model_for_provider(configured_settings, vendor),
    )
    # get_video_provider 的别名表中 jianyu/modelsai 映射 volcengine；数据库
    # CHECK 约束接受 jianyu/modelsai/volcengine/doubao 等历史 key。这里统一
    # 规范化到主 key，避免别名任务在恢复/取消链路无法路由。
    task_provider_key = _STABLE_VIDEO_PROVIDER_KEYS.get(vendor, vendor)
    return RuntimeVideoModel(
        provider=vendor,
        model=effective_model,
        reference_image_audit_required=vendor == "volcengine",
        task_provider_key=task_provider_key,
    )


# 规范化厂商名到数据库 CHECK 约束与 get_video_provider 均接受的任务 key。
_STABLE_VIDEO_PROVIDER_KEYS = {
    "volcengine": "volcengine",
    "bailian": "bailian",
    "openai": "openai",
    "gemini": "gemini",
    "neolink": "neolink",
    "entpilot": "entpilot",
}
