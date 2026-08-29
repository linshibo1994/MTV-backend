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
    fallback_models = {
        "volcengine": configured_settings.seedance_model,
        "neolink": configured_settings.seedance_model,
        "bailian": configured_settings.dashscope_video_model,
        "openai": configured_settings.openai_video_model,
        "gemini": configured_settings.google_video_model,
        "mgrouter": getattr(
            configured_settings,
            "mgrouter_video_model",
            "AICC-Doubao-Seedance-2.0",
        ),
    }
    model = (
        configured_settings.seedance_model
        if provider == "neolink"
        else configured_settings.video_gen_model or fallback_models[provider]
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
        raw_provider = configured_settings.video_default_provider
        model = configured_settings.video_gen_model or configured_settings.seedance_model
    else:
        raw_provider = provider_cfg.provider_key
        model = provider_cfg.model
    provider = require_capability(raw_provider, "video")
    return RuntimeVideoModel(
        provider=provider,
        model=model,
        reference_image_audit_required=provider == "volcengine",
        task_provider_key=raw_provider,
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
    fallback_models = {
        "volcengine": configured_settings.seedance_model,
        "neolink": configured_settings.seedance_model,
        "bailian": configured_settings.dashscope_video_model,
        "openai": configured_settings.openai_video_model,
        "gemini": configured_settings.google_video_model,
        "mgrouter": getattr(
            configured_settings,
            "mgrouter_video_model",
            "AICC-Doubao-Seedance-2.0",
        ),
    }
    # 与 env 模式 resolve_env_video_model 保持一致：请求未显式指定 model 时，
    # 优先使用服务端能力级默认 VIDEO_GEN_MODEL，再回退该厂商专属保底模型。
    effective_model = (
        normalized_model
        or configured_settings.video_gen_model
        or fallback_models[vendor]
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
    "mgrouter": "mgrouter",
}
