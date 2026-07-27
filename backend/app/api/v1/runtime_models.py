"""运行时模型配置 API。"""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.models.user import User
from app.schemas.runtime_models import RuntimeModelOut, RuntimeModelsOut, RuntimeVideoModelOut
from app.services.runtime_models import (
    resolve_chat_llm_model,
    resolve_image_gen_model,
    resolve_text_llm_model,
    resolve_video_model,
)

router = APIRouter()


@router.get(
    "/runtime-models",
    response_model=RuntimeModelsOut,
    summary="获取当前运行时模型配置",
)
async def get_runtime_models(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> RuntimeModelsOut:
    """返回当前实际生效的模型厂商和模型名称（仅登录用户可读）。"""
    # 保留显式依赖，确保该接口和其他项目内接口使用相同的登录态保护。
    del user
    text_llm = resolve_text_llm_model()
    chat_llm = resolve_chat_llm_model()
    image_gen = resolve_image_gen_model()
    video_gen = await resolve_video_model(db)
    return RuntimeModelsOut(
        text_llm=RuntimeModelOut(**text_llm.__dict__),
        chat_llm=RuntimeModelOut(**chat_llm.__dict__),
        image_gen=RuntimeModelOut(**image_gen.__dict__),
        video_gen=RuntimeVideoModelOut(
            provider=video_gen.provider,
            model=video_gen.model,
            reference_image_audit_required=video_gen.reference_image_audit_required,
        ),
    )
