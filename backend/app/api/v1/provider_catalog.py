"""供应商目录 API — GET /provider-catalog。

返回前端模型选择器所需的供应商/模型清单；登录即可读，
不返回密钥、base_url 或任何配置完整性信息。
"""

from fastapi import APIRouter, Depends

from app.core.auth import get_current_user
from app.models.user import User
from app.services.provider_catalog import ProviderCatalogOut, build_provider_catalog

router = APIRouter()


@router.get(
    "/provider-catalog",
    response_model=ProviderCatalogOut,
    summary="获取可用供应商与模型目录",
)
async def get_provider_catalog(
    user: User = Depends(get_current_user),
) -> ProviderCatalogOut:
    """返回各供应商的能力、推荐模型与保底模型（仅登录用户可读）。"""
    # 保留显式依赖，确保该接口和其他项目内接口使用相同的登录态保护。
    del user
    return build_provider_catalog()
