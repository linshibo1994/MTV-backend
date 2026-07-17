"""画布模块 API 路由 —— T5。

端点:
- GET    /materials/<id>                     画布项目元数据
- GET    /materials/<id>/canvas             画布完整状态 {material_id, nodes[], edges[]}
- POST   /materials/<mid>/canvas/delta       增量保存 (节点移动/增删/参数变更)
- GET    /materials/<mid>/image/<image_id>  图片资产元数据 (实际 URL 由前端拼 media_proxy_prefix+storage_key)

注: GET /scripts/<script_id>/canvas-project/ensure 由 T4 资产库模块实现，此处不重复。

鉴权: 每个端点先加载 Materials -> 取 script_id -> 加载 Script -> 调用
permissions.check_script_access 做资源级授权。写端点 (delta) 用 require_write=True。
与 asset_library.ensure_canvas_project 的 _load_script_or_403 模式一致。
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.errors import NotFoundError
from app.core.permissions import Action, check_script_access
from app.crud import materials as materials_crud
from app.models.materials import Materials
from app.models.script import Script
from app.models.user import User
from app.schemas.materials import (
    CanvasDeltaIn,
    CanvasStateOut,
    ImageAssetOut,
    MaterialsOut,
)

router = APIRouter(prefix="/materials", tags=["画布"])


async def _authorize_material(
    db: AsyncSession,
    material_id: str,
    user: User,
    require_write: bool = False,
) -> Materials:
    """加载画布项目并校验当前用户对所属剧本的访问权限。

    - 不存在 -> 404；存在但无权由权限层返回 403/401
    - 无权 -> check_script_access 抛 ForbiddenError/NotAuthenticatedError
    返回 Materials ORM 对象。
    """
    material = await db.get(Materials, material_id)
    if material is None:
        raise NotFoundError("画布项目不存在")
    script = (
        await db.execute(select(Script).where(Script.id == material.script_id))
    ).scalar_one_or_none()
    if script is None:
        raise NotFoundError("剧本不存在")
    await check_script_access(
        db, user, script, action=Action.EDIT if require_write else Action.READ
    )
    return material


@router.get("/{material_id}", response_model=MaterialsOut)
async def get_materials(
    material_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> MaterialsOut:
    """获取画布项目元数据。"""
    await _authorize_material(db, material_id, user, require_write=False)
    material = await materials_crud.get_materials(db, material_id)
    return await materials_crud.to_materials_out(material)


@router.get("/{material_id}/canvas", response_model=CanvasStateOut)
async def get_canvas(
    material_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> CanvasStateOut:
    """获取画布完整状态 (nodes + edges)。"""
    await _authorize_material(db, material_id, user, require_write=False)
    return await materials_crud.get_canvas(db, material_id)


@router.post("/{material_id}/canvas/delta", response_model=CanvasStateOut)
async def post_canvas_delta(
    material_id: str,
    delta: CanvasDeltaIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> CanvasStateOut:
    """增量保存画布 —— 节点移动/增删/参数变更的 diff。

    body 为变更的 nodes/edges 差异数据，服务端 upsert/delete 后返回最新画布状态。
    """
    # 写端点 require_write=True
    await _authorize_material(db, material_id, user, require_write=True)
    state = await materials_crud.apply_delta(db, material_id, delta)
    await db.commit()
    return state


@router.get("/{material_id}/image/{image_id}", response_model=ImageAssetOut)
async def get_image_asset(
    material_id: str,
    image_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ImageAssetOut:
    """获取图片资产元数据。

    返回 storage_key/mime_type/size_bytes/source；
    实际图片 URL 由前端拼接 ``settings.media_proxy_prefix + storage_key`` 走鉴权代理加载。
    """
    await _authorize_material(db, material_id, user, require_write=False)
    return await materials_crud.get_image_asset(db, material_id, image_id)
