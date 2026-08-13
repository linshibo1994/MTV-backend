"""平台管理员提示词模板版本管理 API。

路由需由主集成任务显式挂载到 ``app.api.router``；本模块不自行注册。
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.errors import BadRequestError, ForbiddenError
from app.models.user import User
from app.schemas.prompt_template import (
    PromptTemplateActivateIn,
    PromptTemplateCreateIn,
    PromptTemplateListOut,
    PromptTemplateOut,
)
from app.services.audit import log as audit_log
from app.services.prompt_template import (
    PromptTemplateValidationError,
    activate_prompt_template_version,
    create_prompt_template_version,
    get_active_prompt_template,
    get_prompt_template_version,
    list_prompt_template_versions,
)

router = APIRouter(prefix="/prompt-templates")


def _require_platform_admin(user: User) -> None:
    """沿用当前平台管理员门禁；后续细粒度平台权限应替换此处。"""
    if not user.enabled or not user.is_admin:
        raise ForbiddenError("仅平台管理员可管理提示词模板")


@router.get("", response_model=PromptTemplateListOut, summary="列出提示词模板版本")
async def list_prompt_templates(
    key: str | None = Query(default=None, max_length=128),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> PromptTemplateListOut:
    _require_platform_admin(user)
    items, total = await list_prompt_template_versions(db, key=key, limit=limit, offset=offset)
    return PromptTemplateListOut(
        items=[PromptTemplateOut.model_validate(item) for item in items],
        total=total,
    )


@router.post("", response_model=PromptTemplateOut, status_code=201, summary="创建模板新版本")
async def create_prompt_template(
    payload: PromptTemplateCreateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> PromptTemplateOut:
    _require_platform_admin(user)
    try:
        template = await create_prompt_template_version(
            db,
            key=payload.key,
            content=payload.content,
            variables_schema=payload.variables_schema,
            actor_user_id=user.user_id,
            activate=payload.activate,
        )
    except PromptTemplateValidationError:
        raise BadRequestError("提示词模板定义无效") from None
    await audit_log(
        db,
        actor=user,
        action="prompt_template_version_create",
        resource_type="prompt_template",
        resource_id=template.id,
        before=None,
        after={
            "key": template.key,
            "version": template.version,
            "content_sha256": template.content_sha256,
            "is_active": template.is_active,
        },
    )
    await db.commit()
    return PromptTemplateOut.model_validate(template)


@router.post(
    "/{key}/activate",
    response_model=PromptTemplateOut,
    summary="激活指定模板版本",
)
async def activate_prompt_template(
    key: str,
    payload: PromptTemplateActivateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> PromptTemplateOut:
    _require_platform_admin(user)
    previous_active = await get_active_prompt_template(db, key=key)
    try:
        template = await activate_prompt_template_version(db, key=key, version=payload.version)
    except PromptTemplateValidationError:
        raise BadRequestError("提示词模板定义无效") from None
    await audit_log(
        db,
        actor=user,
        action="prompt_template_activate",
        resource_type="prompt_template",
        resource_id=template.id,
        before=(
            {
                "key": previous_active.key,
                "version": previous_active.version,
                "content_sha256": previous_active.content_sha256,
            }
            if previous_active is not None
            else None
        ),
        after={
            "key": template.key,
            "version": template.version,
            "content_sha256": template.content_sha256,
        },
    )
    await db.commit()
    return PromptTemplateOut.model_validate(template)


@router.get(
    "/{key}/versions/{version}",
    response_model=PromptTemplateOut,
    summary="获取提示词模板版本",
)
async def get_prompt_template(
    key: str,
    version: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> PromptTemplateOut:
    _require_platform_admin(user)
    template = await get_prompt_template_version(db, key=key, version=version)
    return PromptTemplateOut.model_validate(template)
