"""会话鉴权 — httpOnly Cookie 会话解析。

使用 Starlette SessionMiddleware (itsdangerous signed Cookie) 实现 httpOnly 会话。
前端不可读取 Cookie，防 XSS。未登录统一 401 {"detail":"未登录"}。

多租户支持: 会话中存储 active_org_id 标识当前活动租户，
get_current_user 解析后注入到 user.active_org_id 供下游权限校验使用。
"""

from collections.abc import AsyncGenerator

from fastapi import Depends, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.errors import NotAuthenticatedError
from app.models.user import User
from app.models.user_organization import UserOrganization


async def resolve_active_org(
    db: AsyncSession,
    user: User,
    requested_org_id: str | None = None,
) -> str | None:
    """解析有效活动组织；指定组织无效时不回退，默认组织则稳定回退。"""
    if requested_org_id is not None:
        requested_result = await db.execute(
            select(UserOrganization.user_id)
            .where(
                UserOrganization.user_id == user.user_id,
                UserOrganization.org_id == requested_org_id,
            )
            .limit(1)
        )
        if requested_result.scalar_one_or_none() is not None:
            return requested_org_id
        return None

    if user.org_id is not None:
        primary_result = await db.execute(
            select(UserOrganization.user_id)
            .where(
                UserOrganization.user_id == user.user_id,
                UserOrganization.org_id == user.org_id,
            )
            .limit(1)
        )
        if primary_result.scalar_one_or_none() is not None:
            return user.org_id

    fallback_result = await db.execute(
        select(UserOrganization.org_id)
        .where(UserOrganization.user_id == user.user_id)
        .order_by(UserOrganization.org_id)
        .limit(1)
    )
    return fallback_result.scalar_one_or_none()


async def get_current_user(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> AsyncGenerator[User, None]:
    """从 httpOnly 会话中解析当前用户。

    session 由 SessionMiddleware 解析，存储在 request.session。
    解析后注入 active_org_id 到 user 对象，供权限校验使用。
    """
    session = request.session if hasattr(request, "session") else {}
    user_id = session.get("user_id")
    if not user_id:
        raise NotAuthenticatedError()

    result = await db.execute(select(User).where(User.user_id == user_id))
    user = result.scalar_one_or_none()
    if user is None or not user.enabled:
        raise NotAuthenticatedError()

    # 注入活动租户 ID（08§1.4 step2: 从服务端会话确定用户与活动租户）
    requested_org_id = session.get("active_org_id")
    active_org_id = await resolve_active_org(db, user, requested_org_id)
    if requested_org_id is not None and active_org_id is None:
        if hasattr(request, "session"):
            request.session.clear()
        raise NotAuthenticatedError()

    # 动态属性注入 — 不落库，仅用于本次请求的权限校验上下文
    setattr(user, "active_org_id", active_org_id)

    yield user
