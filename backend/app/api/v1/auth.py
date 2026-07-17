"""鉴权路由 — /auth/login、/auth/logout、/auth/me、/auth/switch-org。

httpOnly Cookie 会话由 SessionMiddleware 解析；登录成功写入
request.session['user_id'] 与 request.session['active_org_id']，
并额外下发 tgw_l7_route 兼容 cookie（负载均衡粘性标识）。
未登录统一 401 {"detail":"未登录"}。

多租户: 登录时默认活动租户取用户首个可访问企业；
POST /auth/switch-org 切换活动租户；auth/me 返回当前活动租户标识。
"""

from __future__ import annotations

import hashlib
import hmac
import logging

from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user, resolve_active_org
from app.core.config import settings
from app.core.database import get_db
from app.core.errors import ForbiddenError, NotAuthenticatedError, NotFoundError
from app.core.security import DUMMY_PASSWORD_HASH, verify_password
from app.models.organization import Organization
from app.models.user import User
from app.models.user_organization import UserOrganization
from app.schemas.auth import LoginIn, OrgOut, SwitchOrgIn, UserOut
from app.services.audit import best_effort_log as audit_log
from app.services.audit import best_effort_log_independently as audit_log_independently

router = APIRouter(prefix="/auth", tags=["auth"])
logger = logging.getLogger(__name__)


async def _build_user_out(db: AsyncSession, user: User, active_org_id: str | None) -> UserOut:
    """组装 UserOut，含 organizations[]（join organizations 表取最新名称）与活动租户标识。"""
    # 查用户可访问企业关联表，再 join organizations 取最新 org_name/org_code
    result = await db.execute(
        select(Organization, UserOrganization)
        .join(UserOrganization, UserOrganization.org_id == Organization.id)
        .where(UserOrganization.user_id == user.user_id)
    )
    rows = result.all()
    orgs = [
        OrgOut(id=org.id, org_name=org.org_name, org_code=org.org_code)
        for org, _uo in rows
    ]
    return UserOut(
        user_id=user.user_id,
        username=user.username,
        enabled=user.enabled,
        is_admin=user.is_admin,
        account_type=user.account_type,
        org_id=user.org_id,
        department_id=user.department_id,
        can_manage_accounts=user.can_manage_accounts,
        management_scope=user.management_scope,
        organizations=orgs,
        active_org_id=active_org_id,
    )


def _make_tgw_route_value(user_id: str) -> str:
    """生成 tgw_l7_route cookie 值 — 基于 user_id 的 HMAC 哈希，不直接暴露 user_id。

    真实平台 tgw_l7_route 为负载均衡粘性标识，通常为随机/哈希值。
    此处用 HMAC-SHA256(user_id, session_secret) 截取前 32 字符，
    既保证同一用户粘性一致，又避免 user_id 直接泄露。
    """
    secret = settings.session_secret.encode("utf-8")
    msg = user_id.encode("utf-8")
    digest = hmac.new(secret, msg, hashlib.sha256).hexdigest()
    return digest[:32]


@router.post("/login", response_model=UserOut)
async def login(
    payload: LoginIn,
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
) -> UserOut:
    """登录：校验用户名/密码 -> 建立 httpOnly 会话 -> 下发 tgw_l7_route -> 返回 UserOut。"""
    result = await db.execute(select(User).where(User.username == payload.username))
    user = result.scalar_one_or_none()
    # 用户不存在/未启用/密码错误统一返回同一消息，避免用户名枚举
    if user is None or not user.enabled:
        # 执行一次 dummy bcrypt 校验，抹平与"密码错误"路径的时延侧信道
        verify_password(payload.password, DUMMY_PASSWORD_HASH)
        await audit_log(
            db,
            actor=user,
            action="login_failed",
            resource_type="session",
            before={"username": payload.username, "reason": "user_not_found_or_disabled"},
            after=None,
            commit=True,
        )
        raise NotAuthenticatedError("用户名或密码错误")
    if not verify_password(payload.password, user.password_hash):
        await audit_log(
            db,
            actor=user,
            action="login_failed",
            resource_type="session",
            before={"username": payload.username, "reason": "wrong_password"},
            after=None,
            commit=True,
        )
        raise NotAuthenticatedError("用户名或密码错误")

    # 确定默认活动租户：优先取用户主 org_id，否则取首个可访问企业
    active_org_id = await resolve_active_org(db, user)

    # 写入 httpOnly 会话（SessionMiddleware 已签发 Cookie）
    request.session.clear()
    request.session["user_id"] = user.user_id
    if active_org_id is not None:
        request.session["active_org_id"] = active_org_id

    # tgw_l7_route 兼容 cookie：负载均衡粘性标识
    # 值用 HMAC 哈希（非 user_id 明文），httponly=False 以便 LB 读取
    response.set_cookie(
        key=settings.tgw_l7_route_cookie,
        value=_make_tgw_route_value(user.user_id),
        httponly=False,
        samesite="lax",
        max_age=settings.session_max_age,
        secure=settings.is_prod,
    )

    # 登录成功审计（08§1.4 step7）
    await audit_log(
        db,
        actor=user,
        action="login_success",
        resource_type="session",
        resource_id=user.user_id,
        before=None,
        after={"active_org_id": active_org_id},
        commit=True,
    )

    return await _build_user_out(db, user, active_org_id)


@router.post("/logout")
async def logout(request: Request, response: Response) -> dict:
    """退出登录：清除会话与兼容 cookie。"""
    request.session.clear()
    response.delete_cookie(settings.session_cookie_name)
    response.delete_cookie(settings.tgw_l7_route_cookie)
    return {"detail": "已退出"}


@router.post("/switch-org", response_model=UserOut)
async def switch_org(
    payload: SwitchOrgIn,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> UserOut:
    """切换活动租户 — 校验用户对该企业的访问权限后更新会话。"""
    organization_result = await db.execute(
        select(Organization.id).where(Organization.id == payload.org_id)
    )
    if organization_result.scalar_one_or_none() is None:
        try:
            await audit_log_independently(
                actor=user,
                action="switch_org_denied",
                resource_type="organization",
                resource_id=payload.org_id,
                before={"reason": "organization_not_found"},
            )
        except Exception:
            logger.exception("切换企业拒绝审计不可用")
        raise NotFoundError("企业不存在")

    # 企业存在后再校验成员关系，明确区分 404 与 403。
    membership_result = await db.execute(
        select(UserOrganization).where(
            UserOrganization.user_id == user.user_id,
            UserOrganization.org_id == payload.org_id,
        )
    )
    if membership_result.scalar_one_or_none() is None:
        try:
            await audit_log_independently(
                actor=user,
                action="switch_org_denied",
                resource_type="organization",
                resource_id=payload.org_id,
                before={"reason": "not_a_member"},
            )
        except Exception:
            logger.exception("切换企业拒绝审计不可用")
        raise ForbiddenError("无权访问该企业")

    from_org_id = getattr(user, "active_org_id", None)
    request.session["active_org_id"] = payload.org_id

    # 切换租户审计（08§1.4 step7）
    await audit_log(
        db,
        actor=user,
        action="switch_org",
        resource_type="organization",
        resource_id=payload.org_id,
        before={"from_org_id": from_org_id},
        after={"to_org_id": payload.org_id},
        commit=True,
    )

    return await _build_user_out(db, user, payload.org_id)


@router.get("/me", response_model=UserOut)
async def me(
    request: Request,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> UserOut:
    """返回当前登录用户对象，含 organizations[] 与当前活动租户标识。"""
    active_org_id = getattr(user, "active_org_id", None)
    return await _build_user_out(db, user, active_org_id)
