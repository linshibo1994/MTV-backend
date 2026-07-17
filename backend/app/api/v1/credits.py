"""积分路由 — /credits/accounts、/credits/logs、/credits/grant。

- GET /credits/accounts：当前用户可访问账户（个人/部门/企业维度）
- GET /credits/logs：按账户取流水，时间倒序（资源级授权：account_id 由 URL
  携带，服务端重新校验归属，08 §4.1/AC-G03）
- POST /credits/grant：管理员下发积分（校验 management_scope/can_manage_accounts
  且 from_account 在调用者管理范围内，08 §1.2/§1.4 第4步）

列表响应统一 {"items":[...]}（T0.3 全局约定）。
reserve/settle/release/refund 为内部服务接口，供 T6/T8 异步 worker 调用，
不在此处暴露 HTTP 端点。
"""

import logging

from fastapi import APIRouter, Depends, Header, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.errors import (
    ConflictError,
    ForbiddenError,
    InsufficientCreditsError,
    NotFoundError,
)
from app.crud.credit import (
    account_in_management_scope,
    assert_account_accessible,
    get_account_by_id,
    get_accounts_for_user,
    get_logs,
    get_user_by_user_id,
    user_in_grant_target_scope,
)
from app.models.user import User
from app.schemas.credit import (
    AccountListOut,
    AccountOut,
    GrantIn,
    GrantOut,
    LogListOut,
    LogOut,
)
from app.services.audit import best_effort_log as audit_log
from app.services.audit import best_effort_log_independently as audit_log_independently
from app.services.credits import grant as grant_service

router = APIRouter(prefix="/credits", tags=["credits"])
logger = logging.getLogger(__name__)


async def _audit_credit_event(db: AsyncSession | None = None, **kwargs) -> None:
    """审计异常不得覆盖积分主请求的成功或拒绝状态。"""
    try:
        if db is None:
            await audit_log_independently(**kwargs)
        else:
            await audit_log(db, commit=False, **kwargs)
    except Exception:
        logger.exception("积分审计不可用")


async def _deny_grant(
    db: AsyncSession,
    user: User,
    resource_id: str | None,
    reason: str,
) -> None:
    await _audit_credit_event(
        db,
        actor=user,
        action="credit_access_denied",
        resource_type="credit_account",
        resource_id=resource_id,
        before={"operation": "grant", "reason": reason},
    )
    raise ForbiddenError(reason)


@router.get("/accounts", response_model=AccountListOut)
async def list_accounts(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> AccountListOut:
    """返回当前用户可访问的积分账户列表（{items:[...]} 包装）。"""
    accounts = await get_accounts_for_user(db, user)
    return AccountListOut(items=[AccountOut.model_validate(a) for a in accounts])


@router.get("/logs", response_model=LogListOut)
async def list_logs(
    account_id: str = Query(..., description="账户 ID"),
    limit: int = Query(50, ge=1, le=500, description="返回条数上限"),
    offset: int = Query(0, ge=0, description="分页偏移量"),
    log_type: str | None = Query(
        None,
        description="流水类型过滤：grant=已获取/settle=已消耗/refund=已返还"
        "（02§6.8 弹窗三 Tab），不传则返回全部类型",
    ),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> LogListOut:
    """按账户取流水，时间倒序。

    资源级授权：account_id 由 URL 携带，服务端重新校验调用者对该账户的访问
    权限（个人/部门/组织维度或管理员/管理范围覆盖），不在范围内则 403。

    支持按 log_type 过滤（02§6.8 三 Tab）与 offset 分页（T0.3）。
    """
    try:
        await assert_account_accessible(db, user, account_id)
    except ForbiddenError:
        await _audit_credit_event(
            db,
            actor=user,
            action="credit_access_denied",
            resource_type="credit_account",
            resource_id=account_id,
            before={"operation": "list_logs", "reason": "account_forbidden"},
        )
        raise
    logs = await get_logs(
        db, account_id, limit, log_type=log_type, offset=offset
    )
    return LogListOut(items=[LogOut.model_validate(log) for log in logs])


@router.post("/grant", response_model=GrantOut)
async def grant_credits(
    payload: GrantIn,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    idempotency_key: str = Header(
        ..., alias="Idempotency-Key", min_length=1, max_length=36
    ),
) -> GrantOut:
    """管理员下发积分：from 账户向 to 账户转账。

    授权检查顺序（08 §1.4）：
    1. 会话有效且 enabled（get_current_user 已保证）
    2. can_manage_accounts 且 management_scope 非 none；is_admin 不跨组织隐式放行
    3. 加载 from 账户（不信任 URL 外字段），校验其在调用者 management_scope 内
       — scope=department 仅本部门账户，scope=org 仅活动组织账户。
       部门管理员不可越权挪用其他部门/组织额度。
    4. 加载 to 账户并同样校验 management_scope 归属：防止跨部门/组织越权
       转移（部门管理员只能下发到本部门账户，组织管理员到本组织账户）。
    """
    if not (user.can_manage_accounts and user.management_scope != "none"):
        await _deny_grant(db, user, None, "无积分下发权限")

    # 加载 from 账户并校验管理范围归属（08 §1.4 第3/4步）
    from_account = await get_account_by_id(db, payload.from_account_id)
    if from_account is None:
        raise NotFoundError("下发来源账户不存在")
    if not account_in_management_scope(from_account, user):
        await _deny_grant(
            db,
            user,
            from_account.id,
            "无权从该账户下发积分：不在管理范围内",
        )

    # 加载 to 账户并校验管理范围归属，防止跨部门/组织越权转移
    to_account = await get_account_by_id(db, payload.to_account_id)
    if to_account is None:
        raise NotFoundError("下发目标账户不存在")
    if not account_in_management_scope(
        to_account, user, allow_user_target=True
    ):
        await _deny_grant(
            db,
            user,
            to_account.id,
            "无权向该账户下发积分：不在管理范围内",
        )

    # 解析 target 身份（01§7.6 下发记录契约：grant 流水 target_* 非空）。
    # user 字段只表达真实用户；组织/部门目标由流水 target_owner_* 表达。
    target_user_id: str | None = None
    target_username: str | None = None
    if to_account.owner_type == "user":
        target_user_id = to_account.owner_id
        target_user = await get_user_by_user_id(db, to_account.owner_id)
        if target_user is None:
            raise NotFoundError("下发目标用户不存在")
        if isinstance(target_user, User):
            if not await user_in_grant_target_scope(db, target_user, user):
                await _deny_grant(
                    db,
                    user,
                    to_account.id,
                    "无权向该用户下发积分：目标用户不在当前活动企业同部门范围内",
                )
            target_username = target_user.username
        else:  # 兼容旧 mock：真实实现 get_user_by_user_id 只返回 User 或 None。
            target_username = str(target_user)

    try:
        await grant_service(
            db,
            from_id=payload.from_account_id,
            to_id=payload.to_account_id,
            points=payload.points,
            actor_id=user.user_id,
            actor_username=user.username,
            target_user_id=target_user_id,
            target_username=target_username,
            note=payload.note,
            idempotency_key=idempotency_key,
        )
    except (InsufficientCreditsError, ConflictError) as exc:
        await _audit_credit_event(
            actor=user,
            action="credit_grant_failed",
            resource_type="credit_account",
            resource_id=payload.from_account_id,
            before={
                "from_account_id": payload.from_account_id,
                "to_account_id": payload.to_account_id,
                "points": payload.points,
                "status_code": exc.status_code,
                "reason": exc.detail,
            },
        )
        raise
    await _audit_credit_event(
        actor=user,
        action="credit_grant",
        resource_type="credit_account",
        resource_id=payload.from_account_id,
        after={
            "from_account_id": payload.from_account_id,
            "to_account_id": payload.to_account_id,
            "points": payload.points,
        },
    )
    return GrantOut(detail="下发成功")
