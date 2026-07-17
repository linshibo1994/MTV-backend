"""积分数据访问层。

按 owner 维度返回当前用户可访问的账户、流水查询、按 owner 取账户。
仅做查询拼装，事务/锁逻辑在 services/credits.py。
"""

import logging

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.credit import CreditAccount
from app.models.credit_log import CreditLog
from app.models.department import Department
from app.models.user import User
from app.models.user_organization import UserOrganization

logger = logging.getLogger(__name__)


async def get_account_by_owner(
    db: AsyncSession, owner_type: str, owner_id: str
) -> CreditAccount | None:
    """按 (owner_type, owner_id) 唯一取账户。"""
    result = await db.execute(
        select(CreditAccount).where(
            CreditAccount.owner_type == owner_type,
            CreditAccount.owner_id == owner_id,
        )
    )
    return result.scalar_one_or_none()


async def get_account_by_id(
    db: AsyncSession, account_id: str
) -> CreditAccount | None:
    """按主键取账户。"""
    result = await db.execute(
        select(CreditAccount).where(CreditAccount.id == account_id)
    )
    return result.scalar_one_or_none()


async def get_username_by_user_id(
    db: AsyncSession, user_id: str
) -> str | None:
    """按 user_id 取用户名（用于 grant 流水 target_username 解析）。

    仅当目标账户 owner_type=user 时调用；部门/组织账户无对应用户名，
    返回 None（target_user_id 仍写 owner_id，01§7.6 下发记录契约）。
    """
    result = await db.execute(
        select(User.username).where(User.user_id == user_id)
    )
    return result.scalar_one_or_none()


async def get_user_by_user_id(db: AsyncSession, user_id: str) -> User | None:
    """按 user_id 实时取用户，用于 grant 目标身份和部门关系校验。"""
    result = await db.execute(select(User).where(User.user_id == user_id))
    return result.scalar_one_or_none()


async def user_in_grant_target_scope(
    db: AsyncSession,
    target_user: User,
    manager: User,
) -> bool:
    """实时校验 grant 目标用户仍属于 active_org 及管理员管理范围。

    该判断只信任 User、UserOrganization、Department 当前关系，不信任
    CreditAccount.department_id 这类可能滞后的账户元数据。
    """
    active_org_id = getattr(manager, "active_org_id", None)
    if not active_org_id or not target_user.enabled:
        return False

    membership_result = await db.execute(
        select(UserOrganization.user_id).where(
            UserOrganization.user_id == target_user.user_id,
            UserOrganization.org_id == active_org_id,
        ).limit(1)
    )
    if membership_result.scalar_one_or_none() is None:
        return False

    if manager.management_scope == "org":
        return True

    if manager.management_scope != "department":
        return False
    if active_org_id != manager.org_id:
        return False
    if not manager.department_id or target_user.department_id != manager.department_id:
        return False

    department_result = await db.execute(
        select(Department.id).where(
            Department.id == manager.department_id,
            Department.org_id == active_org_id,
        ).limit(1)
    )
    return department_result.scalar_one_or_none() is not None


async def get_accounts_for_user(db: AsyncSession, user: User) -> list[CreditAccount]:
    """返回当前用户可访问的账户列表。

    可见范围：
    - 个人账户：owner_type=user, owner_id=user_id
    - 部门账户：owner_type=department, owner_id=department_id（若有）
    - 企业账户：owner_type=org, owner_id=org_id（若有）
    """
    conds = [
        (CreditAccount.owner_type == "user")
        & (CreditAccount.owner_id == user.user_id)
    ]
    active_org_id = getattr(user, "active_org_id", None)
    if user.department_id and active_org_id == user.org_id:
        conds.append(
            (CreditAccount.owner_type == "department")
            & (CreditAccount.owner_id == user.department_id)
            & (CreditAccount.org_id == active_org_id)
        )
    if active_org_id:
        conds.append(
            (CreditAccount.owner_type == "org")
            & (CreditAccount.owner_id == active_org_id)
            & (CreditAccount.org_id == active_org_id)
        )

    result = await db.execute(
        select(CreditAccount)
        .where(or_(*conds))
        .order_by(CreditAccount.created_at, CreditAccount.id)
    )
    return list(result.scalars().all())


async def get_logs(
    db: AsyncSession,
    account_id: str,
    limit: int = 50,
    *,
    log_type: str | None = None,
    offset: int = 0,
) -> list[CreditLog]:
    """按账户取流水，时间倒序。

    支持按 log_type 过滤（02§6.8 弹窗三 Tab：grant=已获取/settle=已消耗/
    refund=已返还）与 offset 分页（T0.3 列表参数 limit/offset）。无 log_type
    时返回全部类型；服务端过滤避免 limit 截断导致 Tab 数据丢失。
    """
    stmt = (
        select(CreditLog)
        .where(CreditLog.account_id == account_id)
        .order_by(CreditLog.created_at.desc(), CreditLog.id.desc())
    )
    if log_type is not None:
        stmt = stmt.where(CreditLog.log_type == log_type)
    if offset:
        stmt = stmt.offset(offset)
    stmt = stmt.limit(limit)
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def is_account_accessible(
    db: AsyncSession, user: User, account_id: str
) -> bool:
    """校验用户能否访问指定积分账户（资源级授权，08 §4.1/AC-G03）。

    可访问条件（满足任一）：
    - 个人账户：owner_type=user 且 owner_id=user_id
    - 部门账户：owner_type=department 且 owner_id=用户所属 department_id
    - 组织账户：owner_type=org 且 owner_id=会话 active_org_id
    - management_scope=department：可读本部门账户
    - management_scope=org：可读本组织所有部门/组织账户

    account_id 由 URL 携带，属「改 URL id」，服务端必须重新校验归属。
    """
    acc = await get_account_by_id(db, account_id)
    if acc is None:
        return False

    active_org_id = getattr(user, "active_org_id", None)

    # 个人账户
    if acc.owner_type == "user" and acc.owner_id == user.user_id:
        return True
    # 部门账户
    if acc.owner_type == "department":
        if (
            active_org_id == user.org_id
            and acc.org_id == active_org_id
            and user.department_id
            and acc.owner_id == user.department_id
        ):
            return True
        if (
            user.management_scope == "department"
            and active_org_id == user.org_id
            and acc.org_id == active_org_id
            and user.department_id
            and acc.owner_id == user.department_id
        ):
            return True
        if user.management_scope == "org":
            # 数据不变量守卫：department 账户应回填 org_id 以支持 scope=org
            # 管理员判定。若 org_id 为空（脏数据/未回填），显式拒绝并告警，
            # 避免静默放行或误判（08§2.4 账户归属不变量）。
            if acc.org_id is None:
                logger.warning(
                    "部门账户 org_id 为空，scope=org 访问校验失效："
                    "account_id=%s owner_id=%s",
                    acc.id,
                    acc.owner_id,
                )
                return False
            if active_org_id and acc.org_id == active_org_id:
                return True
        return False
    # 组织账户
    if acc.owner_type == "org":
        if (
            active_org_id
            and acc.owner_id == active_org_id
            and acc.org_id == active_org_id
        ):
            return True
        if (
            user.management_scope == "org"
            and active_org_id
            and acc.org_id == active_org_id
        ):
            return True
        return False
    return False


async def assert_account_accessible(
    db: AsyncSession, user: User, account_id: str
) -> CreditAccount:
    """资源级授权：不可访问则 raise ForbiddenError，返回账户对象。"""
    from app.core.errors import ForbiddenError, NotFoundError

    acc = await get_account_by_id(db, account_id)
    if acc is None:
        raise NotFoundError("积分账户不存在")
    if not await is_account_accessible(db, user, account_id):
        raise ForbiddenError("无权访问该积分账户")
    return acc


def account_in_management_scope(
    account: CreditAccount,
    user: User,
    *,
    allow_user_target: bool = False,
) -> bool:
    """校验账户是否在用户的管理(management_scope)范围内（用于 grant 下发）。

    - scope=department：仅允许本部门账户（owner_type=department 且 owner_id
      =user.department_id，或该部门下的组织账户 org_id 匹配）。部门管理员不可
      越权挪用其他部门/组织额度（08 §1.2/§1.4 第4步）。
    - scope=org：允许活动组织内所有账户（org_id=active_org_id）。
    - is_admin 不隐式跨组织放行，仍须具备明确 management_scope。
    """
    active_org_id = getattr(user, "active_org_id", None)
    if user.management_scope == "org":
        if account.owner_type == "user" and not allow_user_target:
            return False
        # 数据不变量守卫：scope=org 依赖 account.org_id 判定组织范围。
        # 若账户 org_id 为空（脏数据），显式拒绝并告警，避免误放行。
        if account.org_id is None:
            logger.warning(
                "账户 org_id 为空，scope=org 下发范围校验失效："
                "account_id=%s owner_type=%s owner_id=%s",
                account.id,
                account.owner_type,
                account.owner_id,
            )
            return False
        return bool(active_org_id) and account.org_id == active_org_id
    if user.management_scope == "department":
        if active_org_id != user.org_id or account.org_id != active_org_id:
            return False
        # 部门管理员仅可从本部门账户下发
        if account.owner_type == "department":
            return bool(user.department_id) and account.owner_id == user.department_id
        if allow_user_target and account.owner_type == "user":
            return (
                bool(user.department_id)
                and account.department_id == user.department_id
            )
        # 组织/个人账户不在部门管理员下发范围内
        return False
    return False
