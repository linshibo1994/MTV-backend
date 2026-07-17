"""审计日志服务 — 记录敏感操作的操作者/动作/资源/前后值。

08§1.4 step7: 对发布/删除/角色/积分变更追加审计事件。
08§4.1: 删除/发布/权限/积分调整均有操作者/时间/资源/前后值审计。

使用方式:
    from app.services.audit import log as audit_log
    await audit_log(db, actor=user, action="delete", resource_type="script",
                    resource_id=script.id, before=script.to_dict(), after=None)
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.audit_event import AuditEvent
from app.models.user import User

logger = logging.getLogger(__name__)


async def log(
    db: AsyncSession,
    *,
    actor: User | str | None,
    action: str,
    resource_type: str,
    resource_id: str | None = None,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
) -> AuditEvent:
    """写入一条审计事件。

    参数:
        actor: 操作者 User 对象或 user_id 字符串（系统操作传 None）
        action: 动作类型，如 "delete" / "publish" / "role_change" / "credit_adjust"
        resource_type: 资源类型，如 "script" / "subject" / "credit_account"
        resource_id: 资源 ID
        before: 变更前状态快照（dict）
        after: 变更后状态快照（dict）

    返回:
        创建的 AuditEvent 对象（已 flush，未 commit）
    """
    actor_user_id: str | None
    actor_username: str | None
    if isinstance(actor, User):
        actor_user_id = actor.user_id
        actor_username = actor.username
    else:
        actor_user_id = actor
        actor_username = None

    event = AuditEvent(
        actor_user_id=actor_user_id,
        actor_username=actor_username,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        before=before,
        after=after,
    )
    db.add(event)
    await db.flush()
    return event


async def best_effort_log(
    db: AsyncSession,
    *,
    actor: User | str | None,
    action: str,
    resource_type: str,
    resource_id: str | None = None,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    commit: bool = False,
) -> AuditEvent | None:
    """尽力写入审计；审计故障不得覆盖主业务的认证/授权结果。"""
    try:
        if commit:
            event = await log(
                db,
                actor=actor,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                before=before,
                after=after,
            )
        else:
            begin_nested = getattr(db, "begin_nested", None)
            if begin_nested is None:
                event = await log(
                    db,
                    actor=actor,
                    action=action,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    before=before,
                    after=after,
                )
            else:
                async with db.begin_nested():
                    event = await log(
                        db,
                        actor=actor,
                        action=action,
                        resource_type=resource_type,
                        resource_id=resource_id,
                        before=before,
                        after=after,
                    )
        if commit:
            await db.commit()
        return event
    except Exception:
        logger.exception(
            "审计写入失败：action=%s resource_type=%s resource_id=%s",
            action,
            resource_type,
            resource_id,
        )
        if commit:
            try:
                await db.rollback()
            except Exception:
                logger.exception("审计事务回滚失败")
        return None


async def best_effort_log_independently(
    *,
    actor: User | str | None,
    action: str,
    resource_type: str,
    resource_id: str | None = None,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    session_factory: Callable[[], Any] | None = None,
) -> AuditEvent | None:
    """使用独立短事务尽力持久化审计，不影响调用方业务事务。"""
    try:
        audit_session_factory: Callable[[], Any]
        if session_factory is None:
            from app.core.database import async_session_factory as default_session_factory

            audit_session_factory = default_session_factory
        else:
            audit_session_factory = session_factory

        async with audit_session_factory() as audit_db:
            return await best_effort_log(
                audit_db,
                actor=actor,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                before=before,
                after=after,
                commit=True,
            )
    except Exception:
        logger.exception(
            "独立审计会话不可用：action=%s resource_type=%s resource_id=%s",
            action,
            resource_type,
            resource_id,
        )
        return None
