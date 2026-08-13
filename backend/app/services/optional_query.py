"""灰度发布期间可选表查询的事务隔离工具。"""

from __future__ import annotations

from typing import Any

from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession


async def execute_optional_query(db: AsyncSession, statement: Any) -> Any:
    """在 SAVEPOINT 内执行可选查询，避免语句错误污染外层业务事务。

    轻量测试会话可能没有 ``begin_nested``，此时维持直接执行契约；真实
    ``AsyncSession`` 始终使用嵌套事务。
    """

    begin_nested = getattr(db, "begin_nested", None)
    if not callable(begin_nested):
        return await db.execute(statement)  # type: ignore[call-overload]
    async with begin_nested():
        return await db.execute(statement)  # type: ignore[call-overload]


def can_fallback_from_optional_query(exc: Exception) -> bool:
    """语句/配置级失败可降级；断连或网络级失败必须交给调用链处理。"""

    if isinstance(exc, InterfaceError):
        return False
    if isinstance(exc, OperationalError) and exc.connection_invalidated:
        return False
    if isinstance(exc, DBAPIError) and exc.connection_invalidated:
        return False
    return True


__all__ = ["can_fallback_from_optional_query", "execute_optional_query"]
