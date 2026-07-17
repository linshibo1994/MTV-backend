"""UUID v4 生成工具 — 统一主键格式。"""

from uuid import UUID, uuid4


def new_id() -> str:
    """生成 UUID v4 字符串。"""
    return str(uuid4())


def is_uuid(value: str) -> bool:
    try:
        UUID(value)
        return True
    except (ValueError, AttributeError, TypeError):
        return False
