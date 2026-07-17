"""FastAPI 通用依赖 — 聚合 get_db / get_redis / get_current_user。

T0.2 契约: core/deps.py 提供 get_db / get_current_user / get_redis。
get_current_user 实际定义在 core/auth.py，此处重导出以统一依赖来源。
"""

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.redis import get_redis

__all__ = ["get_db", "get_redis", "get_current_user"]
