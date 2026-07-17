"""AuditEvent（审计事件）ORM 模型。

08§1.4 step7: 对发布/删除/角色/积分变更追加审计事件。
08§4.1: 删除/发布/权限/积分调整均有操作者/时间/资源/前后值审计。
"""

from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, UUIDPKMixin


class AuditEvent(UUIDPKMixin, Base):
    """审计事件 — 记录敏感操作的操作者、动作、资源与前后值。

    覆盖场景:
    - 发布/删除/归档剧本
    - 项目成员角色变更
    - 积分调整（reserve/settle/release/refund/grant）
    - 供应商切换
    - 资产审核通过/拒绝
    """

    __tablename__ = "audit_events"

    # 操作者
    actor_user_id: Mapped[str | None] = mapped_column(
        String(36), nullable=True, index=True, comment="操作者用户 ID"
    )
    actor_username: Mapped[str | None] = mapped_column(
        String(128), nullable=True, comment="操作者用户名（冗余便于查询）"
    )

    # 动作描述: delete | publish | role_change | credit_adjust | provider_switch | asset_approve | ...
    action: Mapped[str] = mapped_column(
        String(64), nullable=False, comment="审计动作类型"
    )

    # 资源
    resource_type: Mapped[str] = mapped_column(
        String(64), nullable=False, comment="资源类型: script/subject/job/credit_account/..."
    )
    resource_id: Mapped[str | None] = mapped_column(
        String(36), nullable=True, index=True, comment="资源 ID"
    )

    # 前后值快照（JSON）
    before: Mapped[dict | None] = mapped_column(
        JSONB, nullable=True, comment="变更前状态快照"
    )
    after: Mapped[dict | None] = mapped_column(
        JSONB, nullable=True, comment="变更后状态快照"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
