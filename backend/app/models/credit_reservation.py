"""CreditReservation（积分预扣预约）ORM 模型。"""

from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, UUIDPKMixin


class CreditReservation(UUIDPKMixin, Base):
    """积分预扣预约 — 串联 reserve/settle/release。

    幂等由全生命周期唯一约束保证：(source_type, source_id) 不可复用。
    失败重试必须创建新的 attempt/source_id，避免同一业务来源重复扣费。
    """

    __tablename__ = "credit_reservations"
    __table_args__ = (
        UniqueConstraint(
            "source_type",
            "source_id",
            name="uq_credit_reservations_source",
        ),
        Index("ix_credit_reservations_source", "source_type", "source_id"),
        CheckConstraint(
            "reserved_points > 0", name="ck_credit_reservations_points_positive"
        ),
        CheckConstraint(
            "actual_points IS NULL OR actual_points >= 0",
            name="ck_credit_reservations_actual_nonnegative",
        ),
        CheckConstraint(
            "(status IN ('reserved', 'released') AND actual_points IS NULL) OR "
            "(status IN ('captured', 'refunded') AND actual_points IS NOT NULL)",
            name="ck_credit_reservations_actual_by_status",
        ),
        CheckConstraint(
            "status IN ('reserved', 'captured', 'released', 'refunded')",
            name="ck_credit_reservations_status",
        ),
    )

    account_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("credit_accounts.id"), nullable=False, index=True
    )
    source_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_id: Mapped[str] = mapped_column(String(36), nullable=False)
    reserved_points: Mapped[int] = mapped_column(Integer, nullable=False)
    # settle 时写入的实际结算扣减额，refund 上限校验依赖此字段。
    # nullable 兼容未结算预约与迁移前数据；captured 历史记录缺值时 refund
    # 会 fail closed，需人工核对后再返还。
    actual_points: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # reserved | captured | released | refunded
    # 状态机（08 §2.4）：reserved --settle--> captured --refund--> refunded；
    # reserved --release--> released。refunded 仅可从 captured 转入。
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="reserved")

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False,
    )
    released_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    account = relationship("CreditAccount", back_populates="reservations")
