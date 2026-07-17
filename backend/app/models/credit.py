"""CreditAccount（积分账户）ORM 模型。"""

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class CreditAccount(UUIDPKMixin, TimestampMixin, Base):
    """积分账户 — 按 owner 维度（user/org/department）挂载。"""

    __tablename__ = "credit_accounts"
    __table_args__ = (
        UniqueConstraint("owner_type", "owner_id", name="uq_credit_accounts_owner"),
        CheckConstraint(
            "balance_points >= 0", name="ck_credit_accounts_balance_nonnegative"
        ),
        CheckConstraint(
            "frozen_points >= 0", name="ck_credit_accounts_frozen_nonnegative"
        ),
        CheckConstraint(
            "frozen_points <= balance_points",
            name="ck_credit_accounts_frozen_within_balance",
        ),
        CheckConstraint(
            "owner_type IN ('user', 'org', 'department')",
            name="ck_credit_accounts_owner_type",
        ),
        CheckConstraint(
            "owner_type = 'user' OR "
            "(owner_type = 'org' AND org_id IS NOT NULL "
            "AND org_id = owner_id AND department_id IS NULL) OR "
            "(owner_type = 'department' AND org_id IS NOT NULL "
            "AND department_id IS NOT NULL AND department_id = owner_id)",
            name="ck_credit_accounts_owner_scope",
        ),
        CheckConstraint(
            "status IN ('active', 'disabled')", name="ck_credit_accounts_status"
        ),
    )

    # user | org | department
    owner_type: Mapped[str] = mapped_column(String(32), nullable=False)
    owner_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    org_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    department_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)

    balance_points: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    frozen_points: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # active | disabled
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    disabled_reason: Mapped[str | None] = mapped_column(String(512), nullable=True)
    disabled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # 流水/预约为无界集合，禁用自动预加载（lazy="selectin" 会在任何账户查询时
    # 全量加载历史，列表接口随之退化）。改为 lazy="raise"，需显式 selectinload
    # 才加载，避免 GET /credits/accounts 触发 N 行级全量加载。
    logs = relationship(
        "CreditLog", back_populates="account", lazy="raise"
    )
    reservations = relationship(
        "CreditReservation", back_populates="account", lazy="raise"
    )
