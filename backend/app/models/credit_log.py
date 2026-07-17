"""CreditLog（积分流水）ORM 模型。"""

from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    event,
    func,
)
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, UUIDPKMixin


class CreditLog(UUIDPKMixin, Base):
    """积分流水 — reserve/settle/release/grant/refund 全量记录。"""

    __tablename__ = "credit_logs"
    __table_args__ = (
        Index("ix_credit_logs_account_created", "account_id", "created_at"),
        Index("ix_credit_logs_reservation", "reservation_id"),
        UniqueConstraint(
            "account_id",
            "source_type",
            "source_id",
            "log_type",
            name="uq_credit_logs_idempotent_source",
        ),
        UniqueConstraint(
            "idempotency_key",
            name="uq_credit_logs_grant_idempotency_key",
        ),
        CheckConstraint(
            "log_type IN ('reserve', 'settle', 'release', 'grant', 'refund')",
            name="ck_credit_logs_log_type",
        ),
        CheckConstraint(
            "(log_type = 'reserve' AND points_delta = 0 AND frozen_delta > 0 "
            "AND reservation_id IS NOT NULL) OR "
            "(log_type = 'settle' AND points_delta <= 0 AND frozen_delta < 0 "
            "AND reservation_id IS NOT NULL) OR "
            "(log_type = 'release' AND points_delta = 0 AND frozen_delta < 0 "
            "AND reservation_id IS NOT NULL) OR "
            "(log_type = 'refund' AND points_delta >= 0 AND frozen_delta = 0 "
            "AND reservation_id IS NOT NULL) OR "
            "(log_type = 'grant' AND points_delta != 0 AND frozen_delta = 0 "
            "AND reservation_id IS NULL)",
            name="ck_credit_logs_effect_shape",
        ),
        CheckConstraint(
            "balance_after >= 0", name="ck_credit_logs_balance_nonnegative"
        ),
        CheckConstraint(
            "frozen_after >= 0 AND frozen_after <= balance_after",
            name="ck_credit_logs_frozen_valid",
        ),
    )

    account_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("credit_accounts.id"), nullable=False, index=True
    )
    actor_user_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    actor_username: Mapped[str | None] = mapped_column(String(128), nullable=True)
    target_user_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    target_username: Mapped[str | None] = mapped_column(String(128), nullable=True)
    target_owner_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    target_owner_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    reservation_id: Mapped[str | None] = mapped_column(String(36), nullable=True)

    source_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    # grant 仅在扣方流水写入该值，使同一 key 在数据库中形成一个全局 claim。
    idempotency_key: Mapped[str | None] = mapped_column(String(36), nullable=True)
    # reserve | settle | release | grant | refund
    log_type: Mapped[str] = mapped_column(String(32), nullable=False)

    points_delta: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    frozen_delta: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    balance_after: Mapped[int] = mapped_column(Integer, nullable=False)
    frozen_after: Mapped[int] = mapped_column(Integer, nullable=False)
    note: Mapped[str | None] = mapped_column(String(1024), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False,
    )

    account = relationship("CreditAccount", back_populates="logs")


@event.listens_for(CreditLog, "before_update")
def _reject_credit_log_update(*_args: object) -> None:
    raise InvalidRequestError("积分流水不可更新")


@event.listens_for(CreditLog, "before_delete")
def _reject_credit_log_delete(*_args: object) -> None:
    raise InvalidRequestError("积分流水不可删除")
