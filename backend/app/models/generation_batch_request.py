"""批量生图幂等请求事实。"""

from sqlalchemy import CheckConstraint, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.core.ids import new_id
from app.models.base import Base, TimestampMixin


class GenerationBatchRequest(TimestampMixin, Base):
    """记录批量请求的规范化载荷与稳定目标集合。"""

    __tablename__ = "generation_batch_requests"
    __table_args__ = (
        UniqueConstraint(
            "owner_user_id",
            "idempotency_key",
            name="uq_generation_batch_requests_owner_idempotency_key",
        ),
        CheckConstraint(
            "trim(idempotency_key) <> ''",
            name="ck_generation_batch_requests_idempotency_key_not_blank",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    owner_user_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    idempotency_key: Mapped[str] = mapped_column(String(256), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    target_subject_ids: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    job_ids: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    skipped_subject_ids: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    failed_items: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="created")
