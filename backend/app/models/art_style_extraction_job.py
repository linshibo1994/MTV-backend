"""参考图画风提取异步任务模型。"""

from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class ArtStyleExtractionJob(UUIDPKMixin, TimestampMixin, Base):
    """冻结参考图输入并通过 Outbox 异步提取画风。"""

    __tablename__ = "art_style_extraction_jobs"
    __table_args__ = (
        UniqueConstraint(
            "script_id",
            "actor_user_id",
            "idempotency_key",
            name="uq_art_style_extraction_jobs_actor_idempotency",
        ),
        Index(
            "uq_art_style_extraction_jobs_active_name",
            "script_id",
            "name",
            unique=True,
            postgresql_where=text("status IN ('queued', 'running')"),
            sqlite_where=text("status IN ('queued', 'running')"),
        ),
        Index("ix_art_style_extraction_jobs_script_status", "script_id", "status"),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed')",
            name="ck_art_style_extraction_jobs_status",
        ),
        CheckConstraint("attempt_count >= 1", name="ck_art_style_extraction_jobs_attempt_positive"),
        CheckConstraint("billing_mode = 'unmetered'", name="ck_art_style_extraction_jobs_unmetered"),
    )

    script_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("scripts.id", ondelete="CASCADE"), nullable=False, index=True
    )
    actor_user_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_asset_ids: Mapped[list] = mapped_column(JSONB, nullable=False)
    source_hashes: Mapped[list] = mapped_column(JSONB, nullable=False)
    input_snapshot: Mapped[list] = mapped_column(JSONB, nullable=False)
    input_version_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued", index=True)
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=1,
        server_default=text("1"),
    )
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    lease_token: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    leased_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    extraction_provider: Mapped[str | None] = mapped_column(String(64), nullable=True)
    extraction_model: Mapped[str | None] = mapped_column(String(255), nullable=True)
    art_style_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("art_styles.id", ondelete="SET NULL"), nullable=True, index=True
    )
    billing_mode: Mapped[str] = mapped_column(
        String(32), nullable=False, default="unmetered", server_default=text("'unmetered'")
    )
