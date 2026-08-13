"""提示词模板不可变版本 ORM 模型。"""

from sqlalchemy import Boolean, CheckConstraint, Index, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDPKMixin


class PromptTemplate(UUIDPKMixin, TimestampMixin, Base):
    """提示词模板版本；正文创建后不可修改，仅允许切换激活版本。"""

    __tablename__ = "prompt_templates"
    __table_args__ = (
        UniqueConstraint("key", "version", name="uq_prompt_templates_key_version"),
        CheckConstraint("version >= 1", name="ck_prompt_templates_version_positive"),
        CheckConstraint("trim(key) <> ''", name="ck_prompt_templates_key_not_blank"),
        CheckConstraint("trim(content) <> ''", name="ck_prompt_templates_content_not_blank"),
        Index(
            "uq_prompt_templates_active_key",
            "key",
            unique=True,
            postgresql_where=text("is_active = true"),
            sqlite_where=text("is_active = 1"),
        ),
    )

    key: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    variables_schema: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    created_by_user_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)

