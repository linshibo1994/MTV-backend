"""User（用户）ORM 模型。"""

from sqlalchemy import Boolean, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.ids import new_id
from app.models.base import Base, TimestampMixin


class User(TimestampMixin, Base):
    """用户 — 鉴权主体，多租户层级：用户 -> 部门 -> 企业。"""

    __tablename__ = "users"

    user_id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=new_id
    )
    username: Mapped[str] = mapped_column(
        String(128), nullable=False, unique=True, comment="登录用户名"
    )
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_admin: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # personal | team
    account_type: Mapped[str] = mapped_column(String(32), nullable=False, default="personal")
    org_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("organizations.id"), nullable=True, index=True
    )
    department_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("departments.id"), nullable=True, index=True
    )

    can_manage_accounts: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # none | department | org
    management_scope: Mapped[str] = mapped_column(String(32), nullable=False, default="none")

    organization = relationship("Organization", lazy="selectin")
    department = relationship("Department", lazy="selectin")
