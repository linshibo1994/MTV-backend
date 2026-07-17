"""ProjectMember（团队项目成员）ORM 模型。"""

from sqlalchemy import CheckConstraint, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, UUIDPKMixin


class ProjectMember(UUIDPKMixin, Base):
    """团队项目成员 — 团队 scope 剧本的协作权限。"""

    __tablename__ = "project_members"
    __table_args__ = (
        UniqueConstraint("script_id", "user_id", name="uq_project_members_script_user"),
        CheckConstraint(
            "project_role IN ('owner', 'manager', 'editor', 'reviewer', 'viewer')",
            name="ck_project_members_role",
        ),
    )

    script_id: Mapped[str] = mapped_column(String(36), ForeignKey("scripts.id"), nullable=False, index=True)
    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.user_id"), nullable=False, index=True)
    # owner | editor | viewer | reviewer | manager
    project_role: Mapped[str] = mapped_column(String(32), nullable=False, default="viewer")
