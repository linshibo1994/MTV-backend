"""Department（部门）ORM 模型。"""

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, UUIDPKMixin


class Department(UUIDPKMixin, Base):
    """部门 — 隶属于企业，为积分账户 owner 维度之一。"""

    __tablename__ = "departments"

    org_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("organizations.id"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, comment="部门名称")

    organization = relationship("Organization", lazy="selectin")
