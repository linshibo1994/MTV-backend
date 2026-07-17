"""Organization（企业）ORM 模型。"""

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, UUIDPKMixin


class Organization(UUIDPKMixin, Base):
    """企业（多租户顶层）。"""

    __tablename__ = "organizations"

    org_name: Mapped[str] = mapped_column(String(255), nullable=False, comment="企业名称")
    org_code: Mapped[str] = mapped_column(
        String(128), nullable=False, unique=True, comment="企业编码"
    )
