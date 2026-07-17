"""UserOrganization（用户可访问企业关联）ORM 模型。

记录用户可访问的企业列表（多对多）。
仅存 user_id + org_id 外键，org_name/org_code 由 auth/me
join organizations 表实时获取，避免组织重命名后冗余字段陈旧。

设计决策（对齐 TASK T1.1）: TASK T1.1 列出 (user_id, org_id, org_name,
org_code) 冗余列，但组织重命名场景下冗余 org_name/org_code 需同步刷新，
否则 /auth/me 返回陈旧名称。本实现采用 join organizations 实时取值的方案
（功能上 /auth/me 仍返回 org_name/org_code），作为已接受的设计决策，避免
冗余同步开销。若未来要求去 join 化查询，再补冗余列并在 Organization 重命名
时级联刷新。
"""

from sqlalchemy import ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, UUIDPKMixin


class UserOrganization(UUIDPKMixin, Base):
    """用户可访问企业关联 — auth/me 的 organizations[] 数据来源。

    仅存外键关系，组织名称/编码实时 join organizations 表获取。
    """

    __tablename__ = "user_organizations"
    __table_args__ = (
        UniqueConstraint("user_id", "org_id", name="uq_user_organizations_user_org"),
    )

    user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.user_id"), nullable=False, index=True
    )
    org_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("organizations.id"), nullable=False, index=True
    )
