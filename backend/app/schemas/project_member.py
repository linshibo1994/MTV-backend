"""团队项目成员管理的请求与响应契约。"""

from typing import Literal

from pydantic import BaseModel, Field, field_validator

ProjectRole = Literal["owner", "manager", "editor", "reviewer", "viewer"]


class ProjectMemberCreateIn(BaseModel):
    """添加项目成员。"""

    user_id: str = Field(min_length=1, max_length=36)
    project_role: ProjectRole = "viewer"

    @field_validator("user_id")
    @classmethod
    def strip_user_id(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("user_id 不得为空")
        return value


class ProjectMemberRoleUpdateIn(BaseModel):
    """修改项目成员角色。"""

    project_role: ProjectRole


class ProjectMemberOut(BaseModel):
    """项目成员列表项。"""

    member_id: str
    user_id: str
    username: str
    project_role: ProjectRole
    enabled: bool


class ProjectMemberListOut(BaseModel):
    """项目成员列表。"""

    items: list[ProjectMemberOut]
    total: int
