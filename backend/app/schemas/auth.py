"""鉴权 Pydantic 模型 — UserOut / LoginIn / OrgOut / SwitchOrgIn。

字段对齐 docs/Claude/02 0.3 用户与租户模型 与 04 1.1 User。
新增 active_org_id 表示当前会话活动租户（TASK T1.2）。
新增 SwitchOrgIn 用于 POST /auth/switch-org。
"""

from pydantic import BaseModel, ConfigDict, Field


class OrgOut(BaseModel):
    """企业（auth/me organizations[] 项）。"""

    model_config = ConfigDict(from_attributes=True)

    id: str
    org_name: str
    org_code: str


class UserOut(BaseModel):
    """当前用户对象 — GET /auth/me 与 POST /auth/login 返回。"""

    model_config = ConfigDict(from_attributes=True)

    user_id: str
    username: str
    enabled: bool
    is_admin: bool
    # personal | team
    account_type: str
    org_id: str | None = None
    department_id: str | None = None
    can_manage_accounts: bool
    # none | department | org
    management_scope: str
    organizations: list[OrgOut] = Field(default_factory=list)
    # 当前会话活动租户标识（TASK T1.2）
    active_org_id: str | None = None


class LoginIn(BaseModel):
    """登录入参。"""

    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=256)


class SwitchOrgIn(BaseModel):
    """切换活动租户入参。"""

    org_id: str = Field(min_length=1, max_length=36)
