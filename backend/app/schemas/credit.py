"""积分 Pydantic 模型 — AccountOut / LogOut / GrantIn。

字段对齐 docs/Claude/04 1.3 CreditAccount、1.4 CreditLog 与 02 6.2/6.3。
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class AccountOut(BaseModel):
    """积分账户出参。"""

    model_config = ConfigDict(from_attributes=True)

    id: str
    # user | org | department
    owner_type: str
    owner_id: str
    org_id: str | None = None
    department_id: str | None = None
    balance_points: int
    frozen_points: int
    # active | disabled
    status: str
    disabled_reason: str | None = None
    disabled_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class LogOut(BaseModel):
    """积分流水出参。"""

    model_config = ConfigDict(from_attributes=True)

    id: str
    account_id: str
    actor_user_id: str | None = None
    actor_username: str | None = None
    target_user_id: str | None = None
    target_username: str | None = None
    target_owner_type: str | None = None
    target_owner_id: str | None = None
    reservation_id: str | None = None
    source_type: str | None = None
    source_id: str | None = None
    # reserve | settle | release | grant | refund
    log_type: str
    points_delta: int
    frozen_delta: int
    balance_after: int
    frozen_after: int
    note: str | None = None
    created_at: datetime


class GrantIn(BaseModel):
    """管理员下发积分入参。"""

    from_account_id: str
    to_account_id: str
    points: int = Field(gt=0, description="下发积分数量，必须为正")
    note: str | None = None


class GrantOut(BaseModel):
    """管理员下发积分出参 — 成功响应形态。

    与统一约定区分：错误体用 {"detail":...}（由 app.core.errors 抛出），
    成功响应使用明确的 response_model schema，前端可按状态码+schema 区分。
    """

    detail: str = "下发成功"


class AccountListOut(BaseModel):
    """积分账户列表出参 — 遵循 T0.3 全局列表响应约定 {items:[...]}。"""

    items: list[AccountOut]


class LogListOut(BaseModel):
    """积分流水列表出参 — 遵循 T0.3 全局列表响应约定 {items:[...]}。

    total 不返回（流水无全量计数需求，与 04§2.2 CreditLog[] 语义一致）。
    """

    items: list[LogOut]
