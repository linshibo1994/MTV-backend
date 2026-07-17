"""授权检查 — 两轴分离：组织轴 + 内容轴。

检查顺序 (08 权限模型 1.4):
1. 验证会话有效且用户 enabled
2. 从服务端会话确定用户与活动租户
3. 加载资源及其租户/所有者，不信任 URL 外的客户端归属字段
4. 验证组织/部门成员关系
5. 验证个人所有权或项目角色
6. 验证具体动作与资源当前状态
7. 对发布/删除/角色/积分变更追加审计事件
"""

from __future__ import annotations

import logging
from enum import StrEnum

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.errors import ForbiddenError, NotAuthenticatedError
from app.models.script import Script
from app.models.user import User

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Action 枚举 — 对齐 08§1.3 九动作矩阵
# ---------------------------------------------------------------------------


class Action(StrEnum):
    """授权动作枚举，对齐 08§1.3 权限矩阵。

    粒度说明（设计决策）:
    - 当前 6 动作覆盖内容轴；08§1.3 矩阵的"编辑主体"与"编辑剧本/分集"
      合并为 EDIT，Reviewer 对主体的只读需求由 READ 覆盖（粒度可接受）。
    - org 轴动作（管理组织账户 / 查看·调整积分）不在本模块，由 T2 积分
      模块承接。如需更细粒度，可新增 EDIT_SUBJECT 等枚举并扩展矩阵。
    """

    READ = "read"                  # 查看项目
    EDIT = "edit"                  # 编辑剧本/分集/主体
    RUN_AI = "run_ai"             # 运行 AI 任务
    REVIEW = "review"            # 审核资产
    MANAGE_MEMBERS = "manage_members"  # 管理项目成员
    DELETE = "delete"            # 删除/归档项目


# ---------------------------------------------------------------------------
# 权限矩阵 — project_role -> 允许的 Action 集合
# 矩阵来源: 08§1.3
#
# | 动作            | Owner | Viewer | Editor | Reviewer | ProjectManager |
# | 查看项目        | 允许  | 允许   | 允许  | 允许    | 允许          |
# | 编辑剧本/分集   | 允许  | 禁止   | 允许  | 禁止    | 允许          |
# | 运行 AI 任务    | 允许  | 禁止   | 允许  | 禁止    | 允许          |
# | 编辑主体        | 允许  | 禁止   | 允许  | 只读    | 允许          |
# | 审核资产        | 允许  | 禁止   | 可配置| 允许    | 允许          |
# | 管理项目成员    | N/A   | 禁止   | 禁止  | 禁止    | 允许          |
# | 删除/归档项目   | 允许  | 禁止   | 禁止  | 禁止    | 允许          |
#
# Editor 审核资产 = "可配置"，由 settings.editor_can_review 开关控制（默认关闭）。
# Reviewer 编辑主体 = "只读"：READ 允许、EDIT 禁止。
# ---------------------------------------------------------------------------

# Editor 审核资产 = "可配置"(08§1.3): 通过 settings.editor_can_review
# 运行时开关启用，默认关闭。环境变量 EDITOR_CAN_REVIEW=true 可放开。
_editor_permissions: set[Action] = {Action.READ, Action.EDIT, Action.RUN_AI}
if settings.editor_can_review:
    _editor_permissions = _editor_permissions | {Action.REVIEW}

# 每个项目角色允许的动作集合
_ROLE_PERMISSIONS: dict[str, set[Action]] = {
    "owner": {
        Action.READ, Action.EDIT, Action.RUN_AI, Action.REVIEW,
        Action.MANAGE_MEMBERS, Action.DELETE,
    },
    "viewer": {
        Action.READ,
    },
    "editor": _editor_permissions,
    "reviewer": {
        Action.READ, Action.REVIEW,
    },
    "manager": {
        Action.READ, Action.EDIT, Action.RUN_AI, Action.REVIEW,
        Action.MANAGE_MEMBERS, Action.DELETE,
    },
}


def _role_allows(role: str, action: Action) -> bool:
    """检查项目角色是否允许指定动作。"""
    return action in _ROLE_PERMISSIONS.get(role, set())


# ---------------------------------------------------------------------------
# 核心授权函数
# ---------------------------------------------------------------------------


async def check_script_access(
    db: AsyncSession,
    user: User,
    script: Script,
    action: Action = Action.READ,
    require_write: bool | None = None,
) -> None:
    """校验用户对剧本的访问权限（资源级）。

    按 08§1.4 授权检查顺序执行：
    1. 会话有效且用户 enabled
    2. 活动租户校验（team scope 下 script.org_id 须与用户活动租户一致）
    3. 加载资源及其所有者
    4. 验证组织/部门成员关系
    5. 验证个人所有权或项目角色
    6. 验证具体动作与角色权限

    - personal scope: 仅 owner_user_id 对应用户可访问
    - team scope: 需为显式 ProjectMember，按 project_role 校验动作
    - 组织管理员不自动获得读权限（08§1.2 OrganizationAdmin 需显式成员关系）

    兼容旧调用点: require_write kwarg 与 action 互斥，传 require_write=True
    等价于 action=Action.EDIT，False 等价于 Action.READ。新代码应直接用 action。
    """
    # 0. 兼容旧 kwarg: require_write -> action 映射（与 action 互斥）
    if require_write is not None:
        if action is not Action.READ:
            raise TypeError(
                "check_script_access: action 与 require_write 互斥，不可同时指定"
            )
        action = Action.EDIT if require_write else Action.READ

    # 1. 会话有效且用户 enabled
    if not user.enabled:
        raise NotAuthenticatedError()

    if script.scope not in {"personal", "team"}:
        await _audit_access_denied(db, user, script, action, "未知剧本范围")
        raise ForbiddenError("剧本范围无效")

    # 2. 活动租户校验（team scope 下逐请求重新校验 UserOrganization 成员关系）
    # 08§1.4 step4 验证组织成员关系: 不信任会话中的 active_org_id 作为最终凭据，
    # 每次请求重新查询 UserOrganization 确认用户仍属于剧本所属组织。会话中的
    # active_org_id 仅用于快速短路（与剧本 org_id 不一致直接拒绝）；成员关系以
    # DB 现状为准，用户被移出组织后立即失效，无需等待会话过期。部门级隔离
    # 暂未启用（management_scope=department 由 T2/T4 按需扩展）。
    if script.scope == "team":
        active_org_id = getattr(user, "active_org_id", None)
        if active_org_id is None:
            await _audit_access_denied(db, user, script, action, "会话缺少活动租户")
            raise ForbiddenError("请先选择活动企业")
        # 快速短路：会话活动租户与剧本所属组织不一致直接拒绝
        if script.org_id != active_org_id:
            await _audit_access_denied(db, user, script, action, "剧本不属于当前活动租户")
            raise ForbiddenError("剧本不属于当前活动租户")
        # 逐请求重新校验 UserOrganization 成员关系（防会话过期后仍持有权限）
        from app.models.user_organization import UserOrganization

        org_member_result = await db.execute(
            select(UserOrganization.user_id).where(
                UserOrganization.user_id == user.user_id,
                UserOrganization.org_id == script.org_id,
            ).limit(1)
        )
        if org_member_result.scalar_one_or_none() is None:
            await _audit_access_denied(db, user, script, action, "用户不属于剧本所属组织")
            raise ForbiddenError("用户不属于剧本所属组织")

    # 3-5. personal scope: 仅所有者
    if script.scope == "personal":
        if script.owner_user_id != user.user_id:
            await _audit_access_denied(db, user, script, action, "非所有者访问个人剧本")
            raise ForbiddenError("无权访问该剧本")
        # 所有者拥有全部权限，无需进一步校验
        return

    # 3-5. team scope: 需为显式项目成员（08§1.2）
    # 组织管理员不自动获得读权限 — 须显式成员关系
    from app.models.project_member import ProjectMember

    member_result = await db.execute(
        select(ProjectMember).where(
            ProjectMember.script_id == script.id,
            ProjectMember.user_id == user.user_id,
        )
    )
    member = member_result.scalar_one_or_none()

    if member is None:
        # 非项目成员，拒绝所有访问（不再对 is_admin 放行）
        # 08§1.2: OrganizationAdmin 查看项目需显式成员关系
        # 08§1.2: PlatformOperator 默认不读取创作内容
        await _audit_access_denied(db, user, script, action, "非项目成员访问团队项目")
        raise ForbiddenError("无权访问该团队项目，请先加入项目")

    # 6. 验证具体动作与角色权限（08§1.3 矩阵）
    if not _role_allows(member.project_role, action):
        await _audit_access_denied(
            db, user, script, action, f"角色({member.project_role})无权执行({action.value})"
        )
        _raise_action_denied(member.project_role, action)


def _raise_action_denied(role: str, action: Action) -> None:
    """根据角色与动作抛出有针对性的 403 消息。"""
    hints = {
        (Action.EDIT, "viewer"): "只读成员无编辑权限",
        (Action.EDIT, "reviewer"): "审核角色无编辑权限",
        (Action.RUN_AI, "viewer"): "只读成员无权运行 AI 任务",
        (Action.RUN_AI, "reviewer"): "审核角色无权运行 AI 任务",
        (Action.REVIEW, "viewer"): "只读成员无权审核资产",
        (Action.MANAGE_MEMBERS, "viewer"): "无权管理项目成员",
        (Action.MANAGE_MEMBERS, "editor"): "编辑角色无权管理项目成员",
        (Action.MANAGE_MEMBERS, "reviewer"): "审核角色无权管理项目成员",
        (Action.DELETE, "viewer"): "无权删除项目",
        (Action.DELETE, "editor"): "编辑角色无权删除项目",
        (Action.DELETE, "reviewer"): "审核角色无权删除项目",
    }
    msg = hints.get((action, role), f"当前角色({role})无权执行该操作({action.value})")
    raise ForbiddenError(msg)


async def _audit_access_denied(
    db: AsyncSession,
    user: User,
    script: Script,
    action: Action,
    reason: str,
) -> None:
    """记录权限拒绝审计事件（08§1.4 step7 / 08§4.1）。

    审计使用独立短事务提交，避免随后调用方业务事务回滚时丢失拒绝事件。
    审计故障必须被隔离，不能污染调用者事务，也不能覆盖既定 403。
    """
    from app.services.audit import best_effort_log_independently

    try:
        await best_effort_log_independently(
            actor=user,
            action="access_denied",
            resource_type="script",
            resource_id=getattr(script, "id", None),
            before={
                "action": action.value,
                "scope": getattr(script, "scope", None),
                "reason": reason,
            },
            after=None,
        )
    except Exception:
        # 包括审计数据库连接/提交故障；拒绝结果必须保持为 403。
        logger.exception("权限拒绝审计不可用")


# ---------------------------------------------------------------------------
# 资源级授权函数 — 改 URL id 时重新加载资源并校验归属
# 08§4.1: 任意用户改 URL 中 script/episode/subject/materials/shot/job ID 时，
#         服务端执行资源级授权。
#
# 接入状态（非 dead code，按 08§4.1 逐步收敛）:
# - check_timeline_access / check_timeline_script_access: timeline 路由已接入
# - check_episode_access / check_subject_access / check_materials_access /
#   check_job_access / check_video_job_access: 对应路由当前采用本地加载资源 +
#   check_script_access(action=) 两步式授权（功能等价、授权判断仍集中在本模块），
#   这些 helper 为一站式封装（加载+校验+返回资源元组），供后续路由迁移复用，
#   避免每处重写 _load_script_or_403/_validate_script_episode 造成遗漏。
# ---------------------------------------------------------------------------


async def _load_script(db: AsyncSession, script_id: str) -> Script:
    """加载剧本，不存在则 403。"""
    from fastapi import HTTPException, status
    result = await db.execute(select(Script).where(Script.id == script_id))
    script = result.scalar_one_or_none()
    if script is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="剧本不存在")
    return script


async def check_episode_access(
    db: AsyncSession,
    user: User,
    episode_id: str,
    action: Action = Action.READ,
) -> tuple:
    """校验用户对分集的访问权限 — 通过分集回溯剧本。

    返回 (script, episode) 供端点复用，避免二次查询。
    """
    from fastapi import HTTPException, status

    from app.models.episode import Episode

    result = await db.execute(
        select(Episode).where(Episode.episode_id == episode_id)
    )
    episode = result.scalar_one_or_none()
    if episode is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="分集不存在")

    script = await _load_script(db, episode.script_id)
    await check_script_access(db, user, script, action)
    return script, episode


async def check_subject_access(
    db: AsyncSession,
    user: User,
    subject_id: str,
    action: Action = Action.READ,
) -> tuple:
    """校验用户对主体的访问权限 — 通过主体回溯剧本。

    返回 (script, subject) 供端点复用。
    """
    from fastapi import HTTPException, status

    from app.models.subject import Subject

    result = await db.execute(select(Subject).where(Subject.id == subject_id))
    subject = result.scalar_one_or_none()
    if subject is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="主体不存在")

    script = await _load_script(db, subject.script_id)
    await check_script_access(db, user, script, action)
    return script, subject


async def check_materials_access(
    db: AsyncSession,
    user: User,
    material_id: str,
    action: Action = Action.READ,
) -> tuple:
    """校验用户对画布项目的访问权限 — 通过 materials 回溯剧本。

    返回 (script, materials) 供端点复用。
    """
    from fastapi import HTTPException, status

    from app.models.materials import Materials

    result = await db.execute(select(Materials).where(Materials.id == material_id))
    material = result.scalar_one_or_none()
    if material is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="画布项目不存在")

    script = await _load_script(db, material.script_id)
    await check_script_access(db, user, script, action)
    return script, material


async def check_job_access(
    db: AsyncSession,
    user: User,
    job_id: str,
    action: Action = Action.READ,
) -> tuple:
    """校验用户对图片生成任务的访问权限 — 通过 job 回溯剧本。

    返回 (script, job) 供端点复用。
    """
    from fastapi import HTTPException, status

    from app.models.generation_job import GenerationJob

    result = await db.execute(
        select(GenerationJob).where(GenerationJob.job_id == job_id)
    )
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="生成任务不存在")

    # job.script_id 可能为 None，回退到 material.script_id
    script_id = job.script_id
    if script_id is None and job.material_id is not None:
        from app.models.materials import Materials
        mat_result = await db.execute(
            select(Materials).where(Materials.id == job.material_id)
        )
        material = mat_result.scalar_one_or_none()
        if material is not None:
            script_id = material.script_id

    if script_id is None:
        raise ForbiddenError("无法确定任务所属剧本")

    script = await _load_script(db, script_id)
    await check_script_access(db, user, script, action)
    return script, job


async def check_video_job_access(
    db: AsyncSession,
    user: User,
    job_id: str,
    action: Action = Action.READ,
) -> tuple:
    """校验用户对视频生成任务的访问权限 — 通过 job 回溯剧本。

    返回 (script, job) 供端点复用。
    """
    from fastapi import HTTPException, status

    from app.models.video_generation_job import VideoGenerationJob

    result = await db.execute(
        select(VideoGenerationJob).where(VideoGenerationJob.job_id == job_id)
    )
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="视频生成任务不存在")

    script = await _load_script(db, job.script_id)
    await check_script_access(db, user, script, action)
    return script, job


async def check_timeline_access(
    db: AsyncSession,
    user: User,
    arrangement_id: str,
    action: Action = Action.READ,
) -> tuple:
    """校验用户对时间线片段的访问权限 — 通过 arrangement 回溯剧本。

    返回 (script, arrangement) 供端点复用。
    """
    from fastapi import HTTPException, status

    from app.models.video_timeline_arrangement import VideoTimelineArrangement

    result = await db.execute(
        select(VideoTimelineArrangement).where(
            VideoTimelineArrangement.id == arrangement_id
        )
    )
    arrangement = result.scalar_one_or_none()
    if arrangement is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="时间线片段不存在")

    script = await _load_script(db, arrangement.script_id)
    await check_script_access(db, user, script, action)
    return script, arrangement


async def check_timeline_script_access(
    db: AsyncSession,
    user: User,
    script_id: str,
    action: Action = Action.READ,
) -> Script:
    """校验用户对指定剧本下时间线的访问权限。

    用于列表端点（以 script_id 查询参数传入）。
    """
    script = await _load_script(db, script_id)
    await check_script_access(db, user, script, action)
    return script
