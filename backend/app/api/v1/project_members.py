"""团队项目成员管理 API。"""

from typing import cast

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.errors import BadRequestError, ForbiddenError, NotFoundError
from app.core.permissions import Action, check_script_access
from app.crud import project_member as member_crud
from app.models.project_member import ProjectMember
from app.models.script import Script
from app.models.user import User
from app.schemas.project_member import (
    ProjectMemberCreateIn,
    ProjectMemberListOut,
    ProjectMemberOut,
    ProjectMemberRoleUpdateIn,
    ProjectRole,
)
from app.services.audit import best_effort_log as audit_log

router = APIRouter()


def _serialize(member: ProjectMember, target: User) -> ProjectMemberOut:
    return ProjectMemberOut(
        member_id=member.id,
        user_id=member.user_id,
        username=target.username,
        project_role=cast(ProjectRole, member.project_role),
        enabled=target.enabled,
    )


async def _load_team_script_for_manage(
    db: AsyncSession,
    script_id: str,
    user: User,
    *,
    for_update: bool,
) -> Script:
    """加载团队项目并校验成员管理权限。"""
    script = await member_crud.get_script(db, script_id, for_update=for_update)
    if script is None:
        raise NotFoundError("剧本不存在")
    await check_script_access(db, user, script, action=Action.MANAGE_MEMBERS)
    if script.scope != "team":
        raise BadRequestError("个人项目不支持成员管理")
    return script


async def _load_member_for_script(
    db: AsyncSession,
    script_id: str,
    member_id: str,
) -> tuple[ProjectMember, User]:
    """加载成员资源；跨项目 ID 返回 403，避免 IDOR。"""
    record = await member_crud.get_member_with_user(db, member_id)
    if record is None:
        raise NotFoundError("项目成员不存在")
    member, target = record
    if member.script_id != script_id:
        raise ForbiddenError("项目成员不属于当前项目")
    return member, target


@router.get(
    "/scripts/{script_id}/members",
    response_model=ProjectMemberListOut,
)
async def list_project_members(
    script_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ProjectMemberListOut:
    """列出团队项目成员，仅 owner/manager 可访问。"""
    await _load_team_script_for_manage(db, script_id, user, for_update=False)
    records = await member_crud.list_members(db, script_id)
    return ProjectMemberListOut(
        items=[_serialize(member, target) for member, target in records],
        total=len(records),
    )


@router.post(
    "/scripts/{script_id}/members",
    response_model=ProjectMemberOut,
    status_code=201,
)
async def add_project_member(
    script_id: str,
    payload: ProjectMemberCreateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ProjectMemberOut:
    """将当前组织的有效用户加入团队项目。"""
    script = await _load_team_script_for_manage(db, script_id, user, for_update=True)
    member, target = await member_crud.add_member(
        db,
        script=script,
        user_id=payload.user_id,
        project_role=payload.project_role,
    )
    after = {
        "script_id": script_id,
        "user_id": member.user_id,
        "project_role": member.project_role,
    }
    await audit_log(
        db,
        actor=user,
        action="project_member_add",
        resource_type="project_member",
        resource_id=member.id,
        before=None,
        after=after,
    )
    await db.commit()
    return _serialize(member, target)


@router.patch(
    "/scripts/{script_id}/members/{member_id}",
    response_model=ProjectMemberOut,
)
async def update_project_member_role(
    script_id: str,
    member_id: str,
    payload: ProjectMemberRoleUpdateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ProjectMemberOut:
    """修改项目成员角色。"""
    script = await _load_team_script_for_manage(db, script_id, user, for_update=True)
    member, target = await _load_member_for_script(db, script_id, member_id)
    before_role = member.project_role
    changed = await member_crud.update_member_role(
        db,
        script,
        member,
        payload.project_role,
    )
    if changed:
        await audit_log(
            db,
            actor=user,
            action="project_member_role_change",
            resource_type="project_member",
            resource_id=member.id,
            before={
                "script_id": script_id,
                "user_id": member.user_id,
                "project_role": before_role,
            },
            after={
                "script_id": script_id,
                "user_id": member.user_id,
                "project_role": member.project_role,
            },
        )
    await db.commit()
    return _serialize(member, target)


@router.delete("/scripts/{script_id}/members/{member_id}")
async def remove_project_member(
    script_id: str,
    member_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> dict[str, str]:
    """移除项目成员。"""
    script = await _load_team_script_for_manage(db, script_id, user, for_update=True)
    member, _target = await _load_member_for_script(db, script_id, member_id)
    before = {
        "script_id": script_id,
        "user_id": member.user_id,
        "project_role": member.project_role,
    }
    await member_crud.remove_member(db, script, member)
    await audit_log(
        db,
        actor=user,
        action="project_member_remove",
        resource_type="project_member",
        resource_id=member_id,
        before=before,
        after=None,
    )
    await db.commit()
    return {"detail": "已移除"}
