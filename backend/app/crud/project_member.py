"""团队项目成员管理数据访问层。"""

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import noload

from app.core.errors import BadRequestError, ConflictError
from app.core.ids import new_id
from app.models.project_member import ProjectMember
from app.models.script import Script
from app.models.user import User
from app.models.user_organization import UserOrganization

PRIVILEGED_ROLES = {"owner", "manager"}


def _validate_owner_role(
    script: Script,
    *,
    user_id: str,
    project_role: str,
) -> None:
    """owner 角色与 Script.owner_user_id 强绑定，不在成员接口隐式转移所有权。"""
    if user_id == script.owner_user_id and project_role != "owner":
        raise ConflictError("项目所有者必须保留 owner 角色")
    if user_id != script.owner_user_id and project_role == "owner":
        raise ConflictError("owner 角色仅可由项目所有者持有；所有权转移需使用专用流程")


async def get_script(
    db: AsyncSession,
    script_id: str,
    *,
    for_update: bool = False,
) -> Script | None:
    """加载剧本；成员变更时通过行锁串行化权限检查与写入。"""
    statement = (
        select(Script)
        .options(
            noload(Script.episodes),
            noload(Script.subjects),
            noload(Script.parent_subjects),
            noload(Script.materials),
        )
        .where(Script.id == script_id)
    )
    if for_update:
        statement = statement.with_for_update()
    return (await db.execute(statement)).scalar_one_or_none()


async def list_members(
    db: AsyncSession,
    script_id: str,
) -> list[tuple[ProjectMember, User]]:
    """列出项目成员及用户展示信息。"""
    result = await db.execute(
        select(ProjectMember, User)
        .join(User, User.user_id == ProjectMember.user_id)
        .where(ProjectMember.script_id == script_id)
        .order_by(User.username.asc(), ProjectMember.id.asc())
    )
    return [(row[0], row[1]) for row in result.all()]


async def get_member_with_user(
    db: AsyncSession,
    member_id: str,
) -> tuple[ProjectMember, User] | None:
    """按成员资源 ID 加载，归属性由 API 层单独校验以正确返回 IDOR 403。"""
    row = (
        await db.execute(
            select(ProjectMember, User)
            .join(User, User.user_id == ProjectMember.user_id)
            .where(ProjectMember.id == member_id)
        )
    ).one_or_none()
    if row is None:
        return None
    return row[0], row[1]


async def add_member(
    db: AsyncSession,
    *,
    script: Script,
    user_id: str,
    project_role: str,
) -> tuple[ProjectMember, User]:
    """添加同组织的有效用户，同一项目不允许重复成员。"""
    if script.org_id is None:
        raise BadRequestError("团队项目缺少所属组织")
    _validate_owner_role(
        script,
        user_id=user_id,
        project_role=project_role,
    )

    target = (
        await db.execute(
            select(User)
            .join(UserOrganization, UserOrganization.user_id == User.user_id)
            .where(
                User.user_id == user_id,
                User.enabled.is_(True),
                UserOrganization.org_id == script.org_id,
            )
        )
    ).scalar_one_or_none()
    if target is None:
        raise BadRequestError("目标用户不是当前组织的有效成员")

    existing = (
        await db.execute(
            select(ProjectMember.id).where(
                ProjectMember.script_id == script.id,
                ProjectMember.user_id == user_id,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        raise ConflictError("该用户已是项目成员")

    member = ProjectMember(
        id=new_id(),
        script_id=script.id,
        user_id=user_id,
        project_role=project_role,
    )
    db.add(member)
    await db.flush()
    return member, target


async def update_member_role(
    db: AsyncSession,
    script: Script,
    member: ProjectMember,
    project_role: str,
) -> bool:
    """修改成员角色；真实 owner 不得降级，其他成员不得获授 owner。"""
    _validate_owner_role(
        script,
        user_id=member.user_id,
        project_role=project_role,
    )
    if member.project_role == project_role:
        return False
    if member.project_role in PRIVILEGED_ROLES and project_role not in PRIVILEGED_ROLES:
        privileged_count = int(
            (
                await db.execute(
                    select(func.count())
                    .select_from(ProjectMember)
                    .where(
                        ProjectMember.script_id == member.script_id,
                        ProjectMember.project_role.in_(PRIVILEGED_ROLES),
                    )
                )
            ).scalar_one()
        )
        if privileged_count <= 1:
            raise ConflictError("项目必须保留至少一名 owner 或 manager")

    member.project_role = project_role
    await db.flush()
    return True


async def remove_member(
    db: AsyncSession,
    script: Script,
    member: ProjectMember,
) -> None:
    """移除成员；真实 owner 只能经专用所有权转移流程处理。"""
    if member.user_id == script.owner_user_id:
        raise ConflictError("项目所有者不能从成员列表移除；所有权转移需使用专用流程")
    if member.project_role in PRIVILEGED_ROLES:
        privileged_count = int(
            (
                await db.execute(
                    select(func.count())
                    .select_from(ProjectMember)
                    .where(
                        ProjectMember.script_id == member.script_id,
                        ProjectMember.project_role.in_(PRIVILEGED_ROLES),
                    )
                )
            ).scalar_one()
        )
        if privileged_count <= 1:
            raise ConflictError("项目必须保留至少一名 owner 或 manager")

    await db.delete(member)
    await db.flush()
