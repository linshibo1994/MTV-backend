"""主体候选确认 API。"""

from typing import Literal

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.permissions import Action, check_script_access
from app.crud import script as script_crud
from app.models.user import User
from app.schemas.subject_candidate import (
    SubjectCandidateCreateIn,
    SubjectCandidateListOut,
    SubjectCandidateOut,
    SubjectCandidateResolveIn,
)
from app.services.audit import log as audit_log
from app.services.subject_candidates import create_candidate, list_candidates, resolve_candidate

router = APIRouter()


@router.get(
    "/scripts/{script_id}/subject-candidates",
    response_model=SubjectCandidateListOut,
    summary="查询新增主体候选",
)
async def get_subject_candidates(
    script_id: str,
    candidate_status: Literal["pending", "linked", "ignored", "all"] = Query("pending", alias="status"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SubjectCandidateListOut:
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.READ)
    items, total = await list_candidates(
        db,
        script_id=script_id,
        status=None if candidate_status == "all" else candidate_status,
        limit=limit,
        offset=offset,
    )
    return SubjectCandidateListOut(
        items=[SubjectCandidateOut.model_validate(item) for item in items],
        total=total,
    )


@router.post(
    "/scripts/{script_id}/subject-candidates",
    response_model=SubjectCandidateOut,
    status_code=status.HTTP_201_CREATED,
    summary="登记新增主体候选",
)
async def post_subject_candidate(
    script_id: str,
    payload: SubjectCandidateCreateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SubjectCandidateOut:
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.EDIT)
    candidate = await create_candidate(db, script_id=script_id, **payload.model_dump())
    await audit_log(
        db,
        actor=user,
        action="subject_candidate_create",
        resource_type="subject_candidate",
        resource_id=candidate.id,
        after={"script_id": script_id, "status": candidate.status},
    )
    await db.commit()
    await db.refresh(candidate)
    return SubjectCandidateOut.model_validate(candidate)


@router.post(
    "/scripts/{script_id}/subject-candidates/{candidate_id}/resolve",
    response_model=SubjectCandidateOut,
    summary="确认或忽略新增主体候选",
)
async def post_resolve_subject_candidate(
    script_id: str,
    candidate_id: str,
    payload: SubjectCandidateResolveIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> SubjectCandidateOut:
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.REVIEW)
    candidate, replayed = await resolve_candidate(
        db,
        script_id=script_id,
        candidate_id=candidate_id,
        expected_version=payload.expected_version,
        action=payload.action,
        actor_user_id=user.user_id,
        subject_id=payload.subject_id,
    )
    if not replayed:
        await audit_log(
            db,
            actor=user,
            action=f"subject_candidate_{candidate.status}",
            resource_type="subject_candidate",
            resource_id=candidate.id,
            before={"status": "pending", "version": payload.expected_version},
            after={
                "status": candidate.status,
                "version": candidate.version,
                "linked_subject_id": candidate.linked_subject_id,
            },
        )
    await db.commit()
    await db.refresh(candidate)
    return SubjectCandidateOut.model_validate(candidate)
