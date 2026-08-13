"""项目级动态 ArtStyle API。"""

from fastapi import APIRouter, Depends, Header, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.permissions import Action, check_script_access
from app.crud import script as script_crud
from app.models.user import User
from app.schemas.art_style import (
    ArtStyleExtractIn,
    ArtStyleExtractionJobOut,
    ArtStyleListOut,
    ArtStyleOut,
    ArtStyleUpdateIn,
)
from app.services.art_style_extraction_jobs import (
    get_art_style_extraction_job,
    queue_art_style_extraction,
)
from app.services.art_styles import list_art_styles, update_art_style
from app.services.audit import log as audit_log

router = APIRouter()


@router.get("/scripts/{script_id}/art-styles", response_model=ArtStyleListOut)
async def get_art_styles(
    script_id: str,
    include_inactive: bool = Query(False),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ArtStyleListOut:
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.READ)
    rows, total = await list_art_styles(
        db,
        script_id=script_id,
        include_inactive=include_inactive,
    )
    return ArtStyleListOut(items=[ArtStyleOut.model_validate(row) for row in rows], total=total)


@router.post(
    "/scripts/{script_id}/art-styles/extract",
    response_model=ArtStyleExtractionJobOut,
    status_code=202,
)
async def post_extract_art_style(
    script_id: str,
    payload: ArtStyleExtractIn,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1, max_length=128),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ArtStyleExtractionJobOut:
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.RUN_AI)
    row = await queue_art_style_extraction(
        db,
        script_id=script_id,
        name=payload.name,
        asset_ids=list(dict.fromkeys(payload.asset_ids)),
        actor_user_id=user.user_id,
        idempotency_key=idempotency_key,
    )
    await audit_log(
        db,
        actor=user,
        action="art_style_extract",
        resource_type="art_style_extraction_job",
        resource_id=row.id,
        after={
            "script_id": script_id,
            "source_asset_ids": row.source_asset_ids,
            "input_version_hash": row.input_version_hash,
            "billing_mode": row.billing_mode,
        },
    )
    await db.commit()
    await db.refresh(row)
    return ArtStyleExtractionJobOut.model_validate(row)


@router.get(
    "/scripts/{script_id}/art-styles/extractions/{job_id}",
    response_model=ArtStyleExtractionJobOut,
)
async def get_art_style_extraction(
    script_id: str,
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ArtStyleExtractionJobOut:
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.READ)
    row = await get_art_style_extraction_job(db, script_id=script_id, job_id=job_id)
    return ArtStyleExtractionJobOut.model_validate(row)


@router.patch("/scripts/{script_id}/art-styles/{art_style_id}", response_model=ArtStyleOut)
async def patch_art_style(
    script_id: str,
    art_style_id: str,
    payload: ArtStyleUpdateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> ArtStyleOut:
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.EDIT)
    row = await update_art_style(
        db,
        script_id=script_id,
        art_style_id=art_style_id,
        updates=payload.model_dump(exclude_unset=True),
        actor_user_id=user.user_id,
    )
    await audit_log(
        db,
        actor=user,
        action="art_style_update",
        resource_type="art_style",
        resource_id=row.id,
        after={"is_active": row.is_active, "visual_style": row.visual_style},
    )
    await db.commit()
    await db.refresh(row)
    return ArtStyleOut.model_validate(row)
