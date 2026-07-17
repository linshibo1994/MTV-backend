"""流水线人工重新提取与失败重试 API。"""

from typing import Annotated

from fastapi import APIRouter, Depends, Header, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.permissions import Action, check_script_access
from app.crud import script as script_crud
from app.models.user import User
from app.schemas.pipeline_runs import PipelineRunCreateIn, PipelineRunOut
from app.services.pipeline_runs import create_pipeline_run

router = APIRouter(prefix="/scripts", tags=["流水线"])


@router.post(
    "/{script_id}/episodes/{episode_id}/pipeline-runs",
    response_model=PipelineRunOut,
    status_code=status.HTTP_202_ACCEPTED,
    summary="手动重新提取或显式重试失败流水线",
)
async def create_manual_pipeline_run(
    script_id: str,
    episode_id: str,
    body: PipelineRunCreateIn,
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=128,
            description="必填；同一键和同一请求只创建一个流水线 run",
        ),
    ],
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> PipelineRunOut:
    """需要 RUN_AI 权限；Viewer/Reviewer、跨资源与旧输入版本均被拒绝。"""
    script = await script_crud.get_script(db, script_id)
    await check_script_access(db, user, script, action=Action.RUN_AI)
    episode = await script_crud.get_episode(db, script_id, episode_id)
    return await create_pipeline_run(
        db,
        script=script,
        episode_id=episode.episode_id,
        actor=user,
        body=body,
        idempotency_key=idempotency_key,
    )
