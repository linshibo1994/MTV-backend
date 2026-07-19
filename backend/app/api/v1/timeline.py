"""时间线编排模块 API 路由。

端点:
- GET    /video-timeline-arrangements            查询时间线 (script_id + episode_id)
- POST   /video-timeline-arrangements            添加片段 (query: script_id, episode_id; body: TimelineItemIn)
- PATCH  /video-timeline-arrangements/{id}       更新片段 (row_order/is_selected/trim)
- PUT    /video-timeline-arrangements/reorder     批量重排序
- POST   /video-timeline-arrangements/render      触发合成 (body: {script_id, episode_id} -> job_id)
- GET    /video-timeline-arrangements/render/{job_id}  查询合成状态
- POST   /video-timeline-arrangements/render/{job_id}/cancel  取消合成

约定:
- router 已在 app/api/router.py 聚合到 /v3api 前缀下挂载
- 错误统一 {"detail":...}，鉴权 get_current_user
- 资源级鉴权: check_timeline_script_access (script_id 端点) / check_timeline_access (arrangement_id 端点)
- 不修改 models/ / router.py / main.py
"""

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.database import get_db
from app.core.permissions import Action, check_timeline_access, check_timeline_script_access
from app.crud import timeline as timeline_crud
from app.models.timeline_render_job import TIMELINE_IDEMPOTENCY_KEY_MAX_LENGTH
from app.models.user import User
from app.schemas.timeline import (
    RenderJobOut,
    RenderRequestIn,
    RenderStatusOut,
    ReorderIn,
    TimelineItemIn,
    TimelineItemOut,
    TimelineOut,
    TimelineUpdateIn,
)
from app.services import timeline_render

router = APIRouter()


async def _get_arrangement_or_404(
    db: AsyncSession, arrangement_id: str
) -> "object":
    """按 id 取片段，不存在则 404。"""

    arrangement = await timeline_crud.get_arrangement(db, arrangement_id)
    if arrangement is None:
        raise HTTPException(status_code=404, detail="时间线片段不存在")
    return arrangement


@router.get(
    "/video-timeline-arrangements",
    response_model=TimelineOut,
    summary="查询时间线编排",
)
async def list_arrangements(
    script_id: str = Query(..., description="剧本 ID"),
    episode_id: str = Query(..., description="分集 ID"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> TimelineOut:
    """按 script_id + episode_id 返回时间线，按 row_order 升序。"""

    # 资源级鉴权: 校验当前用户对该剧本的 READ 权限
    await check_timeline_script_access(db, user, script_id, Action.READ)

    items = await timeline_crud.list_arrangements(db, script_id, episode_id)
    return TimelineOut(
        script_id=script_id,
        episode_id=episode_id,
        items=[TimelineItemOut.model_validate(it) for it in items],
    )


@router.post(
    "/video-timeline-arrangements",
    response_model=TimelineItemOut,
    status_code=status.HTTP_201_CREATED,
    summary="添加片段到时间线",
)
async def add_arrangement(
    payload: TimelineItemIn,
    script_id: str = Query(..., description="剧本 ID"),
    episode_id: str = Query(..., description="分集 ID"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> TimelineItemOut:
    """添加片段到时间线末尾，row_order 自动递增。"""

    # 资源级鉴权: 编辑剧本需 EDIT 权限
    await check_timeline_script_access(db, user, script_id, Action.EDIT)

    arrangement = await timeline_crud.add_arrangement(
        db,
        script_id=script_id,
        episode_id=episode_id,
        unit_runtime_key=payload.unit_runtime_key,
        job_id=payload.job_id,
    )
    await db.commit()
    return TimelineItemOut.model_validate(arrangement)


@router.patch(
    "/video-timeline-arrangements/{arrangement_id}",
    response_model=TimelineItemOut,
    summary="更新时间线片段",
)
async def update_arrangement(
    arrangement_id: str,
    payload: TimelineUpdateIn,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> TimelineItemOut:
    """更新单个片段的 row_order / is_selected / trim_start / trim_end。"""

    # 资源级鉴权: 校验该片段归属剧本的 EDIT 权限，并回取 arrangement
    _script, arrangement = await check_timeline_access(
        db, user, arrangement_id, Action.EDIT
    )

    # 校验 trim_end 不早于 trim_start
    if (
        payload.trim_end_seconds is not None
        and payload.trim_start_seconds is not None
        and payload.trim_end_seconds <= payload.trim_start_seconds
    ):
        raise HTTPException(status_code=422, detail="裁出点必须晚于裁入点")
    # 部分更新时也校验与现有值的关系
    fields_set = payload.model_fields_set
    new_start = (
        payload.trim_start_seconds
        if "trim_start_seconds" in fields_set and payload.trim_start_seconds is not None
        else arrangement.trim_start_seconds
    )
    new_end = (
        payload.trim_end_seconds
        if "trim_end_seconds" in fields_set
        else arrangement.trim_end_seconds
    )
    if new_end is not None and new_end <= new_start:
        raise HTTPException(status_code=422, detail="裁出点必须晚于裁入点")

    arrangement = await timeline_crud.update_arrangement(
        db,
        arrangement,
        row_order=payload.row_order,
        is_selected=payload.is_selected,
        trim_start_seconds=payload.trim_start_seconds,
        trim_end_seconds=payload.trim_end_seconds,
        expected_version=payload.expected_version,
        fields_set=payload.model_fields_set,
    )
    await db.commit()
    return TimelineItemOut.model_validate(arrangement)


@router.put(
    "/video-timeline-arrangements/reorder",
    response_model=TimelineOut,
    summary="批量重排序时间线",
)
async def reorder_arrangements(
    payload: ReorderIn,
    script_id: str = Query(..., description="剧本 ID"),
    episode_id: str = Query(..., description="分集 ID"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> TimelineOut:
    """批量更新片段 row_order。

    body: {items: [{id, row_order}]}。更新后按新 row_order 升序返回完整时间线。
    """

    # 资源级鉴权: 重排序属于编辑操作
    await check_timeline_script_access(db, user, script_id, Action.EDIT)

    items_dicts = [
        {
            "id": it.id,
            "row_order": it.row_order,
            "expected_version": it.expected_version,
        }
        for it in payload.items
    ]
    # 传入 script_id/episode_id，crud 层 WHERE 限定本剧本本集，防止越权改写 (IDOR)
    await timeline_crud.reorder(
        db,
        items_dicts,
        script_id=script_id,
        episode_id=episode_id,
    )
    await db.commit()

    items = await timeline_crud.list_arrangements(db, script_id, episode_id)
    return TimelineOut(
        script_id=script_id,
        episode_id=episode_id,
        items=[TimelineItemOut.model_validate(it) for it in items],
    )


@router.delete(
    "/video-timeline-arrangements/{arrangement_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="删除时间线片段",
)
async def remove_arrangement(
    arrangement_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> None:
    """从时间线移除单个片段。"""

    # 资源级鉴权: 校验该片段归属剧本的 EDIT 权限，并回取 arrangement
    _script, arrangement = await check_timeline_access(
        db, user, arrangement_id, Action.EDIT
    )
    await timeline_crud.remove_arrangement(db, arrangement)
    await db.commit()


@router.post(
    "/video-timeline-arrangements/render",
    response_model=RenderJobOut,
    status_code=status.HTTP_202_ACCEPTED,
    summary="合成最终视频",
)
async def render_timeline(
    payload: RenderRequestIn,
    idempotency_key: str | None = Header(
        None,
        alias="Idempotency-Key",
        max_length=TIMELINE_IDEMPOTENCY_KEY_MAX_LENGTH,
    ),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> RenderJobOut:
    """按 row_order 排列选中片段 + trim 裁剪，异步合成最终视频。

    返回 job_id，通过 GET /video-timeline-arrangements/render/{job_id} 轮询状态。
    """

    # 资源级鉴权: 触发合成属于运行生成任务，需 RUN_AI 权限
    await check_timeline_script_access(db, user, payload.script_id, Action.RUN_AI)

    result = await timeline_render.render(
        db,
        payload.script_id,
        payload.episode_id,
        user,
        idempotency_key=idempotency_key,
    )
    await db.commit()
    return RenderJobOut(
        job_id=result["job_id"],
        status=result["status"],
        output_storage_key=result.get("output_storage_key"),
    )


@router.get(
    "/video-timeline-arrangements/render/{job_id}",
    response_model=RenderStatusOut,
    summary="查询合成任务状态",
)
async def get_render_status(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> RenderStatusOut:
    """轮询渲染任务状态 / 进度 / 产物 storage_key。"""

    data = await timeline_render.get_render_status(job_id, db, user)
    return RenderStatusOut(**data)


@router.post(
    "/video-timeline-arrangements/render/{job_id}/cancel",
    response_model=RenderStatusOut,
    summary="取消合成任务",
)
async def cancel_render(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> RenderStatusOut:
    """将渲染任务置为 canceled；worker 在下一段循环前检查并提前退出。

    仅在任务处于 starting/processing 时可取消；终态 (succeeded/failed/canceled) 返回当前状态。
    """

    data = await timeline_render.cancel_render(job_id, db, user)
    await db.commit()
    return RenderStatusOut(**data)


@router.post(
    "/video-timeline-arrangements/render/{job_id}/retry",
    response_model=RenderJobOut,
    status_code=status.HTTP_202_ACCEPTED,
    summary="重试可重试的合成任务",
)
async def retry_render(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> RenderJobOut:
    """failed 且 retryable 的任务创建新的不可变 attempt。"""

    data = await timeline_render.retry_render(job_id, db, user)
    await db.commit()
    return RenderJobOut(**data)
