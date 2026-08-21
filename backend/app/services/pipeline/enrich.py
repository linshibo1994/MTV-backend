"""enrich 阶段的兼容实现。

视觉主体设计提示词已下线。保留该叶子阶段仅用于兼容既有流水线 DAG、任务租约与
最终结算；阶段不会调用 LLM、读取历史提示词或写入任何提示词数据。
"""

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.pipeline import input_episode_is_current, set_stage


async def run_enrich(
    db: AsyncSession,
    script_id: str,
    *,
    episode_id: str | None = None,
    input_version: int | None = None,
    input_client_save_seq: int | None = None,
    provider=None,
    provider_attempt: int | None = None,
    run_id: str | None = None,
    commit: bool = False,
    max_concurrency: int | None = None,
) -> int | dict[str, object]:
    """完成兼容叶子阶段，不生成或持久化服务端设计提示词。"""
    del provider, provider_attempt, commit, max_concurrency
    if not await input_episode_is_current(
        db,
        script_id,
        episode_id,
        input_version,
        input_client_save_seq,
    ):
        return 0

    detail: dict[str, object] = {
        "count": 0,
        "input_version": input_version,
        "input_client_save_seq": input_client_save_seq,
    }
    await set_stage(
        db,
        script_id,
        "enrich",
        status="running",
        current=0,
        total=0,
        detail=detail,
    )
    return detail if run_id is not None else 0
