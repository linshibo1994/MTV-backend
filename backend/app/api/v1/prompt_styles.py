"""风格目录与提示词优化 API。"""

import time
from collections import defaultdict

from fastapi import APIRouter, Depends

from app.core.auth import get_current_user
from app.core.errors import TooManyRequestsError
from app.models.user import User
from app.schemas.prompt_style import (
    PromptOptimizeBatchIn,
    PromptOptimizeBatchOut,
    PromptOptimizeIn,
    PromptOptimizeOut,
    StyleListOut,
    StyleOut,
)
from app.services.prompt_style import get_styles, optimize_prompt, optimize_prompts

router = APIRouter()

_OPTIMIZE_CALLS: dict[str, list[float]] = defaultdict(list)
_OPTIMIZE_WINDOW = 60.0
_OPTIMIZE_MAX = 20


def _check_optimize_rate(user_id: str) -> None:
    now = time.monotonic()
    calls = _OPTIMIZE_CALLS[user_id]
    _OPTIMIZE_CALLS[user_id] = [t for t in calls if now - t < _OPTIMIZE_WINDOW]
    if len(_OPTIMIZE_CALLS[user_id]) >= _OPTIMIZE_MAX:
        raise TooManyRequestsError("提示词优化请求过于频繁，请稍后再试")
    _OPTIMIZE_CALLS[user_id].append(now)


@router.get(
    "/prompt-styles",
    response_model=StyleListOut,
    summary="获取提示词风格目录",
)
async def list_prompt_styles(
    target: str | None = None,
    user: User = Depends(get_current_user),
) -> StyleListOut:
    """返回全部风格，或按图像、视频生成目标过滤。"""
    del user
    return StyleListOut(styles=[StyleOut(**style) for style in get_styles(target)])


@router.post(
    "/prompt-optimize",
    response_model=PromptOptimizeOut,
    summary="优化生成提示词",
)
async def optimize_generation_prompt(
    payload: PromptOptimizeIn,
    user: User = Depends(get_current_user),
) -> PromptOptimizeOut:
    """调用对话 LLM 优化提示词；供应商失败时由服务层降级。"""
    _check_optimize_rate(user.user_id)
    del user
    result = await optimize_prompt(payload.prompt, payload.style_id, payload.target)
    return PromptOptimizeOut(**result)


@router.post(
    "/prompt-optimize/batch",
    response_model=PromptOptimizeBatchOut,
    summary="批量优化生成提示词",
)
async def optimize_generation_prompts(
    payload: PromptOptimizeBatchIn,
    user: User = Depends(get_current_user),
) -> PromptOptimizeBatchOut:
    """一次请求优化多条提示词，结果顺序与输入保持一致。"""
    # 批量请求整体计一次限流，避免前端逐项调用消耗单条额度。
    _check_optimize_rate(user.user_id)
    del user
    results = await optimize_prompts(
        [
            (item.prompt, item.style_id, item.target)
            for item in payload.items
        ]
    )
    return PromptOptimizeBatchOut(items=[PromptOptimizeOut(**result) for result in results])
