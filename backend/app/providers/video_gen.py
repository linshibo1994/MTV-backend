"""视频生成供应商抽象与工厂。

统一覆盖火山方舟 Seedance、mgrouter AICC、阿里百炼万相、OpenAI Sora 和 Google Gemini Veo；
未知厂商立即失败，避免静默切换后产生错误计费。
"""

import asyncio
import logging
import re
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import Context
from dataclasses import dataclass, field
from enum import StrEnum
from typing import NoReturn, Protocol, runtime_checkable

from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded

from app.core.config import settings

logger = logging.getLogger(__name__)

CancellationProbe = Callable[[], Awaitable[bool]]
TaskCreatedCallback = Callable[[str], Awaitable[None]]
SubmissionStateCallback = Callable[[], Awaitable[None]]
_VIDEO_CLIENT_CLOSE_TIMEOUT = 2.0
_VIDEO_CLIENT_CLOSE_ORPHANS: set[asyncio.Task[object]] = set()
_VIDEO_CLIENT_CLOSE_ORPHANS_LOCK = threading.Lock()
_VIDEO_SUBMIT_DIAGNOSTIC_TEXT_FIELDS = ("code", "param", "type", "request_id")
_VIDEO_SUBMIT_DIAGNOSTIC_TOKEN = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z",
    re.ASCII,
)


def sanitize_video_submit_rejection_diagnostics(
    value: object,
) -> dict[str, int | str]:
    """仅接受固定 JSON 标量与短 ASCII token；其他输入一律丢弃。"""

    if type(value) is not dict:
        return {}
    raw = value
    diagnostics: dict[str, int | str] = {}
    status_code = raw.get("status_code")
    if type(status_code) is int and 400 <= status_code < 500 and status_code != 408:
        diagnostics["status_code"] = status_code
    for field_name in _VIDEO_SUBMIT_DIAGNOSTIC_TEXT_FIELDS:
        field_value = raw.get(field_name)
        if type(field_value) is str and _VIDEO_SUBMIT_DIAGNOSTIC_TOKEN.fullmatch(field_value):
            diagnostics[field_name] = field_value
    return diagnostics


class _VideoClientCloseSlots:
    """跨线程、跨事件循环限制未收敛的客户端 close 数量。"""

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._lock = threading.Lock()
        self._tokens: set[object] = set()

    def reserve(self) -> object | None:
        token = object()
        with self._lock:
            if len(self._tokens) >= self._limit:
                return None
            self._tokens.add(token)
        return token

    def release(self, token: object) -> None:
        with self._lock:
            self._tokens.discard(token)


_VIDEO_CLIENT_CLOSE_SLOTS = _VideoClientCloseSlots(limit=1)


class VideoCancellationOutcome(StrEnum):
    """远端视频任务取消结果。"""

    CANCELED = "canceled"
    UNSUPPORTED = "unsupported"
    FAILED = "failed"


@dataclass(frozen=True)
class VideoCancellationResult:
    provider_task_id: str
    outcome: VideoCancellationOutcome
    detail: str | None = None


class VideoGenerationCancelled(RuntimeError):
    """本地任务取消后终止 Provider 等待，不进入 worker 失败补偿。"""

    def __init__(self, result: VideoCancellationResult) -> None:
        self.result = result
        super().__init__(
            "视频生成已取消 "
            f"provider_task_id={result.provider_task_id} outcome={result.outcome.value}"
        )


class VideoGenerationRemoteTerminalError(RuntimeError):
    """Provider 已确认远端任务失败或取消，可安全结束本地 attempt。"""

    _NORMALIZED_STATUSES = {
        "failed": "failed",
        "error": "failed",
        "cancelled": "canceled",
        "canceled": "canceled",
    }

    def __init__(self, *, provider_task_id: str, terminal_status: str) -> None:
        normalized = self._NORMALIZED_STATUSES.get(terminal_status.strip().lower())
        if not provider_task_id or normalized is None:
            raise ValueError("无效的视频远端终态证据")
        self.provider_task_id = provider_task_id
        self.terminal_status = normalized
        super().__init__(f"video_generation_remote_terminal:{normalized}")


class VideoGenerationSubmissionUncertain(RuntimeError):
    """提交请求可能已被远端接受，但客户端没有取得可恢复任务 ID。"""

    def __init__(self) -> None:
        super().__init__("video_generation_submission_uncertain")


class VideoGenerationSubmitRejected(RuntimeError):
    """Provider 已明确拒绝提交，远端没有创建计费任务。"""

    def __init__(self, *, diagnostics: object = None) -> None:
        self.diagnostics = sanitize_video_submit_rejection_diagnostics(diagnostics)
        super().__init__("video_generation_submit_rejected")


class VideoTaskCheckpointError(RuntimeError):
    """远端任务 ID checkpoint 未形成可继续执行的权威事实。"""

    def __init__(self, message: str, *, provider_task_id: str) -> None:
        super().__init__(message)
        self.provider_task_id = provider_task_id


class VideoTaskCheckpointUnavailable(VideoTaskCheckpointError):
    """远端任务 ID checkpoint 的持久通道暂时不可用。"""


class VideoTaskCheckpointConflict(VideoTaskCheckpointError):
    """远端任务 ID 与本地权威记录冲突。"""


@dataclass(frozen=True)
class VideoGenerationRequest:
    prompt: str
    reference_images: list[dict]
    reference_image_names: list[str]
    reference_audios: list[dict]
    reference_audio_names: list[str]
    model: str
    job_id: str
    first_frame: str | None = None
    duration_seconds: int | None = None
    metadata: dict = field(default_factory=dict)
    cancel_requested: CancellationProbe | None = field(
        default=None,
        compare=False,
        repr=False,
    )
    on_task_created: TaskCreatedCallback | None = field(
        default=None,
        compare=False,
        repr=False,
    )
    before_submit: SubmissionStateCallback | None = field(
        default=None,
        compare=False,
        repr=False,
    )
    on_submission_uncertain: SubmissionStateCallback | None = field(
        default=None,
        compare=False,
        repr=False,
    )


@dataclass(frozen=True)
class VideoGenerationResult:
    video_storage_key: str
    provider_job_id: str | None = None
    metrics: dict = field(default_factory=dict)


@dataclass(frozen=True)
class VideoGenerationProgress:
    stage: str
    message: str
    progress: int
    provider_task_id: str | None = None


ProgressCallback = Callable[[VideoGenerationProgress], None]
RemoteCancelCallback = Callable[[str], Awaitable[VideoCancellationResult]]


async def await_video_task_checkpoint(
    on_task_created: TaskCreatedCallback | None,
    provider_task_id: str,
) -> None:
    """远端 ID 可见后等待持久 checkpoint，禁止越过该异步边界。"""

    if on_task_created is None:
        return
    try:
        await on_task_created(provider_task_id)
    except TimeLimitExceeded:
        raise
    except VideoTaskCheckpointError:
        raise
    except SoftTimeLimitExceeded as exc:
        raise VideoTaskCheckpointUnavailable(
            "video_task_checkpoint_unavailable",
            provider_task_id=provider_task_id,
        ) from exc
    except Exception as exc:
        raise VideoTaskCheckpointUnavailable(
            "video_task_checkpoint_unavailable",
            provider_task_id=provider_task_id,
        ) from exc


def _video_submit_status_code(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    for raw_status in (
        getattr(exc, "status_code", None),
        getattr(response, "status_code", None),
        getattr(exc, "code", None),
    ):
        if raw_status is None:
            continue
        try:
            status_code = int(raw_status)
        except (TypeError, ValueError):
            continue
        if 100 <= status_code <= 599:
            return status_code
    return None


def _video_submit_was_explicitly_rejected(exc: Exception) -> bool:
    status_code = _video_submit_status_code(exc)
    return status_code is not None and 400 <= status_code < 500 and status_code != 408


def _replace_video_submit_sensitive_values(
    value: object,
    *,
    sensitive_values: tuple[str, ...],
) -> object:
    """候选标量先做 exact 替换；替换标记会被 ASCII token 校验拒绝。"""

    if type(value) is not str:
        return value
    redacted = value
    for sensitive in sensitive_values:
        if type(sensitive) is str and sensitive:
            redacted = redacted.replace(sensitive, "[REDACTED]")
    return redacted


def _video_submit_rejection_diagnostics(
    exc: Exception,
    *,
    sensitive_values: tuple[str, ...],
) -> dict[str, int | str]:
    """从 SDK 固定属性提取诊断；绝不读取 message、body、request 或 response 内容。"""

    raw_diagnostics: dict[str, object] = {
        "status_code": _video_submit_status_code(exc),
    }
    for field_name in _VIDEO_SUBMIT_DIAGNOSTIC_TEXT_FIELDS:
        raw_diagnostics[field_name] = _replace_video_submit_sensitive_values(
            getattr(exc, field_name, None),
            sensitive_values=sensitive_values,
        )
    return sanitize_video_submit_rejection_diagnostics(raw_diagnostics)


async def _notify_video_submission_uncertain(
    callback: SubmissionStateCallback | None,
) -> None:
    if callback is None:
        return
    try:
        await callback()
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except Exception as exc:
        raise VideoGenerationSubmissionUncertain() from exc


async def execute_video_submission[SubmissionResult](
    submit: Callable[[], Awaitable[SubmissionResult]],
    *,
    before_submit: SubmissionStateCallback | None,
    on_submission_uncertain: SubmissionStateCallback | None,
    rejection_sensitive_values: tuple[str, ...] = (),
) -> SubmissionResult:
    """先持久化提交意图，再把提交响应异常归一为可判权的 typed error。"""

    if before_submit is not None:
        await before_submit()
    try:
        return await submit()
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        raise
    except VideoGenerationSubmissionUncertain:
        raise
    except VideoGenerationSubmitRejected as exc:
        rejection_diagnostics = sanitize_video_submit_rejection_diagnostics(
            exc.diagnostics
        )
    except Exception as exc:
        if _video_submit_was_explicitly_rejected(exc):
            rejection_diagnostics = _video_submit_rejection_diagnostics(
                exc,
                sensitive_values=rejection_sensitive_values,
            )
        else:
            await _notify_video_submission_uncertain(on_submission_uncertain)
            raise VideoGenerationSubmissionUncertain() from exc
    # 必须离开原始 SDK exception handler 后再抛出，避免 provider 内容进入 cause/context traceback。
    raise VideoGenerationSubmitRejected(diagnostics=rejection_diagnostics)


async def raise_video_submission_uncertain(
    callback: SubmissionStateCallback | None,
) -> NoReturn:
    """提交已返回但缺少任务 ID，同样按响应不确定处理。"""

    await _notify_video_submission_uncertain(callback)
    raise VideoGenerationSubmissionUncertain()


async def _close_video_client(
    close: Callable[[], Awaitable[object]],
    *,
    provider_key: str,
    primary_error: BaseException | None,
) -> None:
    """有界关闭客户端；普通 close 错误不得覆盖正在传播的主异常。"""

    close_slot = _VIDEO_CLIENT_CLOSE_SLOTS.reserve()
    if close_slot is None:
        _poison_worker_async_runtime(provider_key)
        exc = TimeoutError("video_provider_client_close_orphan_pending")
        if primary_error is None:
            raise exc
        _log_video_client_close_failure(provider_key, exc)
        return

    async def _invoke_close() -> object:
        return await close()

    # 空 task-scope 上下文避免 WorkerAsyncRuntime 在 invocation 收尾时再次等待它。
    close_coroutine = _invoke_close()
    try:
        task = asyncio.create_task(
            close_coroutine,
            name=f"video-provider-close:{provider_key}",
            context=Context(),
        )
    except BaseException:
        close_coroutine.close()
        _VIDEO_CLIENT_CLOSE_SLOTS.release(close_slot)
        raise

    def _finalize_close(completed: asyncio.Task[object]) -> None:
        _VIDEO_CLIENT_CLOSE_SLOTS.release(close_slot)
        _consume_video_client_close_orphan(completed, provider_key)

    task.add_done_callback(_finalize_close)
    _register_worker_async_runtime_detached_close(
        task,
        on_abandon=lambda: _abandon_video_client_close(task, close_slot),
    )
    try:
        done, _pending = await asyncio.wait(
            {task},
            timeout=_VIDEO_CLIENT_CLOSE_TIMEOUT,
        )
    except (asyncio.CancelledError, SoftTimeLimitExceeded, TimeLimitExceeded):
        _register_video_client_close_orphan(task, provider_key)
        task.cancel()
        _poison_worker_async_runtime(provider_key)
        raise

    if task in done:
        _finalize_close(task)
        try:
            task.result()
        except (asyncio.CancelledError, SoftTimeLimitExceeded, TimeLimitExceeded):
            raise
        except Exception as exc:
            if primary_error is None:
                raise
            _log_video_client_close_failure(provider_key, exc)
        return

    timeout_error = TimeoutError("video_provider_client_close_timeout")
    _register_video_client_close_orphan(task, provider_key)
    task.cancel()
    try:
        await asyncio.sleep(0)
    finally:
        if not task.done():
            _poison_worker_async_runtime(provider_key)
    if primary_error is None:
        raise timeout_error
    _log_video_client_close_failure(provider_key, timeout_error)


def _register_video_client_close_orphan(
    task: asyncio.Task[object],
    provider_key: str,
) -> None:
    with _VIDEO_CLIENT_CLOSE_ORPHANS_LOCK:
        _VIDEO_CLIENT_CLOSE_ORPHANS.add(task)
    if task.done():
        _consume_video_client_close_orphan(task, provider_key)


def _consume_video_client_close_orphan(
    task: asyncio.Task[object],
    provider_key: str,
) -> None:
    with _VIDEO_CLIENT_CLOSE_ORPHANS_LOCK:
        was_orphan = task in _VIDEO_CLIENT_CLOSE_ORPHANS
        _VIDEO_CLIENT_CLOSE_ORPHANS.discard(task)
    if not was_orphan or task.cancelled():
        return
    error = task.exception()
    if error is not None:
        _log_video_client_close_failure(provider_key, error)


def _abandon_video_client_close(task: asyncio.Task[object], close_slot: object) -> None:
    _VIDEO_CLIENT_CLOSE_SLOTS.release(close_slot)
    with _VIDEO_CLIENT_CLOSE_ORPHANS_LOCK:
        _VIDEO_CLIENT_CLOSE_ORPHANS.discard(task)


def _register_worker_async_runtime_detached_close(
    task: asyncio.Task[object],
    *,
    on_abandon: Callable[[], None],
) -> None:
    from app.workers.async_runtime import (
        register_current_worker_async_runtime_detached_task,
    )

    register_current_worker_async_runtime_detached_task(
        task,
        on_abandon=on_abandon,
    )


def _poison_worker_async_runtime(provider_key: str) -> None:
    try:
        from app.workers.async_runtime import poison_current_worker_async_runtime

        poison_current_worker_async_runtime()
    except Exception as exc:  # noqa: BLE001 - 关闭超时路径不得覆盖主异常
        logger.error(
            "毒化视频 Provider 异步运行时失败 provider_key=%s error_type=%s",
            provider_key,
            type(exc).__name__,
        )


def _log_video_client_close_failure(provider_key: str, exc: BaseException) -> None:
    logger.warning(
        "关闭视频 Provider 客户端失败 provider_key=%s error_type=%s",
        provider_key,
        type(exc).__name__,
    )


@asynccontextmanager
async def managed_video_client[ClientT](
    client: ClientT,
    close: Callable[[], Awaitable[object]],
    *,
    provider_key: str,
) -> AsyncIterator[ClientT]:
    """统一管理视频 Provider 客户端，并保持主异常与控制流信号。"""

    primary_error: BaseException | None = None
    try:
        yield client
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        await _close_video_client(
            close,
            provider_key=provider_key,
            primary_error=primary_error,
        )


async def raise_if_video_cancel_requested_before_submit(
    cancel_requested: CancellationProbe | None,
) -> None:
    """远端任务尚未创建时直接终止，避免产生无效调用和费用。"""
    if cancel_requested is None or not await cancel_requested():
        return
    raise VideoGenerationCancelled(
        VideoCancellationResult(
            provider_task_id="",
            outcome=VideoCancellationOutcome.CANCELED,
            detail="本地取消发生在远端任务提交前",
        )
    )


async def raise_if_video_cancel_requested(
    cancel_requested: CancellationProbe | None,
    provider_task_id: str,
    cancel_remote: RemoteCancelCallback,
) -> None:
    """在远端任务 ID 已知后检查本地终态，并尽力停止远端任务。"""
    if cancel_requested is None or not await cancel_requested():
        return
    result = await cancel_remote(provider_task_id)
    raise VideoGenerationCancelled(result)


def unsupported_video_cancellation(
    provider_key: str,
    provider_task_id: str,
) -> VideoCancellationResult:
    """为没有可靠取消语义的 Provider 返回可解释降级结果。"""
    return VideoCancellationResult(
        provider_task_id=provider_task_id,
        outcome=VideoCancellationOutcome.UNSUPPORTED,
        detail=f"{provider_key} 当前未提供可靠的远端视频任务取消能力",
    )


@runtime_checkable
class VideoGenProvider(Protocol):
    """图生视频供应商协议 — 多参考图 + 参考音频 → 视频 storage_key。"""

    @property
    def provider_key(self) -> str:
        """供应商标识，如 jianyu / modelsai。"""
        ...

    async def generate(
        self,
        request: VideoGenerationRequest,
        on_progress: ProgressCallback | None = None,
    ) -> VideoGenerationResult:
        """使用 typed 请求执行视频生成。"""
        ...

    async def cancel(self, provider_task_id: str) -> VideoCancellationResult:
        """尽力取消已知 ID 的远端任务，并显式返回能力或失败状态。"""
        ...

    async def generate_all_reference(
        self,
        prompt: str,
        reference_images: list[dict],
        reference_audios: list[dict],
        model: str,
        *,
        first_frame: str | None = None,
        duration_seconds: int | None = None,
        job_id: str | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> str:
        """多参考图图生视频。

        Args:
            prompt: 镜头组 prompt（含镜头分时段 + 角色 + 叙事目的 + 约束）。
            reference_images: 参考图列表，元素 {kind:'storage', storage_key, mime_type, size_bytes, sha256}。
            reference_audios: 参考音频列表（结构同参考图，可空）。
            model: 视频模型，如 doubao-seedance-2-0-260128。
            first_frame: 首帧参考图 storage_key（上一镜头组尾帧，用于链式衔接，可空）。
            duration_seconds: 期望视频时长（秒），转发到 provider 请求体，可空。
            job_id: 调用方视频任务 ID，供 provider 拼 storage_key 追溯，可空。
            on_progress: 可选进度回调 (stage, message, progress 0-100)。

        Returns:
            生成视频的 storage_key（存入对象存储后的路径）。
        """
        ...


def get_video_provider(provider_key: str | None = None) -> VideoGenProvider:
    """按 provider_key 获取视频生成供应商实例。

    路由策略（.env 主导 + 别名兼容）：
    - jianyu / modelsai / volcengine / doubao / seedance -> 豆包 Seedance (volcengine)
    - bailian / wanx / wan2 / qwen-video -> 阿里百炼通义万相视频
    - openai / sora -> OpenAI Sora
    - gemini / google / veo -> Google Gemini Veo
    - mgrouter / aicc -> mgrouter AICC Seedance
    - 未传 provider_key -> 按 settings.video_gen_provider 选厂商
    - 未识别 -> 明确报错，不静默回退
    缺少对应密钥时实例可创建，但调用 generate 时抛 RuntimeError。
    """
    key = (provider_key or settings.video_gen_provider or "").strip().lower()
    # 1) 已知别名
    if key in ("jianyu", "modelsai", "volcengine", "doubao", "seedance"):
        from app.providers.video_gen_volcengine import VolcengineVideoGenProvider

        return VolcengineVideoGenProvider(provider_key=key or settings.video_default_provider)
    if key in ("bailian", "wanx", "wan2", "qwen-video"):
        from app.providers.video_gen_bailian import BailianVideoGenProvider

        return BailianVideoGenProvider(provider_key=key or "bailian")
    if key in ("openai", "sora"):
        from app.providers.video_gen_openai import OpenAIVideoGenProvider

        return OpenAIVideoGenProvider(provider_key=key or "openai")
    if key in ("gemini", "google", "veo"):
        from app.providers.video_gen_google import GoogleVideoGenProvider

        return GoogleVideoGenProvider(provider_key=key)
    if key in ("mgrouter", "mg-router", "aicc", "aicc-seedance"):
        from app.providers.video_gen_mgrouter import MgrouterVideoGenProvider

        return MgrouterVideoGenProvider(provider_key="mgrouter")

    # 未识别的非空 key 必须失败，不能静默调用并产生其他厂商费用。
    from app.providers.registry import require_capability

    require_capability(key, "video")
    raise AssertionError(f"遗漏的视频 provider 实现: {key}")
