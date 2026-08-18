"""字节豆包 Seedance 2.0 视频生成供应商实现。

底层模型 doubao-seedance-2-0-260128 (Seedance 2.0)，使用火山方舟
``AsyncArk.content_generation.tasks`` 接口。jianyu / modelsai 两个 provider_key
路由到同一底层模型。

参考: PLAN 5.2 模型映射表。真实接入需火山方舟 Ark API Key
（VOLCENGINE_API_KEY 或 VIDEO_GEN_API_KEY），使用
https://ark.cn-beijing.volces.com/api/v3 端点与 Bearer Token 鉴权；
缺 key 或缺 SDK 时 generate_all_reference 抛 RuntimeError，由 worker 捕获后 release 积分，
不阻塞队列（PLAN 5.4 / 5.5）。

实现要点:
1. _check_keys: 校验 Ark API Key，缺失抛 RuntimeError。
2. _submit_and_poll: 用 AsyncArk 提交内容生成任务并轮询至终态，成功后下载 video_url。
3. _store_video: 将视频字节经 storage.put 存入对象存储，返回 storage_key。

参考图、首帧和参考音频以 storage_key 形式传入，提交前经 storage.get 读取并编码为
data URL，避免要求私有对象存储暴露公网地址。
"""

import asyncio
import base64
import logging
import re
import time
from inspect import isawaitable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import urlsplit

from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded

from app.core.config import settings
from app.core.reference_media_limits import (
    ReferenceMediaCounts,
    ReferenceMediaLimitExceeded,
    validate_reference_media_counts,
)
from app.providers.video_gen import (
    CancellationProbe,
    SubmissionStateCallback,
    TaskCreatedCallback,
    VideoCancellationOutcome,
    VideoCancellationResult,
    VideoGenerationProgress,
    VideoGenerationRemoteTerminalError,
    VideoGenerationRequest,
    VideoGenerationResult,
    VideoGenerationSubmitRejected,
    await_video_task_checkpoint,
    execute_video_submission,
    managed_video_client,
    raise_if_video_cancel_requested,
    raise_if_video_cancel_requested_before_submit,
    raise_video_submission_uncertain,
)

logger = logging.getLogger(__name__)

# 轮询间隔（秒）
_POLL_INTERVAL = 5.0
# 轮询超时（秒），约 10 分钟
_POLL_TIMEOUT = 600
_CANCEL_SDK_TIMEOUT = 5.0
_CANCEL_TOTAL_TIMEOUT = 8.0
_MAX_TOTAL_REFERENCE_BYTES = 200 * 1024 * 1024
_ARK_CONTENT_DOWNLOAD_HOST = re.compile(
    r"(?:ark-content-generation|ark-acg)-(?P<region>[a-z0-9]+(?:-[a-z0-9]+)*)\."
    r"tos-(?P=region)\.volces\.com"
)


def _without_duplicate_first_frame(
    reference_images: list[dict],
    first_frame: str | None,
) -> list[dict]:
    """首帧已通过独立 role 提交时，不再作为普通参考图重复发送。"""

    if not first_frame:
        return list(reference_images)
    return [
        reference
        for reference in reference_images
        if not isinstance(reference, dict) or reference.get("storage_key") != first_frame
    ]


class VolcengineVideoGenProvider:
    """豆包 Seedance 2.0 (volcengine) 图生视频实现。

    通过 volcengine 视频生成 2.0 的 all_reference 接口提交多参考图任务，
    轮询任务完成，将视频下载后存入对象存储，返回 storage_key。

    提供 on_progress 回调驱动 worker 进度更新。
    """

    def __init__(self, *, provider_key: str = "modelsai") -> None:
        # jianyu | modelsai，仅标识不同，底层模型一致
        self._provider_key = provider_key

    @property
    def provider_key(self) -> str:
        return self._provider_key

    async def cancel(self, provider_task_id: str) -> VideoCancellationResult:
        """通过 Ark tasks.delete 尽力取消远端内容生成任务。"""
        if not provider_task_id:
            return VideoCancellationResult(
                provider_task_id=provider_task_id,
                outcome=VideoCancellationOutcome.FAILED,
                detail="缺少火山方舟远端任务 ID",
            )
        result: VideoCancellationResult | None = None
        try:
            self._check_keys()
            client = self._build_client(
                timeout=_CANCEL_SDK_TIMEOUT,
                max_retries=0,
            )
            async with managed_video_client(
                client,
                lambda: self._close_client_safely(client),
                provider_key=self.provider_key,
            ):
                result = await self._cancel_task(
                    client.content_generation.tasks,
                    provider_task_id,
                )
            return result
        except (asyncio.CancelledError, SoftTimeLimitExceeded, TimeLimitExceeded):
            raise
        except Exception as exc:  # noqa: BLE001 - 取消是尽力操作，不能覆盖本地终态
            if result is not None:
                logger.warning(
                    "关闭火山方舟取消客户端失败 task_id=%s error=%s",
                    provider_task_id,
                    type(exc).__name__,
                )
                return result
            logger.warning(
                "取消火山方舟视频任务失败 task_id=%s error=%s",
                provider_task_id,
                type(exc).__name__,
            )
            return VideoCancellationResult(
                provider_task_id=provider_task_id,
                outcome=VideoCancellationOutcome.FAILED,
                detail=f"火山方舟远端取消失败: {type(exc).__name__}",
            )

    @staticmethod
    async def _cancel_task(tasks, provider_task_id: str) -> VideoCancellationResult:
        try:
            await asyncio.wait_for(
                tasks.delete(task_id=provider_task_id),
                timeout=_CANCEL_TOTAL_TIMEOUT,
            )
        except (asyncio.CancelledError, SoftTimeLimitExceeded, TimeLimitExceeded):
            raise
        except Exception as exc:  # noqa: BLE001 - 返回显式失败供调用方降级
            if _is_ark_task_not_found(exc):
                logger.info(
                    "火山方舟视频任务已不存在 task_id=%s",
                    provider_task_id,
                )
                return VideoCancellationResult(
                    provider_task_id=provider_task_id,
                    outcome=VideoCancellationOutcome.CANCELED,
                    detail="火山方舟远端任务已不存在",
                )
            logger.warning(
                "火山方舟 tasks.delete 失败 task_id=%s error=%s",
                provider_task_id,
                type(exc).__name__,
            )
            return VideoCancellationResult(
                provider_task_id=provider_task_id,
                outcome=VideoCancellationOutcome.FAILED,
                detail=f"火山方舟 tasks.delete 失败: {type(exc).__name__}",
            )
        return VideoCancellationResult(
            provider_task_id=provider_task_id,
            outcome=VideoCancellationOutcome.CANCELED,
        )

    async def _cancel_remote_task(self, tasks, provider_task_id: str) -> VideoCancellationResult:
        """取消远端任务；兼容网关可覆盖为明确的不支持结果。"""
        return await self._cancel_task(tasks, provider_task_id)

    def _resolve_ark_api_key(self) -> str:
        """解析 Ark API Key：显式 VIDEO_GEN_API_KEY 优先，否则回退 VOLCENGINE_API_KEY。"""
        if settings.is_explicitly_configured("video_gen_api_key") and settings.video_gen_api_key:
            return settings.video_gen_api_key
        return settings.volcengine_api_key

    def _resolve_ark_base_url(self) -> str:
        """解析 Ark Base URL：显式 VIDEO_GEN_BASE_URL 优先，否则回退 Ark 默认端点。"""
        if settings.is_explicitly_configured("video_gen_base_url") and settings.video_gen_base_url:
            return settings.video_gen_base_url
        return settings.volcengine_ark_base_url

    def _check_keys(self) -> None:
        """校验火山方舟 Ark API Key，缺失抛 RuntimeError（不阻塞队列）。"""
        # 存量部署可能仍只配了 AK/SK，给出明确迁移提示，避免静默不可用。
        if (
            not self._resolve_ark_api_key()
            and (settings.volcengine_access_key or settings.volcengine_secret_key)
        ):
            logger.warning(
                "VOLCENGINE_ACCESS_KEY/SECRET_KEY 已废弃，视频生成改用 Ark API Key，"
                "请在 .env 配置 VOLCENGINE_API_KEY 或 VIDEO_GEN_API_KEY"
            )
        if not self._resolve_ark_api_key():
            raise RuntimeError(
                "缺少火山方舟 Ark API Key（VOLCENGINE_API_KEY 或 VIDEO_GEN_API_KEY），"
                "无法调用豆包 Seedance 视频生成"
            )

    def _build_client(
        self,
        *,
        timeout: float | None = None,
        max_retries: int = 0,
    ):
        """构造火山方舟异步客户端。"""
        try:
            from volcenginesdkarkruntime import AsyncArk
        except ImportError as exc:  # pragma: no cover - 依赖缺失兜底
            raise RuntimeError(
                "缺少 volcenginesdkarkruntime，无法调用豆包 Seedance 视频生成"
            ) from exc

        client_kwargs: dict[str, Any] = {
            "api_key": self._resolve_ark_api_key(),
            "base_url": self._resolve_ark_base_url(),
            # Ark 视频 submit 无稳定幂等 token，网络失败交由 durable intent 判权。
            "max_retries": max_retries,
        }
        if timeout is not None:
            client_kwargs["timeout"] = timeout
        return AsyncArk(
            **client_kwargs,
        )

    @staticmethod
    async def _close_client(client) -> None:
        """兼容 Ark SDK 同步与异步 close，由统一生命周期负责超时和隔离。"""
        close_result = client.close()
        if isawaitable(close_result):
            await close_result

    async def _close_client_safely(self, client) -> None:
        """保留旧扩展入口；安全边界仍由外层 managed_video_client 统一提供。"""
        await self._close_client(client)

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
        on_progress=None,
    ) -> str:
        request = VideoGenerationRequest(
            prompt=prompt,
            reference_images=reference_images,
            reference_image_names=[],
            reference_audios=reference_audios,
            reference_audio_names=[],
            model=model,
            job_id=job_id or "unknown",
            first_frame=first_frame,
            duration_seconds=duration_seconds,
        )

        def _legacy_progress(progress: VideoGenerationProgress) -> None:
            if on_progress is not None:
                on_progress(progress.stage, progress.message, progress.progress)

        result = await self.generate(request, _legacy_progress if on_progress else None)
        return result.video_storage_key

    async def generate(
        self,
        request: VideoGenerationRequest,
        on_progress=None,
    ) -> VideoGenerationResult:
        """多参考图图生视频 → 返回视频 storage_key。

        流程:
        1. 校验密钥
        2. 构造请求参数（prompt + 参考图/首帧/参考音频 base64）
        3. 提交 volcengine 视频生成任务（异步）
        4. 轮询任务状态，回调进度
        5. 任务完成 → 下载视频字节 → 存入对象存储 → 返回 storage_key
        6. 失败抛 RuntimeError

        Args:
            model: 视频模型，默认 doubao-seedance-2-0-260128。
            first_frame: 首帧参考图 storage_key（上一组尾帧，链式衔接）。
            duration_seconds: 期望视频时长（秒），转发到 provider 请求体 duration 字段。
            job_id: 调用方视频任务 ID，用于拼 storage_key 便于追溯（缺失回退 task_id）。
            on_progress: 回调 (stage:str, message:str, progress:int)。
        """
        existing_task_id = str(request.metadata.get("provider_task_id") or "")
        if existing_task_id:
            await await_video_task_checkpoint(
                request.on_task_created,
                existing_task_id,
            )
        self._check_keys()
        use_model = request.model or settings.seedance_model

        def _notify(
            stage: str,
            message: str,
            progress: int,
            provider_task_id: str | None = None,
        ) -> None:
            if on_progress is not None:
                try:
                    on_progress(
                        VideoGenerationProgress(
                            stage=stage,
                            message=message,
                            progress=progress,
                            provider_task_id=provider_task_id,
                        )
                    )
                except Exception:  # noqa: BLE001 — 进度回调失败不应中断生成
                    logger.warning("视频生成进度回调失败", exc_info=True)

        # 提交阶段
        _notify("submitting", "正在提交视频生成任务", 10)
        if not existing_task_id:
            await raise_if_video_cancel_requested_before_submit(
                request.cancel_requested
            )
            reference_images = _without_duplicate_first_frame(
                request.reference_images,
                request.first_frame,
            )
            try:
                validate_reference_media_counts(
                    ReferenceMediaCounts(
                        images=len(reference_images),
                        audios=len(request.reference_audios),
                        videos=len(request.reference_videos),
                        first_frames=1 if request.first_frame else 0,
                    )
                )
            except ReferenceMediaLimitExceeded as exc:
                raise VideoGenerationSubmitRejected(
                    diagnostics={"code": exc.code}
                ) from None
            await self._preflight_reference_sizes(
                reference_images,
                request.reference_audios,
                request.first_frame,
                request.reference_videos,
            )

            # Ark 的 content 字段接收 URL；这里使用 data URL 承载私有存储中的媒体。
            reference_budget = [0]
            image_urls = await self._encode_storage_refs(
                reference_images,
                "image/png",
                budget=reference_budget,
            )
            audio_urls = await self._encode_storage_refs(
                request.reference_audios,
                "audio/mpeg",
                budget=reference_budget,
            )
            video_urls = await self._encode_storage_refs(
                request.reference_videos,
                "video/mp4",
                budget=reference_budget,
            )
            first_frame_url: str | None = None
            if request.first_frame:
                first_frame_url = await self._encode_single(
                    request.first_frame,
                    budget=reference_budget,
                )
        else:
            image_urls = []
            audio_urls = []
            video_urls = []
            first_frame_url = None

        # ===== volcengine SDK 调用 =====
        submit_kwargs: dict[str, Any] = {}
        if video_urls:
            submit_kwargs["video_urls"] = video_urls
        video_url, task_meta = await self._submit_and_poll(
            use_model, request.prompt, image_urls, audio_urls, first_frame_url, _notify,
            duration_seconds=request.duration_seconds,
            job_id=request.job_id,
            provider_task_id=existing_task_id or None,
            cancel_requested=request.cancel_requested,
            on_task_created=(None if existing_task_id else request.on_task_created),
            before_submit=request.before_submit,
            on_submission_uncertain=request.on_submission_uncertain,
            **submit_kwargs,
        )

        with TemporaryDirectory(prefix="mtv-volcengine-video-") as temp_dir:
            video_path = Path(temp_dir) / "video.mp4"
            task_id = str(task_meta.get("task_id") or "")

            async def _check_download_cancellation() -> None:
                await raise_if_video_cancel_requested(
                    request.cancel_requested,
                    task_id,
                    self.cancel,
                )

            await _check_download_cancellation()
            _notify("downloading", "正在下载生成视频", 90, task_meta.get("task_id"))
            await self._download_to_file(
                video_url,
                video_path,
                cancellation_probe=_check_download_cancellation,
            )
            await _check_download_cancellation()
            _notify("storing", "正在存储视频", 95)
            storage_key = await self._store_video_file(
                video_path,
                task_meta,
                request.metadata,
            )

        _notify("succeeded", "视频生成完成", 100)
        return VideoGenerationResult(
            video_storage_key=storage_key,
            provider_job_id=task_meta.get("task_id"),
            metrics={
                "provider_key": self.provider_key,
                "task_id": task_meta.get("task_id"),
                "volcengine_status": task_meta.get("volcengine_status"),
            },
        )

    async def _encode_storage_refs(
        self,
        refs: list[dict],
        default_mime: str,
        *,
        budget: list[int] | None = None,
    ) -> list[str]:
        """批量读取参考媒体并编码为 data URL；任一项缺失时关闭失败。"""
        from app.providers.storage import get_storage
        from app.providers.streaming_io import read_storage_bytes_limited

        if not refs:
            return []
        storage = get_storage()
        encoded: list[str] = []
        active_budget = budget if budget is not None else [0]
        if default_mime.startswith("audio/"):
            too_large_error = "reference_audio_too_large"
        elif default_mime.startswith("video/"):
            too_large_error = "reference_video_too_large"
        else:
            too_large_error = "reference_image_too_large"
        for ref in refs:
            key = ref.get("storage_key") if isinstance(ref, dict) else None
            if not key:
                continue
            data = await read_storage_bytes_limited(
                storage,
                key,
                too_large_error=too_large_error,
            )
            self._consume_reference_budget(active_budget, len(data))
            mime_type = str(ref.get("mime_type") or default_mime).split(";", 1)[0].strip()
            if "/" not in mime_type:
                mime_type = default_mime
            encoded.append(self._as_data_url(data, mime_type))
        return encoded

    async def _encode_single(
        self,
        storage_key: str,
        *,
        budget: list[int] | None = None,
    ) -> str:
        """读取单个首帧并编码为图片 data URL。"""
        from app.providers.storage import get_storage
        from app.providers.streaming_io import read_storage_bytes_limited

        data = await read_storage_bytes_limited(
            get_storage(),
            storage_key,
            too_large_error="reference_image_too_large",
        )
        self._consume_reference_budget(budget if budget is not None else [0], len(data))
        return self._as_data_url(data, "image/png")

    async def _preflight_reference_sizes(
        self,
        reference_images: list[dict],
        reference_audios: list[dict],
        first_frame: str | None,
        reference_videos: list[dict] | None = None,
    ) -> None:
        """在读取任一多模态输入前校验单项与聚合体积。"""
        from app.providers.storage import get_storage
        from app.providers.streaming_io import MAX_REFERENCE_MEDIA_BYTES

        storage = get_storage()
        get_size = getattr(storage, "get_size", None)
        if not callable(get_size):
            return
        media: list[tuple[str, str]] = []
        media.extend(
            (str(ref["storage_key"]), "reference_image_too_large")
            for ref in reference_images
            if isinstance(ref, dict) and ref.get("storage_key")
        )
        media.extend(
            (str(ref["storage_key"]), "reference_audio_too_large")
            for ref in reference_audios
            if isinstance(ref, dict) and ref.get("storage_key")
        )
        media.extend(
            (str(ref["storage_key"]), "reference_video_too_large")
            for ref in reference_videos or []
            if isinstance(ref, dict) and ref.get("storage_key")
        )
        if first_frame:
            media.append((first_frame, "reference_image_too_large"))

        total = 0
        for storage_key, too_large_error in media:
            try:
                size = int(await get_size(storage_key))
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError("reference_read_failed") from exc
            if size <= 0:
                raise RuntimeError("reference_read_failed")
            if size > MAX_REFERENCE_MEDIA_BYTES:
                raise RuntimeError(too_large_error)
            total += size
            if total > _MAX_TOTAL_REFERENCE_BYTES:
                raise RuntimeError("reference_media_total_too_large")

    @staticmethod
    def _consume_reference_budget(budget: list[int], size: int) -> None:
        budget[0] += size
        if budget[0] > _MAX_TOTAL_REFERENCE_BYTES:
            raise RuntimeError("reference_media_total_too_large")

    @staticmethod
    def _as_data_url(data: bytes, mime_type: str) -> str:
        return f"data:{mime_type};base64,{base64.b64encode(data).decode('ascii')}"

    def _build_content(
        self,
        prompt: str,
        image_urls: list[str],
        audio_urls: list[str],
        first_frame_url: str | None,
        video_urls: list[str] | None = None,
    ) -> list[dict]:
        """构造 Ark 内容生成 API 的多模态 content。"""
        content: list[dict] = [{"type": "text", "text": prompt}]
        if first_frame_url:
            content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": first_frame_url},
                    "role": "first_frame",
                }
            )
        content.extend(
            {
                "type": "image_url",
                "image_url": {"url": url},
                "role": "reference_image",
            }
            for url in image_urls
        )
        content.extend(
            {
                "type": "audio_url",
                "audio_url": {"url": url},
                "role": "reference_audio",
            }
            for url in audio_urls
        )
        content.extend(
            {
                "type": "video_url",
                "video_url": {"url": url},
                "role": "reference_video",
            }
            for url in video_urls or []
        )
        return content

    def _build_create_kwargs(
        self,
        model: str,
        prompt: str,
        image_urls: list[str],
        audio_urls: list[str],
        first_frame_url: str | None,
        duration_seconds: int | None,
        video_urls: list[str] | None = None,
    ) -> dict:
        """构造 Ark 创建任务参数，兼容网关可覆盖供应商专属字段。"""
        create_kwargs: dict = {
            "model": model,
            "content": self._build_content(
                prompt,
                image_urls,
                audio_urls,
                first_frame_url,
                video_urls,
            ),
            # 参考音频是可选的声音约束，不应成为是否生成音轨的开关。
            # 没有参考音频时也让 Seedance 根据 Prompt 生成对白、环境音和音效。
            "generate_audio": True,
            "return_last_frame": True,
        }
        if duration_seconds and duration_seconds > 0:
            create_kwargs["duration"] = duration_seconds
        return create_kwargs

    async def _submit_and_poll(
        self,
        model: str,
        prompt: str,
        image_urls: list[str],
        audio_urls: list[str],
        first_frame_url: str | None,
        _notify,
        *,
        video_urls: list[str] | None = None,
        duration_seconds: int | None = None,
        job_id: str | None = None,
        provider_task_id: str | None = None,
        cancel_requested: CancellationProbe | None = None,
        on_task_created: TaskCreatedCallback | None = None,
        before_submit: SubmissionStateCallback | None = None,
        on_submission_uncertain: SubmissionStateCallback | None = None,
    ) -> tuple[str, dict]:
        """通过 Ark 提交视频任务并轮询至完成，返回下载 URL 与任务元数据。"""
        client = self._build_client()
        async with managed_video_client(
            client,
            lambda: self._close_client_safely(client),
            provider_key=self.provider_key,
        ):
            tasks = client.content_generation.tasks
            task_id = provider_task_id
            if task_id:
                await await_video_task_checkpoint(on_task_created, task_id)
            else:
                await raise_if_video_cancel_requested_before_submit(
                    cancel_requested
                )
                create_kwargs = self._build_create_kwargs(
                    model,
                    prompt,
                    image_urls,
                    audio_urls,
                    first_frame_url,
                    duration_seconds,
                    video_urls,
                )
                async def _submit_video():
                    return await tasks.create(**create_kwargs)

                submit_resp = await execute_video_submission(
                    _submit_video,
                    before_submit=before_submit,
                    on_submission_uncertain=on_submission_uncertain,
                    rejection_sensitive_values=tuple(
                        value
                        for value in [
                            self._resolve_ark_api_key(),
                            prompt,
                            first_frame_url,
                            *image_urls,
                            *audio_urls,
                            *(video_urls or []),
                        ]
                        if isinstance(value, str) and value
                    ),
                )
                task_id = self._extract_task_id(submit_resp)
                if not task_id:
                    await raise_video_submission_uncertain(on_submission_uncertain)
                await await_video_task_checkpoint(on_task_created, task_id)
                await raise_if_video_cancel_requested(
                    cancel_requested,
                    task_id,
                    lambda remote_id: self._cancel_remote_task(tasks, remote_id),
                )
                _notify("submitted", "视频生成任务已提交", 15, task_id)

            _notify("polling", "视频生成中，轮询任务状态", 20, task_id)
            deadline = time.monotonic() + _POLL_TIMEOUT
            started_at = time.monotonic()
            last_progress = 20
            result_resp = None
            while True:
                await raise_if_video_cancel_requested(
                    cancel_requested,
                    task_id,
                    lambda remote_id: self._cancel_remote_task(tasks, remote_id),
                )
                result_resp = await tasks.get(task_id=task_id)
                await raise_if_video_cancel_requested(
                    cancel_requested,
                    task_id,
                    lambda remote_id: self._cancel_remote_task(tasks, remote_id),
                )
                status = self._extract_status(result_resp)
                if status in {
                    "failed",
                    "error",
                    "expired",
                    "succeeded",
                    "cancelled",
                    "canceled",
                }:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError(
                        f"{self.provider_key} 视频生成轮询超时 task_id={task_id}"
                    )
                pct = min(90, 20 + int((time.monotonic() - started_at) / _POLL_TIMEOUT * 70))
                if pct > last_progress:
                    last_progress = pct
                    _notify("generating", "视频生成中", pct, task_id)
                await asyncio.sleep(_POLL_INTERVAL)

            status = self._extract_status(result_resp)
            if status != "succeeded":
                raise VideoGenerationRemoteTerminalError(
                    provider_task_id=task_id,
                    terminal_status=status,
                )

            video_url = self._extract_video_url(result_resp)
            if not video_url:
                raise RuntimeError(
                    f"{self.provider_key} 视频生成完成但未返回 video_url task_id={task_id}"
                )

            return video_url, {
                "task_id": task_id,
                "volcengine_status": status,
                "video_url": video_url,
                "last_frame_url": self._extract_last_frame_url(result_resp),
                "job_id": job_id,
            }

    @staticmethod
    def _extract_task_id(resp) -> str:
        """从 Ark 提交响应中提取 task_id，兼容测试用 dict。"""
        direct = getattr(resp, "id", None)
        if direct:
            return str(direct)
        data = resp
        if not isinstance(data, dict):
            data = getattr(resp, "__dict__", {}) or {}
            # SDK 返回对象时尝试取 .data
            data = getattr(resp, "data", data) or {}
        if isinstance(data, dict):
            inner = data.get("data", data)
            if isinstance(inner, dict):
                return str(inner.get("task_id") or inner.get("id") or "")
        return ""

    @staticmethod
    def _extract_status(resp) -> str:
        """从 Ark 轮询响应中提取状态。"""
        direct = getattr(resp, "status", None)
        if direct:
            return str(direct).lower()
        data = resp
        if not isinstance(data, dict):
            data = getattr(resp, "data", {}) or {}
        if isinstance(data, dict):
            inner = data.get("data", data)
            if isinstance(inner, dict):
                return str(inner.get("status") or inner.get("code") or "").lower()
        return ""

    @staticmethod
    def _extract_video_url(resp) -> str:
        """从 Ark 轮询响应中提取 video_url。"""
        content_obj = getattr(resp, "content", None)
        direct = getattr(content_obj, "video_url", None)
        if direct:
            return str(direct)
        data = resp
        if not isinstance(data, dict):
            data = getattr(resp, "data", {}) or {}
        if isinstance(data, dict):
            inner = data.get("data", data)
            if isinstance(inner, dict):
                # 兼容 video_url / origin_video_url / resp_data
                url = inner.get("video_url") or inner.get("origin_video_url") or ""
                if not url:
                    # 部分响应把 URL 放在 content/resp_data 列表
                    content = inner.get("content") or inner.get("resp_data") or []
                    if isinstance(content, dict):
                        url = content.get("video_url") or content.get("url") or ""
                    elif isinstance(content, list) and content:
                        first = content[0]
                        if isinstance(first, dict):
                            url = first.get("video_url") or first.get("url") or ""
                return str(url or "")
        return ""

    @staticmethod
    def _extract_last_frame_url(resp) -> str:
        content = getattr(resp, "content", None)
        if content is not None:
            return str(getattr(content, "last_frame_url", "") or "")
        if isinstance(resp, dict):
            content = resp.get("content") or resp.get("data", {}).get("content")
            if isinstance(content, dict):
                return str(content.get("last_frame_url") or "")
        return ""

    async def _download_to_file(
        self,
        url: str,
        path: str | Path,
        *,
        cancellation_probe=None,
    ) -> int:
        """逐跳校验视频 URL，并把响应流写入临时文件。"""
        from app.providers._url_safety import MAX_PROVIDER_VIDEO_BYTES, safe_download_to_file

        return await safe_download_to_file(
            url,
            path,
            max_bytes=MAX_PROVIDER_VIDEO_BYTES,
            timeout=120.0,
            url_validator=self._validate_download_host,
            cancellation_probe=cancellation_probe,
        )

    def _validate_download_host(self, url: str) -> None:
        """仅允许 Ark API 主机或区域一致的官方内容桶通过 HTTPS 443 下载。"""
        try:
            parsed = urlsplit(url)
            hostname = (parsed.hostname or "").lower()
            port = parsed.port or 443
        except ValueError as exc:
            raise RuntimeError("provider_download_url_invalid") from exc

        try:
            configured = urlsplit(self._resolve_ark_base_url())
            configured_host = (configured.hostname or "").lower()
            configured_port = configured.port or 443
        except ValueError:
            configured_host = ""
            configured_port = 0

        configured_origin = (
            configured.scheme.lower() == "https"
            and configured_port == 443
            and hostname == configured_host
        )
        official_content_origin = _ARK_CONTENT_DOWNLOAD_HOST.fullmatch(hostname) is not None
        if (
            parsed.scheme.lower() != "https"
            or not hostname
            or port != 443
            or parsed.username is not None
            or parsed.password is not None
            or not (configured_origin or official_content_origin)
        ):
            raise RuntimeError("provider_download_url_invalid")

    async def _store_video_file(
        self,
        path: str | Path,
        task_meta: dict,
        metadata: dict,
    ) -> str:
        """将临时视频文件原子提升为本次尝试的正式对象。

        走 app.providers.storage 抽象（T10 实现），storage_key 形如
        generate_images/video/<job_or_task>.mp4。
        """
        from app.providers.storage import get_storage
        from app.providers.streaming_io import atomic_put_file, storage_attempt_suffix

        storage = get_storage()
        ident = task_meta.get("job_id") or task_meta.get("task_id") or "unknown"
        attempt_suffix = storage_attempt_suffix(metadata)
        suffix = f"-{attempt_suffix}" if attempt_suffix else ""
        storage_key = f"generate_images/video/{ident}{suffix}.mp4"
        return await atomic_put_file(
            storage,
            storage_key,
            path,
            attempt_id=attempt_suffix or "legacy",
            preserve_existing_final=not attempt_suffix,
        )


def _is_ark_task_not_found(exc: Exception) -> bool:
    """仅把 SDK 的结构化 HTTP 404 识别为幂等取消成功。"""
    try:
        from volcenginesdkarkruntime._exceptions import ArkAPIStatusError
    except ImportError:  # pragma: no cover - SDK 缺失时不会产生 Ark 状态异常
        return False
    if not isinstance(exc, ArkAPIStatusError):
        return False
    response = getattr(exc, "response", None)
    return (
        getattr(exc, "status_code", None) == 404
        and getattr(response, "status_code", None) == 404
    )
