"""OpenAI Sora 视频生成供应商实现 — openai SDK videos 资源。

走 openai.AsyncOpenAI 的 videos 接口（openai>=1.x 内置）：
- 提交 client.videos.create(model, prompt, size, seconds, input_reference?) -> job
- 轮询 client.videos.retrieve(id) 直到 status==completed/failed
- 下载 GET /videos/{id}/content -> 共享安全下载器流式写入临时文件
下载后存入对象存储返回 storage_key，不长期持有 OpenAI 下载链接。

模型：sora-2 / sora-2-pro；图生视频用 input_reference。
缺 OPENAI_API_KEY 时抛 RuntimeError，由 worker 捕获后 release 积分。
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

from app.core.config import settings
from app.core.reference_media_limits import (
    ReferenceMediaCounts,
    ReferenceMediaLimitExceeded,
    effective_reference_media_limits_for_group,
    reference_media_limits_for_provider,
    validate_reference_media_counts,
)
from app.providers._url_safety import MAX_PROVIDER_VIDEO_BYTES
from app.providers.registry import ProviderConfigurationError
from app.providers.streaming_io import CancellationCallback
from app.providers.video_gen import (
    CancellationProbe,
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
    unsupported_video_cancellation,
)

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 5.0
_POLL_TIMEOUT = 600  # 10 分钟
_OPENAI_API_BASE_URL = "https://api.openai.com/v1"

# Sora 横纵档位
_ASPECT_TO_SIZE: dict[str, str] = {
    "16:9": "1280x720",
    "9:16": "720x1280",
    "1:1": "1024x1024",
}


def _map_size(aspect: str) -> str:
    return _ASPECT_TO_SIZE.get(aspect or "", "1280x720")


def _explicit_api_base_url() -> str | None:
    value = settings.video_gen_base_url.strip()
    if settings.is_explicitly_configured("video_gen_base_url") and value:
        return value
    return None


class OpenAIVideoGenProvider:
    """OpenAI Sora 视频生成实现。"""

    def __init__(self, *, provider_key: str = "openai") -> None:
        self._provider_key = provider_key

    @property
    def provider_key(self) -> str:
        return self._provider_key

    async def cancel(self, provider_task_id: str) -> VideoCancellationResult:
        """Sora delete 未承诺终止运行中生成，故不误报为已取消。"""
        return unsupported_video_cancellation(self.provider_key, provider_task_id)

    def _check_keys(self) -> None:
        if not self._api_key():
            raise ProviderConfigurationError(
                "未配置 VIDEO_GEN_API_KEY（或 OPENAI_API_KEY），无法调用 Sora 视频生成"
            )

    def _build_client(self):
        from openai import AsyncOpenAI

        return AsyncOpenAI(
            api_key=self._api_key(),
            base_url=_explicit_api_base_url(),
            # 视频 submit 没有可证明的服务端幂等契约，禁止 SDK 在响应丢失后自动重提。
            max_retries=0,
        )

    @staticmethod
    def _api_key() -> str:
        if (
            settings.is_explicitly_configured("video_gen_api_key")
            and settings.video_gen_api_key
        ):
            return settings.video_gen_api_key
        return settings.openai_api_key or settings.video_gen_api_key

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
        result = await self.generate(request, _legacy_progress(on_progress) if on_progress else None)
        return result.video_storage_key

    async def generate(
        self,
        request: VideoGenerationRequest,
        on_progress=None,
    ) -> VideoGenerationResult:
        """文生/图生视频 → 返回视频 storage_key。"""
        if request.reference_videos:
            raise VideoGenerationSubmitRejected(
                diagnostics={"code": "ReferenceVideoUnsupported"}
            )
        logger.warning(
            "OpenAI Sora Videos API 已弃用并计划于 2026-09-24 关闭，请迁移视频供应商"
        )
        existing_video_id = str((request.metadata or {}).get("provider_task_id") or "")
        use_model = request.model or settings.video_gen_model or "sora-2"
        if not existing_video_id:
            await raise_if_video_cancel_requested_before_submit(
                request.cancel_requested
            )
            # Sora 只会读取一张 input_reference。直调入口也必须拒绝多余图片、
            # 声音或视频，不能悄悄截断后继续提交。
            reference_images = [
                reference
                for reference in request.reference_images
                if not request.first_frame
                or not isinstance(reference, dict)
                or reference.get("storage_key") != request.first_frame
            ]
            try:
                validate_reference_media_counts(
                    ReferenceMediaCounts(
                        images=len(reference_images),
                        audios=len(request.reference_audios),
                        videos=len(request.reference_videos),
                        first_frames=1 if request.first_frame else 0,
                    ),
                    limits=effective_reference_media_limits_for_group(
                        reference_media_limits_for_provider("openai", use_model),
                        reserve_first_frame=bool(request.first_frame),
                    ),
                )
            except ReferenceMediaLimitExceeded as exc:
                raise VideoGenerationSubmitRejected(diagnostics={"code": exc.code}) from None
        self._check_keys()

        def _notify(stage: str, message: str, progress: int, task_id: str | None = None) -> None:
            if on_progress is not None:
                try:
                    on_progress(
                        VideoGenerationProgress(
                            stage=stage, message=message, progress=progress,
                            provider_task_id=task_id,
                        )
                    )
                except Exception:  # noqa: BLE001
                    logger.warning("视频生成进度回调失败", exc_info=True)

        with TemporaryDirectory(prefix="mtv-openai-video-") as temp_dir:
            video_path = Path(temp_dir) / "video.mp4"
            client = self._build_client()
            video_id = existing_video_id
            async with managed_video_client(
                client,
                client.close,
                provider_key=self.provider_key,
            ):
                if video_id:
                    await await_video_task_checkpoint(
                        request.on_task_created,
                        video_id,
                    )
                    await raise_if_video_cancel_requested(
                        request.cancel_requested,
                        video_id,
                        self.cancel,
                    )
                    _notify("resuming", "正在恢复视频生成任务", 15, video_id)
                else:
                    _notify("submitting", "正在提交视频生成任务", 10)
                    create_kwargs: dict[str, Any] = {
                        "model": use_model,
                        "prompt": request.prompt[:32000],
                        "size": _map_size((request.metadata or {}).get("aspect", "16:9")),
                    }
                    seconds = self._map_duration(request.duration_seconds, use_model)
                    if seconds:
                        create_kwargs["seconds"] = seconds
                    input_reference = await self._load_input_reference(request)
                    if input_reference is not None:
                        create_kwargs["input_reference"] = input_reference
                    await raise_if_video_cancel_requested_before_submit(
                        request.cancel_requested
                    )
                    async def _submit_video():
                        return await client.videos.create(**create_kwargs)

                    job = await execute_video_submission(
                        _submit_video,
                        before_submit=request.before_submit,
                        on_submission_uncertain=request.on_submission_uncertain,
                    )
                    video_id = str(getattr(job, "id", None) or "")
                    if not video_id:
                        await raise_video_submission_uncertain(
                            request.on_submission_uncertain
                        )
                    await await_video_task_checkpoint(
                        request.on_task_created,
                        video_id,
                    )
                    await raise_if_video_cancel_requested(
                        request.cancel_requested,
                        video_id,
                        self.cancel,
                    )
                    _notify("submitted", "视频生成任务已提交", 15, video_id)

                await self._poll_and_download_to_file(
                    client,
                    video_id,
                    video_path,
                    _notify,
                    cancel_requested=request.cancel_requested,
                )
            _notify("storing", "正在存储视频", 95)
            storage_key = await self._store_video_file(
                video_path,
                video_id,
                request.job_id,
                request.metadata,
            )
        _notify("succeeded", "视频生成完成", 100)
        return VideoGenerationResult(
            video_storage_key=storage_key,
            provider_job_id=video_id,
            metrics={"provider_key": self.provider_key, "video_id": video_id},
        )

    @staticmethod
    def _map_duration(duration_seconds: int | None, model: str) -> str | None:
        """Sora seconds 仅接受 '4'/'8'/'12'（pro 支持 16/20）。"""
        if not duration_seconds or duration_seconds <= 0:
            return None
        allowed_values = (4, 8, 12, 16, 20) if model.endswith("-pro") else (4, 8, 12)
        for allowed in allowed_values:
            if duration_seconds <= allowed:
                return str(allowed)
        return str(allowed_values[-1])

    @staticmethod
    async def _load_input_reference(
        request: VideoGenerationRequest,
    ) -> dict | None:
        """加载首帧/参考图为 Sora input_reference（data URL 形式）。

        Sora 的 input_reference 接受 {"image_url": <url|data-url>} 或 {"file_id": ...}。
        此处把私有存储中的参考图编码为 data URL，避免要求对象存储暴露公网地址。
        """
        import base64

        storage_key = request.first_frame
        mime_type = "image/png"
        if not storage_key and request.reference_images:
            reference = request.reference_images[0]
            storage_key = reference.get("storage_key")
            mime_type = reference.get("mime_type") or mime_type
        if not storage_key:
            return None
        from app.providers.storage import get_storage
        from app.providers.streaming_io import read_storage_bytes_limited

        data = await read_storage_bytes_limited(
            get_storage(),
            storage_key,
            too_large_error="reference_image_too_large",
        )
        b64 = base64.b64encode(data).decode("ascii")
        return {"image_url": f"data:{mime_type};base64,{b64}"}

    async def _poll_and_download_to_file(
        self,
        client,
        video_id: str,
        path: str | Path,
        _notify,
        *,
        cancel_requested: CancellationProbe | None = None,
    ) -> int:
        """轮询 Sora 任务至终态，并把响应流写入临时文件。"""
        _notify("polling", "视频生成中，轮询任务状态", 20, video_id)
        deadline = asyncio.get_event_loop().time() + _POLL_TIMEOUT
        started_at = asyncio.get_event_loop().time()
        last_progress = 20
        while True:
            await raise_if_video_cancel_requested(
                cancel_requested,
                video_id,
                self.cancel,
            )
            v = await client.videos.retrieve(video_id)
            await raise_if_video_cancel_requested(
                cancel_requested,
                video_id,
                self.cancel,
            )
            status = str(getattr(v, "status", "") or "").strip().lower()
            if status in ("completed", "failed", "error", "cancelled", "canceled"):
                break
            elapsed = asyncio.get_event_loop().time() - started_at
            pct = min(90, 20 + int(elapsed / _POLL_TIMEOUT * 70))
            if pct > last_progress:
                last_progress = pct
                _notify("generating", "视频生成中", pct, video_id)
            if asyncio.get_event_loop().time() > deadline:
                raise RuntimeError(f"Sora 视频轮询超时 video_id={video_id}")
            await asyncio.sleep(_POLL_INTERVAL)

        status = str(getattr(v, "status", "") or "").strip().lower()
        if status != "completed":
            raise VideoGenerationRemoteTerminalError(
                provider_task_id=video_id,
                terminal_status=status,
            )
        _notify("downloading", "正在下载生成视频", 90, video_id)

        await raise_if_video_cancel_requested(
            cancel_requested,
            video_id,
            self.cancel,
        )

        async def _check_download_cancellation() -> None:
            await raise_if_video_cancel_requested(
                cancel_requested,
                video_id,
                self.cancel,
            )

        written = await self._download_to_file(
            video_id,
            path,
            cancellation_probe=_check_download_cancellation,
        )
        await raise_if_video_cancel_requested(
            cancel_requested,
            video_id,
            self.cancel,
        )
        return written

    async def _download_to_file(
        self,
        video_id: str,
        path: str | Path,
        *,
        cancellation_probe: CancellationCallback | None = None,
    ) -> int:
        """经共享安全下载器请求 Sora 内容端点，禁止 SDK 自动重定向。"""
        from app.providers._url_safety import safe_download_to_file

        return await safe_download_to_file(
            self._build_download_url(video_id),
            path,
            max_bytes=MAX_PROVIDER_VIDEO_BYTES,
            timeout=120.0,
            extra_headers={"Authorization": f"Bearer {self._api_key()}"},
            url_validator=self._validate_download_host,
            cancellation_probe=cancellation_probe,
        )

    def _build_download_url(self, video_id: str) -> str:
        """按 SDK 的资源路径构造内容 URL，并转义不可信任务 ID。"""
        value = video_id.strip()
        if not value:
            raise RuntimeError("provider_download_url_invalid")
        return f"{self._api_base_url()}/videos/{quote(value, safe='')}/content"

    def _validate_download_host(self, url: str) -> None:
        """每一跳只允许配置的 OpenAI API HTTPS:443 精确源。"""
        try:
            parsed = urlsplit(url)
            configured = urlsplit(self._api_base_url())
            port = parsed.port or 443
            configured_port = configured.port or 443
        except ValueError as exc:
            raise RuntimeError("provider_download_url_invalid") from exc
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or port != 443
            or configured.scheme.lower() != "https"
            or not configured.hostname
            or configured_port != 443
            or parsed.hostname.lower() != configured.hostname.lower()
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise RuntimeError("provider_download_url_invalid")

    @staticmethod
    def _api_base_url() -> str:
        configured_value = _explicit_api_base_url() or _OPENAI_API_BASE_URL
        try:
            parsed = urlsplit(configured_value)
            port = parsed.port or 443
        except ValueError as exc:
            raise RuntimeError("provider_download_url_invalid") from exc
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or port != 443
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise RuntimeError("provider_download_url_invalid")
        return urlunsplit(("https", parsed.netloc, parsed.path.rstrip("/"), "", ""))

    async def _store_video_file(
        self,
        path: str | Path,
        video_id: str,
        job_id: str | None,
        metadata: dict,
    ) -> str:
        from app.providers.storage import get_storage
        from app.providers.streaming_io import atomic_put_file, storage_attempt_suffix

        storage = get_storage()
        ident = job_id or video_id or "unknown"
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

def _legacy_progress(on_progress):
    def _wrap(progress: VideoGenerationProgress) -> None:
        if on_progress is not None:
            on_progress(progress.stage, progress.message, progress.progress)

    return _wrap
