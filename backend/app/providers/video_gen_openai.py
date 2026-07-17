"""OpenAI Sora 视频生成供应商实现 — openai SDK videos 资源。

走 openai.AsyncOpenAI 的 videos 接口（openai>=1.x 内置）：
- 提交 client.videos.create(model, prompt, size, seconds, input_reference?) -> job
- 轮询 client.videos.retrieve(id) 直到 status==completed/failed
- 下载 client.videos.download_content(id) -> 二进制 MP4
下载后存入对象存储返回 storage_key，不长期持有 OpenAI 下载链接。

模型：sora-2 / sora-2-pro；图生视频用 input_reference。
缺 OPENAI_API_KEY 时抛 RuntimeError，由 worker 捕获后 release 积分。
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any

from app.core.config import settings
from app.providers.video_gen import (
    VideoGenerationProgress,
    VideoGenerationRequest,
    VideoGenerationResult,
)

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 5.0
_POLL_TIMEOUT = 600  # 10 分钟

# Sora 横纵档位
_ASPECT_TO_SIZE: dict[str, str] = {
    "16:9": "1280x720",
    "9:16": "720x1280",
    "1:1": "1024x1024",
}


def _map_size(aspect: str) -> str:
    return _ASPECT_TO_SIZE.get(aspect or "", "1280x720")


class OpenAIVideoGenProvider:
    """OpenAI Sora 视频生成实现。"""

    def __init__(self, *, provider_key: str = "openai") -> None:
        self._provider_key = provider_key

    @property
    def provider_key(self) -> str:
        return self._provider_key

    def _check_keys(self) -> None:
        if not settings.openai_api_key:
            raise RuntimeError("未配置 OPENAI_API_KEY，无法调用 Sora 视频生成")

    def _build_client(self):
        from openai import AsyncOpenAI

        return AsyncOpenAI(api_key=settings.openai_api_key)

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
        self._check_keys()
        use_model = request.model or settings.video_gen_model or "sora-2"

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

        client = self._build_client()
        video_id = str((request.metadata or {}).get("provider_task_id") or "")
        try:
            if video_id:
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
                job = await client.videos.create(**create_kwargs)
                video_id = str(getattr(job, "id", None) or "")
                if not video_id:
                    raise RuntimeError("Sora 提交未返回 video id")
                _notify("submitted", "视频生成任务已提交", 15, video_id)

            video_bytes = await self._poll_and_download(client, video_id, _notify)
        finally:
            await client.close()

        _notify("storing", "正在存储视频", 95)
        storage_key = await self._store_video(video_bytes, video_id, request.job_id)
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

        try:
            data = await get_storage().get(storage_key)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("reference_read_failed") from exc
        if not data:
            raise RuntimeError("reference_read_failed")
        b64 = base64.b64encode(data).decode("ascii")
        return {"image_url": f"data:{mime_type};base64,{b64}"}

    async def _poll_and_download(self, client, video_id: str, _notify) -> bytes:
        """轮询 Sora 任务至终态并下载二进制 MP4。"""
        _notify("polling", "视频生成中，轮询任务状态", 20, video_id)
        deadline = asyncio.get_event_loop().time() + _POLL_TIMEOUT
        started_at = asyncio.get_event_loop().time()
        last_progress = 20
        while True:
            v = await client.videos.retrieve(video_id)
            status = getattr(v, "status", "")
            if status in ("completed", "failed", "cancelled"):
                break
            elapsed = asyncio.get_event_loop().time() - started_at
            pct = min(90, 20 + int(elapsed / _POLL_TIMEOUT * 70))
            if pct > last_progress:
                last_progress = pct
                _notify("generating", "视频生成中", pct, video_id)
            if asyncio.get_event_loop().time() > deadline:
                raise RuntimeError(f"Sora 视频轮询超时 video_id={video_id}")
            await asyncio.sleep(_POLL_INTERVAL)

        status = getattr(v, "status", "")
        if status != "completed":
            raise RuntimeError(f"Sora 视频生成失败 video_id={video_id} status={status}")
        _notify("downloading", "正在下载生成视频", 90, video_id)
        content = await client.videos.download_content(video_id)
        try:
            if hasattr(content, "aread"):
                data = await content.aread()
            elif hasattr(content, "read"):
                maybe_data = content.read()
                data = await maybe_data if inspect.isawaitable(maybe_data) else maybe_data
            else:
                data = bytes(content)
        finally:
            if hasattr(content, "aclose"):
                await content.aclose()
        if len(data) > 1024 * 1024 * 1024:
            raise RuntimeError("video_download_too_large")
        return data

    async def _store_video(self, video_bytes: bytes, video_id: str, job_id: str | None) -> str:
        from app.providers.storage import get_storage

        storage = get_storage()
        ident = job_id or video_id or "unknown"
        storage_key = f"generate_images/video/{ident}.mp4"
        await storage.put(storage_key, video_bytes)
        return storage_key


def _legacy_progress(on_progress):
    def _wrap(progress: VideoGenerationProgress) -> None:
        if on_progress is not None:
            on_progress(progress.stage, progress.message, progress.progress)

    return _wrap
