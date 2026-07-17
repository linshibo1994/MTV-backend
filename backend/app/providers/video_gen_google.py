"""Google Gemini Veo 视频生成供应商实现。

使用 ``google-genai`` 的异步 ``models.generate_videos`` 长任务接口：提交后通过
``operations.get`` 轮询，完成后下载首个生成视频并写入统一对象存储。
"""

from __future__ import annotations

import asyncio
import logging

from google import genai
from google.genai import types as gtypes

from app.core.config import settings
from app.providers.registry import ProviderConfigurationError, UnsupportedCapabilityError
from app.providers.video_gen import (
    VideoGenerationProgress,
    VideoGenerationRequest,
    VideoGenerationResult,
)

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 10.0
_POLL_TIMEOUT = 900
_MAX_VIDEO_BYTES = 1024 * 1024 * 1024


class GoogleVideoGenProvider:
    """Google Veo 文生/图生视频实现。"""

    def __init__(self, *, provider_key: str = "gemini") -> None:
        self._provider_key = provider_key

    @property
    def provider_key(self) -> str:
        return self._provider_key

    def _build_client(self) -> genai.Client:
        api_key = (
            settings.video_gen_api_key
            if settings.is_explicitly_configured("video_gen_api_key")
            and settings.video_gen_api_key
            else settings.gemini_api_key or settings.google_api_key or settings.video_gen_api_key
        )
        if not api_key:
            raise ProviderConfigurationError(
                "未配置 VIDEO_GEN_API_KEY（或 GEMINI_API_KEY/GOOGLE_API_KEY），"
                "无法调用 Gemini Veo 视频生成"
            )
        http_options = (
            gtypes.HttpOptions(base_url=settings.video_gen_base_url.rstrip("/"))
            if settings.is_explicitly_configured("video_gen_base_url")
            and settings.video_gen_base_url
            else None
        )
        return genai.Client(api_key=api_key, http_options=http_options)

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

    async def generate(self, request: VideoGenerationRequest, on_progress=None) -> VideoGenerationResult:
        """提交 Veo 长任务，轮询完成后保存 MP4。"""

        def _notify(stage: str, message: str, progress: int, task_id: str | None = None) -> None:
            if on_progress is None:
                return
            try:
                on_progress(
                    VideoGenerationProgress(
                        stage=stage,
                        message=message,
                        progress=progress,
                        provider_task_id=task_id,
                    )
                )
            except Exception:  # noqa: BLE001
                logger.warning("视频生成进度回调失败", exc_info=True)

        client = self._build_client()
        operation_name = str((request.metadata or {}).get("provider_task_id") or "")
        use_model = request.model or settings.video_gen_model or settings.google_video_model
        try:
            if operation_name:
                _notify("resuming", "正在恢复 Veo 视频生成任务", 15, operation_name)
                operation = gtypes.GenerateVideosOperation.model_construct(name=operation_name)
            else:
                _notify("submitting", "正在提交 Veo 视频生成任务", 10)
                image, reference_images = await self._load_input_media(request)
                config = self._build_config(request, reference_images)
                operation = await client.aio.models.generate_videos(
                    model=use_model,
                    source=gtypes.GenerateVideosSource(
                        prompt=request.prompt[:32000],
                        image=image,
                    ),
                    config=config,
                )
                operation_name = str(operation.name or "")
                if not operation_name:
                    raise RuntimeError("Gemini Veo 提交未返回 operation name")
                _notify("submitted", "Veo 视频生成任务已提交", 15, operation_name)

            operation = await self._poll(client, operation, _notify)
            video_bytes = await self._read_video(client, operation)
        finally:
            await client.aio.aclose()

        _notify("storing", "正在存储视频", 95, operation_name)
        storage_key = await self._store_video(video_bytes, operation_name, request.job_id)
        _notify("succeeded", "视频生成完成", 100, operation_name)
        return VideoGenerationResult(
            video_storage_key=storage_key,
            provider_job_id=operation_name,
            metrics={"provider_key": self.provider_key, "operation_name": operation_name},
        )

    @staticmethod
    def _build_config(
        request: VideoGenerationRequest,
        reference_images: list[gtypes.VideoGenerationReferenceImage] | None = None,
    ) -> gtypes.GenerateVideosConfig:
        kwargs: dict = {
            "number_of_videos": 1,
            "aspect_ratio": str((request.metadata or {}).get("aspect") or "16:9"),
        }
        if request.duration_seconds and request.duration_seconds > 0:
            kwargs["duration_seconds"] = request.duration_seconds
        if reference_images:
            kwargs["reference_images"] = reference_images
        return gtypes.GenerateVideosConfig(**kwargs)

    @staticmethod
    async def _load_input_media(
        request: VideoGenerationRequest,
    ) -> tuple[gtypes.Image | None, list[gtypes.VideoGenerationReferenceImage]]:
        if request.reference_audios:
            raise UnsupportedCapabilityError(
                "Gemini Veo 不支持参考音频输入；请移除 reference_audios 或选择支持该能力的厂商"
            )

        storage_key = request.first_frame
        mime_type = "image/png"
        remaining_references = list(request.reference_images)
        if not storage_key and request.reference_images:
            reference = request.reference_images[0]
            storage_key = reference.get("storage_key")
            mime_type = str(reference.get("mime_type") or mime_type)
            remaining_references = remaining_references[1:]
        elif storage_key:
            # first_frame 已作为输入首帧时，不再把同一素材重复作为 reference asset。
            remaining_references = [
                reference
                for reference in remaining_references
                if reference.get("storage_key") != storage_key
            ]

        if len(remaining_references) > 3:
            raise UnsupportedCapabilityError(
                "Gemini Veo 最多支持 3 张额外参考图，当前请求超出限制"
            )

        input_image = (
            await GoogleVideoGenProvider._load_storage_image(storage_key, mime_type)
            if storage_key
            else None
        )
        references: list[gtypes.VideoGenerationReferenceImage] = []
        for reference in remaining_references:
            reference_key = reference.get("storage_key")
            if not reference_key:
                continue
            image = await GoogleVideoGenProvider._load_storage_image(
                str(reference_key),
                str(reference.get("mime_type") or "image/png"),
            )
            references.append(
                gtypes.VideoGenerationReferenceImage(
                    image=image,
                    reference_type=gtypes.VideoGenerationReferenceType.ASSET,
                )
            )
        return input_image, references

    @staticmethod
    async def _load_storage_image(storage_key: str, mime_type: str) -> gtypes.Image:
        """从私有存储读取一张 Veo 输入图并执行体积限制。"""

        from app.providers.storage import get_storage

        try:
            data = await get_storage().get(storage_key)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("reference_read_failed") from exc
        if not data:
            raise RuntimeError("reference_read_failed")
        if len(data) > 50 * 1024 * 1024:
            raise RuntimeError("reference_image_too_large")
        return gtypes.Image(image_bytes=data, mime_type=mime_type)

    async def _poll(self, client: genai.Client, operation, _notify):
        deadline = asyncio.get_running_loop().time() + _POLL_TIMEOUT
        started_at = asyncio.get_running_loop().time()
        last_progress = 20
        _notify("polling", "Veo 视频生成中", 20, operation.name)
        while not operation.done:
            if asyncio.get_running_loop().time() > deadline:
                raise RuntimeError(f"Gemini Veo 轮询超时 operation={operation.name}")
            await asyncio.sleep(_POLL_INTERVAL)
            operation = await client.aio.operations.get(operation)
            elapsed = asyncio.get_running_loop().time() - started_at
            progress = min(90, 20 + int(elapsed / _POLL_TIMEOUT * 70))
            if progress > last_progress:
                last_progress = progress
                _notify("generating", "Veo 视频生成中", progress, operation.name)
        if operation.error:
            raise RuntimeError(f"Gemini Veo 生成失败 operation={operation.name}")
        return operation

    @staticmethod
    async def _read_video(client: genai.Client, operation) -> bytes:
        response = operation.result or operation.response
        generated = getattr(response, "generated_videos", None) if response else None
        if not generated or not generated[0].video:
            raise RuntimeError("Gemini Veo 完成但未返回视频")
        video = generated[0].video
        if video.video_bytes:
            data = video.video_bytes
        else:
            data = await client.aio.files.download(file=video)  # type: ignore[arg-type]
        if not data:
            raise RuntimeError("Gemini Veo 返回空视频")
        if len(data) > _MAX_VIDEO_BYTES:
            raise RuntimeError("video_download_too_large")
        return data

    @staticmethod
    async def _store_video(video_bytes: bytes, operation_name: str, job_id: str | None) -> str:
        from app.providers.storage import get_storage

        ident = job_id or operation_name.rsplit("/", 1)[-1] or "unknown"
        storage_key = f"generate_images/video/{ident}.mp4"
        await get_storage().put(storage_key, video_bytes)
        return storage_key


def _legacy_progress(on_progress):
    def _wrap(progress: VideoGenerationProgress) -> None:
        if on_progress is not None:
            on_progress(progress.stage, progress.message, progress.progress)

    return _wrap
