"""Google Gemini Veo 视频生成供应商实现。

使用 ``google-genai`` 的异步 ``models.generate_videos`` 长任务接口：提交后通过
``operations.get`` 轮询，完成后下载首个生成视频并写入统一对象存储。
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import unquote, urlencode, urlsplit, urlunsplit

from google import genai
from google.genai import types as gtypes

from app.core.config import settings
from app.providers._url_safety import MAX_PROVIDER_VIDEO_BYTES, safe_download_to_file
from app.providers.registry import ProviderConfigurationError, UnsupportedCapabilityError
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

_POLL_INTERVAL = 10.0
_POLL_TIMEOUT = 900
_MAX_INLINE_VIDEO_BYTES = 16 * 1024 * 1024
_GOOGLE_DOWNLOAD_ORIGIN = "https://generativelanguage.googleapis.com"
_GOOGLE_STORAGE_REDIRECT_HOSTS = frozenset({"storage.googleapis.com"})
_GOOGLE_FILE_PATH = re.compile(r"/v1beta/files/[0-9A-Za-z_-]+(?::download)?")


class GoogleVideoGenProvider:
    """Google Veo 文生/图生视频实现。"""

    def __init__(self, *, provider_key: str = "gemini") -> None:
        self._provider_key = provider_key

    @property
    def provider_key(self) -> str:
        return self._provider_key

    async def cancel(self, provider_task_id: str) -> VideoCancellationResult:
        """当前 google-genai 异步 operations 仅暴露 get，不支持可靠取消。"""
        return unsupported_video_cancellation(self.provider_key, provider_task_id)

    def _build_client(self) -> genai.Client:
        api_key = self._api_key()
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

        if request.reference_videos:
            raise VideoGenerationSubmitRejected(
                diagnostics={"code": "ReferenceVideoUnsupported"}
            )

        operation_name = str((request.metadata or {}).get("provider_task_id") or "")
        if not operation_name:
            await raise_if_video_cancel_requested_before_submit(
                request.cancel_requested
            )

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
        use_model = request.model or settings.video_gen_model or settings.google_video_model
        with TemporaryDirectory(prefix="mtv-google-video-") as temp_dir:
            video_path = Path(temp_dir) / "video.mp4"
            async with managed_video_client(
                client,
                client.aio.aclose,
                provider_key=self.provider_key,
            ):
                if operation_name:
                    await await_video_task_checkpoint(
                        request.on_task_created,
                        operation_name,
                    )
                    await raise_if_video_cancel_requested(
                        request.cancel_requested,
                        operation_name,
                        self.cancel,
                    )
                    _notify("resuming", "正在恢复 Veo 视频生成任务", 15, operation_name)
                    operation = gtypes.GenerateVideosOperation.model_construct(name=operation_name)
                else:
                    _notify("submitting", "正在提交 Veo 视频生成任务", 10)
                    image, reference_images = await self._load_input_media(request)
                    config = self._build_config(request, reference_images)
                    await raise_if_video_cancel_requested_before_submit(
                        request.cancel_requested
                    )
                    async def _submit_video():
                        return await client.aio.models.generate_videos(
                            model=use_model,
                            source=gtypes.GenerateVideosSource(
                                prompt=request.prompt[:32000],
                                image=image,
                            ),
                            config=config,
                        )

                    operation = await execute_video_submission(
                        _submit_video,
                        before_submit=request.before_submit,
                        on_submission_uncertain=request.on_submission_uncertain,
                    )
                    operation_name = str(operation.name or "")
                    if not operation_name:
                        await raise_video_submission_uncertain(
                            request.on_submission_uncertain
                        )
                    await await_video_task_checkpoint(
                        request.on_task_created,
                        operation_name,
                    )
                    await raise_if_video_cancel_requested(
                        request.cancel_requested,
                        operation_name,
                        self.cancel,
                    )
                    _notify("submitted", "Veo 视频生成任务已提交", 15, operation_name)

                operation = await self._poll(
                    client,
                    operation,
                    _notify,
                    cancel_requested=request.cancel_requested,
                )
                await raise_if_video_cancel_requested(
                    request.cancel_requested,
                    operation_name,
                    self.cancel,
                )
                _notify("downloading", "正在下载生成视频", 90, operation_name)
                await self._read_video_to_file(operation, video_path)
                await raise_if_video_cancel_requested(
                    request.cancel_requested,
                    operation_name,
                    self.cancel,
                )
            _notify("storing", "正在存储视频", 95, operation_name)
            storage_key = await self._store_video_file(
                video_path,
                operation_name,
                request.job_id,
                request.metadata,
            )
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
        from app.providers.streaming_io import read_storage_bytes_limited

        data = await read_storage_bytes_limited(
            get_storage(),
            storage_key,
            too_large_error="reference_image_too_large",
        )
        return gtypes.Image(image_bytes=data, mime_type=mime_type)

    async def _poll(
        self,
        client: genai.Client,
        operation,
        _notify,
        *,
        cancel_requested: CancellationProbe | None = None,
    ):
        deadline = asyncio.get_running_loop().time() + _POLL_TIMEOUT
        started_at = asyncio.get_running_loop().time()
        last_progress = 20
        _notify("polling", "Veo 视频生成中", 20, operation.name)
        while not operation.done:
            await raise_if_video_cancel_requested(
                cancel_requested,
                str(operation.name or ""),
                self.cancel,
            )
            if asyncio.get_running_loop().time() > deadline:
                raise RuntimeError(f"Gemini Veo 轮询超时 operation={operation.name}")
            await asyncio.sleep(_POLL_INTERVAL)
            operation = await client.aio.operations.get(operation)
            await raise_if_video_cancel_requested(
                cancel_requested,
                str(operation.name or ""),
                self.cancel,
            )
            elapsed = asyncio.get_running_loop().time() - started_at
            progress = min(90, 20 + int(elapsed / _POLL_TIMEOUT * 70))
            if progress > last_progress:
                last_progress = progress
                _notify("generating", "Veo 视频生成中", progress, operation.name)
        if operation.error:
            error = operation.error
            error_status = (
                error.get("status")
                if isinstance(error, dict)
                else getattr(error, "status", None)
            )
            error_code = (
                error.get("code")
                if isinstance(error, dict)
                else getattr(error, "code", None)
            )
            terminal_status = (
                "canceled"
                if str(error_status or "").strip().lower() in {"cancelled", "canceled"}
                or error_code == 1
                else "failed"
            )
            raise VideoGenerationRemoteTerminalError(
                provider_task_id=str(operation.name or ""),
                terminal_status=terminal_status,
            )
        return operation

    async def _read_video_to_file(self, operation, path: str | Path) -> int:
        """将 Veo 内联字节或认证下载 URI 流式写入临时文件。"""
        response = operation.result or operation.response
        generated = getattr(response, "generated_videos", None) if response else None
        if not generated or not generated[0].video:
            raise RuntimeError("Gemini Veo 完成但未返回视频")
        video = generated[0].video
        if video.video_bytes is not None:
            data = video.video_bytes
            if len(data) > _MAX_INLINE_VIDEO_BYTES:
                raise RuntimeError("video_inline_too_large")

            async def _inline_chunks():
                yield data

            from app.providers.streaming_io import async_chunks_to_file

            return await async_chunks_to_file(
                _inline_chunks(),
                path,
                max_bytes=_MAX_INLINE_VIDEO_BYTES,
                empty_error="Gemini Veo 返回空视频",
            )

        uri = str(getattr(video, "uri", None) or "")
        if not uri:
            raise RuntimeError("Gemini Veo 完成但未返回视频下载地址")
        download_url = self._build_download_url(uri)
        return await safe_download_to_file(
            download_url,
            path,
            max_bytes=MAX_PROVIDER_VIDEO_BYTES,
            timeout=120.0,
            extra_headers={"x-goog-api-key": self._api_key()},
            url_validator=self._validate_download_hop,
        )

    def _build_download_url(self, uri: str) -> str:
        """把 Veo 文件资源 URI 规范化为受信任的媒体下载端点。"""
        value = uri.strip()
        if not value:
            raise RuntimeError("provider_download_url_invalid")

        if "://" in value:
            parsed = urlsplit(value)
        else:
            relative = urlsplit(value)
            resource = relative.path.lstrip("/")
            if resource.startswith("v1beta/files/"):
                path = f"/{resource}"
            elif resource.startswith("files/"):
                path = f"/v1beta/{resource}"
            else:
                path = f"/v1beta/files/{resource}"
            parsed = urlsplit(f"{self._relative_download_origin()}{path}")

        self._validate_google_file_endpoint(urlunsplit(parsed))
        normalized_path = unquote(parsed.path)
        path = (
            normalized_path
            if normalized_path.endswith(":download")
            else f"{normalized_path}:download"
        )
        return urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                path,
                urlencode({"alt": "media"}),
                "",
            )
        )

    @staticmethod
    def _relative_download_origin() -> str:
        if (
            settings.is_explicitly_configured("video_gen_base_url")
            and settings.video_gen_base_url
        ):
            parsed = urlsplit(settings.video_gen_base_url)
            try:
                parsed.port
            except ValueError as exc:
                raise RuntimeError("provider_download_url_invalid") from exc
            if (
                parsed.scheme != "https"
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
            ):
                raise RuntimeError("provider_download_url_invalid")
            return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
        return _GOOGLE_DOWNLOAD_ORIGIN

    def _validate_google_file_endpoint(self, url: str) -> None:
        parsed = urlsplit(url)
        allowed_origins = {
            ("generativelanguage.googleapis.com", 443),
        }
        if (
            settings.is_explicitly_configured("video_gen_base_url")
            and settings.video_gen_base_url
        ):
            configured = urlsplit(settings.video_gen_base_url)
            if configured.scheme == "https" and configured.hostname:
                allowed_origins.add((configured.hostname.lower(), configured.port or 443))
        try:
            origin = ((parsed.hostname or "").lower(), parsed.port or 443)
        except ValueError as exc:
            raise RuntimeError("provider_download_url_invalid") from exc
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or origin not in allowed_origins
            or parsed.username is not None
            or parsed.password is not None
            or _GOOGLE_FILE_PATH.fullmatch(unquote(parsed.path)) is None
        ):
            raise RuntimeError("provider_download_url_invalid")

    def _validate_download_hop(self, url: str) -> None:
        parsed = urlsplit(url)
        try:
            port = parsed.port or 443
        except ValueError as exc:
            raise RuntimeError("provider_download_url_invalid") from exc
        if (
            parsed.scheme == "https"
            and (parsed.hostname or "").lower() in _GOOGLE_STORAGE_REDIRECT_HOSTS
            and port == 443
            and parsed.username is None
            and parsed.password is None
        ):
            return
        self._validate_google_file_endpoint(url)

    @staticmethod
    def _api_key() -> str:
        if (
            settings.is_explicitly_configured("video_gen_api_key")
            and settings.video_gen_api_key
        ):
            return settings.video_gen_api_key
        return settings.gemini_api_key or settings.google_api_key or settings.video_gen_api_key

    async def _store_video_file(
        self,
        path: str | Path,
        operation_name: str,
        job_id: str | None,
        metadata: dict,
    ) -> str:
        from app.providers.storage import get_storage
        from app.providers.streaming_io import atomic_put_file, storage_attempt_suffix

        ident = job_id or operation_name.rsplit("/", 1)[-1] or "unknown"
        attempt_suffix = storage_attempt_suffix(metadata)
        suffix = f"-{attempt_suffix}" if attempt_suffix else ""
        storage_key = f"generate_images/video/{ident}{suffix}.mp4"
        return await atomic_put_file(
            get_storage(),
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
