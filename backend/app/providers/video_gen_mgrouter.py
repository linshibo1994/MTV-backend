"""MGRouter AICC Seedance 视频生成供应商实现。"""

from __future__ import annotations

import asyncio
import base64
import logging
from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from urllib.parse import quote, urlsplit, urlunsplit

import httpx
from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded

from app.core.config import settings
from app.providers.registry import ProviderConfigurationError
from app.providers.video_gen import (
    CancellationProbe,
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
    sanitize_video_submit_rejection_diagnostics,
)

if TYPE_CHECKING:
    from app.providers.storage import StorageProvider

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 5.0
_POLL_TIMEOUT = 600.0
_REQUEST_TIMEOUT = 60.0
_DOWNLOAD_TIMEOUT = 120.0
_CANCEL_TIMEOUT = 8.0
_DEFAULT_DURATION = 5
_MIN_DURATION = 4
_MAX_DURATION = 15
_ALLOWED_RESOLUTIONS = frozenset({"480p", "720p", "1080p"})
_ALLOWED_RATIOS = frozenset({"adaptive", "21:9", "16:9", "4:3", "1:1", "3:4", "9:16"})
_ALLOWED_IMAGE_MIME_TYPES = frozenset({"image/jpeg", "image/png", "image/webp"})
_MAX_REFERENCE_IMAGES = 9
_MAX_REFERENCE_AUDIOS = 3
_REFERENCE_URL_EXPIRY_SECONDS = 900


class MgrouterVideoGenProvider:
    """通过 MGRouter 的 AICC 火山兼容协议生成视频。"""

    def __init__(self, *, provider_key: str = "mgrouter") -> None:
        self._provider_key = provider_key

    @property
    def provider_key(self) -> str:
        return self._provider_key

    async def cancel(self, provider_task_id: str) -> VideoCancellationResult:
        """尽力取消排队中的远端任务；接口不支持取消已运行任务。"""
        task_id = provider_task_id.strip()
        if not task_id:
            return VideoCancellationResult(
                provider_task_id=provider_task_id,
                outcome=VideoCancellationOutcome.FAILED,
                detail="缺少 MGRouter 远端任务 ID",
            )

        result: VideoCancellationResult | None = None
        try:
            self._check_keys()
            http = self._build_client(timeout=_CANCEL_TIMEOUT)
            async with managed_video_client(
                http,
                http.aclose,
                provider_key=self.provider_key,
            ):
                response = await asyncio.wait_for(
                    http.delete(
                        self._task_url(task_id),
                        headers=self._headers(),
                    ),
                    timeout=_CANCEL_TIMEOUT,
                )
                if 200 <= response.status_code < 300 or response.status_code == 404:
                    result = VideoCancellationResult(
                        provider_task_id=provider_task_id,
                        outcome=VideoCancellationOutcome.CANCELED,
                        detail=("MGRouter 远端任务已不存在" if response.status_code == 404 else None),
                    )
                elif response.status_code == 409:
                    result = VideoCancellationResult(
                        provider_task_id=provider_task_id,
                        outcome=VideoCancellationOutcome.FAILED,
                        detail="MGRouter 远端任务已进入不可取消状态",
                    )
                else:
                    result = VideoCancellationResult(
                        provider_task_id=provider_task_id,
                        outcome=VideoCancellationOutcome.FAILED,
                        detail=f"MGRouter 远端取消失败 status={response.status_code}",
                    )
            return result
        except (asyncio.CancelledError, SoftTimeLimitExceeded, TimeLimitExceeded):
            raise
        except Exception as exc:  # noqa: BLE001 - 取消失败不能覆盖本地终态
            if result is not None:
                logger.warning(
                    "关闭 MGRouter 取消客户端失败 task_id=%s error_type=%s",
                    provider_task_id,
                    type(exc).__name__,
                )
                return result
            logger.warning(
                "取消 MGRouter 视频任务失败 task_id=%s error_type=%s",
                provider_task_id,
                type(exc).__name__,
            )
            return VideoCancellationResult(
                provider_task_id=provider_task_id,
                outcome=VideoCancellationOutcome.FAILED,
                detail=f"MGRouter 远端取消失败: {type(exc).__name__}",
            )

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
        result = await self.generate(
            request,
            _legacy_progress(on_progress) if on_progress is not None else None,
        )
        return result.video_storage_key

    async def generate(
        self,
        request: VideoGenerationRequest,
        on_progress=None,
    ) -> VideoGenerationResult:
        """提交或恢复远端任务，下载结果并返回对象存储键。"""
        existing_task_id = str((request.metadata or {}).get("provider_task_id") or "")
        if existing_task_id:
            await await_video_task_checkpoint(request.on_task_created, existing_task_id)
        else:
            await raise_if_video_cancel_requested_before_submit(request.cancel_requested)
        self._check_keys()

        def _notify(
            stage: str,
            message: str,
            progress: int,
            provider_task_id: str | None = None,
        ) -> None:
            if on_progress is None:
                return
            try:
                on_progress(
                    VideoGenerationProgress(
                        stage=stage,
                        message=message,
                        progress=progress,
                        provider_task_id=provider_task_id,
                    )
                )
            except Exception:  # noqa: BLE001 - 进度通知不能中断生成
                logger.warning("MGRouter 视频生成进度回调失败", exc_info=True)

        use_model = (request.model or self._configured_model()).strip()
        if not use_model:
            raise ProviderConfigurationError("MGRouter 视频模型不能为空")
        task_id = existing_task_id
        http = self._build_client(timeout=_REQUEST_TIMEOUT)
        async with managed_video_client(
            http,
            http.aclose,
            provider_key=self.provider_key,
        ):
            if task_id:
                await raise_if_video_cancel_requested(
                    request.cancel_requested,
                    task_id,
                    self.cancel,
                )
                _notify("resuming", "正在恢复视频生成任务", 15, task_id)
            else:
                body, sensitive_values = await self._build_request_body(request, use_model)
                await raise_if_video_cancel_requested_before_submit(request.cancel_requested)
                _notify("submitting", "正在提交视频生成任务", 10)

                async def _submit_video() -> httpx.Response:
                    return await http.post(
                        self._tasks_url(),
                        headers=self._headers(),
                        json=body,
                    )

                response = await execute_video_submission(
                    _submit_video,
                    before_submit=request.before_submit,
                    on_submission_uncertain=request.on_submission_uncertain,
                    rejection_sensitive_values=(self._api_key(), *sensitive_values),
                )
                if not 200 <= response.status_code < 300:
                    if 400 <= response.status_code < 500 and response.status_code != 408:
                        raise VideoGenerationSubmitRejected(
                            diagnostics=self._submit_rejection_diagnostics(
                                response,
                                sensitive_values=(self._api_key(), *sensitive_values),
                            )
                        )
                    await raise_video_submission_uncertain(request.on_submission_uncertain)
                try:
                    payload = response.json()
                except Exception:  # noqa: BLE001 - 提交响应无法判权时必须保持冻结
                    payload = None
                if not isinstance(payload, dict):
                    await raise_video_submission_uncertain(request.on_submission_uncertain)
                task_id = str(payload.get("id") or "").strip()
                if not task_id:
                    await raise_video_submission_uncertain(request.on_submission_uncertain)
                await await_video_task_checkpoint(request.on_task_created, task_id)
                await raise_if_video_cancel_requested(
                    request.cancel_requested,
                    task_id,
                    self.cancel,
                )
                _notify("submitted", "视频生成任务已提交", 15, task_id)

            result_payload = await self._poll(
                http,
                task_id,
                _notify,
                cancel_requested=request.cancel_requested,
            )

        with TemporaryDirectory(prefix="mtv-mgrouter-video-") as temp_dir:
            video_path = Path(temp_dir) / "video.mp4"

            async def _check_download_cancellation() -> None:
                await raise_if_video_cancel_requested(
                    request.cancel_requested,
                    task_id,
                    self.cancel,
                )

            await _check_download_cancellation()
            _notify("downloading", "正在下载生成视频", 90, task_id)
            await self._download_to_file(
                task_id,
                video_path,
                cancellation_probe=_check_download_cancellation,
            )
            await _check_download_cancellation()
            _notify("storing", "正在存储视频", 95, task_id)
            storage_key = await self._store_video_file(
                video_path,
                task_id,
                request.job_id,
                request.metadata,
            )

        _notify("succeeded", "视频生成完成", 100, task_id)
        metrics: dict = {
            "provider_key": self.provider_key,
            "task_id": task_id,
            "mgrouter_status": "succeeded",
        }
        usage = self._safe_usage(result_payload.get("usage"))
        if usage:
            metrics["usage"] = usage
        return VideoGenerationResult(
            video_storage_key=storage_key,
            provider_job_id=task_id,
            metrics=metrics,
        )

    async def _build_request_body(
        self,
        request: VideoGenerationRequest,
        model: str,
    ) -> tuple[dict, tuple[str, ...]]:
        prompt = request.prompt.strip()
        if not prompt:
            raise VideoGenerationSubmitRejected(diagnostics={"code": "PromptRequired"})
        if len(request.reference_audios) > _MAX_REFERENCE_AUDIOS:
            raise VideoGenerationSubmitRejected(diagnostics={"code": "TooManyReferenceAudios"})
        if request.reference_audios and (request.first_frame or not request.reference_images):
            raise VideoGenerationSubmitRejected(diagnostics={"code": "ReferenceAudioRequiresReferenceImage"})

        content: list[dict] = [{"type": "text", "text": prompt}]
        sensitive_values = [prompt]
        if request.first_frame:
            mime_type = self._first_frame_mime_type(request)
            encoded = await self._encode_storage_image(request.first_frame, mime_type)
            content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": encoded},
                    "role": "first_frame",
                }
            )
            sensitive_values.append(encoded)
        else:
            if len(request.reference_images) > _MAX_REFERENCE_IMAGES:
                raise VideoGenerationSubmitRejected(diagnostics={"code": "TooManyReferenceImages"})
            for reference in request.reference_images:
                storage_key, mime_type = self._storage_image_reference(reference)
                encoded = await self._encode_storage_image(storage_key, mime_type)
                content.append(
                    {
                        "type": "image_url",
                        "image_url": {"url": encoded},
                        "role": "reference_image",
                    }
                )
                sensitive_values.append(encoded)

            if request.reference_audios:
                from app.providers.storage import get_storage

                storage = get_storage()
                for reference in request.reference_audios:
                    audio_url = await self._external_audio_url(reference, storage)
                    content.append(
                        {
                            "type": "audio_url",
                            "audio_url": {"url": audio_url},
                            "role": "reference_audio",
                        }
                    )
                    sensitive_values.append(audio_url)

        body = {
            "model": model,
            "content": content,
            "duration": self._duration(request.duration_seconds),
            "resolution": self._resolution(),
            "ratio": self._ratio(),
            "generate_audio": self._boolean_setting("mgrouter_generate_audio"),
            "watermark": self._boolean_setting("mgrouter_watermark"),
        }
        return body, tuple(sensitive_values)

    async def _poll(
        self,
        http: httpx.AsyncClient,
        task_id: str,
        _notify,
        *,
        cancel_requested: CancellationProbe | None = None,
    ) -> dict:
        """轮询至远端终态，并返回成功响应。"""
        deadline = asyncio.get_running_loop().time() + _POLL_TIMEOUT
        started_at = asyncio.get_running_loop().time()
        last_progress = 20
        _notify("polling", "视频生成中，轮询任务状态", 20, task_id)
        while True:
            await raise_if_video_cancel_requested(
                cancel_requested,
                task_id,
                self.cancel,
            )
            response = await http.get(
                self._task_url(task_id),
                headers=self._headers(),
            )
            await raise_if_video_cancel_requested(
                cancel_requested,
                task_id,
                self.cancel,
            )
            if response.status_code == 404:
                raise RuntimeError("mgrouter_video_task_not_found")
            if response.status_code != 200:
                raise RuntimeError("mgrouter_video_poll_failed")
            payload = self._response_object(response)
            status = str(payload.get("status") or "").strip().lower()
            if status == "succeeded":
                content = payload.get("content")
                video_url = content.get("video_url") if isinstance(content, Mapping) else None
                if not isinstance(video_url, str) or not video_url.strip():
                    raise RuntimeError("mgrouter_video_result_missing")
                return payload
            if status in {"failed", "cancelled"}:
                raise VideoGenerationRemoteTerminalError(
                    provider_task_id=task_id,
                    terminal_status=status,
                )
            if status not in {"queued", "running"}:
                raise RuntimeError("mgrouter_video_task_status_unknown")

            now = asyncio.get_running_loop().time()
            if now > deadline:
                raise RuntimeError("mgrouter_video_poll_timeout")
            progress = min(89, 20 + int((now - started_at) / _POLL_TIMEOUT * 69))
            if progress > last_progress:
                last_progress = progress
                _notify("generating", "视频生成中", progress, task_id)
            await asyncio.sleep(_POLL_INTERVAL)

    async def _download_to_file(
        self,
        task_id: str,
        path: str | Path,
        *,
        cancellation_probe=None,
    ) -> int:
        """通过 MGRouter 固定鉴权代理安全地流式下载视频。"""
        from app.providers._url_safety import MAX_PROVIDER_VIDEO_BYTES, safe_download_to_file

        return await safe_download_to_file(
            self._download_url(task_id),
            path,
            max_bytes=MAX_PROVIDER_VIDEO_BYTES,
            timeout=_DOWNLOAD_TIMEOUT,
            extra_headers={"Authorization": f"Bearer {self._api_key()}"},
            url_validator=self._validate_download_origin,
            cancellation_probe=cancellation_probe,
        )

    async def _encode_storage_image(self, storage_key: str, mime_type: str) -> str:
        """优先给 MGRouter 提供短期 HTTPS URL，无法直读时回退为 data URL。"""

        from app.providers.storage import get_storage
        from app.providers.streaming_io import read_storage_bytes_limited

        storage = get_storage()
        try:
            url = await storage.external_read_url(
                storage_key,
                expires_seconds=_REFERENCE_URL_EXPIRY_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001 - 本地存储等后端可安全回退 data URL
            logger.warning(
                "MGRouter 参考图片外部读取 URL 生成失败，回退 data URL error_type=%s",
                type(exc).__name__,
            )
        else:
            if isinstance(url, str) and url.strip():
                value = url.strip()
                try:
                    self._validate_external_reference_url(value)
                except ProviderConfigurationError:
                    logger.warning("MGRouter 参考图片外部读取 URL 不安全，回退 data URL")
                else:
                    return value

        data = await read_storage_bytes_limited(
            storage,
            storage_key,
            too_large_error="reference_image_too_large",
        )
        return f"data:{mime_type};base64,{base64.b64encode(data).decode('ascii')}"

    async def _external_audio_url(
        self,
        reference: object,
        storage: StorageProvider,
    ) -> str:
        """把私有存储音频转换为供 AICC 拉取的短期 HTTPS URL。"""
        if not isinstance(reference, Mapping):
            raise ProviderConfigurationError("MGRouter 参考音频必须是 storage 引用")
        storage_key = reference.get("storage_key")
        if not isinstance(storage_key, str) or not storage_key:
            raise ProviderConfigurationError("MGRouter 参考音频缺少 storage_key")
        try:
            url = await storage.external_read_url(
                storage_key,
                expires_seconds=_REFERENCE_URL_EXPIRY_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001 - 存储实现错误统一转换为配置错误
            logger.warning(
                "MGRouter 参考音频外部读取 URL 生成失败 error_type=%s",
                type(exc).__name__,
            )
            raise ProviderConfigurationError("MGRouter 参考音频外部读取 URL 生成失败") from None
        if not isinstance(url, str) or not url.strip():
            raise ProviderConfigurationError("当前存储后端无法为 MGRouter 参考音频提供外部 HTTPS URL")
        value = url.strip()
        self._validate_external_reference_url(value)
        return value

    async def _store_video_file(
        self,
        path: str | Path,
        task_id: str,
        job_id: str | None,
        metadata: dict,
    ) -> str:
        from app.providers.storage import get_storage
        from app.providers.streaming_io import atomic_put_file, storage_attempt_suffix

        ident = job_id or task_id or "unknown"
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

    @staticmethod
    def _build_client(*, timeout: float) -> httpx.AsyncClient:
        # HTTPX 默认不重试；显式 transport 固定这一提交语义。
        return httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
            transport=httpx.AsyncHTTPTransport(retries=0),
        )

    def _check_keys(self) -> None:
        if not self._api_key():
            raise ProviderConfigurationError(
                "未配置 VIDEO_GEN_API_KEY（或 MGROUTER_API_KEY），无法调用 MGRouter 视频生成"
            )

    @staticmethod
    def _api_key() -> str:
        if settings.is_explicitly_configured("video_gen_api_key") and settings.video_gen_api_key:
            return settings.video_gen_api_key
        return str(getattr(settings, "mgrouter_api_key", "") or "")

    @staticmethod
    def _configured_model() -> str:
        if settings.is_explicitly_configured("video_gen_model") and settings.video_gen_model:
            return settings.video_gen_model
        return str(
            getattr(settings, "mgrouter_video_model", "AICC-Doubao-Seedance-2.0")
            or "AICC-Doubao-Seedance-2.0"
        )

    @classmethod
    def _configured_base_url(cls) -> str:
        if settings.is_explicitly_configured("video_gen_base_url") and settings.video_gen_base_url:
            return settings.video_gen_base_url
        return str(getattr(settings, "mgrouter_base_url", "https://mgrouter.com") or "")

    @classmethod
    def _api_origin(cls) -> str:
        value = cls._configured_base_url().strip()
        try:
            parsed = urlsplit(value)
            port = parsed.port or 443
        except ValueError as exc:
            raise ProviderConfigurationError("MGROUTER_BASE_URL 必须是合法的 HTTPS origin") from exc
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or port != 443
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path.rstrip("/") not in {"", "/api/v3"}
            or parsed.query
            or parsed.fragment
        ):
            raise ProviderConfigurationError("MGROUTER_BASE_URL 必须是 HTTPS:443 根 origin 或 /api/v3 基址")
        return urlunsplit(("https", parsed.netloc, "", "", ""))

    @classmethod
    def _tasks_url(cls) -> str:
        return f"{cls._api_origin()}/api/v3/contents/generations/tasks"

    @classmethod
    def _task_url(cls, task_id: str) -> str:
        value = task_id.strip()
        if not value:
            raise RuntimeError("mgrouter_video_task_id_invalid")
        return f"{cls._tasks_url()}/{quote(value, safe='')}"

    @classmethod
    def _download_url(cls, task_id: str) -> str:
        value = task_id.strip()
        if not value:
            raise RuntimeError("mgrouter_video_task_id_invalid")
        return f"{cls._api_origin()}/v1/videos/{quote(value, safe='')}/content"

    @classmethod
    def _validate_download_origin(cls, url: str) -> None:
        try:
            parsed = urlsplit(url)
            configured = urlsplit(cls._api_origin())
            port = parsed.port or 443
        except ValueError as exc:
            raise RuntimeError("provider_download_url_invalid") from exc
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or port != 443
            or parsed.username is not None
            or parsed.password is not None
            or parsed.hostname.lower() != (configured.hostname or "").lower()
        ):
            raise RuntimeError("provider_download_url_invalid")

    @staticmethod
    def _validate_external_reference_url(url: str) -> None:
        from app.providers._url_safety import validate_public_http_url

        try:
            parsed = urlsplit(url)
            port = parsed.port or 443
            validate_public_http_url(url)
        except (TypeError, ValueError) as exc:
            logger.warning(
                "MGRouter 参考音频外部读取 URL 校验失败 error_type=%s",
                type(exc).__name__,
            )
            raise ProviderConfigurationError("MGRouter 参考音频外部读取 URL 必须是绝对 HTTPS URL") from None
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or port != 443
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ProviderConfigurationError("MGRouter 参考音频外部读取 URL 必须是绝对 HTTPS URL")

    @staticmethod
    def _headers() -> dict[str, str]:
        return {
            "Authorization": f"Bearer {MgrouterVideoGenProvider._api_key()}",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _duration(duration_seconds: int | None) -> int:
        if duration_seconds is None:
            return _DEFAULT_DURATION
        if type(duration_seconds) is not int:
            raise VideoGenerationSubmitRejected(diagnostics={"code": "DurationOutOfRange"})
        if duration_seconds == 3:
            return _MIN_DURATION
        if not _MIN_DURATION <= duration_seconds <= _MAX_DURATION:
            raise VideoGenerationSubmitRejected(diagnostics={"code": "DurationOutOfRange"})
        return duration_seconds

    @staticmethod
    def _resolution() -> str:
        value = str(getattr(settings, "mgrouter_resolution", "720p") or "").strip().lower()
        if value not in _ALLOWED_RESOLUTIONS:
            raise ProviderConfigurationError("MGROUTER_RESOLUTION 配置无效")
        return value

    @staticmethod
    def _ratio() -> str:
        value = str(getattr(settings, "mgrouter_ratio", "adaptive") or "").strip().lower()
        if value not in _ALLOWED_RATIOS:
            raise ProviderConfigurationError("MGROUTER_RATIO 配置无效")
        return value

    @staticmethod
    def _boolean_setting(field_name: str) -> bool:
        value = getattr(settings, field_name, None)
        if type(value) is not bool:
            raise ProviderConfigurationError(f"{field_name.upper()} 必须是布尔值")
        return value

    @staticmethod
    def _storage_image_reference(reference: object) -> tuple[str, str]:
        if not isinstance(reference, Mapping):
            raise ProviderConfigurationError("MGRouter 参考图片必须是 storage 引用")
        storage_key = reference.get("storage_key")
        if not isinstance(storage_key, str) or not storage_key:
            raise ProviderConfigurationError("MGRouter 参考图片缺少 storage_key")
        mime_type = MgrouterVideoGenProvider._normalize_image_mime_type(
            reference.get("mime_type"),
            storage_key,
        )
        return storage_key, mime_type

    @staticmethod
    def _first_frame_mime_type(request: VideoGenerationRequest) -> str:
        for reference in request.reference_images:
            if isinstance(reference, Mapping) and reference.get("storage_key") == request.first_frame:
                return MgrouterVideoGenProvider._normalize_image_mime_type(
                    reference.get("mime_type"),
                    request.first_frame or "",
                )
        return MgrouterVideoGenProvider._normalize_image_mime_type(None, request.first_frame or "")

    @staticmethod
    def _normalize_image_mime_type(value: object, storage_key: str) -> str:
        mime_type = str(value or "").split(";", 1)[0].strip().lower()
        if mime_type == "image/jpg":
            mime_type = "image/jpeg"
        if not mime_type:
            suffix = Path(storage_key).suffix.lower()
            mime_type = {
                ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg",
                ".webp": "image/webp",
            }.get(suffix, "image/png")
        if mime_type not in _ALLOWED_IMAGE_MIME_TYPES:
            raise ProviderConfigurationError("MGRouter 仅支持 JPEG、PNG 或 WebP 参考图片")
        return mime_type

    @staticmethod
    def _response_object(response: httpx.Response) -> dict:
        try:
            payload = response.json()
        except Exception as exc:  # noqa: BLE001 - 上游响应不得泄漏到错误信息
            raise RuntimeError("mgrouter_video_response_invalid") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("mgrouter_video_response_invalid")
        return payload

    @staticmethod
    def _submit_rejection_diagnostics(
        response: httpx.Response,
        *,
        sensitive_values: tuple[str, ...] = (),
    ) -> dict[str, int | str]:
        """保留可定位 4xx 的白名单元数据，绝不读取响应正文。"""

        raw: dict[str, object] = {"status_code": response.status_code}
        content_type = response.headers.get("content-type")
        if isinstance(content_type, str):
            raw["content_type"] = content_type
        try:
            payload = response.json()
        except Exception:  # noqa: BLE001 - 诊断失败不改变明确拒绝分类
            payload = None
        if isinstance(payload, Mapping):
            diagnostic_sources: list[Mapping] = [payload]
            for field_name in ("error", "detail"):
                nested = payload.get(field_name)
                if isinstance(nested, Mapping):
                    diagnostic_sources.append(nested)
            for field_name in ("code", "param", "type", "request_id"):
                for source in diagnostic_sources:
                    value = source.get(field_name)
                    if value is not None:
                        raw[field_name] = value
                        break
        if not raw.get("request_id"):
            for header_name in ("x-request-id", "request-id"):
                request_id = response.headers.get(header_name)
                if isinstance(request_id, str) and request_id:
                    raw["request_id"] = request_id
                    break
        return sanitize_video_submit_rejection_diagnostics(
            raw,
            sensitive_values=sensitive_values,
        )

    @staticmethod
    def _safe_usage(value: object) -> dict[str, int]:
        if not isinstance(value, Mapping):
            return {}
        usage: dict[str, int] = {}
        for field_name in ("completion_tokens", "total_tokens"):
            field_value = value.get(field_name)
            if type(field_value) is int and field_value >= 0:
                usage[field_name] = field_value
        return usage


def _legacy_progress(on_progress):
    def _wrap(progress: VideoGenerationProgress) -> None:
        on_progress(progress.stage, progress.message, progress.progress)

    return _wrap
