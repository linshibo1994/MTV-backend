"""EntPilot Seedance 视频生成供应商实现。

EntPilot 提供与 Seedance 2.0/2.5 对应的异步 HTTP 接口：

* ``POST /v1/videos`` 创建任务；
* ``GET /v1/videos/{task_id}`` 查询任务；
* 完成后从响应中的 ``video_url``（或兼容字段）下载结果。

请求体使用文档规定的 ``content[]`` 多模态格式。供应商没有公开远端取消
接口，因此取消操作显式返回 ``UNSUPPORTED``，由上层保留取消债务供审计。
"""

from __future__ import annotations

import asyncio
import base64
import logging
from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import quote, urlsplit

import httpx
from billiard.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded

from app.core.config import settings
from app.core.reference_media_limits import (
    ReferenceMediaCounts,
    ReferenceMediaLimitExceeded,
    effective_reference_media_limits_for_group,
    reference_media_limits_for_provider,
    validate_reference_media_counts,
)
from app.providers.registry import ProviderConfigurationError
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
    sanitize_video_submit_rejection_diagnostics,
    unsupported_video_cancellation,
)

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 5.0
_POLL_TIMEOUT = 600.0
_REQUEST_TIMEOUT = 60.0
_DOWNLOAD_TIMEOUT = 120.0
_REFERENCE_URL_EXPIRY_SECONDS = 900
_DEFAULT_BASE_URL = "https://ka.ent-pilot.com"
_DEFAULT_MODEL = "seedance-2.0-720p"
_DEFAULT_DURATION = 5
_MIN_DURATION = 4
_MAX_DURATION_20 = 15
_MAX_DURATION_25 = 30
_ALLOWED_RESOLUTIONS = frozenset({"480p", "720p", "1080p", "4k"})
_REGULAR_RATIOS = frozenset({"16:9", "4:3", "1:1", "3:4", "9:16", "21:9"})
_SPECIAL_TASK_TYPES = frozenset({"edit", "extend"})
_ALLOWED_IMAGE_MIME_TYPES = frozenset(
    {
        "image/jpeg",
        "image/png",
        "image/webp",
        "image/bmp",
        "image/tiff",
        "image/gif",
    }
)
_KNOWN_MODELS = frozenset(
    {
        "seedance-2.0-1080p",
        "seedance-2.0-480p",
        "seedance-2.0-4k",
        "seedance-2.0-720p",
        "seedance-2.0-fast-480p",
        "seedance-2.0-fast-720p",
        "seedance-2.5-1080p",
        "seedance-2.5-480p",
        "seedance-2.5-720p",
    }
)
_IN_PROGRESS_STATUSES = frozenset(
    {"queued", "processing", "in_process", "running", "pending", "submitted", "generating"}
)
_SUCCESS_STATUSES = frozenset({"completed", "succeeded", "success"})
_FAILED_STATUSES = frozenset({"failed", "error", "cancelled", "canceled", "expired"})


class EntPilotVideoGenProvider:
    """通过 EntPilot Seedance 2.0/2.5 API 生成视频。"""

    def __init__(self, *, provider_key: str = "entpilot") -> None:
        self._provider_key = provider_key

    @property
    def provider_key(self) -> str:
        return self._provider_key

    async def cancel(self, provider_task_id: str) -> VideoCancellationResult:
        """文档未提供远端取消接口，返回显式不支持结果。"""

        return unsupported_video_cancellation(self.provider_key, provider_task_id)

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
        """提交或恢复异步任务，下载并原子保存视频产物。"""

        use_model = (request.model or self._configured_model()).strip()
        family = self._model_family(use_model)
        existing_task_id = str((request.metadata or {}).get("provider_task_id") or "").strip()
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
            except Exception:  # noqa: BLE001 - 进度回调不得中断生成
                logger.warning("EntPilot 视频生成进度回调失败", exc_info=True)

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
                body, sensitive_values = await self._build_request_body(
                    request,
                    use_model,
                    family=family,
                )
                await raise_if_video_cancel_requested_before_submit(request.cancel_requested)
                _notify("submitting", "正在提交视频生成任务", 10)

                async def _submit_video() -> httpx.Response:
                    return await http.post(
                        self._videos_url(),
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
                payload = self._response_object_or_none(response)
                if payload is None:
                    await raise_video_submission_uncertain(request.on_submission_uncertain)
                task_id = self._extract_task_id(payload)
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

        video_url = self._extract_video_url(result_payload)
        if not video_url:
            raise RuntimeError("entpilot_video_result_missing")
        with TemporaryDirectory(prefix="mtv-entpilot-video-") as temp_dir:
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
                video_url,
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
        metrics: dict[str, Any] = {
            "provider_key": self.provider_key,
            "task_id": task_id,
            "entpilot_status": "completed",
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
        *,
        family: str | None = None,
    ) -> tuple[dict[str, Any], tuple[str, ...]]:
        """按 Seedance 2.0/2.5 文档构造 content[] 请求体。"""

        prompt = request.prompt.strip()
        if not prompt:
            raise VideoGenerationSubmitRejected(diagnostics={"code": "PromptRequired"})
        model_family = family or self._model_family(model)
        metadata = request.metadata or {}
        task_type = metadata.get("omni_reference_task_type")
        if task_type is not None:
            if not isinstance(task_type, str) or task_type not in _SPECIAL_TASK_TYPES:
                raise VideoGenerationSubmitRejected(diagnostics={"code": "InvalidTaskType"})
            self._validate_special_task(request, model_family=model_family, task_type=task_type)
        reference_images = [
            reference
            for reference in request.reference_images
            if not request.first_frame
            or not isinstance(reference, Mapping)
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
                    reference_media_limits_for_provider("entpilot", model),
                    reserve_first_frame=bool(request.first_frame),
                ),
            )
        except ReferenceMediaLimitExceeded as exc:
            raise VideoGenerationSubmitRejected(diagnostics={"code": exc.code}) from None

        content: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
        sensitive_values: list[str] = [prompt]
        if request.first_frame:
            encoded = await self._encode_storage_image(
                request.first_frame,
                self._first_frame_mime_type(request),
            )
            content.append(
                {
                    "type": "image_url",
                    "role": "first_frame",
                    "image_url": {"url": encoded},
                }
            )
            sensitive_values.append(encoded)
        else:
            for reference in reference_images:
                storage_key, mime_type = self._storage_image_reference(reference)
                encoded = await self._encode_storage_image(storage_key, mime_type)
                item: dict[str, Any] = {
                    "type": "image_url",
                    "role": "reference_image",
                    "image_url": {"url": encoded},
                }
                subject_type = reference.get("subject_type") if isinstance(reference, Mapping) else None
                if isinstance(subject_type, str) and subject_type.strip():
                    item["subject_type"] = subject_type.strip()
                content.append(item)
                sensitive_values.append(encoded)

            for reference in request.reference_videos:
                video_url = await self._external_media_url(reference, media_name="参考视频")
                content.append(
                    {
                        "type": "video_url",
                        "role": "reference_video",
                        "video_url": {"url": video_url},
                    }
                )
                sensitive_values.append(video_url)

            for reference in request.reference_audios:
                audio_url = await self._external_media_url(reference, media_name="参考音频")
                content.append(
                    {
                        "type": "audio_url",
                        "role": "reference_audio",
                        "audio_url": {"url": audio_url},
                    }
                )
                sensitive_values.append(audio_url)

        duration = self._duration(
            request.duration_seconds,
            model_family=model_family,
            task_type=task_type,
        )
        ratio = self._ratio(
            request,
            model_family=model_family,
            has_first_frame=bool(request.first_frame),
            task_type=task_type,
        )
        body: dict[str, Any] = {
            "model": model,
            "duration": duration,
            "generate_audio": self._generate_audio(request),
            "ratio": ratio,
            # 文档保留 prompt 作为 NewAPI 兼容字段；content 中的 text 才是主值。
            "prompt": prompt,
            "content": content,
        }
        # 2.0 文档要求显式 resolution；2.5 的分辨率由模型名决定。
        if model_family == "2.0":
            body["resolution"] = self._resolution_from_model(model)
        seed = metadata.get("seed")
        if seed is not None:
            if (
                not isinstance(seed, int)
                or isinstance(seed, bool)
                or not -1 <= seed <= 2_147_483_647
            ):
                raise VideoGenerationSubmitRejected(diagnostics={"code": "SeedOutOfRange"})
            body["seed"] = seed
        if task_type is not None:
            body["omni_reference_task_type"] = task_type
            # 文档示例要求特殊 2.5 任务显式携带由模型名推导的分辨率。
            body["resolution"] = self._resolution_from_model(model)
        return body, tuple(sensitive_values)

    async def _poll(
        self,
        http: httpx.AsyncClient,
        task_id: str,
        _notify,
        *,
        cancel_requested: CancellationProbe | None = None,
    ) -> dict[str, Any]:
        """轮询任务至明确终态，并返回完成响应。"""

        deadline = asyncio.get_running_loop().time() + _POLL_TIMEOUT
        started_at = asyncio.get_running_loop().time()
        last_progress = 20
        _notify("polling", "视频生成中，轮询任务状态", 20, task_id)
        while True:
            await raise_if_video_cancel_requested(cancel_requested, task_id, self.cancel)
            response = await http.get(self._task_url(task_id), headers=self._headers())
            await raise_if_video_cancel_requested(cancel_requested, task_id, self.cancel)
            if response.status_code == 404:
                raise RuntimeError("entpilot_video_task_not_found")
            if response.status_code != 200:
                raise RuntimeError("entpilot_video_poll_failed")
            payload = self._response_object(response)
            status = self._extract_status(payload)
            if status in _SUCCESS_STATUSES:
                if not self._extract_video_url(payload):
                    raise RuntimeError("entpilot_video_result_missing")
                return payload
            if status in _FAILED_STATUSES:
                raise VideoGenerationRemoteTerminalError(
                    provider_task_id=task_id,
                    terminal_status=status,
                )
            if status not in _IN_PROGRESS_STATUSES:
                raise RuntimeError("entpilot_video_task_status_unknown")
            now = asyncio.get_running_loop().time()
            if now > deadline:
                raise RuntimeError("entpilot_video_poll_timeout")
            progress_value = payload.get("progress")
            if isinstance(progress_value, int) and not isinstance(progress_value, bool):
                progress = max(20, min(89, progress_value))
            else:
                progress = min(89, 20 + int((now - started_at) / _POLL_TIMEOUT * 69))
            if progress > last_progress:
                last_progress = progress
                _notify("generating", "视频生成中", progress, task_id)
            await asyncio.sleep(_POLL_INTERVAL)

    async def _download_to_file(
        self,
        video_url: str,
        path: str | Path,
        *,
        cancellation_probe=None,
    ) -> int:
        """安全流式下载供应商返回的公网视频地址。"""

        from app.providers._url_safety import MAX_PROVIDER_VIDEO_BYTES, safe_download_to_file

        return await safe_download_to_file(
            video_url,
            path,
            max_bytes=MAX_PROVIDER_VIDEO_BYTES,
            timeout=_DOWNLOAD_TIMEOUT,
            cancellation_probe=cancellation_probe,
        )

    async def _encode_storage_image(self, storage_key: str, mime_type: str) -> str:
        """优先提供短期外部 URL，无法提供时使用受限大小 data URL。"""

        from app.providers.storage import get_storage
        from app.providers.streaming_io import read_storage_bytes_limited

        storage = get_storage()
        try:
            url = await storage.external_read_url(
                storage_key,
                expires_seconds=_REFERENCE_URL_EXPIRY_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001 - 本地存储可回退 data URL
            logger.warning(
                "EntPilot 参考图片外部 URL 生成失败，回退 data URL error_type=%s",
                type(exc).__name__,
            )
        else:
            if isinstance(url, str) and url.strip():
                try:
                    self._validate_external_reference_url(url.strip())
                except ProviderConfigurationError:
                    logger.warning("EntPilot 参考图片外部 URL 不安全，回退 data URL")
                else:
                    return url.strip()
        data = await read_storage_bytes_limited(
            storage,
            storage_key,
            too_large_error="reference_image_too_large",
        )
        return f"data:{mime_type};base64,{base64.b64encode(data).decode('ascii')}"

    async def _external_media_url(self, reference: object, *, media_name: str) -> str:
        """将私有存储媒体转换为供应商可读取的短期 HTTPS URL。"""

        from app.providers.storage import get_storage

        if not isinstance(reference, Mapping):
            raise ProviderConfigurationError(f"EntPilot {media_name}必须是 storage 引用")
        storage_key = reference.get("storage_key")
        if not isinstance(storage_key, str) or not storage_key.strip():
            raise ProviderConfigurationError(f"EntPilot {media_name}缺少 storage_key")
        storage = get_storage()
        try:
            url = await storage.external_read_url(
                storage_key.strip(),
                expires_seconds=_REFERENCE_URL_EXPIRY_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "EntPilot %s外部 URL 生成失败 error_type=%s",
                media_name,
                type(exc).__name__,
            )
            raise ProviderConfigurationError(f"EntPilot {media_name}外部 URL 生成失败") from None
        if not isinstance(url, str) or not url.strip():
            raise ProviderConfigurationError(f"当前存储后端无法为 EntPilot {media_name}提供外部 HTTPS URL")
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
        try:
            return await atomic_put_file(
                get_storage(),
                storage_key,
                path,
                attempt_id=attempt_suffix or "legacy",
                preserve_existing_final=not attempt_suffix,
            )
        except (
            asyncio.CancelledError,
            SoftTimeLimitExceeded,
            TimeLimitExceeded,
        ):
            raise
        except Exception as exc:
            from app.providers.video_gen import VideoGenerationDownloadError

            raise VideoGenerationDownloadError(f"store_failed:{type(exc).__name__}") from exc

    @staticmethod
    def _build_client(*, timeout: float) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
            transport=httpx.AsyncHTTPTransport(retries=0),
        )

    def _check_keys(self) -> None:
        if not self._api_key():
            raise ProviderConfigurationError(
                "未配置 VIDEO_GEN_API_KEY（或 ENTPILOT_API_KEY），无法调用 EntPilot Seedance 视频生成"
            )

    @staticmethod
    def _api_key() -> str:
        if (
            getattr(settings, "is_explicitly_configured", lambda _name: False)("video_gen_api_key")
            and getattr(settings, "video_gen_api_key", "")
        ):
            return str(settings.video_gen_api_key).strip()
        return str(getattr(settings, "entpilot_api_key", "") or "").strip()

    @classmethod
    def _configured_model(cls) -> str:
        if (
            getattr(settings, "is_explicitly_configured", lambda _name: False)("video_gen_model")
            and getattr(settings, "video_gen_model", "")
        ):
            return str(settings.video_gen_model).strip()
        return str(getattr(settings, "entpilot_video_model", _DEFAULT_MODEL) or _DEFAULT_MODEL).strip()

    @classmethod
    def _configured_base_url(cls) -> str:
        if (
            getattr(settings, "is_explicitly_configured", lambda _name: False)("video_gen_base_url")
            and getattr(settings, "video_gen_base_url", "")
        ):
            return str(settings.video_gen_base_url).strip()
        return str(getattr(settings, "entpilot_base_url", _DEFAULT_BASE_URL) or _DEFAULT_BASE_URL).strip()

    @classmethod
    def _api_origin(cls) -> str:
        value = cls._configured_base_url().rstrip("/")
        try:
            parsed = urlsplit(value)
            parsed.port
        except ValueError as exc:
            raise ProviderConfigurationError("ENTPILOT_BASE_URL 必须是合法的 HTTPS URL") from exc
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or parsed.port not in {None, 443}
            or parsed.path not in {"", "/"}
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ProviderConfigurationError("ENTPILOT_BASE_URL 必须使用不含凭据和查询参数的 HTTPS URL")
        return value

    @classmethod
    def _videos_url(cls) -> str:
        return f"{cls._api_origin()}/v1/videos"

    @classmethod
    def _task_url(cls, task_id: str) -> str:
        value = task_id.strip()
        if not value:
            raise RuntimeError("entpilot_video_task_id_invalid")
        return f"{cls._videos_url()}/{quote(value, safe='')}"

    @staticmethod
    def _headers() -> dict[str, str]:
        return {
            "Authorization": f"Bearer {EntpilotVideoGenProvider._api_key()}",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _model_family(model: str) -> str:
        normalized = model.strip().lower()
        if not normalized or normalized not in _KNOWN_MODELS:
            raise VideoGenerationSubmitRejected(diagnostics={"code": "UnsupportedModel"})
        # 模型清单与前端目录、供应商当前公开能力保持严格一致。
        return "2.5" if normalized.startswith("seedance-2.5-") else "2.0"

    @classmethod
    def _validate_special_task(
        cls,
        request: VideoGenerationRequest,
        *,
        model_family: str,
        task_type: str,
    ) -> None:
        """提前校验 Seedance 2.5 全模态编辑/延长任务的硬约束。"""

        if model_family != "2.5":
            raise VideoGenerationSubmitRejected(
                diagnostics={"code": "SpecialTaskRequiresSeedance25"}
            )
        if not request.reference_videos:
            raise VideoGenerationSubmitRejected(
                diagnostics={"code": f"{task_type.title()}RequiresReferenceVideo"}
            )
        if request.first_frame:
            raise VideoGenerationSubmitRejected(
                diagnostics={"code": "SpecialTaskCannotMixFirstFrame"}
            )

        metadata = request.metadata or {}
        ratio = metadata.get("ratio")
        if ratio is None:
            ratio = getattr(settings, "entpilot_ratio", "16:9") or "16:9"
        if str(ratio).strip().lower() != "adaptive":
            raise VideoGenerationSubmitRejected(
                diagnostics={"code": f"{task_type.title()}RatioMustBeAdaptive"}
            )

        # 请求模型当前没有单独的视频元数据字段；若调用方携带了时长，
        # 这里尽早校验，缺少时则交给上游按实际媒体时长继续判断。
        for reference in request.reference_videos:
            if not isinstance(reference, Mapping):
                continue
            for field_name in ("duration_seconds", "video_duration_seconds", "duration"):
                if field_name not in reference or reference[field_name] is None:
                    continue
                if not cls._valid_reference_video_duration(reference[field_name]):
                    raise VideoGenerationSubmitRejected(
                        diagnostics={
                            "code": f"{task_type.title()}ReferenceVideoDurationOutOfRange"
                        }
                    )

        declared_durations = metadata.get("reference_video_durations")
        if declared_durations is not None:
            values = (
                declared_durations
                if isinstance(declared_durations, (list, tuple))
                else [declared_durations]
            )
            if not values or not all(
                cls._valid_reference_video_duration(value) for value in values
            ):
                raise VideoGenerationSubmitRejected(
                    diagnostics={
                        "code": f"{task_type.title()}ReferenceVideoDurationOutOfRange"
                    }
                )

        if task_type == "edit" and request.duration_seconds != -1:
            raise VideoGenerationSubmitRejected(
                diagnostics={"code": "EditDurationMustBeMinusOne"}
            )
        if task_type == "extend" and request.duration_seconds is not None:
            duration = request.duration_seconds
            if (
                not isinstance(duration, int)
                or isinstance(duration, bool)
                or not _MIN_DURATION <= duration <= _MAX_DURATION_25
            ):
                raise VideoGenerationSubmitRejected(
                    diagnostics={"code": "ExtendDurationOutOfRange"}
                )

    @staticmethod
    def _valid_reference_video_duration(value: object) -> bool:
        """判断参考视频时长是否落在文档规定的 4-30 秒范围。"""

        if isinstance(value, bool):
            return False
        if not isinstance(value, (int, float, str)):
            return False
        try:
            numeric = float(str(value))
        except (OverflowError, ValueError):
            return False
        return (
            numeric == numeric
            and numeric not in {float("inf"), float("-inf")}
            and 4 <= numeric <= 30
        )

    @staticmethod
    def _duration(
        duration_seconds: int | None,
        *,
        model_family: str = "2.0",
        task_type: str | None = None,
    ) -> int:
        if task_type == "edit":
            if duration_seconds != -1:
                raise VideoGenerationSubmitRejected(
                    diagnostics={"code": "EditDurationMustBeMinusOne"}
                )
            return -1
        if task_type == "extend":
            if duration_seconds is None:
                return _DEFAULT_DURATION
            if (
                not isinstance(duration_seconds, int)
                or isinstance(duration_seconds, bool)
                or not _MIN_DURATION <= duration_seconds <= _MAX_DURATION_25
            ):
                raise VideoGenerationSubmitRejected(
                    diagnostics={"code": "ExtendDurationOutOfRange"}
                )
            return duration_seconds
        if task_type is not None and task_type not in _SPECIAL_TASK_TYPES:
            raise VideoGenerationSubmitRejected(diagnostics={"code": "InvalidTaskType"})
        if duration_seconds is None:
            return _DEFAULT_DURATION
        if (
            not isinstance(duration_seconds, int)
            or isinstance(duration_seconds, bool)
            or duration_seconds <= 0
        ):
            raise VideoGenerationSubmitRejected(diagnostics={"code": "DurationOutOfRange"})
        maximum = _MAX_DURATION_25 if model_family == "2.5" else _MAX_DURATION_20
        if model_family == "2.0":
            # SD2.0 文档明确要求越界时自动限制到有效范围。
            return max(_MIN_DURATION, min(maximum, duration_seconds))
        if not _MIN_DURATION <= duration_seconds <= maximum:
            raise VideoGenerationSubmitRejected(diagnostics={"code": "DurationOutOfRange"})
        return duration_seconds

    @classmethod
    def _resolution_from_model(cls, model: str) -> str:
        value = model.strip().lower().rsplit("-", 1)[-1]
        if value not in _ALLOWED_RESOLUTIONS:
            raise ProviderConfigurationError("Seedance 模型分辨率无效")
        return value

    @classmethod
    def _ratio(
        cls,
        request: VideoGenerationRequest,
        *,
        model_family: str,
        has_first_frame: bool,
        task_type: str | None = None,
    ) -> str:
        configured = (request.metadata or {}).get("ratio")
        if configured is None:
            configured = getattr(settings, "entpilot_ratio", "16:9") or "16:9"
        value = str(configured).strip().lower()
        if task_type in _SPECIAL_TASK_TYPES:
            if value != "adaptive":
                raise VideoGenerationSubmitRejected(
                    diagnostics={"code": f"{task_type.title()}RatioMustBeAdaptive"}
                )
            return value
        if has_first_frame and model_family == "2.5":
            # Seedance 2.5 文档要求首帧/首尾帧使用 auto。
            value = "auto"
        if value == "adaptive":
            raise VideoGenerationSubmitRejected(
                diagnostics={"code": "AdaptiveRatioRequiresSpecialTask"}
            )
        # ``auto`` 只用于 Seedance 2.5 首帧/首尾帧；2.0 及普通任务必须
        # 显式给出文档规定的画幅比例。
        if value not in _REGULAR_RATIOS and not (
            value == "auto" and has_first_frame and model_family == "2.5"
        ):
            raise ProviderConfigurationError("ENTPILOT_RATIO 配置无效")
        return value

    @staticmethod
    def _generate_audio(request: VideoGenerationRequest) -> bool:
        value = (request.metadata or {}).get("generate_audio")
        if value is None:
            value = getattr(settings, "entpilot_generate_audio", True)
        if not isinstance(value, bool):
            raise ProviderConfigurationError("ENTPILOT_GENERATE_AUDIO 必须是布尔值")
        return value

    @staticmethod
    def _storage_image_reference(reference: object) -> tuple[str, str]:
        if not isinstance(reference, Mapping):
            raise ProviderConfigurationError("EntPilot 参考图片必须是 storage 引用")
        storage_key = reference.get("storage_key")
        if not isinstance(storage_key, str) or not storage_key.strip():
            raise ProviderConfigurationError("EntPilot 参考图片缺少 storage_key")
        return storage_key.strip(), EntpilotVideoGenProvider._normalize_image_mime_type(
            reference.get("mime_type"), storage_key
        )

    @classmethod
    def _first_frame_mime_type(cls, request: VideoGenerationRequest) -> str:
        for reference in request.reference_images:
            if isinstance(reference, Mapping) and reference.get("storage_key") == request.first_frame:
                return cls._normalize_image_mime_type(reference.get("mime_type"), request.first_frame or "")
        return cls._normalize_image_mime_type(None, request.first_frame or "")

    @staticmethod
    def _normalize_image_mime_type(value: object, storage_key: str) -> str:
        mime_type = str(value or "").split(";", 1)[0].strip().lower()
        if mime_type == "image/jpg":
            mime_type = "image/jpeg"
        elif mime_type in {"image/x-ms-bmp", "image/x-bmp"}:
            mime_type = "image/bmp"
        elif mime_type in {"image/tif", "image/x-tiff"}:
            mime_type = "image/tiff"
        if not mime_type:
            mime_type = {
                ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg",
                ".webp": "image/webp",
            }.get(Path(storage_key).suffix.lower(), "image/png")
        if mime_type not in _ALLOWED_IMAGE_MIME_TYPES:
            raise ProviderConfigurationError(
                "EntPilot 仅支持 JPEG、PNG、WebP、BMP、TIFF 或 GIF 参考图片"
            )
        return mime_type

    @staticmethod
    def _response_object(response: httpx.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except Exception as exc:  # noqa: BLE001 - 不泄露上游正文
            raise RuntimeError("entpilot_video_response_invalid") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("entpilot_video_response_invalid")
        return payload

    @classmethod
    def _response_object_or_none(cls, response: httpx.Response) -> dict[str, Any] | None:
        try:
            payload = response.json()
        except Exception:  # noqa: BLE001 - 提交响应无法判权
            return None
        return payload if isinstance(payload, dict) else None

    @classmethod
    def _extract_task_id(cls, payload: Mapping[str, Any]) -> str:
        for key in ("task_id", "id"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        nested = payload.get("data")
        if isinstance(nested, Mapping):
            return cls._extract_task_id(nested)
        return ""

    @classmethod
    def _extract_status(cls, payload: Mapping[str, Any]) -> str:
        value = payload.get("status")
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
        nested = payload.get("data")
        if isinstance(nested, Mapping):
            return cls._extract_status(nested)
        return ""

    @classmethod
    def _extract_video_url(cls, payload: Mapping[str, Any]) -> str:
        """提取可下载的结果地址，优先使用绝对 HTTPS URL。

        部分 EntPilot 响应会在顶层保留相对占位地址，而把真实地址放在
        ``data`` 中。不能把相对地址直接交给下载器，否则会在任务完成后才
        失败；同时仅接受下载器支持的公网 HTTP(S) 地址。
        """

        candidates: list[str] = []

        def collect(value: object) -> None:
            if isinstance(value, Mapping):
                for key in ("video_url", "result_url", "url"):
                    candidate = value.get(key)
                    if isinstance(candidate, str) and candidate.strip():
                        candidates.append(candidate.strip())
                    elif isinstance(candidate, (Mapping, list, tuple)):
                        collect(candidate)
                # 兼容文档中的嵌套 data，以及部分网关包装的结果容器。
                # 不递归请求 content[]，其中可能是输入参考媒体的 URL。
                for key in ("data", "output_assets", "result"):
                    nested = value.get(key)
                    if isinstance(nested, (Mapping, list, tuple)):
                        collect(nested)
            elif isinstance(value, (list, tuple)):
                for item in value:
                    collect(item)

        collect(payload)

        # 结果下载器会再次执行 DNS/公网校验；这里先过滤明显不合法的候选，
        # 并把 HTTPS 排在 HTTP 前面，避免相对占位值遮蔽真实地址。
        from app.providers._url_safety import validate_public_http_url

        ranked: list[tuple[int, int, str]] = []
        for index, candidate in enumerate(candidates):
            try:
                parsed = urlsplit(candidate)
                if parsed.username is not None or parsed.password is not None or parsed.fragment:
                    continue
                validate_public_http_url(candidate)
            except (TypeError, ValueError):
                continue
            rank = 0 if parsed.scheme.lower() == "https" else 1
            ranked.append((rank, index, candidate))
        if ranked:
            ranked.sort(key=lambda item: (item[0], item[1]))
            return ranked[0][2]
        return ""

    @staticmethod
    def _submit_rejection_diagnostics(
        response: httpx.Response,
        *,
        sensitive_values: tuple[str, ...] = (),
    ) -> dict[str, int | str]:
        raw: dict[str, object] = {
            "status_code": response.status_code,
            "content_type": response.headers.get("content-type"),
        }
        try:
            payload = response.json()
        except Exception:  # noqa: BLE001
            payload = None
        if isinstance(payload, Mapping):
            sources: list[Mapping[str, Any]] = [payload]
            for name in ("error", "detail"):
                nested = payload.get(name)
                if isinstance(nested, Mapping):
                    sources.append(nested)
            for name in ("code", "param", "type", "request_id"):
                for source in sources:
                    if source.get(name) is not None:
                        raw[name] = source[name]
                        break
        if not raw.get("request_id"):
            for header_name in ("x-request-id", "request-id"):
                request_id = response.headers.get(header_name)
                if request_id:
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
            if (
                isinstance(field_value, int)
                and not isinstance(field_value, bool)
                and field_value >= 0
            ):
                usage[field_name] = field_value
        return usage

    @staticmethod
    def _validate_external_reference_url(url: str) -> None:
        from app.providers._url_safety import validate_public_http_url

        try:
            parsed = urlsplit(url)
            parsed.port
            validate_public_http_url(url)
        except (TypeError, ValueError):
            raise ProviderConfigurationError("EntPilot 参考媒体外部 URL 必须是绝对 HTTPS URL") from None
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ProviderConfigurationError("EntPilot 参考媒体外部 URL 必须是绝对 HTTPS URL")


# 保留旧的 Python 符号拼写，避免已经导入适配器的 worker/测试在升级时失效。
EntpilotVideoGenProvider = EntPilotVideoGenProvider

__all__ = ["EntPilotVideoGenProvider", "EntpilotVideoGenProvider"]
