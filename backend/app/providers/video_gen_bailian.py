"""阿里百炼通义万相视频生成供应商实现 — DashScope 异步任务接口。

非 OpenAI 兼容协议，走原生 DashScope REST：
- 提交 POST {base_url}/api/v1/services/aigc/video-generation/video-synthesis
  请求头必带 X-DashScope-Async: enable
- 轮询 GET {base_url}/api/v1/tasks/{task_id}，状态 PENDING/RUNNING/SUCCEEDED/FAILED/UNKNOWN
- 成功后 output.video_url 为公网直链（24h 过期），下载后存入对象存储返回 storage_key

模型：
- 文生视频: wanx2.1-t2v-turbo / wanx2.1-t2v-plus
- 图生视频: wanx2.1-i2v-turbo / wanx2.1-i2v-plus

参考图/首帧以 storage_key 形式传入，提交前经 storage.get 读取并编码为
data URL（百炼 img_url 支持公网 URL / oss:// / data:image base64 三种）。
缺 DASHSCOPE_API_KEY 时抛 RuntimeError，由 worker 捕获后 release 积分。
"""

from __future__ import annotations

import asyncio
import base64
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

import httpx

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

_POLL_INTERVAL = 15.0  # 官方建议 15s
_POLL_TIMEOUT = 600  # 10 分钟
_BAILIAN_RESULT_DOWNLOAD_HOSTS = frozenset(
    {
        "dashscope-result.oss-cn-hangzhou.aliyuncs.com",
        "dashscope-result-sh.oss-accelerate.aliyuncs.com",
        # 万相 3.0 等新模型的结果落在北京/新加坡桶（官方文档 video_url 示例）。
        "dashscope-result-bj.oss-cn-beijing.aliyuncs.com",
        "dashscope-result-sg.oss-ap-southeast-1.aliyuncs.com",
    }
)

# 2.7 使用新版 media 协议，早期 I2V 模型使用 img_url 协议。
# 万相 3.0（wan3.0-video / wan3.0-video-prime 及后续变体）也使用 media
# 协议，但 reference_audio 与 first_frame/last_frame 官方声明互斥。
_WAN27_I2V_PREFIX = "wan2.7-i2v"
_WAN30_PREFIX = "wan3.0-video"
_WAN30_MAX_PROMPT_LENGTH = 20000
_LEGACY_I2V_PREFIXES = (
    "wanx2.1-i2v",
    "wan2.2-i2v",
    "wan2.5-i2v",
    "wan2.6-i2v",
    "wanx2.1-kf2v",
)
_WAN26_FLASH_PREFIX = "wan2.6-i2v-flash"
_WAN26_FLASH_DURATIONS = frozenset({5, 10})
_HAPPYHORSE_I2V_MODELS = frozenset({"happyhorse-1.0-i2v", "happyhorse-1.1-i2v"})
_HAPPYHORSE_I2V_MIN_DURATION = 3
_HAPPYHORSE_I2V_MAX_DURATION = 15
_HAPPYHORSE_MAX_CHINESE_PROMPT_LENGTH = 2500


class BailianVideoGenProvider:
    """阿里百炼通义万相视频生成实现（httpx 直连，零额外依赖）。"""

    def __init__(self, *, provider_key: str = "bailian") -> None:
        self._provider_key = provider_key

    @property
    def provider_key(self) -> str:
        return self._provider_key

    async def cancel(self, provider_task_id: str) -> VideoCancellationResult:
        """当前 DashScope 视频接口未接入可靠的运行中取消语义。"""
        return unsupported_video_cancellation(self.provider_key, provider_task_id)

    def _check_keys(self) -> None:
        if not self._api_key():
            raise ProviderConfigurationError(
                "未配置 VIDEO_GEN_API_KEY（或 DASHSCOPE_API_KEY），"
                "无法调用通义万相视频生成"
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
        task_id = str((request.metadata or {}).get("provider_task_id") or "")
        if task_id:
            await await_video_task_checkpoint(
                request.on_task_created,
                task_id,
            )
        else:
            await raise_if_video_cancel_requested_before_submit(
                request.cancel_requested
            )
        use_model = request.model or settings.video_gen_model or "wanx2.1-t2v-turbo"
        if not task_id:
            # 历史任务可能在统一限额上线前创建；首帧会通过独立字段提交，
            # 不能同时再按普通参考图重复计数。
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
                        reference_media_limits_for_provider("bailian", use_model),
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

        _notify("submitting", "正在提交视频生成任务", 10)
        base_url = self._base_url()
        api_root = self._api_root(base_url)
        api_key = self._api_key()
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "X-DashScope-Async": "enable",
        }
        http = httpx.AsyncClient(timeout=60.0)
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
                body = await self._build_body(request, use_model, _notify)
                await raise_if_video_cancel_requested_before_submit(
                    request.cancel_requested
                )
                async def _submit_video():
                    return await http.post(
                        f"{api_root}/services/aigc/video-generation/video-synthesis",
                        headers=headers,
                        json=body,
                    )

                submit = await execute_video_submission(
                    _submit_video,
                    before_submit=request.before_submit,
                    on_submission_uncertain=request.on_submission_uncertain,
                )
                if submit.status_code != 200:
                    if 400 <= submit.status_code < 500 and submit.status_code != 408:
                        raise VideoGenerationSubmitRejected(
                            diagnostics=self._submit_rejection_diagnostics(submit)
                        )
                    await raise_video_submission_uncertain(
                        request.on_submission_uncertain
                    )
                try:
                    task_id = str((submit.json().get("output") or {}).get("task_id") or "")
                except Exception:
                    await raise_video_submission_uncertain(
                        request.on_submission_uncertain
                    )
                if not task_id:
                    await raise_video_submission_uncertain(
                        request.on_submission_uncertain
                    )
                await await_video_task_checkpoint(
                    request.on_task_created,
                    task_id,
                )
                await raise_if_video_cancel_requested(
                    request.cancel_requested,
                    task_id,
                    self.cancel,
                )
                _notify("submitted", "视频生成任务已提交", 15, task_id)

            video_url = await self._poll(
                http,
                api_root,
                api_key,
                task_id,
                _notify,
                cancel_requested=request.cancel_requested,
            )

        with TemporaryDirectory(prefix="mtv-bailian-video-") as temp_dir:
            video_path = Path(temp_dir) / "video.mp4"
            await raise_if_video_cancel_requested(
                request.cancel_requested,
                task_id,
                self.cancel,
            )
            _notify("downloading", "正在下载生成视频", 90, task_id)
            await self._download_to_file(video_url, video_path)
            await raise_if_video_cancel_requested(
                request.cancel_requested,
                task_id,
                self.cancel,
            )
            _notify("storing", "正在存储视频", 95)
            storage_key = await self._store_video_file(
                video_path,
                task_id,
                request.job_id,
                request.metadata,
            )
        _notify("succeeded", "视频生成完成", 100)
        return VideoGenerationResult(
            video_storage_key=storage_key,
            provider_job_id=task_id,
            metrics={"provider_key": self.provider_key, "task_id": task_id},
        )

    async def _build_body(
        self,
        request: VideoGenerationRequest,
        model: str,
        _notify,
    ) -> dict:
        """按模型能力构造百炼文生/图生视频请求体。"""
        normalized_model = model.lower()
        prompt_limit = 800
        if normalized_model in _HAPPYHORSE_I2V_MODELS:
            prompt_limit = _HAPPYHORSE_MAX_CHINESE_PROMPT_LENGTH
        elif normalized_model.startswith(_WAN30_PREFIX):
            prompt_limit = _WAN30_MAX_PROMPT_LENGTH
        input_obj: dict = {"prompt": (request.prompt or "")[:prompt_limit]}
        if (
            normalized_model.startswith(_WAN26_FLASH_PREFIX)
            and request.duration_seconds not in _WAN26_FLASH_DURATIONS
        ):
            raise VideoGenerationSubmitRejected(
                diagnostics={"code": "Wan26DurationMustBe5Or10"}
            )
        if normalized_model in _HAPPYHORSE_I2V_MODELS and request.duration_seconds is not None and not (
            _HAPPYHORSE_I2V_MIN_DURATION
            <= request.duration_seconds
            <= _HAPPYHORSE_I2V_MAX_DURATION
        ):
            raise VideoGenerationSubmitRejected(
                diagnostics={"code": "HappyHorseDurationMustBe3To15"}
            )
        image_ref = self._first_image_ref(request)
        if normalized_model.startswith(_WAN30_PREFIX):
            # 万相 3.0 同样走 media 协议，但官方文档明确 reference_xx/file/link
            # 与 first_frame/last_frame 类型互斥，不能在同一请求中混用；因此
            # 存在首帧图时保守省略参考音频（wan2.7 的 driving_audio 是旧协议
            # 专属类型名，不能照搬）。无首帧图时音频可独立作为 reference_audio
            # 提交；两者皆无则纯文生（wan3.0 的 prompt 与 media 必填其一，
            # 无需强制要图）。
            media: list[dict] = []
            if image_ref is not None:
                _notify("encoding", "正在编码首帧参考图", 12)
                media.append(
                    {
                        "type": "first_frame",
                        "url": await self._encode_reference(image_ref, "image/png"),
                    }
                )
            elif request.reference_audios:
                media.append(
                    {
                        "type": "reference_audio",
                        "url": await self._encode_reference(
                            request.reference_audios[0], "audio/mpeg"
                        ),
                    }
                )
            if media:
                input_obj["media"] = media
        elif normalized_model.startswith(_WAN27_I2V_PREFIX):
            if image_ref is None:
                raise RuntimeError("Wan 2.7 图生视频需提供首帧参考图")
            _notify("encoding", "正在编码首帧参考图", 12)
            media = [
                {
                    "type": "first_frame",
                    "url": await self._encode_reference(image_ref, "image/png"),
                }
            ]
            audio_ref = request.reference_audios[0] if request.reference_audios else None
            if audio_ref is not None:
                media.append(
                    {
                        "type": "driving_audio",
                        "url": await self._encode_reference(audio_ref, "audio/mpeg"),
                    }
                )
            input_obj["media"] = media
        elif normalized_model.startswith(_LEGACY_I2V_PREFIXES):
            if image_ref is None:
                raise RuntimeError("图生视频需提供首帧参考图")
            _notify("encoding", "正在编码首帧参考图", 12)
            input_obj["img_url"] = await self._encode_reference(image_ref, "image/png")
            audio_ref = request.reference_audios[0] if request.reference_audios else None
            if audio_ref is not None and normalized_model.startswith(("wan2.5-i2v", "wan2.6-i2v")):
                input_obj["audio_url"] = await self._encode_reference(audio_ref, "audio/mpeg")
        elif normalized_model in _HAPPYHORSE_I2V_MODELS:
            if image_ref is None:
                raise RuntimeError("HappyHorse 图生视频需提供首帧参考图")
            _notify("encoding", "正在编码首帧参考图", 12)
            input_obj["media"] = [
                {
                    "type": "first_frame",
                    "url": await self._encode_reference(image_ref, "image/png"),
                }
            ]
            happyhorse_parameters: dict = {
                "resolution": "1080P",
                "watermark": False,
            }
            if request.duration_seconds is not None:
                happyhorse_parameters["duration"] = request.duration_seconds
            return {"model": model, "input": input_obj, "parameters": happyhorse_parameters}
        parameters: dict = {
            "prompt_extend": True,
            "watermark": False,
        }
        if normalized_model.startswith(_WAN26_FLASH_PREFIX):
            parameters["shot_type"] = "multi"
        if request.duration_seconds and request.duration_seconds > 0:
            if not normalized_model.startswith(("wanx2.1-", "wan2.2-")):
                parameters["duration"] = request.duration_seconds
        return {"model": model, "input": input_obj, "parameters": parameters}

    @staticmethod
    def _first_image_ref(request: VideoGenerationRequest) -> dict | None:
        if request.first_frame:
            return {"storage_key": request.first_frame, "mime_type": "image/png"}
        for reference in request.reference_images:
            if isinstance(reference, dict) and isinstance(reference.get("storage_key"), str):
                return reference
        return None

    async def _encode_reference(self, reference: dict, default_mime_type: str) -> str:
        """读取单个媒体参考并按其真实 MIME 类型编码为 data URL。"""
        storage_key = reference.get("storage_key")
        if not isinstance(storage_key, str) or not storage_key:
            raise RuntimeError("参考素材缺少 storage_key")
        mime_type = reference.get("mime_type")
        if not isinstance(mime_type, str) or "/" not in mime_type:
            mime_type = default_mime_type
        return await self._encode_single(storage_key, mime_type)

    async def _encode_single(self, storage_key: str, mime_type: str = "image/png") -> str:
        """读取媒体并编码为百炼接受的 data URL。"""
        from app.providers.storage import get_storage
        from app.providers.streaming_io import read_storage_bytes_limited

        storage = get_storage()
        data = await read_storage_bytes_limited(
            storage,
            storage_key,
            too_large_error="reference_image_too_large",
        )
        b64 = base64.b64encode(data).decode("ascii")
        return f"data:{mime_type};base64,{b64}"

    async def _poll(
        self,
        http: httpx.AsyncClient,
        api_root: str,
        api_key: str,
        task_id: str,
        _notify,
        *,
        cancel_requested: CancellationProbe | None = None,
    ) -> str:
        """轮询任务至终态，返回 video_url。"""
        poll_headers = {"Authorization": f"Bearer {api_key}"}
        deadline = asyncio.get_event_loop().time() + _POLL_TIMEOUT
        started_at = asyncio.get_event_loop().time()
        last_progress = 20
        _notify("polling", "视频生成中，轮询任务状态", 20, task_id)
        while True:
            await raise_if_video_cancel_requested(
                cancel_requested,
                task_id,
                self.cancel,
            )
            resp = await http.get(
                f"{api_root}/tasks/{task_id}", headers=poll_headers
            )
            await raise_if_video_cancel_requested(
                cancel_requested,
                task_id,
                self.cancel,
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"通义万相视频轮询失败 status={resp.status_code}"
                )
            data = resp.json().get("output") or {}
            status = (data.get("task_status") or "").upper()
            if status == "SUCCEEDED":
                url = data.get("video_url")
                if not url:
                    raise RuntimeError(f"通义万相视频完成但未返回 video_url task_id={task_id}")
                return str(url)
            if status in ("FAILED", "ERROR", "CANCELLED", "CANCELED"):
                raise VideoGenerationRemoteTerminalError(
                    provider_task_id=task_id,
                    terminal_status=status,
                )
            if status == "UNKNOWN":
                raise RuntimeError("bailian_video_task_status_unknown")
            elapsed = asyncio.get_event_loop().time() - started_at
            pct = min(90, 20 + int(elapsed / _POLL_TIMEOUT * 70))
            if pct > last_progress:
                last_progress = pct
                _notify("generating", "视频生成中", pct, task_id)
            if asyncio.get_event_loop().time() > deadline:
                raise RuntimeError(f"通义万相视频轮询超时 task_id={task_id}")
            await asyncio.sleep(_POLL_INTERVAL)

    async def _download_to_file(self, url: str, path: str | Path) -> int:
        """逐跳校验厂商 URL，并把视频流写入临时文件。"""
        from app.providers._url_safety import MAX_PROVIDER_VIDEO_BYTES, safe_download_to_file

        return await safe_download_to_file(
            url,
            path,
            max_bytes=MAX_PROVIDER_VIDEO_BYTES,
            timeout=120.0,
            url_validator=self._validate_download_host,
        )

    @staticmethod
    def _validate_download_host(url: str) -> None:
        try:
            parsed = urlsplit(url)
            configured = urlsplit(BailianVideoGenProvider._base_url())
            hostname = (parsed.hostname or "").lower()
            configured_host = (configured.hostname or "").lower()
            port = parsed.port or 443
            configured_port = configured.port or 443
        except ValueError as exc:
            raise RuntimeError("provider_download_url_invalid") from exc
        configured_origin = (
            configured.scheme.lower() == "https"
            and bool(configured_host)
            and configured_port == 443
            and configured.username is None
            and configured.password is None
            and hostname == configured_host
        )
        if (
            parsed.scheme.lower() != "https"
            or not hostname
            or port != 443
            or parsed.username is not None
            or parsed.password is not None
            or not (
                configured_origin
                or hostname in _BAILIAN_RESULT_DOWNLOAD_HOSTS
            )
        ):
            raise RuntimeError("provider_download_url_invalid")

    @staticmethod
    def _api_key() -> str:
        if (
            settings.is_explicitly_configured("video_gen_api_key")
            and settings.video_gen_api_key
        ):
            return settings.video_gen_api_key
        return settings.dashscope_api_key or settings.video_gen_api_key

    @staticmethod
    def _base_url() -> str:
        if (
            settings.is_explicitly_configured("video_gen_base_url")
            and settings.video_gen_base_url
        ):
            configured = settings.video_gen_base_url.rstrip("/")
            # 百炼 OpenAI 兼容地址只供 LLM 使用；视频必须走原生 DashScope REST。
            if configured.endswith("/compatible-mode/v1"):
                return configured.removesuffix("/compatible-mode/v1")
            return configured
        return settings.dashscope_base_url.rstrip("/")

    @staticmethod
    def _api_root(base_url: str) -> str:
        """兼容配置根域名与已包含 /api/v1 的百炼视频地址。"""
        normalized = base_url.rstrip("/")
        return normalized if normalized.endswith("/api/v1") else f"{normalized}/api/v1"

    @staticmethod
    def _submit_rejection_diagnostics(response: httpx.Response | object) -> dict[str, int | str]:
        """提取可公开的百炼 4xx 诊断，禁止回传原始 message/body。"""
        raw: dict[str, object] = {"status_code": getattr(response, "status_code", None)}
        try:
            payload = response.json()  # type: ignore[union-attr]
        except Exception:  # noqa: BLE001 - 诊断解析失败不影响明确拒绝分类
            payload = None
        if isinstance(payload, dict):
            for field_name in ("code", "param", "type", "request_id"):
                raw[field_name] = payload.get(field_name)
        return sanitize_video_submit_rejection_diagnostics(raw)

    async def _store_video_file(
        self,
        path: str | Path,
        task_id: str,
        job_id: str | None,
        metadata: dict,
    ) -> str:
        """将临时视频文件原子提升为本次尝试的正式对象。"""
        from app.providers.storage import get_storage
        from app.providers.streaming_io import atomic_put_file, storage_attempt_suffix

        storage = get_storage()
        ident = job_id or task_id or "unknown"
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
    """把旧式 (stage, message, progress) 回调包装为 VideoGenerationProgress 调用。"""
    def _wrap(progress: VideoGenerationProgress) -> None:
        if on_progress is not None:
            on_progress(progress.stage, progress.message, progress.progress)

    return _wrap
