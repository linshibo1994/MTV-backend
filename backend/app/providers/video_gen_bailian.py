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
    unsupported_video_cancellation,
)

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 15.0  # 官方建议 15s
_POLL_TIMEOUT = 600  # 10 分钟
_BAILIAN_RESULT_DOWNLOAD_HOSTS = frozenset(
    {
        "dashscope-result.oss-cn-hangzhou.aliyuncs.com",
    }
)

# 图生视频模型前缀（input 用 img_url）；其余视为文生视频（input 用 prompt）
_I2V_PREFIXES = ("wanx2.1-i2v", "wan2.2-i2v", "wan2.5-i2v", "wan2.6-i2v", "wanx2.1-kf2v")


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
        self._check_keys()
        use_model = request.model or settings.video_gen_model or "wanx2.1-t2v-turbo"

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
                        f"{base_url}/api/v1/services/aigc/video-generation/video-synthesis",
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
                        raise VideoGenerationSubmitRejected()
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
                base_url,
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
        """构造文生/图生视频请求体；图生视频时预编码首帧/参考图为 data URL。"""
        input_obj: dict = {"prompt": (request.prompt or "")[:800]}
        is_i2v = model.lower().startswith(_I2V_PREFIXES)
        img_storage = None
        if is_i2v:
            img_storage = request.first_frame or (
                request.reference_images[0].get("storage_key")
                if request.reference_images
                else None
            )
            if img_storage:
                _notify("encoding", "正在编码参考图", 12)
                input_obj["img_url"] = await self._encode_single(img_storage)
            elif not request.prompt:
                raise RuntimeError("图生视频需提供参考图（reference_images 或 first_frame）")
        parameters: dict = {
            "prompt_extend": True,
            "watermark": False,
        }
        if request.duration_seconds and request.duration_seconds > 0:
            # wanx2.1 固定 5 秒，仅记录期望值；新模型支持 2-15s
            parameters["duration"] = request.duration_seconds
        return {"model": model, "input": input_obj, "parameters": parameters}

    async def _encode_single(self, storage_key: str) -> str:
        """读取首帧/参考图并编码为 data URL（百炼 img_url 支持 data:image base64）。"""
        from app.providers.storage import get_storage
        from app.providers.streaming_io import read_storage_bytes_limited

        storage = get_storage()
        data = await read_storage_bytes_limited(
            storage,
            storage_key,
            too_large_error="reference_image_too_large",
        )
        b64 = base64.b64encode(data).decode("ascii")
        return f"data:image/png;base64,{b64}"

    async def _poll(
        self,
        http: httpx.AsyncClient,
        base_url: str,
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
                f"{base_url}/api/v1/tasks/{task_id}", headers=poll_headers
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
            return settings.video_gen_base_url.rstrip("/")
        return settings.dashscope_base_url.rstrip("/")

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
