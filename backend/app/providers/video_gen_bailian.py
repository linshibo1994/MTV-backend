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
from urllib.parse import urlsplit

import httpx

from app.core.config import settings
from app.providers.video_gen import (
    VideoGenerationProgress,
    VideoGenerationRequest,
    VideoGenerationResult,
)

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 15.0  # 官方建议 15s
_POLL_TIMEOUT = 600  # 10 分钟

# 图生视频模型前缀（input 用 img_url）；其余视为文生视频（input 用 prompt）
_I2V_PREFIXES = ("wanx2.1-i2v", "wan2.2-i2v", "wan2.5-i2v", "wan2.6-i2v", "wanx2.1-kf2v")


class BailianVideoGenProvider:
    """阿里百炼通义万相视频生成实现（httpx 直连，零额外依赖）。"""

    def __init__(self, *, provider_key: str = "bailian") -> None:
        self._provider_key = provider_key

    @property
    def provider_key(self) -> str:
        return self._provider_key

    def _check_keys(self) -> None:
        if not settings.dashscope_api_key:
            raise RuntimeError(
                "未配置 DASHSCOPE_API_KEY，无法调用通义万相视频生成"
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
        base_url = settings.dashscope_base_url.rstrip("/")
        api_key = settings.dashscope_api_key
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "X-DashScope-Async": "enable",
        }
        task_id = str((request.metadata or {}).get("provider_task_id") or "")

        async with httpx.AsyncClient(timeout=60.0) as http:
            if task_id:
                _notify("resuming", "正在恢复视频生成任务", 15, task_id)
            else:
                body = await self._build_body(request, use_model, _notify)
                submit = await http.post(
                    f"{base_url}/api/v1/services/aigc/video-generation/video-synthesis",
                    headers=headers,
                    json=body,
                )
                if submit.status_code != 200:
                    raise RuntimeError(
                        f"通义万相视频提交失败 status={submit.status_code}"
                    )
                task_id = str((submit.json().get("output") or {}).get("task_id") or "")
                if not task_id:
                    raise RuntimeError("通义万相视频提交未返回 task_id")
                _notify("submitted", "视频生成任务已提交", 15, task_id)

            video_url = await self._poll(http, base_url, api_key, task_id, _notify)
            _notify("downloading", "正在下载生成视频", 90, task_id)
            video_bytes = await self._download(video_url)

        _notify("storing", "正在存储视频", 95)
        storage_key = await self._store_video(video_bytes, task_id, request.job_id)
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

        storage = get_storage()
        try:
            data = await storage.get(storage_key)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("reference_read_failed") from exc
        if not data:
            raise RuntimeError("reference_read_failed")
        if len(data) > 50 * 1024 * 1024:
            raise RuntimeError("reference_image_too_large")
        b64 = base64.b64encode(data).decode("ascii")
        return f"data:image/png;base64,{b64}"

    async def _poll(
        self,
        http: httpx.AsyncClient,
        base_url: str,
        api_key: str,
        task_id: str,
        _notify,
    ) -> str:
        """轮询任务至终态，返回 video_url。"""
        poll_headers = {"Authorization": f"Bearer {api_key}"}
        deadline = asyncio.get_event_loop().time() + _POLL_TIMEOUT
        started_at = asyncio.get_event_loop().time()
        last_progress = 20
        _notify("polling", "视频生成中，轮询任务状态", 20, task_id)
        while True:
            resp = await http.get(
                f"{base_url}/api/v1/tasks/{task_id}", headers=poll_headers
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
            if status in ("FAILED", "UNKNOWN"):
                raise RuntimeError(f"通义万相视频任务失败 task_id={task_id} status={status}")
            elapsed = asyncio.get_event_loop().time() - started_at
            pct = min(90, 20 + int(elapsed / _POLL_TIMEOUT * 70))
            if pct > last_progress:
                last_progress = pct
                _notify("generating", "视频生成中", pct, task_id)
            if asyncio.get_event_loop().time() > deadline:
                raise RuntimeError(f"通义万相视频轮询超时 task_id={task_id}")
            await asyncio.sleep(_POLL_INTERVAL)

    async def _download(self, url: str) -> bytes:
        """下载视频 URL 字节流。"""
        from app.providers._url_safety import safe_download_bytes

        return await safe_download_bytes(
            url,
            max_bytes=1024 * 1024 * 1024,
            timeout=120.0,
            url_validator=self._validate_download_host,
        )

    @staticmethod
    def _validate_download_host(url: str) -> None:
        parsed = urlsplit(url)
        hostname = (parsed.hostname or "").lower()
        dashscope_host = (urlsplit(settings.dashscope_base_url).hostname or "").lower()
        if parsed.scheme != "https" or not (
            hostname == dashscope_host
            or hostname.endswith(".aliyuncs.com")
            or hostname.endswith(".aliyuncs.com.cn")
        ):
            raise RuntimeError("provider_download_url_invalid")

    async def _store_video(self, video_bytes: bytes, task_id: str, job_id: str | None) -> str:
        """将视频字节存入对象存储，返回 storage_key。"""
        from app.providers.storage import get_storage

        storage = get_storage()
        ident = job_id or task_id or "unknown"
        storage_key = f"generate_images/video/{ident}.mp4"
        await storage.put(storage_key, video_bytes)
        return storage_key


def _legacy_progress(on_progress):
    """把旧式 (stage, message, progress) 回调包装为 VideoGenerationProgress 调用。"""
    def _wrap(progress: VideoGenerationProgress) -> None:
        if on_progress is not None:
            on_progress(progress.stage, progress.message, progress.progress)

    return _wrap
