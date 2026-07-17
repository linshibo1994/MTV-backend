"""阿里百炼通义万相文生图 Provider — DashScope 异步任务接口。

非 OpenAI 兼容协议，走原生 DashScope REST：
- 提交 POST {base_url}/api/v1/services/aigc/text2image/image-synthesis
  请求头必带 X-DashScope-Async: enable（缺则报错 current user api does not support
  synchronous calls）
- 轮询 GET {base_url}/api/v1/tasks/{task_id}，状态 PENDING/RUNNING/SUCCEEDED/FAILED/UNKNOWN
- 成功后 results[].url 为 OSS 临时链接（24h 过期），立即下载为 bytes 返回
- size 格式用星号分隔（1024*1024），非 OpenAI 的 1024x1024

模型：wan2.1-t2i-plus / wan2.1-t2i-turbo / wanx-v1 / qwen-image 等。
缺 dashscope_api_key 时抛 RuntimeError，由 worker 捕获后 release 积分。
"""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlsplit

import httpx

from app.core.config import settings
from app.providers.image_gen import ImageResult

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 2.0
_POLL_TIMEOUT = 300  # 5 分钟

# aspect -> 通义万相 size（宽*高，星号分隔）
_ASPECT_TO_SIZE: dict[str, str] = {
    "16:9": "1280*720",
    "9:16": "720*1280",
    "1:1": "1024*1024",
    "4:3": "1024*768",
    "3:4": "768*1024",
    "3:2": "1536*1024",
    "2:3": "1024*1536",
}


def _map_size(size: str, aspect: str) -> str:
    """按 aspect 选取通义万相 size；aspect 缺失时默认 16:9 横版。"""
    if aspect and aspect in _ASPECT_TO_SIZE:
        return _ASPECT_TO_SIZE[aspect]
    return _ASPECT_TO_SIZE["16:9"]


class BailianImageGenProvider:
    """阿里百炼通义万相文生图实现（httpx 直连，零额外依赖）。"""

    async def generate(
        self, prompt: str, model: str, size: str, aspect: str, count: int = 1
    ) -> list[ImageResult]:
        api_key = settings.image_gen_api_key or settings.dashscope_api_key
        if not api_key:
            raise RuntimeError("未配置 DASHSCOPE_API_KEY，无法调用通义万相文生图")
        use_model = model or settings.image_gen_model or "wan2.1-t2i-plus"
        base_url = (settings.image_gen_base_url or settings.dashscope_base_url).rstrip("/")
        wanx_size = _map_size(size, aspect)
        n = max(count, 1)

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "X-DashScope-Async": "enable",
        }
        body = {
            "model": use_model,
            "input": {"prompt": prompt},
            "parameters": {"size": wanx_size, "n": n, "prompt_extend": True, "watermark": False},
        }

        async with httpx.AsyncClient(timeout=60.0) as http:
            submit = await http.post(
                f"{base_url}/api/v1/services/aigc/text2image/image-synthesis",
                headers=headers,
                json=body,
            )
            if submit.status_code != 200:
                raise RuntimeError(
                    f"通义万相文生图提交失败 status={submit.status_code}"
                )
            task_id = (submit.json().get("output") or {}).get("task_id")
            if not task_id:
                raise RuntimeError("通义万相文生图提交未返回 task_id")

            # 轮询
            urls = await self._poll(http, base_url, api_key, task_id)
            if len(urls) < n:
                logger.warning(
                    "通义万相返回图片数 %d 少于请求 %d task_id=%s", len(urls), n, task_id
                )
            # 立即下载（URL 24h 过期）
            results: list[ImageResult] = []
            for url in urls:
                results.append(await self._download(url, base_url=base_url))
            return results

    async def _poll(
        self,
        http: httpx.AsyncClient,
        base_url: str,
        api_key: str,
        task_id: str,
    ) -> list[str]:
        """轮询任务至终态，返回图片 URL 列表。"""
        poll_headers = {"Authorization": f"Bearer {api_key}"}
        deadline = asyncio.get_event_loop().time() + _POLL_TIMEOUT
        while True:
            resp = await http.get(
                f"{base_url}/api/v1/tasks/{task_id}", headers=poll_headers
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"通义万相轮询失败 status={resp.status_code}"
                )
            data = resp.json().get("output") or {}
            status = (data.get("task_status") or "").upper()
            if status == "SUCCEEDED":
                results = data.get("results") or []
                return [r["url"] for r in results if r.get("url")]
            if status in ("FAILED", "UNKNOWN"):
                raise RuntimeError(f"通义万相文生图任务失败 task_id={task_id} status={status}")
            if asyncio.get_event_loop().time() > deadline:
                raise RuntimeError(f"通义万相文生图轮询超时 task_id={task_id}")
            await asyncio.sleep(_POLL_INTERVAL)

    async def _download(
        self, url: str, *, base_url: str
    ) -> ImageResult:
        """下载图片 URL 字节流。"""
        from app.providers._url_safety import safe_download_bytes

        data = await safe_download_bytes(
            url,
            max_bytes=50 * 1024 * 1024,
            timeout=60.0,
            url_validator=lambda current: self._validate_download_host(
                current, base_url
            ),
        )
        path = urlsplit(url).path.lower()
        mime_type = "image/jpeg" if path.endswith((".jpg", ".jpeg")) else "image/png"
        return ImageResult(data=data, mime_type=mime_type)

    @staticmethod
    def _validate_download_host(url: str, base_url: str) -> None:
        parsed = urlsplit(url)
        hostname = (parsed.hostname or "").lower()
        provider_host = (urlsplit(base_url).hostname or "").lower()
        if parsed.scheme != "https" or not (
            hostname == provider_host
            or hostname.endswith(".aliyuncs.com")
            or hostname.endswith(".aliyuncs.com.cn")
        ):
            raise RuntimeError("provider_download_url_invalid")
