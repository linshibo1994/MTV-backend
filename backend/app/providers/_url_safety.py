"""厂商返回 URL 的安全下载工具 — SSRF 防护 + 流式大小限制。

复用自 video_gen_volcengine 的 _validate_public_http_url 逐跳校验：
- 仅允许公网 http/https
- 拒绝私网/回环/链路本地/组播/保留/未指定 IP
- 手动逐跳跟随重定向并逐跳校验（不依赖 httpx follow_redirects）

safe_download_bytes 在下载前/中双重检查体积上限，防止超大响应 OOM：
- 先查 Content-Length 头快速拒绝
- 再流式逐块累计，超限立即中断连接
"""

from __future__ import annotations

import ipaddress
import socket
from collections.abc import Callable
from urllib.parse import urlparse

import httpx


def validate_public_http_url(url: str) -> None:
    """校验 URL 为公网 http/https，拒绝私网/回环等内网地址。

    raise ValueError(video_url_not_allowed) 表示不可信目标。
    """
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("video_url_not_allowed")
    host = parsed.hostname
    try:
        candidates = [ipaddress.ip_address(host)]
    except ValueError:
        if host in {"localhost"} or host.endswith(".localhost"):
            raise ValueError("video_url_not_allowed")
        infos = socket.getaddrinfo(host, parsed.port or (443 if parsed.scheme == "https" else 80))
        candidates = [ipaddress.ip_address(info[4][0]) for info in infos]
    for ip in candidates:
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            raise ValueError("video_url_not_allowed")


async def safe_download_bytes(
    url: str,
    *,
    max_bytes: int,
    timeout: float = 120.0,
    max_redirects: int = 5,
    extra_headers: dict[str, str] | None = None,
    url_validator: Callable[[str], None] | None = None,
) -> bytes:
    """安全下载厂商返回的 URL 字节流。

    流程：逐跳 SSRF 校验 → 不跟随重定向（手动逐跳）→ Content-Length 预检 →
    流式累计字节数，超 max_bytes 立即中断。
    """
    headers = extra_headers or {}
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as http:
        current = url
        for _ in range(max_redirects):
            validate_public_http_url(current)
            if url_validator is not None:
                url_validator(current)
            async with http.stream("GET", current, headers=headers) as resp:
                if resp.status_code in {301, 302, 303, 307, 308}:
                    location = resp.headers.get("location")
                    if not location:
                        raise RuntimeError("download_redirect_missing")
                    current = str(resp.url.join(location))
                    continue
                resp.raise_for_status()
                content_length = resp.headers.get("content-length")
                if content_length and int(content_length) > max_bytes:
                    raise RuntimeError("download_too_large")
                total = 0
                buf = bytearray()
                async for chunk in resp.aiter_bytes(65536):
                    total += len(chunk)
                    if total > max_bytes:
                        raise RuntimeError("download_too_large")
                    buf.extend(chunk)
                return bytes(buf)
        raise RuntimeError("download_too_many_redirects")
