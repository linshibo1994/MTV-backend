"""本地文件系统存储实现 —— 开发默认。

- root = settings.storage_root
- 生成图存 ``storage_root/generate_images/<mid>/<id>.png``
- 上传存 ``storage_root/upload_images/<mid>/<id>.<ext>``

所有同步 I/O 经 asyncio.to_thread 包装，避免在 async 事件循环中阻塞 worker 线程。
"""

from __future__ import annotations

import asyncio
import errno
import os
import stat
from collections.abc import AsyncIterator
from pathlib import Path

from app.core.config import settings
from app.providers.storage import normalize_key, validate_key_prefix


class LocalStorageProvider:
    """本地 FS 存储提供者。"""

    def __init__(self, root: str | None = None) -> None:
        self.root = root or settings.storage_root
        os.makedirs(self.root, exist_ok=True)

    def _full_path(self, key: str) -> str:
        rel = validate_key_prefix(key)
        # 防止目录穿越和符号链接逃逸：真实解析后必须仍在根目录内。
        root = Path(self.root).resolve()
        full = (root / rel).resolve()
        try:
            full.relative_to(root)
        except ValueError as exc:
            raise ValueError("非法 storage_key") from exc
        return str(full)

    def signed_url(self, key: str, *, expires_seconds: int = 300) -> str:
        rel = validate_key_prefix(key)
        return f"{settings.media_proxy_prefix.rstrip('/')}/{rel}"

    async def put(self, key: str, data: bytes) -> str:
        full = self._full_path(key)
        await asyncio.to_thread(self._write_file, full, data)
        return normalize_key(key)

    async def put_file(self, key: str, path: str) -> str:
        full = self._full_path(key)
        await asyncio.to_thread(self._copy_file, path, full)
        return normalize_key(key)

    async def promote(self, source_key: str, final_key: str) -> str:
        source = self._full_path(source_key)
        final = self._full_path(final_key)
        await asyncio.to_thread(self._promote_file, source, final)
        return normalize_key(final_key)

    @staticmethod
    def _promote_file(source: str, final: str) -> None:
        os.makedirs(os.path.dirname(final), exist_ok=True)
        os.replace(source, final)

    @staticmethod
    def _write_file(full: str, data: bytes) -> None:
        os.makedirs(os.path.dirname(full), exist_ok=True)
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(full, flags, 0o600)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise ValueError("非法 storage_key") from exc
            raise
        with os.fdopen(fd, "wb") as f:
            f.write(data)

    @staticmethod
    def _copy_file(source: str, full: str) -> None:
        os.makedirs(os.path.dirname(full), exist_ok=True)
        read_flags = os.O_RDONLY
        write_flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        if hasattr(os, "O_NOFOLLOW"):
            read_flags |= os.O_NOFOLLOW
            write_flags |= os.O_NOFOLLOW
        try:
            src_fd = os.open(source, read_flags)
            dst_fd = os.open(full, write_flags, 0o600)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise ValueError("非法 storage_key") from exc
            raise
        with os.fdopen(src_fd, "rb") as src, os.fdopen(dst_fd, "wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)

    async def get(self, key: str) -> bytes:
        full = self._full_path(key)
        return await asyncio.to_thread(self._read_file, full)

    @staticmethod
    def _read_file(full: str) -> bytes:
        with LocalStorageProvider._open_read_file(full) as f:
            return f.read()

    async def delete(self, key: str) -> None:
        full = self._full_path(key)
        await asyncio.to_thread(self._delete_file, full)

    @staticmethod
    def _delete_file(full: str) -> None:
        if os.path.exists(full):
            os.remove(full)

    async def exists(self, key: str) -> bool:
        full = self._full_path(key)
        return await asyncio.to_thread(self._exists_file, full)

    @staticmethod
    def _exists_file(full: str) -> bool:
        try:
            return stat.S_ISREG(os.lstat(full).st_mode)
        except FileNotFoundError:
            return False

    async def get_size(self, key: str) -> int:
        full = self._full_path(key)
        return await asyncio.to_thread(self._size_file, full)

    @staticmethod
    def _size_file(full: str) -> int:
        info = os.lstat(full)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("非法 storage_key")
        return info.st_size

    async def iter_chunks(
        self,
        key: str,
        *,
        chunk_size: int = 65536,
        start: int = 0,
        end: int | None = None,
    ) -> AsyncIterator[bytes]:
        """流式分块读取本地文件，支持 Range (start/end 含)。"""
        full = self._full_path(key)
        total = await asyncio.to_thread(self._size_file, full)
        if end is None or end >= total:
            end = total - 1
        if start < 0:
            start = 0
        if start > end:
            return

        # 单次打开文件句柄并顺序分块读取，避免每块重复 open/seek/close 系统调用
        # 大文件(512MB/64KB≈8192块)在流式媒体场景下吞吐显著提升
        f = await asyncio.to_thread(self._open_read_file, full)
        try:
            if start > 0:
                await asyncio.to_thread(f.seek, start)
            offset = start
            while offset <= end:
                read_len = min(chunk_size, end - offset + 1)
                chunk = await asyncio.to_thread(f.read, read_len)
                if not chunk:
                    break
                yield chunk
                offset += len(chunk)
        finally:
            await asyncio.to_thread(f.close)

    @staticmethod
    def _open_read_file(full: str):
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(full, flags)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise ValueError("非法 storage_key") from exc
            raise
        return os.fdopen(fd, "rb")
