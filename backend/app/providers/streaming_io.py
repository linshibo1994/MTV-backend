"""视频 Provider 的流式文件写入与尝试级原子上传辅助。"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import uuid
from collections.abc import AsyncIterable, Awaitable, Callable, Mapping
from pathlib import Path
from typing import BinaryIO

from app.providers.storage import StorageProvider

logger = logging.getLogger(__name__)

STREAM_CHUNK_SIZE = 1024 * 1024
MAX_REFERENCE_MEDIA_BYTES = 50 * 1024 * 1024
_SAFE_ATTEMPT_PART = re.compile(r"[^0-9A-Za-z_-]+")
_WORKER_ATTEMPT_ID = re.compile(r"[0-9a-f]{16}")
CancellationCallback = Callable[[], Awaitable[None]]


async def async_chunks_to_file(
    chunks: AsyncIterable[bytes],
    path: str | Path,
    *,
    max_bytes: int,
    declared_size: int | None = None,
    too_large_error: str = "download_too_large",
    empty_error: str = "download_empty",
    cancellation_probe: CancellationCallback | None = None,
) -> int:
    """把异步字节流写入文件，并在失败、取消或空响应时删除局部文件。"""
    destination = Path(path)
    file_obj: BinaryIO | None = None
    try:
        if max_bytes <= 0:
            raise ValueError("max_bytes 必须大于 0")
        if declared_size is not None and declared_size < 0:
            raise ValueError("declared_size 必须大于等于 0")
        if declared_size is not None and declared_size > max_bytes:
            raise RuntimeError(too_large_error)
        if cancellation_probe is not None:
            await cancellation_probe()

        await asyncio.to_thread(destination.parent.mkdir, parents=True, exist_ok=True)
        file_obj = await asyncio.to_thread(destination.open, "wb")
        total = 0
        async for chunk in chunks:
            if cancellation_probe is not None:
                await cancellation_probe()
            if not chunk:
                continue
            total += len(chunk)
            if total > max_bytes:
                raise RuntimeError(too_large_error)
            await asyncio.to_thread(_write_all, file_obj, chunk)

        if total == 0:
            raise RuntimeError(empty_error)
        await asyncio.to_thread(file_obj.flush)
        await asyncio.to_thread(file_obj.close)
        file_obj = None
        return total
    except BaseException:
        if file_obj is not None:
            await _best_effort_close(file_obj)
        await _best_effort_unlink(destination)
        raise


def storage_attempt_suffix(metadata: Mapping[str, object] | None) -> str:
    """保留 worker 的尝试摘要；其他值先不可逆哈希再进入对象 key。"""
    attempt_id = (metadata or {}).get("storage_attempt_id")
    if attempt_id is None or str(attempt_id) == "":
        return ""
    value = str(attempt_id)
    if _WORKER_ATTEMPT_ID.fullmatch(value):
        return value
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


async def atomic_put_file(
    storage: StorageProvider,
    final_key: str,
    path: str | Path,
    *,
    attempt_id: str,
    preserve_existing_final: bool = False,
) -> str:
    """先上传唯一临时对象，再提升为本次尝试的正式对象。"""
    safe_attempt = _SAFE_ATTEMPT_PART.sub("-", attempt_id).strip("-")[:32] or "unknown"
    temp_key = f"{final_key}.upload-{safe_attempt}-{uuid.uuid4().hex}.tmp"
    final_existed = (
        await storage.exists(final_key)
        if preserve_existing_final
        else False
    )
    try:
        await storage.put_file(temp_key, str(path))
        if not await storage.exists(temp_key):
            raise RuntimeError("upload_temp_missing")
        return await storage.promote(temp_key, final_key)
    except BaseException:
        await _best_effort_delete(storage, temp_key)
        if not (preserve_existing_final and final_existed):
            await _best_effort_delete(storage, final_key)
        raise


async def read_storage_bytes_limited(
    storage: StorageProvider,
    storage_key: str,
    *,
    max_bytes: int = MAX_REFERENCE_MEDIA_BYTES,
    read_error: str = "reference_read_failed",
    too_large_error: str = "reference_media_too_large",
) -> bytes:
    """先按存储元数据预检，再读取并复核小型参考媒体。"""
    if max_bytes <= 0:
        raise ValueError("max_bytes 必须大于 0")
    get_size = getattr(storage, "get_size", None)
    if callable(get_size):
        try:
            reported_size = int(await get_size(storage_key))
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(read_error) from exc
        if reported_size <= 0:
            raise RuntimeError(read_error)
        if reported_size > max_bytes:
            raise RuntimeError(too_large_error)

    try:
        data = await storage.get(storage_key)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(read_error) from exc
    if not data:
        raise RuntimeError(read_error)
    if len(data) > max_bytes:
        raise RuntimeError(too_large_error)
    return data


async def _best_effort_close(file_obj: BinaryIO) -> None:
    try:
        await asyncio.to_thread(file_obj.close)
    except BaseException:  # noqa: BLE001
        logger.warning("清理视频临时文件句柄失败", exc_info=True)


def _write_all(file_obj: BinaryIO, chunk: bytes) -> None:
    view = memoryview(chunk)
    written = 0
    while written < len(view):
        count = file_obj.write(view[written:])
        if count is None or count <= 0:
            raise OSError("video_temp_file_short_write")
        written += count


async def _best_effort_unlink(path: Path) -> None:
    try:
        await asyncio.to_thread(path.unlink, missing_ok=True)
    except BaseException:  # noqa: BLE001
        logger.warning("清理视频临时文件失败 path=%s", path, exc_info=True)


async def _best_effort_delete(storage: StorageProvider, key: str) -> None:
    try:
        await storage.delete(key)
    except BaseException:  # noqa: BLE001
        logger.warning("清理视频上传对象失败 storage_key=%s", key, exc_info=True)
        await _enqueue_delete_compensation(key)


async def _enqueue_delete_compensation(storage_key: str) -> None:
    try:
        from app.core.database import async_session_factory
        from app.services.media_storage_jobs import enqueue_delete

        digest = hashlib.sha256(storage_key.encode("utf-8")).hexdigest()
        async with async_session_factory() as db:
            await enqueue_delete(
                db,
                idempotency_key=f"video-delete:{digest}",
                source_key=storage_key,
            )
            await db.commit()
    except BaseException:  # noqa: BLE001
        logger.warning(
            "写入视频对象删除补偿失败 storage_key=%s",
            storage_key,
            exc_info=True,
        )
