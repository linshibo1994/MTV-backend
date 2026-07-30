"""MeiZi 公共媒资中心的 ``StorageProvider`` 实现。

业务层仍使用原有逻辑 ``storage_key``；本模块通过数据库映射保存远端媒资身份，
因此不会把 CDN URL 写进业务表，也不需要持有公共 OSS 的 AK/SK。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import mimetypes
import re
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, cast
from urllib.parse import urlsplit

import httpx
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.core.config import Settings, settings
from app.models.meizi_storage_object import MeiziStorageObject
from app.providers._url_safety import validate_public_http_url
from app.providers.meizi_client import (
    MeiziFileCenterClient,
    MeiziMediaKind,
    MeiziUploadIdentity,
    MeiziUploadResult,
)
from app.providers.safe_http_transport import (
    HostResolver,
    build_safe_async_http_transport,
    resolve_public_ips,
)
from app.providers.storage import (
    StorageObjectConflictError,
    StorageObjectIntegrityError,
    StorageObjectNotFoundError,
    StorageProvider,
    validate_key_prefix,
)

logger = logging.getLogger(__name__)

_UPLOAD_TEMP_SUFFIX = re.compile(r"\.upload-.+-[0-9a-f]{32}$")
_CONTENT_RANGE = re.compile(r"bytes\s+(\d+)-(\d+)/(\d+|\*)", re.IGNORECASE)
_MEIZI_CANARY_KEY = "generate_images/_system/meizi-storage-canary.png"
_MEIZI_CANARY_PAYLOAD = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
    "+A8AAQUBAScY42YAAAAASUVORK5CYII=",
    validate=True,
)

_IMAGE_EXTENSIONS = frozenset(
    {".avif", ".bmp", ".gif", ".heic", ".heif", ".jpeg", ".jpg", ".png", ".tif", ".tiff", ".webp"}
)
_AUDIO_EXTENSIONS = frozenset(
    {".aac", ".flac", ".m4a", ".mp3", ".oga", ".ogg", ".opus", ".wav"}
)
_VIDEO_EXTENSIONS = frozenset(
    {".avi", ".m4v", ".mkv", ".mov", ".mp4", ".mpeg", ".mpg", ".ts", ".webm"}
)
_FILE_EXTENSIONS = frozenset(
    {
        ".csv",
        ".doc",
        ".docx",
        ".json",
        ".md",
        ".odt",
        ".pdf",
        ".ppt",
        ".pptx",
        ".rtf",
        ".srt",
        ".txt",
        ".vtt",
        ".xls",
        ".xlsx",
        ".xml",
        ".yaml",
        ".yml",
    }
)


def meizi_delete_namespace(config: Settings) -> str:
    """计算 frontupload 非敏感身份的稳定命名空间指纹。"""
    payload = json.dumps(
        {
            "base_url": config.meizi_base_url.rstrip("/"),
            "group_id": config.meizi_front_group_id,
            "source": config.meizi_front_source,
            "tenant_id": config.meizi_front_tenant_id,
            "user_name": config.meizi_front_user_name or "",
            "version": 2,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _meizi_orphan_storage_key(
    storage_key: str,
    identity: MeiziUploadIdentity,
) -> str:
    """为已取得远端 ID 但输掉逻辑 key 竞争的上传生成独立清理债务 key。"""
    payload = json.dumps(
        {
            "lib_type": identity.lib_type,
            "media_id": identity.media_id,
            "request_id": identity.request_id,
            "storage_key": storage_key,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(payload.encode()).hexdigest()
    return f"generate_images/_meizi_orphans/{digest}.tmp"


async def run_meizi_storage_canary(
    *,
    storage: StorageProvider | None = None,
) -> dict[str, int | str]:
    """执行真实上传、HTTPS 读取和删除契约，供生产切换门禁调用。"""
    if storage is None:
        from app.providers.storage import get_storage

        storage = get_storage()

    expected_sha256 = hashlib.sha256(_MEIZI_CANARY_PAYLOAD).hexdigest()
    uploaded = False
    try:
        stored_key = await storage.put(_MEIZI_CANARY_KEY, _MEIZI_CANARY_PAYLOAD)
        uploaded = True
        if validate_key_prefix(stored_key) != _MEIZI_CANARY_KEY:
            raise RuntimeError("meizi_canary_storage_key_mismatch")
        size_bytes = await storage.get_size(_MEIZI_CANARY_KEY)
        payload = await storage.get(_MEIZI_CANARY_KEY)
        if (
            size_bytes != len(_MEIZI_CANARY_PAYLOAD)
            or payload != _MEIZI_CANARY_PAYLOAD
            or hashlib.sha256(payload).hexdigest() != expected_sha256
        ):
            raise RuntimeError("meizi_canary_read_mismatch")
    finally:
        if uploaded:
            await storage.delete(_MEIZI_CANARY_KEY)

    if await storage.exists(_MEIZI_CANARY_KEY):
        raise RuntimeError("meizi_canary_delete_unconfirmed")
    return {
        "size_bytes": len(_MEIZI_CANARY_PAYLOAD),
        "sha256": expected_sha256,
    }


class MeiziDeleteNamespaceMismatchError(RuntimeError):
    """当前 frontupload 身份无权删除该映射，防止配置轮换后误删。"""

    def __init__(self) -> None:
        super().__init__("meizi_delete_namespace_mismatch")


@dataclass(frozen=True, slots=True)
class MeiziObjectSnapshot:
    """数据库映射的不可变快照，避免在远端 I/O 期间持有 ORM 会话。"""

    storage_key: str
    media_id: int
    info_id: int
    lib_type: int
    media_kind: MeiziMediaKind
    request_id: str
    public_url: str | None
    mime_type: str
    size_bytes: int
    sha256: str
    delete_namespace: str
    status: str


class MeiziObjectRepository(Protocol):
    async def get(self, storage_key: str) -> MeiziObjectSnapshot | None: ...

    async def reserve_upload(
        self,
        *,
        storage_key: str,
        identity: MeiziUploadIdentity,
        mime_type: str,
        size_bytes: int,
        sha256: str,
        delete_namespace: str,
    ) -> None: ...

    async def activate(
        self,
        *,
        storage_key: str,
        result: MeiziUploadResult,
    ) -> None: ...

    async def mark_compensation(
        self,
        *,
        storage_key: str,
        identity: MeiziUploadIdentity,
        deleted: bool,
    ) -> None: ...

    async def mark_cleanup_pending(self, *, storage_key: str, media_id: int) -> None: ...

    async def begin_delete(self, storage_key: str) -> MeiziObjectSnapshot | None: ...

    async def mark_deleted(self, *, storage_key: str, media_id: int) -> None: ...

    async def promote_if_absent(self, source_key: str, final_key: str) -> bool: ...

    async def claim_stale_cleanup(
        self,
        *,
        stale_before: datetime,
        limit: int,
    ) -> list[MeiziObjectSnapshot]: ...


class SqlAlchemyMeiziObjectRepository:
    """每次操作使用短事务；绝不在第三方 HTTP I/O 期间持有数据库锁。"""

    def __init__(self, session_factory: Callable[[], object] | None = None) -> None:
        self._session_factory = session_factory

    def _sessions(self):
        if self._session_factory is not None:
            return self._session_factory
        from app.core.database import async_session_factory

        return async_session_factory

    async def get(self, storage_key: str) -> MeiziObjectSnapshot | None:
        async with self._sessions()() as session:  # type: ignore[attr-defined]
            row = await session.get(MeiziStorageObject, storage_key)
            return self._snapshot(row) if row is not None else None

    async def reserve_upload(
        self,
        *,
        storage_key: str,
        identity: MeiziUploadIdentity,
        mime_type: str,
        size_bytes: int,
        sha256: str,
        delete_namespace: str,
    ) -> None:
        try:
            async with self._sessions()() as session:  # type: ignore[attr-defined]
                async with session.begin():
                    row = await session.get(
                        MeiziStorageObject,
                        storage_key,
                        with_for_update=True,
                    )
                    if row is None:
                        row = MeiziStorageObject(storage_key=storage_key)
                        session.add(row)
                    elif row.status != "deleted":
                        raise StorageObjectConflictError(storage_key)
                    self._apply_identity(
                        row,
                        identity=identity,
                        mime_type=mime_type,
                        size_bytes=size_bytes,
                        sha256=sha256,
                        delete_namespace=delete_namespace,
                    )
                    await session.flush()
        except IntegrityError as exc:
            raise StorageObjectConflictError(storage_key) from exc

    async def activate(
        self,
        *,
        storage_key: str,
        result: MeiziUploadResult,
    ) -> None:
        async with self._sessions()() as session:  # type: ignore[attr-defined]
            async with session.begin():
                row = await session.get(
                    MeiziStorageObject,
                    storage_key,
                    with_for_update=True,
                )
                if (
                    row is None
                    or row.media_id != result.media_id
                    or row.request_id != result.request_id
                    or row.status not in {"uploading", "active"}
                ):
                    raise StorageObjectConflictError(storage_key)
                row.public_url = result.public_url
                row.status = "active"
                await session.flush()

    async def mark_compensation(
        self,
        *,
        storage_key: str,
        identity: MeiziUploadIdentity,
        deleted: bool,
    ) -> None:
        async with self._sessions()() as session:  # type: ignore[attr-defined]
            async with session.begin():
                row = await session.get(
                    MeiziStorageObject,
                    storage_key,
                    with_for_update=True,
                )
                if (
                    row is None
                    or row.media_id != identity.media_id
                    or row.request_id != identity.request_id
                ):
                    return
                row.status = "deleted" if deleted else "deleting"
                await session.flush()

    async def mark_cleanup_pending(
        self,
        *,
        storage_key: str,
        media_id: int,
    ) -> None:
        async with self._sessions()() as session:  # type: ignore[attr-defined]
            async with session.begin():
                row = await session.get(
                    MeiziStorageObject,
                    storage_key,
                    with_for_update=True,
                )
                if row is None or row.media_id != media_id:
                    return
                row.status = "cleanup_pending"
                await session.flush()

    async def begin_delete(self, storage_key: str) -> MeiziObjectSnapshot | None:
        async with self._sessions()() as session:  # type: ignore[attr-defined]
            async with session.begin():
                row = await session.get(
                    MeiziStorageObject,
                    storage_key,
                    with_for_update=True,
                )
                if row is None or row.status == "deleted":
                    return None
                row.status = "deleting"
                await session.flush()
                return self._snapshot(row)

    async def mark_deleted(self, *, storage_key: str, media_id: int) -> None:
        async with self._sessions()() as session:  # type: ignore[attr-defined]
            async with session.begin():
                row = await session.get(
                    MeiziStorageObject,
                    storage_key,
                    with_for_update=True,
                )
                if row is None or row.media_id != media_id:
                    return
                row.status = "deleted"
                await session.flush()

    async def promote_if_absent(self, source_key: str, final_key: str) -> bool:
        if source_key == final_key:
            row = await self.get(source_key)
            if row is None or row.status != "active":
                raise StorageObjectNotFoundError(source_key)
            return False

        async with self._sessions()() as session:  # type: ignore[attr-defined]
            async with session.begin():
                from app.models.storage_reference_guard import acquire_storage_key_locks

                await acquire_storage_key_locks(session, {source_key, final_key})
                rows = (
                    await session.execute(
                        select(MeiziStorageObject)
                        .where(MeiziStorageObject.storage_key.in_([source_key, final_key]))
                        .order_by(MeiziStorageObject.storage_key)
                        .with_for_update()
                    )
                ).scalars().all()
                by_key = {row.storage_key: row for row in rows}
                source = by_key.get(source_key)
                target = by_key.get(final_key)
                if target is not None and target.status != "deleted":
                    return False
                if source is None or source.status != "active":
                    if target is not None and target.status == "active":
                        return False
                    raise StorageObjectNotFoundError(source_key)
                if target is not None:
                    await session.delete(target)
                    await session.flush()
                source.storage_key = final_key
                await session.flush()
                return True

    async def claim_stale_cleanup(
        self,
        *,
        stale_before: datetime,
        limit: int,
    ) -> list[MeiziObjectSnapshot]:
        """领取超时的上传/删除映射，并用 updated_at 充当可恢复的粗粒度租约。"""
        if limit <= 0:
            return []
        async with self._sessions()() as session:  # type: ignore[attr-defined]
            async with session.begin():
                rows = (
                    await session.execute(
                        select(MeiziStorageObject)
                        .where(
                            MeiziStorageObject.status.in_(
                                ("uploading", "deleting", "cleanup_pending")
                            ),
                            MeiziStorageObject.updated_at <= stale_before,
                        )
                        .order_by(MeiziStorageObject.updated_at, MeiziStorageObject.storage_key)
                        .with_for_update(skip_locked=True)
                        .limit(limit)
                    )
                ).scalars().all()
                claimed_at = datetime.now(UTC)
                for row in rows:
                    if row.status != "cleanup_pending":
                        row.status = "deleting"
                    # 即使原状态也是 deleting，也必须刷新租约时间，避免并发重复领取。
                    row.updated_at = claimed_at
                await session.flush()
                return [self._snapshot(row) for row in rows]

    @staticmethod
    def _apply_identity(
        row: MeiziStorageObject,
        *,
        identity: MeiziUploadIdentity,
        mime_type: str,
        size_bytes: int,
        sha256: str,
        delete_namespace: str,
    ) -> None:
        row.media_id = identity.media_id
        row.info_id = identity.info_id
        row.lib_type = identity.lib_type
        row.media_kind = identity.media_kind
        row.request_id = identity.request_id
        row.public_url = None
        row.mime_type = mime_type
        row.size_bytes = size_bytes
        row.sha256 = sha256
        row.delete_namespace = delete_namespace
        row.status = "uploading"

    @staticmethod
    def _snapshot(row: MeiziStorageObject) -> MeiziObjectSnapshot:
        return MeiziObjectSnapshot(
            storage_key=row.storage_key,
            media_id=row.media_id,
            info_id=row.info_id,
            lib_type=row.lib_type,
            media_kind=cast(MeiziMediaKind, row.media_kind),
            request_id=row.request_id,
            public_url=row.public_url,
            mime_type=row.mime_type,
            size_bytes=row.size_bytes,
            sha256=row.sha256,
            delete_namespace=row.delete_namespace,
            status=row.status,
        )


class MeiziStorageProvider:
    """把 MeiZi 媒资映射为 MTV 的逻辑对象存储契约。"""

    def __init__(
        self,
        *,
        config: Settings = settings,
        client: MeiziFileCenterClient | None = None,
        repository: MeiziObjectRepository | None = None,
        download_client: httpx.AsyncClient | None = None,
        legacy_provider: StorageProvider | None = None,
        public_url_resolver: HostResolver | None = None,
    ) -> None:
        self._config = config
        self._client = client or MeiziFileCenterClient(config=config)
        self._repository = repository or SqlAlchemyMeiziObjectRepository()
        self._download_client = download_client
        self._public_url_resolver = public_url_resolver
        self._delete_namespace = meizi_delete_namespace(config)
        if legacy_provider is None and config.meizi_legacy_local_fallback:
            # 延迟导入，避免 storage 工厂与本地实现之间形成模块循环。
            from app.providers.storage_local import LocalStorageProvider

            legacy_provider = LocalStorageProvider(root=config.storage_root)
        self._legacy_provider = legacy_provider

    async def put(self, key: str, data: bytes) -> str:
        storage_key = validate_key_prefix(key)
        if not data:
            raise ValueError("MeiZi 不接受空媒体文件")
        media_kind, mime_type, filename = classify_storage_key(storage_key)
        return await self._put(
            storage_key=storage_key,
            filename=filename,
            mime_type=mime_type,
            media_kind=media_kind,
            size_bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
            uploader=lambda on_identity, on_compensation: self._client.upload_bytes(
                filename=filename,
                data=data,
                mime_type=mime_type,
                media_kind=media_kind,
                on_identity=on_identity,
                on_compensation=on_compensation,
            ),
        )

    async def put_file(self, key: str, path: str) -> str:
        storage_key = validate_key_prefix(key)
        media_kind, mime_type, filename = classify_storage_key(storage_key)
        size_bytes, sha256 = await asyncio.to_thread(_hash_file, path)
        if size_bytes == 0:
            raise ValueError("MeiZi 不接受空媒体文件")
        return await self._put(
            storage_key=storage_key,
            filename=filename,
            mime_type=mime_type,
            media_kind=media_kind,
            size_bytes=size_bytes,
            sha256=sha256,
            uploader=lambda on_identity, on_compensation: self._client.upload_path(
                filename=filename,
                path=path,
                mime_type=mime_type,
                media_kind=media_kind,
                on_identity=on_identity,
                on_compensation=on_compensation,
            ),
        )

    async def _put(
        self,
        *,
        storage_key: str,
        filename: str,
        mime_type: str,
        media_kind: MeiziMediaKind,
        size_bytes: int,
        sha256: str,
        uploader,
    ) -> str:
        existing = await self._repository.get(storage_key)
        if existing is not None and existing.status == "cleanup_pending":
            await self._cleanup_legacy_copy(storage_key, raise_on_error=True)
            await self._repository.mark_deleted(
                storage_key=storage_key,
                media_id=existing.media_id,
            )
            existing = await self._repository.get(storage_key)
        if existing is not None and existing.status != "deleted":
            self._require_delete_namespace(existing)
        if existing is not None and existing.status == "active":
            if existing.size_bytes == size_bytes and existing.sha256 == sha256:
                await self._cleanup_legacy_copy(storage_key)
                return storage_key
            raise StorageObjectConflictError(storage_key)
        if existing is not None and existing.status in {"uploading", "deleting"}:
            await self._client.shift_delete(
                media_id=existing.media_id,
                lib_type=existing.lib_type,
            )
            await self._repository.mark_deleted(
                storage_key=storage_key,
                media_id=existing.media_id,
            )

        compensation_storage_key = storage_key

        async def on_identity(identity: MeiziUploadIdentity) -> None:
            nonlocal compensation_storage_key
            try:
                await self._repository.reserve_upload(
                    storage_key=storage_key,
                    identity=identity,
                    mime_type=mime_type,
                    size_bytes=size_bytes,
                    sha256=sha256,
                    delete_namespace=self._delete_namespace,
                )
            except StorageObjectConflictError:
                compensation_storage_key = _meizi_orphan_storage_key(
                    storage_key,
                    identity,
                )
                await self._repository.reserve_upload(
                    storage_key=compensation_storage_key,
                    identity=identity,
                    mime_type=mime_type,
                    size_bytes=size_bytes,
                    sha256=sha256,
                    delete_namespace=self._delete_namespace,
                )
                raise

        async def on_compensation(
            identity: MeiziUploadIdentity,
            deleted: bool,
        ) -> None:
            await self._repository.mark_compensation(
                storage_key=compensation_storage_key,
                identity=identity,
                deleted=deleted,
            )

        result = await uploader(on_identity, on_compensation)
        try:
            await self._repository.activate(storage_key=storage_key, result=result)
        except Exception:
            deleted = False
            try:
                await self._client.shift_delete(
                    media_id=result.media_id,
                    lib_type=result.lib_type,
                )
                deleted = True
            finally:
                await self._repository.mark_compensation(
                    storage_key=storage_key,
                    identity=MeiziUploadIdentity(
                        media_id=result.media_id,
                        info_id=result.info_id,
                        lib_type=result.lib_type,
                        media_kind=result.media_kind,
                        request_id=result.request_id,
                    ),
                    deleted=deleted,
                )
            raise
        await self._cleanup_legacy_copy(storage_key)
        return storage_key

    async def promote(self, source_key: str, final_key: str) -> str:
        source = validate_key_prefix(source_key)
        final = validate_key_prefix(final_key)
        if source == final:
            if not await self.exists(source):
                raise StorageObjectNotFoundError(source)
            return final
        source_row, target_row = await asyncio.gather(
            self._repository.get(source),
            self._repository.get(final),
        )
        if (
            source_row is None
            and target_row is None
            and self._legacy_provider is not None
        ):
            return await self._legacy_provider.promote(source, final)
        if await self._repository.promote_if_absent(source, final):
            # promote 允许覆盖；远端映射发布成功后，清理被覆盖的历史本地副本。
            await self._cleanup_legacy_copy(final)
            return final

        source_row = await self._repository.get(source)
        target_row = await self._repository.get(final)
        if target_row is not None and target_row.status == "active":
            if source_row is None or source_row.status != "active":
                await self._cleanup_legacy_copy(source)
                return final
            if (
                source_row.size_bytes == target_row.size_bytes
                and source_row.sha256 == target_row.sha256
            ):
                await self.delete(source)
                return final
            raise StorageObjectConflictError(final)
        raise StorageObjectNotFoundError(source)

    async def promote_if_absent(self, source_key: str, final_key: str) -> bool:
        source = validate_key_prefix(source_key)
        final = validate_key_prefix(final_key)
        source_row, target_row = await asyncio.gather(
            self._repository.get(source),
            self._repository.get(final),
        )
        if target_row is not None and target_row.status != "deleted":
            return False
        if (
            target_row is None
            and self._legacy_provider is not None
            and await self._legacy_provider.exists(final)
        ):
            # 条件发布不能用远端对象覆盖尚未迁移的本地正式对象。
            return False
        if (
            source_row is None
            and target_row is None
            and self._legacy_provider is not None
        ):
            return await self._legacy_provider.promote_if_absent(source, final)
        return await self._repository.promote_if_absent(source, final)

    async def get(self, key: str) -> bytes:
        storage_key = validate_key_prefix(key)
        row = await self._repository.get(storage_key)
        if row is None and self._legacy_provider is not None:
            return await self._legacy_provider.get(storage_key)
        row = self._require_active(storage_key, row)
        if row.size_bytes > self._config.max_media_read_bytes:
            raise RuntimeError("media_read_too_large")
        data = bytearray()
        async for chunk in self.iter_chunks(storage_key):
            data.extend(chunk)
            if len(data) > self._config.max_media_read_bytes:
                raise RuntimeError("media_read_too_large")
        if len(data) != row.size_bytes:
            raise StorageObjectIntegrityError(storage_key)
        return bytes(data)

    async def delete(self, key: str) -> None:
        storage_key = validate_key_prefix(key)
        existing = await self._repository.get(storage_key)
        if existing is None:
            if self._legacy_provider is not None:
                await self._legacy_provider.delete(storage_key)
            return
        if existing.status == "cleanup_pending":
            await self._cleanup_legacy_copy(storage_key, raise_on_error=True)
            await self._repository.mark_deleted(
                storage_key=storage_key,
                media_id=existing.media_id,
            )
            return
        if existing.status == "deleted":
            await self._cleanup_legacy_copy(storage_key, raise_on_error=True)
            return
        self._require_delete_namespace(existing)
        row = await self._repository.begin_delete(storage_key)
        if row is None:
            return
        self._require_delete_namespace(row)
        await self._client.shift_delete(media_id=row.media_id, lib_type=row.lib_type)
        if self._legacy_provider is not None:
            await self._repository.mark_cleanup_pending(
                storage_key=storage_key,
                media_id=row.media_id,
            )
            await self._cleanup_legacy_copy(storage_key, raise_on_error=True)
        await self._repository.mark_deleted(
            storage_key=storage_key,
            media_id=row.media_id,
        )

    async def exists(self, key: str) -> bool:
        storage_key = validate_key_prefix(key)
        row = await self._repository.get(storage_key)
        if row is not None:
            return row.status == "active"
        if self._legacy_provider is not None:
            return await self._legacy_provider.exists(storage_key)
        return False

    async def signed_url(self, key: str, *, expires_seconds: int = 300) -> str | None:
        storage_key = validate_key_prefix(key)
        row = await self._repository.get(storage_key)
        if row is None and self._legacy_provider is not None:
            return await self._legacy_provider.signed_url(
                storage_key,
                expires_seconds=expires_seconds,
            )
        if row is None or row.status != "active" or not row.public_url:
            return None
        del expires_seconds
        # MeiZi 只返回永久公共地址，不能伪装成满足过期语义的 signed URL。
        return None

    async def external_read_url(
        self,
        key: str,
        *,
        expires_seconds: int = 300,
    ) -> str | None:
        storage_key = validate_key_prefix(key)
        row = await self._repository.get(storage_key)
        if row is None and self._legacy_provider is not None:
            return await self._legacy_provider.external_read_url(
                storage_key,
                expires_seconds=expires_seconds,
            )
        if row is None or row.status != "active" or not row.public_url:
            return None
        del expires_seconds
        self._validate_public_media_url(row.public_url)
        parsed = urlsplit(row.public_url)
        try:
            await resolve_public_ips(
                parsed.hostname or "",
                parsed.port or 443,
                resolver=self._public_url_resolver,
                timeout=self._config.meizi_connect_timeout_seconds,
            )
        except ValueError:
            raise RuntimeError("meizi_media_url_invalid") from None
        return row.public_url

    async def get_size(self, key: str) -> int:
        storage_key = validate_key_prefix(key)
        row = await self._repository.get(storage_key)
        if row is None and self._legacy_provider is not None:
            return await self._legacy_provider.get_size(storage_key)
        return self._require_active(storage_key, row).size_bytes

    async def reconcile_stale_deletions(
        self,
        *,
        limit: int | None = None,
    ) -> dict[str, int]:
        """重试清理中断上传和删除失败对象，直到远端成功确认。"""
        batch_size = limit or self._config.meizi_cleanup_batch_size
        stale_before = datetime.now(UTC) - timedelta(
            seconds=self._config.meizi_cleanup_stale_seconds
        )
        rows = await self._repository.claim_stale_cleanup(
            stale_before=stale_before,
            limit=batch_size,
        )
        deleted = 0
        failed = 0
        for row in rows:
            try:
                if row.status != "cleanup_pending":
                    self._require_delete_namespace(row)
                    await self._client.shift_delete(
                        media_id=row.media_id,
                        lib_type=row.lib_type,
                    )
                    if self._legacy_provider is not None:
                        await self._repository.mark_cleanup_pending(
                            storage_key=row.storage_key,
                            media_id=row.media_id,
                        )
                await self._cleanup_legacy_copy(
                    row.storage_key,
                    raise_on_error=True,
                )
                await self._repository.mark_deleted(
                    storage_key=row.storage_key,
                    media_id=row.media_id,
                )
                deleted += 1
            except Exception:
                # 映射保持 deleting/cleanup_pending，租约窗口后继续领取；禁止输出签名 URL。
                failed += 1
                logger.warning(
                    "meizi_stale_cleanup_failed kind=%s media_id=%s",
                    row.media_kind,
                    row.media_id,
                )
        return {
            "claimed": len(rows),
            "deleted": deleted,
            "failed": failed,
        }

    async def iter_chunks(
        self,
        key: str,
        *,
        chunk_size: int = 65536,
        start: int = 0,
        end: int | None = None,
    ) -> AsyncIterator[bytes]:
        storage_key = validate_key_prefix(key)
        if chunk_size <= 0:
            raise ValueError("chunk_size 必须大于 0")
        row = await self._repository.get(storage_key)
        if row is None and self._legacy_provider is not None:
            async for chunk in self._legacy_provider.iter_chunks(
                storage_key,
                chunk_size=chunk_size,
                start=start,
                end=end,
            ):
                yield chunk
            return
        row = self._require_active(storage_key, row)
        if not row.public_url:
            raise StorageObjectNotFoundError(storage_key)
        self._validate_public_media_url(row.public_url)
        total = row.size_bytes
        if start < 0:
            start = 0
        if end is None or end >= total:
            end = total - 1
        if total <= 0 or start > end:
            return
        expected = end - start + 1
        headers = {
            "Accept-Encoding": "identity",
            "Range": f"bytes={start}-{end}",
        }
        try:
            async for chunk in self._stream_remote_chunks(
                storage_key=storage_key,
                url=row.public_url,
                headers=headers,
                chunk_size=chunk_size,
                start=start,
                end=end,
                total=total,
                expected=expected,
            ):
                yield chunk
        except httpx.HTTPError:
            # 公共 URL 可能含签名查询串，禁止 HTTPX 异常链把完整 URL 写进日志。
            raise RuntimeError("meizi_media_read_failed") from None

    async def _stream_remote_chunks(
        self,
        *,
        storage_key: str,
        url: str,
        headers: dict[str, str],
        chunk_size: int,
        start: int,
        end: int,
        total: int,
        expected: int,
    ) -> AsyncIterator[bytes]:
        async with self._download_http() as http:
            async with http.stream("GET", url, headers=headers) as response:
                if response.status_code == 404:
                    raise StorageObjectNotFoundError(storage_key)
                if response.status_code not in {200, 206}:
                    raise RuntimeError("meizi_media_read_failed")
                if response.status_code == 200 and (start != 0 or end != total - 1):
                    raise RuntimeError("meizi_media_range_unsupported")
                if response.status_code == 206:
                    self._validate_content_range(
                        response.headers.get("content-range"),
                        start=start,
                        end=end,
                        total=total,
                    )
                streamed = 0
                async for chunk in response.aiter_bytes(chunk_size):
                    if not chunk:
                        continue
                    streamed += len(chunk)
                    if streamed > expected:
                        raise StorageObjectIntegrityError(storage_key)
                    yield chunk
                if streamed != expected:
                    raise StorageObjectIntegrityError(storage_key)

    async def _active(self, storage_key: str) -> MeiziObjectSnapshot:
        row = await self._repository.get(storage_key)
        return self._require_active(storage_key, row)

    @staticmethod
    def _require_active(
        storage_key: str,
        row: MeiziObjectSnapshot | None,
    ) -> MeiziObjectSnapshot:
        if row is None or row.status != "active":
            raise StorageObjectNotFoundError(storage_key)
        return row

    async def _cleanup_legacy_copy(
        self,
        storage_key: str,
        *,
        raise_on_error: bool = False,
    ) -> None:
        """远端对象成为权威副本后，尽力清理同 key 的历史本地文件。"""
        if self._legacy_provider is None:
            return
        try:
            await self._legacy_provider.delete(storage_key)
        except OSError:
            logger.warning("meizi_legacy_cleanup_failed storage_key=%s", storage_key)
            if raise_on_error:
                raise

    @asynccontextmanager
    async def _download_http(self) -> AsyncIterator[httpx.AsyncClient]:
        if self._download_client is not None:
            yield self._download_client
            return
        timeout = httpx.Timeout(
            connect=self._config.meizi_connect_timeout_seconds,
            read=self._config.meizi_read_timeout_seconds,
            write=self._config.meizi_write_timeout_seconds,
            pool=self._config.meizi_connect_timeout_seconds,
        )
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
            transport=build_safe_async_http_transport(),
        ) as http:
            yield http

    def _validate_public_media_url(self, url: str) -> None:
        try:
            validate_public_http_url(url)
            split = urlsplit(url)
        except (TypeError, ValueError):
            raise RuntimeError("meizi_media_url_invalid") from None
        host = (split.hostname or "").rstrip(".").lower()
        if (
            split.scheme != "https"
            or split.username is not None
            or split.password is not None
            or split.fragment
            or (
                self._config.meizi_public_url_allowed_hosts
                and host not in self._config.meizi_public_url_allowed_hosts
            )
        ):
            raise RuntimeError("meizi_media_url_invalid")

    def _require_delete_namespace(self, row: MeiziObjectSnapshot) -> None:
        if row.delete_namespace != self._delete_namespace:
            raise MeiziDeleteNamespaceMismatchError()

    @staticmethod
    def _validate_content_range(
        value: str | None,
        *,
        start: int,
        end: int,
        total: int,
    ) -> None:
        match = _CONTENT_RANGE.fullmatch((value or "").strip())
        if match is None:
            raise RuntimeError("meizi_media_range_invalid")
        actual_start, actual_end, actual_total = match.groups()
        if (
            int(actual_start) != start
            or int(actual_end) != end
            or actual_total == "*"
            or int(actual_total) != total
        ):
            raise RuntimeError("meizi_media_range_invalid")


def classify_storage_key(storage_key: str) -> tuple[MeiziMediaKind, str, str]:
    """按业务语义识别媒资类型；先剥离已知临时后缀，未知扩展名失败关闭。"""
    normalized = validate_key_prefix(storage_key)
    semantic_key = normalized
    if semantic_key.endswith(".tmp"):
        semantic_key = semantic_key[:-4]
    semantic_key = _UPLOAD_TEMP_SUFFIX.sub("", semantic_key)
    suffix = Path(semantic_key).suffix.lower()
    filename = Path(semantic_key).name
    guessed = mimetypes.guess_type(filename)[0]
    if suffix in _IMAGE_EXTENSIONS:
        return "img", guessed or f"image/{suffix.lstrip('.')}", filename
    if suffix in _AUDIO_EXTENSIONS:
        return "audio", guessed or "audio/mpeg", filename
    if suffix in _VIDEO_EXTENSIONS:
        return "video", guessed or "video/mp4", filename
    if normalized.startswith("upload_scripts/") and suffix in _FILE_EXTENSIONS:
        return "file", guessed or "application/octet-stream", filename
    raise ValueError(f"无法识别 storage_key 媒资类型: {normalized}")


def _hash_file(path: str) -> tuple[int, str]:
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(path)
    digest = hashlib.sha256()
    size = 0
    with source.open("rb") as file_obj:
        while chunk := file_obj.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()
