"""阿里云 OSS 官方 Python SDK V2 的 ``StorageProvider`` 实现。

SDK 为同步接口，所有网络 I/O 均通过 ``asyncio.to_thread`` 执行。生产 Bucket 必须关闭
版本控制，否则 OSS 会忽略目标防覆盖请求头，破坏 ``promote_if_absent`` 的原子语义。
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta
from urllib.parse import urlsplit

import alibabacloud_oss_v2 as oss

from app.core.config import settings
from app.providers.storage import (
    StorageObjectNotFoundError,
    public_url_for_key,
    validate_key_prefix,
)

MAX_SIGNED_URL_SECONDS = 7 * 24 * 60 * 60
MAX_SIMPLE_OBJECT_BYTES = 5 * 1024 * 1024 * 1024
PRIVATE_OBJECT_ACL = "private"


class AliyunOSSStorageProvider:
    """使用 V4 签名和公网 Endpoint 的阿里云 OSS 存储提供者。"""

    def __init__(self) -> None:
        access_key_id = settings.aliyun_oss_access_key_id.get_secret_value().strip()
        access_key_secret = settings.aliyun_oss_access_key_secret.get_secret_value().strip()
        self._bucket = settings.aliyun_oss_bucket.strip()
        if not all(
            (
                settings.aliyun_oss_endpoint,
                access_key_id,
                access_key_secret,
                self._bucket,
                settings.aliyun_oss_region,
            )
        ):
            raise RuntimeError("阿里云 OSS 存储不可用：配置不完整")

        config = oss.config.load_default()
        config.credentials_provider = oss.credentials.StaticCredentialsProvider(
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
        )
        config.region = settings.aliyun_oss_region
        config.endpoint = settings.aliyun_oss_endpoint
        self._client = oss.Client(config)

    async def signed_url(self, key: str, *, expires_seconds: int = 300) -> str:
        rel = validate_key_prefix(key)
        if not 1 <= expires_seconds <= MAX_SIGNED_URL_SECONDS:
            raise ValueError("OSS 签名 URL 有效期必须在 1 秒至 7 天之间")
        result = self._client.presign(
            oss.GetObjectRequest(bucket=self._bucket, key=rel),
            expires=timedelta(seconds=expires_seconds),
        )
        url = result.url or ""
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.netloc:
            raise RuntimeError("阿里云 OSS 未返回有效的 HTTPS 签名 URL")
        return url

    async def external_read_url(
        self,
        key: str,
        *,
        expires_seconds: int = 300,
    ) -> str:
        del expires_seconds
        public_url = public_url_for_key(
            key,
            public_base=settings.aliyun_oss_public_url,
        )
        if public_url is not None:
            return public_url
        raise RuntimeError("OSS 对外读取需要配置 ALIYUN_OSS_PUBLIC_URL")

    async def put(self, key: str, data: bytes) -> str:
        rel = validate_key_prefix(key)
        self._validate_simple_object_size(len(data))
        request = oss.PutObjectRequest(
            bucket=self._bucket,
            key=rel,
            acl=PRIVATE_OBJECT_ACL,
            body=data,
        )
        await asyncio.to_thread(self._client.put_object, request)
        return rel

    async def put_file(self, key: str, path: str) -> str:
        rel = validate_key_prefix(key)
        await asyncio.to_thread(self._put_file, rel, path)
        return rel

    def _put_file(self, key: str, path: str) -> None:
        # 当前发布链路随后使用 CopyObject 晋升，源对象必须保持在其 5 GiB 上限内。
        self._validate_simple_object_size(os.path.getsize(path))
        request = oss.PutObjectRequest(
            bucket=self._bucket,
            key=key,
            acl=PRIVATE_OBJECT_ACL,
        )
        self._client.put_object_from_file(request, path)

    @staticmethod
    def _validate_simple_object_size(size_bytes: int) -> None:
        if size_bytes >= MAX_SIMPLE_OBJECT_BYTES:
            raise ValueError("阿里云 OSS 单对象写入与晋升目前仅支持小于 5 GiB 的文件")

    async def promote(self, source_key: str, final_key: str) -> str:
        source = validate_key_prefix(source_key)
        final = validate_key_prefix(final_key)
        await asyncio.to_thread(self._copy_then_delete, source, final)
        return final

    async def promote_if_absent(self, source_key: str, final_key: str) -> bool:
        source = validate_key_prefix(source_key)
        final = validate_key_prefix(final_key)
        return await asyncio.to_thread(self._copy_if_absent_then_delete, source, final)

    def _copy_then_delete(self, source: str, final: str) -> None:
        try:
            self._client.copy_object(self._copy_request(source, final))
        except oss.exceptions.OperationError as exc:
            if not self._is_not_found_error(exc):
                raise
            if self._object_exists(final):
                return
            raise StorageObjectNotFoundError(source) from exc
        self._delete_object(source)

    def _copy_if_absent_then_delete(self, source: str, final: str) -> bool:
        try:
            self._client.copy_object(
                self._copy_request(source, final, forbid_overwrite=True)
            )
        except oss.exceptions.OperationError as exc:
            if self._is_destination_conflict(exc):
                return False
            if self._is_not_found_error(exc):
                if self._object_exists(final):
                    return False
                raise StorageObjectNotFoundError(source) from exc
            raise
        self._delete_object(source)
        return True

    def _copy_request(
        self,
        source: str,
        final: str,
        *,
        forbid_overwrite: bool = False,
    ) -> oss.CopyObjectRequest:
        return oss.CopyObjectRequest(
            bucket=self._bucket,
            key=final,
            source_bucket=self._bucket,
            source_key=source,
            acl=PRIVATE_OBJECT_ACL,
            forbid_overwrite="true" if forbid_overwrite else None,
        )

    async def get(self, key: str) -> bytes:
        rel = validate_key_prefix(key)
        return await asyncio.to_thread(self._get_object, rel)

    def _get_object(self, key: str) -> bytes:
        result = self._client.get_object(
            oss.GetObjectRequest(bucket=self._bucket, key=key)
        )
        body = result.body
        if body is None:
            raise RuntimeError("阿里云 OSS 响应缺少对象内容")
        try:
            return body.read()
        finally:
            body.close()

    async def delete(self, key: str) -> None:
        rel = validate_key_prefix(key)
        await asyncio.to_thread(self._delete_object, rel)

    def _delete_object(self, key: str) -> None:
        self._client.delete_object(
            oss.DeleteObjectRequest(bucket=self._bucket, key=key)
        )

    async def exists(self, key: str) -> bool:
        rel = validate_key_prefix(key)
        return await asyncio.to_thread(self._object_exists, rel)

    def _object_exists(self, key: str) -> bool:
        return bool(self._client.is_object_exist(bucket=self._bucket, key=key))

    @staticmethod
    def _service_error(
        exc: oss.exceptions.OperationError,
    ) -> oss.exceptions.ServiceError | None:
        cause = exc.unwrap()
        return cause if isinstance(cause, oss.exceptions.ServiceError) else None

    @classmethod
    def _is_not_found_error(cls, exc: oss.exceptions.OperationError) -> bool:
        service_error = cls._service_error(exc)
        if service_error is None:
            return False
        return service_error.code == "NoSuchKey" or (
            service_error.status_code == 404
            and service_error.code in {"404", "BadErrorResponse", "NotFound"}
        )

    @classmethod
    def _is_destination_conflict(cls, exc: oss.exceptions.OperationError) -> bool:
        service_error = cls._service_error(exc)
        return bool(
            service_error is not None
            and service_error.status_code == 409
            and service_error.code == "FileAlreadyExists"
        )

    async def get_size(self, key: str) -> int:
        rel = validate_key_prefix(key)
        return await asyncio.to_thread(self._head_size, rel)

    def _head_size(self, key: str) -> int:
        result = self._client.head_object(
            oss.HeadObjectRequest(bucket=self._bucket, key=key)
        )
        if result.content_length is None:
            raise RuntimeError("阿里云 OSS 响应缺少对象大小")
        return int(result.content_length)

    async def iter_chunks(
        self,
        key: str,
        *,
        chunk_size: int = 65536,
        start: int = 0,
        end: int | None = None,
    ) -> AsyncIterator[bytes]:
        rel = validate_key_prefix(key)
        if chunk_size <= 0:
            raise ValueError("chunk_size 必须大于 0")
        total = await asyncio.to_thread(self._head_size, rel)
        if end is None or end >= total:
            end = total - 1
        if start < 0:
            start = 0
        if start > end:
            return

        body = await asyncio.to_thread(self._open_stream, rel, start, end)
        try:
            chunks = body.iter_bytes(block_size=chunk_size)
            while (chunk := await asyncio.to_thread(self._next_chunk, chunks)) is not None:
                if chunk:
                    yield chunk
        finally:
            await asyncio.to_thread(body.close)

    def _open_stream(self, key: str, start: int, end: int) -> oss.StreamBody:
        result = self._client.get_object(
            oss.GetObjectRequest(
                bucket=self._bucket,
                key=key,
                range_header=f"bytes={start}-{end}",
                range_behavior="standard",
            )
        )
        if result.body is None:
            raise RuntimeError("阿里云 OSS 响应缺少对象内容")
        return result.body

    @staticmethod
    def _next_chunk(chunks: Iterator[bytes]) -> bytes | None:
        return next(chunks, None)
