"""S3 兼容对象存储实现 —— 生产可选 (MinIO/COS)。

依赖 boto3，缺依赖时 ``import`` 失败由 ``app.providers.storage.get_storage`` 工厂回退本地存储。

boto3 为同步 SDK，所有网络 I/O 经 asyncio.to_thread 包装，避免阻塞 async 事件循环。
"""

from __future__ import annotations

import asyncio
import io
from collections.abc import AsyncIterator

import boto3  # noqa: F401  —— 缺失时本模块整体不可导入，由工厂捕获
from botocore.client import BaseClient  # noqa: F401
from botocore.exceptions import ClientError

from app.core.config import settings
from app.providers.storage import StorageObjectNotFoundError, validate_key_prefix


class S3StorageProvider:
    """S3 兼容存储提供者 (MinIO / 腾讯云 COS / AWS S3)。"""

    def __init__(self) -> None:
        self._bucket = settings.s3_bucket
        if not self._bucket or not settings.s3_access_key or not settings.s3_secret_key:
            raise RuntimeError("S3 存储不可用：配置不完整")
        self._client: BaseClient = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint or None,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region or None,
        )

    def signed_url(self, key: str, *, expires_seconds: int = 300) -> str:
        rel = validate_key_prefix(key)
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._bucket, "Key": rel},
            ExpiresIn=expires_seconds,
        )

    async def put(self, key: str, data: bytes) -> str:
        rel = validate_key_prefix(key)
        await asyncio.to_thread(
            self._client.upload_fileobj, io.BytesIO(data), self._bucket, rel
        )
        return rel

    async def put_file(self, key: str, path: str) -> str:
        rel = validate_key_prefix(key)
        await asyncio.to_thread(self._upload_file, path, rel)
        return rel

    def _upload_file(self, path: str, key: str) -> None:
        with open(path, "rb") as file_obj:
            self._client.upload_fileobj(file_obj, self._bucket, key)

    async def promote(self, source_key: str, final_key: str) -> str:
        source = validate_key_prefix(source_key)
        final = validate_key_prefix(final_key)
        await asyncio.to_thread(self._copy_then_delete, source, final)
        return final

    async def promote_if_absent(self, source_key: str, final_key: str) -> bool:
        source = validate_key_prefix(source_key)
        final = validate_key_prefix(final_key)
        return await asyncio.to_thread(self._put_if_absent_then_delete, source, final)

    def _copy_then_delete(self, source: str, final: str) -> None:
        try:
            self._client.copy_object(
                Bucket=self._bucket,
                Key=final,
                CopySource={"Bucket": self._bucket, "Key": source},
            )
        except ClientError as exc:
            if not self._is_not_found_error(exc):
                raise
            # copy/delete 已完成但任务状态提交前崩溃时，重放视为成功。
            if self._head_object(final):
                return
            raise StorageObjectNotFoundError(source) from exc
        self._client.delete_object(Bucket=self._bucket, Key=source)

    def _put_if_absent_then_delete(self, source: str, final: str) -> bool:
        """以目标条件写原子发布；不支持条件写的兼容端必须直接失败。"""
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=source)
        except ClientError as exc:
            if not self._is_not_found_error(exc):
                raise
            if self._head_object(final):
                return False
            raise StorageObjectNotFoundError(source) from exc

        body = response["Body"]
        try:
            try:
                self._client.put_object(
                    Bucket=self._bucket,
                    Key=final,
                    Body=body,
                    ContentLength=int(response["ContentLength"]),
                    IfNoneMatch="*",
                )
            except ClientError as exc:
                if self._is_precondition_failed(exc):
                    return False
                raise
        finally:
            body.close()
        self._client.delete_object(Bucket=self._bucket, Key=source)
        return True

    async def get(self, key: str) -> bytes:
        rel = validate_key_prefix(key)
        return await asyncio.to_thread(self._get_object, rel)

    def _get_object(self, key: str) -> bytes:
        buf = io.BytesIO()
        self._client.download_fileobj(self._bucket, key, buf)
        return buf.getvalue()

    async def delete(self, key: str) -> None:
        rel = validate_key_prefix(key)
        await asyncio.to_thread(
            self._client.delete_object, Bucket=self._bucket, Key=rel
        )

    async def exists(self, key: str) -> bool:
        rel = validate_key_prefix(key)
        return await asyncio.to_thread(self._head_object, rel)

    def _head_object(self, key: str) -> bool:
        # 仅将「对象不存在」(404) 视为 False；网络抖动、凭证失效、5xx 等
        # 真实故障必须向上抛出，避免 exists() 把故障误判为「不存在」、
        # 导致 get_media 对存在的对象返回 404 而非 500，触发错误的恢复流程。
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as exc:
            if self._is_not_found_error(exc):
                return False
            raise  # 其他 ClientError（5xx/凭证/网络）原样上抛

    @staticmethod
    def _is_not_found_error(exc: ClientError) -> bool:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = exc.response.get("Error", {}).get("Code")
        return status == 404 or code in ("404", "NoSuchKey", "NotFound")

    @staticmethod
    def _is_precondition_failed(exc: ClientError) -> bool:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = exc.response.get("Error", {}).get("Code")
        return status == 412 or code in ("412", "PreconditionFailed")

    async def get_size(self, key: str) -> int:
        rel = validate_key_prefix(key)
        return await asyncio.to_thread(self._head_size, rel)

    def _head_size(self, key: str) -> int:
        resp = self._client.head_object(Bucket=self._bucket, Key=key)
        return int(resp.get("ContentLength", 0))

    async def iter_chunks(
        self,
        key: str,
        *,
        chunk_size: int = 65536,
        start: int = 0,
        end: int | None = None,
    ) -> AsyncIterator[bytes]:
        """流式分块读取 S3 对象，支持 Range。

        利用 boto3 get_object 的流式 Body（StreamingBody），在线程中逐块拉取
        后 yield，不将整文件读入内存。
        """
        rel = validate_key_prefix(key)
        total = await asyncio.to_thread(self._head_size, rel)
        if end is None or end >= total:
            end = total - 1
        if start < 0:
            start = 0
        if start > end:
            return

        # 构造 Range header: bytes=start-end
        range_header = f"bytes={start}-{end}"
        # 在线程中打开流式响应，逐块读取
        stream_ctx = await asyncio.to_thread(self._open_stream, rel, range_header)
        try:
            while True:
                chunk = await asyncio.to_thread(stream_ctx.read, chunk_size)
                if not chunk:
                    break
                yield chunk
        finally:
            await asyncio.to_thread(stream_ctx.close)

    def _open_stream(self, key: str, range_header: str):
        """打开 S3 流式读取 Body（同步调用，由 to_thread 执行）。"""
        resp = self._client.get_object(
            Bucket=self._bucket, Key=key, Range=range_header
        )
        return resp["Body"]
