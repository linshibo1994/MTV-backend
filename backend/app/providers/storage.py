"""对象存储抽象 — StorageProvider 协议与工厂。

storage_key 为统一抽象路径，两类前缀：
- ``generate_images/<mid>/<id>.png`` —— AI 生成图
- ``upload_images/<mid>/<id>.<ext>`` —— 用户上传图/视频/音频

实际 URL 由前端拼接 ``settings.media_proxy_prefix + storage_key``，经鉴权代理路由 (T10) 流式返回。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, runtime_checkable

from app.core.config import settings

# storage_key 允许的前缀白名单（防 S3 模式下越权拼接任意 key）
# - generate_images/<mid>/<id>.png    AI 生成图/视频
# - upload_images/<mid>/<id>.<ext>     用户上传图/视频/音频
# - generate_outputs/<script_id>/...  生成产物（时间线合成最终视频等，按 script_id 鉴权）
# - render_outputs/<script_id>/...     旧合成产物前缀，兼容存量记录
ALLOWED_KEY_PREFIXES: tuple[str, ...] = (
    "generate_images/",
    "upload_images/",
    "generate_outputs/",
    "render_outputs/",
)

_storage_singleton: StorageProvider | None = None


@runtime_checkable
class StorageProvider(Protocol):
    """对象存储提供者协议。"""

    async def put(self, key: str, data: bytes) -> str:
        """写入数据，返回最终 storage_key。"""
        ...

    async def put_file(self, key: str, path: str) -> str:
        """从本地临时文件流式写入对象，返回最终 storage_key。"""
        ...

    async def promote(self, source_key: str, final_key: str) -> str:
        """将临时对象晋升为正式对象，返回最终 storage_key。"""
        ...

    async def get(self, key: str) -> bytes:
        """读取数据，返回完整字节流（小文件用）。"""
        ...

    async def delete(self, key: str) -> None:
        """删除对象。"""
        ...

    async def exists(self, key: str) -> bool:
        """对象是否存在。"""
        ...

    def signed_url(self, key: str, *, expires_seconds: int = 300) -> str | None:
        """返回短期访问 URL；不支持直签时返回受控代理 URL 或 None。"""
        ...

    async def get_size(self, key: str) -> int:
        """返回对象字节数（供 Range 响应 Content-Length 使用）。"""
        ...

    def iter_chunks(
        self,
        key: str,
        *,
        chunk_size: int = 65536,
        start: int = 0,
        end: int | None = None,
    ) -> AsyncIterator[bytes]:
        """流式分块读取（支持 Range），大文件真流式返回。

        - ``start`` 起始字节偏移（含）
        - ``end`` 结束字节偏移（含），None 表示读到末尾
        """
        ...


def normalize_key(key: str) -> str:
    """统一 storage_key —— 去除前导斜杠，保证相对路径。"""
    if not key:
        return key
    return key.lstrip("/")


def validate_key_prefix(key: str) -> str:
    """校验 storage_key 必须以允许的前缀开头，返回规范化后的 key。

    防止 S3 模式下绕过目录穿越防护拼接任意 key 下载他人资源。
    """
    rel = normalize_key(key)
    parts = rel.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"非法 storage_key 路径: {key}")
    if not rel.startswith(ALLOWED_KEY_PREFIXES):
        raise ValueError(f"非法 storage_key 前缀: {key}")
    return rel


def parse_mid_from_key(key: str) -> str | None:
    """从 storage_key 解析 material_id。

    ``generate_images/<mid>/<id>.png`` 或 ``upload_images/<mid>/<id>.<ext>``
    第二段为 mid；无法解析返回 None。
    """
    rel = normalize_key(key)
    parts = rel.split("/")
    # 形如 ["generate_images", "<mid>", "<file>"]
    if len(parts) >= 3 and parts[0] in ("generate_images", "upload_images"):
        return parts[1]
    return None


def get_storage() -> StorageProvider:
    """按 settings.storage_driver 返回存储提供者单例。

    - ``local``: 本地文件系统 (开发默认)
    - ``s3``: S3 兼容 (MinIO/COS)，依赖或配置缺失时 fail closed
    """
    global _storage_singleton
    if _storage_singleton is not None:
        return _storage_singleton

    if settings.storage_driver == "s3":
        try:
            from app.providers.storage_s3 import S3StorageProvider

            _storage_singleton = S3StorageProvider()
            return _storage_singleton
        except ImportError as exc:
            raise RuntimeError("S3 存储不可用：缺少依赖") from exc

    from app.providers.storage_local import LocalStorageProvider

    _storage_singleton = LocalStorageProvider()
    return _storage_singleton
