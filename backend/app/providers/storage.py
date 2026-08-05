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

# storage_key 允许的前缀白名单（防对象存储模式下越权拼接任意 key）
# - generate_images/<mid>/<id>.png    AI 生成图/视频
# - upload_images/<mid>/<id>.<ext>     用户上传图/视频/音频
# - generate_outputs/<script_id>/...  生成产物（时间线合成最终视频等，按 script_id 鉴权）
# - render_outputs/<script_id>/...     旧合成产物前缀，兼容存量记录
# - upload_scripts/<script_id>/...     原始剧本文档，按 script_id 鉴权
ALLOWED_KEY_PREFIXES: tuple[str, ...] = (
    "generate_images/",
    "upload_images/",
    "generate_outputs/",
    "render_outputs/",
    "upload_scripts/",
)

_storage_singleton: StorageProvider | None = None


class StorageObjectNotFoundError(FileNotFoundError):
    """promote 的源对象与目标对象均不存在，任务无法通过重试恢复。"""

    def __init__(self, storage_key: str) -> None:
        self.storage_key = normalize_key(storage_key)
        super().__init__("存储源对象不存在")


class StorageObjectIntegrityError(RuntimeError):
    """对象存在但内容摘要或大小不符合任务的持久化预期。"""

    def __init__(self, storage_key: str) -> None:
        self.storage_key = normalize_key(storage_key)
        super().__init__("存储对象完整性校验失败")


class StorageObjectConflictError(RuntimeError):
    """目标 key 已被与当前任务无关的对象占用。"""

    def __init__(self, storage_key: str) -> None:
        self.storage_key = normalize_key(storage_key)
        super().__init__("存储目标对象冲突")


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
        """将临时对象晋升为正式对象；两端均不存在时抛出 StorageObjectNotFoundError。"""
        ...

    async def promote_if_absent(self, source_key: str, final_key: str) -> bool:
        """原子地发布目标且绝不覆盖；本次发布返回 True，目标已存在返回 False。"""
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

    async def signed_url(self, key: str, *, expires_seconds: int = 300) -> str | None:
        """返回短期访问 URL；不支持直签时返回受控代理 URL 或 None。"""
        ...

    async def external_read_url(
        self,
        key: str,
        *,
        expires_seconds: int = 300,
    ) -> str | None:
        """返回供可信外部处理器直接读取的绝对 HTTPS URL。"""
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

    防止对象存储模式下绕过目录穿越防护拼接任意 key 下载他人资源。
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
    - ``oss``: 阿里云 OSS 官方 V2 SDK，依赖或配置缺失时 fail closed
    """
    global _storage_singleton
    if _storage_singleton is not None:
        return _storage_singleton

    if settings.storage_driver == "oss":
        try:
            from app.providers.storage_oss import AliyunOSSStorageProvider

            _storage_singleton = AliyunOSSStorageProvider()
            return _storage_singleton
        except ImportError as exc:
            raise RuntimeError("阿里云 OSS 存储不可用：缺少依赖") from exc

    if settings.storage_driver != "local":
        raise RuntimeError(f"未知存储驱动：{settings.storage_driver}")

    from app.providers.storage_local import LocalStorageProvider

    _storage_singleton = LocalStorageProvider()
    return _storage_singleton
