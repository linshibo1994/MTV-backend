"""对象存储抽象 — StorageProvider 协议与工厂。

storage_key 为统一抽象路径，两类前缀：
- ``generate_images/<mid>/<id>.png`` —— AI 生成图
- ``upload_images/<mid>/<id>.<ext>`` —— 用户上传图/视频/音频

实际 URL 由前端拼接 ``settings.media_proxy_prefix + storage_key``，经鉴权代理路由 (T10) 流式返回。
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote

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
OSS_OBJECT_KEY_ROOT = "MTV"

_PUBLIC_URL_FIELD_NAMES: dict[str, str] = {
    "storage_key": "public_url",
    "storageKey": "publicUrl",
    "video_storage_key": "video_public_url",
    "output_storage_key": "output_public_url",
    "script_file_storage_key": "script_file_public_url",
    "thumbnail_storage_key": "thumbnail_public_url",
    "first_frame": "first_frame_public_url",
    "last_frame": "last_frame_public_url",
    "reference_frame": "reference_frame_public_url",
}

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
    logical_key = _logical_storage_key(rel)
    if not logical_key.startswith(ALLOWED_KEY_PREFIXES):
        raise ValueError(f"非法 storage_key 前缀: {key}")
    return rel


def _logical_storage_key(key: str) -> str:
    """移除 OSS 对象根目录，仅供现有权限与归属规则解析。"""
    root_prefix = f"{OSS_OBJECT_KEY_ROOT}/"
    return key.removeprefix(root_prefix)


def logical_storage_key(key: str) -> str:
    """返回不含对象根目录的合法业务路径，供路由与鉴权解析。"""
    return _logical_storage_key(validate_key_prefix(key))


def storage_key_for_write(key: str) -> str:
    """为新写入的 OSS 对象增加固定 ``MTV/`` 根目录，local 保持原路径。"""
    rel = validate_key_prefix(key)
    if getattr(settings, "storage_driver", "local") == "oss" and not rel.startswith(
        f"{OSS_OBJECT_KEY_ROOT}/"
    ):
        return f"{OSS_OBJECT_KEY_ROOT}/{rel}"
    return rel


def public_url_for_key(key: str, *, public_base: str | None = None) -> str | None:
    """返回配置的 CDN 公网 URL；未启用 OSS 公网域名时返回 ``None``。"""
    if public_base is None:
        if getattr(settings, "storage_driver", "local") != "oss":
            return None
        public_base = str(getattr(settings, "aliyun_oss_public_url", "") or "")
    public_base = public_base.rstrip("/")
    if not public_base:
        return None
    rel = validate_key_prefix(key)
    return f"{public_base}/{quote(rel, safe='/')}"


def public_url_for_output_key(key: object) -> str | None:
    """为 API 出参安全尝试生成 URL；历史脏数据不应导致响应失败。"""
    if not isinstance(key, str) or not key:
        return None
    try:
        return public_url_for_key(key)
    except ValueError:
        return None


def public_urls_for_payload(value: Any) -> Any:
    """为 API 输出中的对象键追加 CDN URL，且不改写持久化 JSON。"""
    if isinstance(value, dict):
        result = {key: public_urls_for_payload(item) for key, item in value.items()}
        for storage_key_field, public_url_field in _PUBLIC_URL_FIELD_NAMES.items():
            storage_key = result.get(storage_key_field)
            public_url = public_url_for_output_key(storage_key)
            if public_url is not None:
                result[public_url_field] = public_url
        return result
    if isinstance(value, list):
        return [public_urls_for_payload(item) for item in value]
    if isinstance(value, tuple):
        return tuple(public_urls_for_payload(item) for item in value)
    return value


def parse_mid_from_key(key: str) -> str | None:
    """从 storage_key 解析 material_id。

    ``generate_images/<mid>/<id>.png`` 或 ``upload_images/<mid>/<id>.<ext>``
    第二段为 mid；无法解析返回 None。
    """
    rel = _logical_storage_key(normalize_key(key))
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
