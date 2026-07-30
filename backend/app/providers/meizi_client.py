"""公司 MeiZi 媒资中心三段式上传与生命周期客户端。"""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Literal
from urllib.parse import urlsplit

import httpx

from app.core.config import Settings, settings
from app.providers._url_safety import validate_public_http_url
from app.providers.safe_http_transport import build_safe_async_http_transport

logger = logging.getLogger(__name__)

MeiziMediaKind = Literal["img", "audio", "video", "file"]

_PROCESSING_VIDEO_STATUSES = frozenset({3, 4, 5})


class MeiziClientError(RuntimeError):
    """不携带凭据或上游响应正文的安全错误。"""


@dataclass(frozen=True, slots=True)
class MeiziUploadResult:
    """完成媒资入库后可持久化的最小远端身份。"""

    media_id: int
    info_id: int
    lib_type: int
    media_kind: MeiziMediaKind
    public_url: str
    request_id: str


@dataclass(frozen=True, slots=True)
class MeiziUploadIdentity:
    """拿到 uploadToken 后立即持久化的远端身份，不包含临时凭据。"""

    media_id: int
    info_id: int
    lib_type: int
    media_kind: MeiziMediaKind
    request_id: str


IdentityCallback = Callable[[MeiziUploadIdentity], Awaitable[None]]
CompensationCallback = Callable[[MeiziUploadIdentity, bool], Awaitable[None]]


def _default_request_id() -> str:
    # MeiZi 参考实现使用十进制雪花 ID；63 位正整数保持同类协议形状。
    return str(uuid.uuid4().int & ((1 << 63) - 1))


def _default_front_request_id() -> str:
    # Java frontupload/info/shiftDelete 协议使用标准 UUID，而上传 token 使用十进制 ID。
    return str(uuid.uuid4())


def _default_clock_ms() -> int:
    return time.time_ns() // 1_000_000


class MeiziFileCenterClient:
    """封装 uploadToken → OSS 直传 → complete 以及 info/shiftDelete。"""

    def __init__(
        self,
        *,
        config: Settings = settings,
        http_client: httpx.AsyncClient | None = None,
        request_id_factory: Callable[[], str] = _default_request_id,
        front_request_id_factory: Callable[[], str] = _default_front_request_id,
        clock_ms: Callable[[], int] = _default_clock_ms,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._config = config
        self._http_client = http_client
        self._request_id_factory = request_id_factory
        self._front_request_id_factory = front_request_id_factory
        self._clock_ms = clock_ms
        self._sleep = sleep

    async def upload_bytes(
        self,
        *,
        filename: str,
        data: bytes,
        mime_type: str,
        media_kind: MeiziMediaKind,
        on_identity: IdentityCallback | None = None,
        on_compensation: CompensationCallback | None = None,
    ) -> MeiziUploadResult:
        with io.BytesIO(data) as stream:
            return await self._upload(
                filename=filename,
                stream=stream,
                mime_type=mime_type,
                media_kind=media_kind,
                on_identity=on_identity,
                on_compensation=on_compensation,
            )

    async def upload_path(
        self,
        *,
        filename: str,
        path: str,
        mime_type: str,
        media_kind: MeiziMediaKind,
        on_identity: IdentityCallback | None = None,
        on_compensation: CompensationCallback | None = None,
    ) -> MeiziUploadResult:
        source = Path(path)
        if not source.is_file():
            raise FileNotFoundError(path)
        # httpx multipart 会从文件句柄分块读取，避免把大视频整体载入内存。
        with source.open("rb") as stream:
            return await self._upload(
                filename=filename,
                stream=stream,
                mime_type=mime_type,
                media_kind=media_kind,
                on_identity=on_identity,
                on_compensation=on_compensation,
            )

    async def shift_delete(
        self,
        *,
        media_id: int,
        lib_type: int,
    ) -> None:
        async with self._http() as http:
            await self._shift_delete(http, media_id=media_id, lib_type=lib_type)

    async def resolve_public_url(
        self,
        *,
        media_kind: MeiziMediaKind,
        info_id: int,
    ) -> str:
        async with self._http() as http:
            return await self._resolve_info_url(
                http,
                media_kind=media_kind,
                info_id=info_id,
            )

    async def _upload(
        self,
        *,
        filename: str,
        stream: BinaryIO,
        mime_type: str,
        media_kind: MeiziMediaKind,
        on_identity: IdentityCallback | None,
        on_compensation: CompensationCallback | None,
    ) -> MeiziUploadResult:
        safe_filename = self._validate_filename(filename)
        lib_type = self._lib_type(media_kind)
        token_request = self._upload_token_request(
            filename=safe_filename,
            lib_type=lib_type,
        )
        upload_token: dict[str, object] | None = None
        media_id: int | None = None
        info_id: int | None = None
        identity: MeiziUploadIdentity | None = None

        async with self._http() as http:
            try:
                try:
                    token_response = await http.post(
                        self._endpoint(f"/{media_kind}/uploadToken"),
                        data=token_request,
                    )
                except httpx.HTTPError:
                    raise MeiziClientError("meizi_upload_token_failed") from None
                token_payload = self._business_payload(
                    token_response,
                    operation="upload_token",
                )
                raw_upload_token = token_payload.get("uploadToken")
                if not isinstance(raw_upload_token, dict):
                    raise MeiziClientError("meizi_upload_token_invalid")
                upload_token = raw_upload_token
                media_id = self._positive_int(upload_token.get("id"), "media_id")
                raw_info_id = upload_token.get("videoId") if media_kind == "video" else media_id
                info_id = self._positive_int(raw_info_id or media_id, "info_id")
                identity = MeiziUploadIdentity(
                    media_id=media_id,
                    info_id=info_id,
                    lib_type=lib_type,
                    media_kind=media_kind,
                    request_id=token_request["requestId"],
                )
                if on_identity is not None:
                    await on_identity(identity)

                await self._upload_to_object_store(
                    http,
                    upload_token=upload_token,
                    stream=stream,
                    mime_type=mime_type,
                )
                await self._complete_upload(
                    http,
                    media_kind=media_kind,
                    token_request=token_request,
                    media_id=media_id,
                )
                public_url = await self._public_url_after_upload(
                    http,
                    media_kind=media_kind,
                    info_id=info_id,
                    cover_url=upload_token.get("coverUrl"),
                )
                return MeiziUploadResult(
                    media_id=media_id,
                    info_id=info_id,
                    lib_type=lib_type,
                    media_kind=media_kind,
                    public_url=public_url,
                    request_id=token_request["requestId"],
                )
            except Exception:
                compensated = False
                if media_id is not None:
                    try:
                        await self._shift_delete(
                            http,
                            media_id=media_id,
                            lib_type=lib_type,
                        )
                        compensated = True
                    except Exception:
                        # 不能记录签名 URL、临时凭据或响应正文。
                        logger.warning(
                            "meizi_upload_compensation_failed kind=%s media_id=%s",
                            media_kind,
                            media_id,
                        )
                if identity is not None and on_compensation is not None:
                    try:
                        await on_compensation(identity, compensated)
                    except Exception:
                        logger.warning(
                            "meizi_upload_compensation_state_failed kind=%s media_id=%s",
                            media_kind,
                            media_id,
                        )
                raise

    async def _upload_to_object_store(
        self,
        http: httpx.AsyncClient,
        *,
        upload_token: dict[str, object],
        stream: BinaryIO,
        mime_type: str,
    ) -> None:
        upload_url = self._required_text(upload_token, "uploadUrl")
        self._require_upload_https_url(upload_url)
        key = self._required_text(upload_token, "key")
        token = self._required_text(upload_token, "token")
        cloud_type = self._positive_int(upload_token.get("cloudType"), "cloud_type")
        if cloud_type == 1:
            form = {"key": key, "token": token}
        else:
            form = {
                "key": key,
                "OSSAccessKeyId": self._required_text(upload_token, "accessid"),
                "policy": self._required_text(upload_token, "policy"),
                "Signature": token,
            }
            callback = upload_token.get("callback")
            if isinstance(callback, str) and callback:
                form["callback"] = callback

        stream.seek(0)
        try:
            response = await http.post(
                upload_url,
                data=form,
                files={"file": ("file", stream, mime_type)},
            )
            response.raise_for_status()
        except httpx.HTTPError:
            raise MeiziClientError("meizi_object_upload_failed") from None

    async def _complete_upload(
        self,
        http: httpx.AsyncClient,
        *,
        media_kind: MeiziMediaKind,
        token_request: dict[str, str],
        media_id: int,
    ) -> None:
        params = {**token_request, "id": str(media_id), "sync": "1"}
        try:
            response = await http.post(
                self._endpoint(f"/{media_kind}/complete"),
                params=params,
                json=[],
            )
            response.raise_for_status()
        except httpx.HTTPError:
            raise MeiziClientError("meizi_complete_failed") from None
        if response.content:
            self._business_payload(response, operation="complete")

    async def _public_url_after_upload(
        self,
        http: httpx.AsyncClient,
        *,
        media_kind: MeiziMediaKind,
        info_id: int,
        cover_url: object,
    ) -> str:
        if media_kind != "video" and isinstance(cover_url, str):
            try:
                self._require_public_media_url(cover_url)
            except (ValueError, MeiziClientError):
                pass
            else:
                return cover_url

        try:
            return await self._resolve_info_url(
                http,
                media_kind=media_kind,
                info_id=info_id,
            )
        except MeiziClientError:
            if media_kind != "video" and isinstance(cover_url, str):
                self._require_public_media_url(cover_url)
                return cover_url
            raise

    async def _resolve_info_url(
        self,
        http: httpx.AsyncClient,
        *,
        media_kind: MeiziMediaKind,
        info_id: int,
    ) -> str:
        attempts = (
            self._config.meizi_processing_poll_attempts
            if media_kind == "video"
            else 1
        )
        for attempt in range(attempts):
            try:
                response = await http.get(
                    self._endpoint(f"/{media_kind}/info"),
                    params={**self._front_auth_params(), "id": str(info_id)},
                )
            except httpx.HTTPError:
                raise MeiziClientError("meizi_media_info_failed") from None
            data = self._business_payload(response, operation="media_info")
            status = data.get("status")
            url = self._extract_info_url(media_kind, data)
            if url:
                self._require_public_media_url(url)
                if media_kind != "video" or status in {None, 1}:
                    return url
            if media_kind != "video" or status not in _PROCESSING_VIDEO_STATUSES:
                raise MeiziClientError("meizi_public_url_unavailable")
            if attempt + 1 < attempts:
                await self._sleep(
                    self._config.meizi_processing_poll_interval_seconds
                )
        raise MeiziClientError("meizi_media_processing_timeout")

    async def _shift_delete(
        self,
        http: httpx.AsyncClient,
        *,
        media_id: int,
        lib_type: int,
    ) -> None:
        try:
            response = await http.post(
                self._endpoint("/media/shiftDelete"),
                params={
                    **self._front_auth_params(),
                    "id": str(media_id),
                    "libType": str(lib_type),
                },
                headers={"Content-Type": "application/json"},
            )
        except httpx.HTTPError:
            raise MeiziClientError("meizi_shift_delete_failed") from None
        self._business_payload(response, operation="shift_delete")

    @asynccontextmanager
    async def _http(self) -> AsyncIterator[httpx.AsyncClient]:
        if self._http_client is not None:
            yield self._http_client
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

    def _upload_token_request(self, *, filename: str, lib_type: int) -> dict[str, str]:
        request_id = self._request_id_factory()
        timestamp = self._clock_ms()
        token = self._signature(
            request_id=request_id,
            timestamp=timestamp,
            tenant_id=self._config.meizi_upload_tenant_id,
            group_id=self._config.meizi_upload_group_id,
            user_name=self._config.meizi_upload_user_name,
            source=self._config.meizi_upload_source,
            salt=self._config.meizi_upload_salt.get_secret_value(),
        )
        return {
            "requestId": request_id,
            "timeStamp": str(timestamp),
            "tenantId": str(self._config.meizi_upload_tenant_id),
            "groupId": str(self._config.meizi_upload_group_id),
            "userName": self._config.meizi_upload_user_name,
            "source": str(self._config.meizi_upload_source),
            "token": token,
            "customDomainId": "-1",
            "fileName": filename,
            "libType": str(lib_type),
        }

    def _front_auth_params(self) -> dict[str, str]:
        request_id = self._front_request_id_factory()
        timestamp = self._clock_ms()
        user_name = self._config.meizi_front_user_name or ""
        token = self._signature(
            request_id=request_id,
            timestamp=timestamp,
            tenant_id=self._config.meizi_front_tenant_id,
            group_id=self._config.meizi_front_group_id,
            user_name=user_name,
            source=self._config.meizi_front_source,
            salt=self._config.meizi_front_salt.get_secret_value(),
        )
        return {
            "requestId": request_id,
            "userName": user_name,
            "tenantId": str(self._config.meizi_front_tenant_id),
            "groupId": str(self._config.meizi_front_group_id),
            "timeStamp": str(timestamp),
            "token": token,
            "source": str(self._config.meizi_front_source),
            "customDomainId": "-1",
        }

    @staticmethod
    def _signature(
        *,
        request_id: str,
        timestamp: int,
        tenant_id: int,
        group_id: int,
        user_name: str,
        source: int,
        salt: str,
    ) -> str:
        raw = (
            f"{request_id}:{timestamp}:{tenant_id}:{group_id}:"
            f"{user_name}:{source}:{salt}"
        )
        return hashlib.md5(  # noqa: S324 - 第三方协议要求 MD5，不用于密码安全
            raw.encode("utf-8"),
            usedforsecurity=False,
        ).hexdigest()

    def _endpoint(self, path: str) -> str:
        return f"{self._config.meizi_base_url.rstrip('/')}/{path.lstrip('/')}"

    def _lib_type(self, media_kind: MeiziMediaKind) -> int:
        return {
            "img": self._config.meizi_lib_type_image,
            "audio": self._config.meizi_lib_type_audio,
            "video": self._config.meizi_lib_type_video,
            "file": self._config.meizi_lib_type_file,
        }[media_kind]

    @staticmethod
    def _business_payload(
        response: httpx.Response,
        *,
        operation: str,
    ) -> dict[str, object]:
        try:
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError):
            # HTTPStatusError 会携带完整签名 URL；禁止通过异常链进入日志。
            raise MeiziClientError(f"meizi_{operation}_failed") from None
        if not isinstance(payload, dict) or payload.get("code") != 0:
            raise MeiziClientError(f"meizi_{operation}_rejected")
        data = payload.get("data")
        if data is None:
            return {}
        if not isinstance(data, dict):
            raise MeiziClientError(f"meizi_{operation}_invalid")
        return data

    @staticmethod
    def _extract_info_url(
        media_kind: MeiziMediaKind,
        data: dict[str, object],
    ) -> str | None:
        nested_name = {
            "img": "mimg",
            "audio": "maudio",
            "video": "mvideo",
            "file": "mfile",
        }[media_kind]
        direct_name = {
            "img": "imgUrl",
            "audio": "audioUrl",
            "video": "videoUrl",
            "file": "fileUrl",
        }[media_kind]
        direct = data.get(direct_name)
        if isinstance(direct, str) and direct:
            return direct
        nested = data.get(nested_name)
        if isinstance(nested, dict):
            for name in ("url", "downloadUrl"):
                value = nested.get(name)
                if isinstance(value, str) and value:
                    return value
        return None

    @staticmethod
    def _positive_int(value: object, field_name: str) -> int:
        if isinstance(value, bool):
            raise MeiziClientError(f"meizi_{field_name}_invalid")
        try:
            parsed = int(value)  # type: ignore[call-overload]
        except (TypeError, ValueError) as exc:
            raise MeiziClientError(f"meizi_{field_name}_invalid") from exc
        if parsed <= 0:
            raise MeiziClientError(f"meizi_{field_name}_invalid")
        return parsed

    @staticmethod
    def _required_text(payload: dict[str, object], field_name: str) -> str:
        value = payload.get(field_name)
        if not isinstance(value, str) or not value:
            raise MeiziClientError(f"meizi_{field_name}_missing")
        return value

    @staticmethod
    def _validate_filename(filename: str) -> str:
        # 只把 basename 发给第三方，拒绝空名和控制字符。
        name = Path(filename).name.strip()
        if (
            not name
            or name in {".", ".."}
            or len(name) > 255
            or any(ord(char) < 32 for char in name)
        ):
            raise ValueError("meizi_filename_invalid")
        return name

    def _require_upload_https_url(self, url: str) -> None:
        self._require_https_url(url, allowed_hosts=self._config.meizi_upload_allowed_hosts)

    def _require_public_media_url(self, url: str) -> None:
        self._require_https_url(
            url,
            allowed_hosts=self._config.meizi_public_url_allowed_hosts,
        )

    @staticmethod
    def _require_https_url(url: str, *, allowed_hosts: list[str]) -> None:
        try:
            validate_public_http_url(url)
            parsed = httpx.URL(url)
            split = urlsplit(url)
        except (TypeError, ValueError, httpx.InvalidURL):
            raise MeiziClientError("meizi_url_not_allowed") from None
        host = parsed.host.rstrip(".").lower()
        if (
            parsed.scheme != "https"
            or split.username is not None
            or split.password is not None
            or split.fragment
            or (allowed_hosts and host not in allowed_hosts)
        ):
            raise MeiziClientError("meizi_url_not_allowed")
