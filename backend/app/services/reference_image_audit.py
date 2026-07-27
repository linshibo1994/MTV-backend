"""视频生成前的最终参考图审核。

审核状态保存在 ``ShotGroup.reference_image_audit``，但只在其中的 ``reference_hash``
与当前最终汇聚出的 storage_key/sha256 清单一致时有效。因此素材替换、主体资产换图或
引用调整后，旧的通过结果会自动失效并回到 ``pending``。
"""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol
from urllib.parse import urlencode, urlsplit

import httpx

from app.core.config import settings
from app.core.errors import ConflictError
from app.providers.storage import get_storage
from app.services.reference_aggregator import AggregatedReferences

ReferenceAuditStatus = Literal["pending", "approved", "rejected", "review", "not_required", "failed"]
_AUDIT_DIMENSIONS = [
    "porn", "sexy", "sensitive1", "sensitive2", "forbidden", "uncomfortable", "badpicture", "underage"
]
_AUDIT_TEXT_DIMENSIONS = ["charillegal", "defraud"]


@dataclass(frozen=True)
class ImageAuditDecision:
    status: Literal["approved", "rejected", "review"]
    advice: str | None
    labels: list[str]
    sub_labels: list[str]
    request_id: str | None


class ReferenceImageAuditProvider(Protocol):
    async def audit(self, *, image_url: str, data_id: str) -> ImageAuditDecision:
        """审核一张公网可访问图片。"""


class VolcengineReferenceImageAuditProvider:
    """veImageX ``SingleImageAudit`` 同步审核实现（AK/SK V4 签名）。"""

    _HOST = "imagex.volcengineapi.com"
    _SERVICE = "imagex"

    def __init__(self) -> None:
        if not settings.volc_image_audit_enabled:
            raise RuntimeError("reference_image_audit_disabled")
        if not settings.volc_imagex_access_key or not settings.volc_imagex_secret_key:
            raise RuntimeError("reference_image_audit_credentials_missing")
        if not settings.volc_imagex_service_id:
            raise RuntimeError("reference_image_audit_service_id_missing")

    async def audit(self, *, image_url: str, data_id: str) -> ImageAuditDecision:
        query = {
            "Action": "SingleImageAudit",
            "Version": "2023-05-01",
            "ServiceId": settings.volc_imagex_service_id,
        }
        body = {
            "AuditAbility": 1,
            "AuditDimensions": _AUDIT_DIMENSIONS,
            "AuditTextDimensions": _AUDIT_TEXT_DIMENSIONS,
            "EnableLargeImageDetect": True,
            "ImageUri": image_url,
            "DataId": data_id,
            "Async": 0,
        }
        payload = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()
        headers = _signed_headers(
            method="POST", host=self._HOST, path="/", query=query, payload=payload,
            access_key=settings.volc_imagex_access_key, secret_key=settings.volc_imagex_secret_key,
            region=settings.volcengine_region, service=self._SERVICE,
        )
        try:
            async with httpx.AsyncClient(timeout=30.0, trust_env=False) as client:
                response = await client.post(
                    f"https://{self._HOST}/?{urlencode(sorted(query.items()))}",
                    content=payload,
                    headers=headers,
                )
        except httpx.HTTPError as exc:
            raise RuntimeError("reference_image_audit_request_failed") from exc
        try:
            raw = response.json()
        except ValueError as exc:
            raise RuntimeError("reference_image_audit_invalid_response") from exc
        if response.status_code != 200 or (raw.get("ResponseMetadata") or {}).get("Error"):
            raise RuntimeError("reference_image_audit_request_rejected")
        result = raw.get("Result") or {}
        advice = result.get("Advice")
        image_type = result.get("ImageType")
        if advice == "block" or image_type == "problem":
            status: Literal["approved", "rejected", "review"] = "rejected"
        elif advice == "review":
            status = "review"
        else:
            status = "approved"
        metadata = raw.get("ResponseMetadata") or {}
        return ImageAuditDecision(
            status=status,
            advice=advice if isinstance(advice, str) else None,
            labels=[str(value) for value in result.get("Label") or []],
            sub_labels=[str(value) for value in result.get("SubLabel") or []],
            request_id=metadata.get("RequestId") if isinstance(metadata.get("RequestId"), str) else None,
        )


def get_reference_image_audit_provider() -> ReferenceImageAuditProvider:
    return VolcengineReferenceImageAuditProvider()


def reference_image_hash(references: AggregatedReferences) -> str | None:
    """返回最终提交给视频模型的参考图版本指纹，顺序也是模型输入的一部分。"""
    if not references.reference_images:
        return None
    payload = [
        {
            "storage_key": image.get("storage_key"), "sha256": image.get("sha256"),
            "mime_type": image.get("mime_type"), "size_bytes": image.get("size_bytes"),
        }
        for image in references.reference_images
    ]
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def include_first_frame(references: AggregatedReferences, first_frame: str | None) -> None:
    """将实际传给 Seedance 的链式首帧也加入审核清单。

    首帧来自上一镜头组的视频帧，未必对应 AssetEntry；其 storage_key 在本项目中为
    不可变产物键，因此使用该键的稳定摘要作为版本标识。
    """
    if not first_frame:
        return
    if any(image.get("storage_key") == first_frame for image in references.reference_images):
        return
    references.reference_images.insert(0, {
        "kind": "storage", "storage_key": first_frame, "mime_type": "image/png",
        "size_bytes": None, "sha256": hashlib.sha256(first_frame.encode()).hexdigest(),
    })
    references.reference_image_names.insert(0, "首帧参考")
    references.reference_image_categories.insert(0, "first_frame")


def current_audit_state(stored: dict | None, references: AggregatedReferences) -> dict:
    """读取可对前端公开的当前状态，陈旧快照绝不复用。"""
    digest = reference_image_hash(references)
    image_count = len(references.reference_images)
    if digest is None:
        return {"status": "not_required", "reference_hash": None, "image_count": 0, "results": []}
    if not isinstance(stored, dict) or stored.get("reference_hash") != digest:
        return {
            "status": "pending", "reference_hash": digest, "image_count": image_count,
            "results": [], "message": "参考图已变更或尚未审核",
        }
    status = stored.get("status")
    if status not in {"approved", "rejected", "review", "failed"}:
        status = "pending"
    return {
        "status": status, "reference_hash": digest, "image_count": image_count,
        "audited_at": stored.get("audited_at"),
        "results": stored.get("results") if isinstance(stored.get("results"), list) else [],
        "message": stored.get("message") if isinstance(stored.get("message"), str) else None,
    }


def audit_not_required_state(references: AggregatedReferences) -> dict:
    """为不使用火山视频供应商的任务构造明确的跳过审核状态。"""
    return {
        "status": "not_required",
        "reference_hash": reference_image_hash(references),
        "image_count": len(references.reference_images),
        "results": [],
        "message": "当前视频供应商不需要参考图审核",
    }


def public_audit_state(state: dict) -> dict:
    """移除仅供服务端校验的指纹及任何上游/存储标识后再返回给前端。"""
    return {
        "status": state["status"],
        "image_count": int(state.get("image_count") or 0),
        "audited_at": state.get("audited_at"),
        "results": [
            {"status": item.get("status")}
            for item in state.get("results") or []
            if isinstance(item, dict) and item.get("status") in {"approved", "rejected", "review"}
        ],
        "message": state.get("message"),
    }


async def audit_references(
    references: AggregatedReferences, *, provider: ReferenceImageAuditProvider | None = None
) -> dict:
    """同步审核全部最终参考图，任一 block/review 即不给出 approved。"""
    digest = reference_image_hash(references)
    if digest is None:
        return {"status": "not_required", "reference_hash": None, "image_count": 0, "results": []}
    provider = provider or get_reference_image_audit_provider()
    results: list[dict] = []
    final_status: ReferenceAuditStatus = "approved"
    for image in references.reference_images:
        storage_key = str(image["storage_key"])
        decision = await provider.audit(
            image_url=_public_image_url(storage_key),
            data_id=f"{digest}:{image.get('sha256', '')}",
        )
        results.append({
            # 对工作台只保存可行动的结论，避免暴露对象键、哈希、RequestId 或风险标签。
            "status": decision.status,
        })
        if decision.status == "rejected":
            final_status = "rejected"
        elif decision.status == "review" and final_status != "rejected":
            final_status = "review"
    return {
        "status": final_status, "reference_hash": digest, "image_count": len(references.reference_images),
        "audited_at": datetime.now(UTC).isoformat(), "results": results,
    }


def ensure_references_approved(stored: dict | None, references: AggregatedReferences) -> dict:
    state = current_audit_state(stored, references)
    if state["status"] not in {"approved", "not_required"}:
        message = {
            "pending": "参考图尚未审核或已变更，请先完成图片审核",
            "rejected": "参考图审核未通过，请替换图片后重新审核",
            "review": "参考图需人工复核，暂不能生成视频",
            "failed": "参考图审核失败，请稍后重试",
        }.get(state["status"], "参考图审核未通过")
        raise ConflictError(message)
    return state


def _public_image_url(storage_key: str) -> str:
    url = get_storage().signed_url(storage_key, expires_seconds=600)
    if not url:
        raise RuntimeError("reference_image_audit_url_unavailable")
    parsed = urlsplit(url)
    if parsed.scheme == "https" and parsed.netloc:
        return url
    # LocalStorageProvider 返回的是受鉴权保护的站内相对地址，不能伪装成公网 URL
    # 交给第三方；生产必须让存储层返回真实 HTTPS 预签名 URL 或专用下载通道。
    raise RuntimeError("reference_image_audit_url_unavailable")


def _signed_headers(
    *, method: str, host: str, path: str, query: dict[str, str], payload: bytes,
    access_key: str, secret_key: str, region: str, service: str,
) -> dict[str, str]:
    """按照火山引擎 V4 规范构造请求头；避免引入只为签名而存在的同步 SDK。"""
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    date = timestamp[:8]
    payload_hash = hashlib.sha256(payload).hexdigest()
    headers = {
        "Content-Type": "application/json", "Host": host,
        "X-Content-Sha256": payload_hash, "X-Date": timestamp,
    }
    signed = {key.lower(): value for key, value in headers.items()}
    canonical_headers = "".join(f"{key}:{signed[key]}\n" for key in sorted(signed))
    signed_headers = ";".join(sorted(signed))
    canonical_request = "\n".join([
        method, path, urlencode(sorted(query.items())), canonical_headers, signed_headers, payload_hash,
    ])
    scope = f"{date}/{region}/{service}/request"
    string_to_sign = "\n".join([
        "HMAC-SHA256", timestamp, scope, hashlib.sha256(canonical_request.encode()).hexdigest(),
    ])
    signing_key = hmac.new(secret_key.encode(), date.encode(), hashlib.sha256).digest()
    signing_key = hmac.new(signing_key, region.encode(), hashlib.sha256).digest()
    signing_key = hmac.new(signing_key, service.encode(), hashlib.sha256).digest()
    signing_key = hmac.new(signing_key, b"request", hashlib.sha256).digest()
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    headers["Authorization"] = (
        f"HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    )
    return headers
