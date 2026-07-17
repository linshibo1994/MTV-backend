"""媒体代理模块 API 路由 —— T10 (sapi 等价)。

挂载于 ``settings.media_proxy_prefix`` (= ``/api/x/cctv/zhihui/flow-app``)，无额外 prefix。

端点:
- GET  /{storage_key:path}                       鉴权代理流式返回媒体
- POST /upload_images/{mid}/{filename}          上传 (校验真实 MIME 后存储)

storage_key 两类前缀 (经 validate_key_prefix 白名单校验):
- ``generate_images/<mid>/<id>.png`` (AI 生成)
- ``upload_images/<mid>/<id>.<ext>`` (用户上传)

鉴权 (08§4.1 资源级授权):
- get_media: 从 storage_key 解析 mid -> Materials -> Script -> check_script_access(require_write=False)
- upload_image: 路径参数 mid -> Materials -> Script -> check_script_access(require_write=True)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import AsyncIterator
from urllib.parse import quote

from fastapi import APIRouter, Depends, Header, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_user
from app.core.config import settings
from app.core.database import get_db
from app.core.errors import ForbiddenError, NotFoundError
from app.core.ids import new_id
from app.core.permissions import Action, check_script_access
from app.crud.asset import add_candidate
from app.models.asset_entry import AssetEntry
from app.models.generation_job import GenerationJob
from app.models.materials import Materials
from app.models.script import Script
from app.models.shot_group import ShotGroup
from app.models.timeline_render_job import TimelineRenderJob
from app.models.user import User
from app.models.video_generation_job import VideoGenerationJob
from app.providers.storage import (
    get_storage,
    parse_mid_from_key,
    validate_key_prefix,
)
from app.services import media_storage_jobs

router = APIRouter(tags=["媒体代理"])
logger = logging.getLogger(__name__)

# 允许上传的真实 MIME 白名单。
_ALLOWED_UPLOAD_MIME_TYPES: set[str] = (
    {
        "image/png",
        "image/jpeg",
        "image/webp",
        "audio/mpeg",
        "audio/wav",
        "audio/mp4",
        "audio/aac",
        "audio/ogg",
        "video/mp4",
        "video/webm",
        "video/quicktime",
    }
)

# 扩展名 → MIME 兜底映射 (探测失败时使用)
_EXT_MIME: dict[str, str] = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".mp4": "video/mp4",
    ".webm": "video/webm",
    ".mov": "video/quicktime",
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
    ".m4a": "audio/mp4",
    ".aac": "audio/aac",
    ".ogg": "audio/ogg",
}

_PIL_FORMAT_MIME: dict[str, str] = {
    "png": "image/png",
    "jpeg": "image/jpeg",
    "webp": "image/webp",
}

_MIME_EXTENSION: dict[str, str] = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/webp": ".webp",
    "audio/mpeg": ".mp3",
    "audio/wav": ".wav",
    "audio/mp4": ".m4a",
    "audio/aac": ".aac",
    "audio/ogg": ".ogg",
    "video/mp4": ".mp4",
    "video/webm": ".webm",
    "video/quicktime": ".mov",
}


def _probe_video_with_ffprobe(path: str) -> bool | None:
    """用 ffprobe 校验视频流可解析性。

    返回 True 表示可解析、False 表示不可解析；ffprobe 不可用时返回 None，
    由调用方决定是否回退其他探测手段。
    """
    import shutil
    import subprocess

    ffprobe = shutil.which("ffprobe")
    if ffprobe is None:
        return None
    try:
        result = subprocess.run(
            [
                ffprobe, "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_type",
                "-of", "csv=p=0",
                path,
            ],
            capture_output=True,
            timeout=30,
        )
        return result.returncode == 0 and b"video" in result.stdout
    except Exception:  # noqa: BLE001  探测异常视为不可解析
        return False


def _mime_from_probe(probe: dict) -> str | None:
    streams = probe.get("streams") or []
    format_info = probe.get("format") or {}
    format_name = str(format_info.get("format_name") or "").lower()
    format_long_name = str(format_info.get("format_long_name") or "").lower()
    tags = format_info.get("tags") or {}
    major_brand = str(tags.get("major_brand") or "").lower()
    has_video = any(stream.get("codec_type") == "video" for stream in streams)
    has_audio = any(stream.get("codec_type") == "audio" for stream in streams)
    audio_codecs = {
        str(stream.get("codec_name") or "").lower()
        for stream in streams
        if stream.get("codec_type") == "audio"
    }

    if has_video:
        if major_brand == "qt" or "quicktime" in format_long_name:
            return "video/quicktime"
        if "webm" in format_name or "matroska" in format_name:
            return "video/webm"
        if any(part in format_name for part in ("mp4", "mov", "m4v", "3gp")):
            return "video/mp4"
        return None

    if has_audio:
        if "wav" in format_name:
            return "audio/wav"
        if "mp3" in format_name or "mp3" in audio_codecs:
            return "audio/mpeg"
        if "aac" in format_name or "aac" in audio_codecs:
            return "audio/aac"
        if "ogg" in format_name:
            return "audio/ogg"
        if any(part in format_name for part in ("mov", "mp4", "m4a")):
            return "audio/mp4"
    return None


def _probe_av_mime_with_ffprobe(path: str) -> str | None:
    """用 ffprobe 返回真实音视频 MIME；工具缺失或不可解析返回 None。"""
    import shutil
    import subprocess

    ffprobe = shutil.which("ffprobe")
    if ffprobe is None:
        return None
    try:
        result = subprocess.run(
            [
                ffprobe,
                "-v",
                "error",
                "-show_streams",
                "-show_format",
                "-of",
                "json",
                path,
            ],
            capture_output=True,
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            return None
        return _mime_from_probe(json.loads(result.stdout.decode("utf-8") or "{}"))
    except Exception:  # noqa: BLE001
        return None


def _probe_av_mime_with_ffmpeg_python(path: str) -> str | None:
    """ffmpeg-python probe 兜底；不可用或不可解析返回 None。"""
    try:
        import ffmpeg  # type: ignore

        return _mime_from_probe(ffmpeg.probe(path))
    except Exception:  # noqa: BLE001
        return None


def _detect_mime(path: str, filename: str) -> str:
    """基于临时文件路径探测真实 MIME —— Pillow(图) / pydub(音频) / ffmpeg(视频)。

    直接对落盘的临时文件做媒体解析，避免将整文件载入内存；
    视频/音频探测依赖缺失或解析失败时 fail-closed 返回 ``application/octet-stream``，
    由调用方按白名单拒绝，符合 08§4.1「上传内容校验真实 MIME 与媒体解析结果」，
    防止可执行文件改后缀冒充 video/* / audio/* 通过校验。
    """
    ext = os.path.splitext(filename)[1].lower()

    # 图片：Pillow 直接打开文件路径（检测文件头，最可靠）
    try:
        from PIL import Image  # type: ignore

        with Image.open(path) as img:
            img.verify()
            fmt = (img.format or "").lower()
            if fmt in _PIL_FORMAT_MIME:
                return _PIL_FORMAT_MIME[fmt]
            if fmt:
                return "application/octet-stream"
    except Exception:  # noqa: BLE001  非图片或 Pillow 缺失
        pass

    # 音频/视频：从真实容器/codec 探测结果映射 MIME；探测工具缺失或解析失败
    # fail-closed，不按扩展名返回。
    if ext in _EXT_MIME and _EXT_MIME[ext].startswith("audio/"):
        return (
            _probe_av_mime_with_ffprobe(path)
            or _probe_av_mime_with_ffmpeg_python(path)
            or "application/octet-stream"
        )

    if ext in _EXT_MIME and _EXT_MIME[ext].startswith("video/"):
        return (
            _probe_av_mime_with_ffprobe(path)
            or _probe_av_mime_with_ffmpeg_python(path)
            or "application/octet-stream"
        )

    return "application/octet-stream"


def _storage_key_for_upload(mid: str, filename: str, mime_type: str | None = None) -> str:
    """构造上传 storage_key: ``upload_images/<mid>/<id>.<ext>``。"""
    ext = _MIME_EXTENSION.get(mime_type or "") or os.path.splitext(filename)[1].lower()
    return f"upload_images/{mid}/{new_id()}{ext}"


def _temp_storage_key_for_upload(mid: str, filename: str, mime_type: str | None = None) -> str:
    """构造上传临时 key，仍位于允许的 upload_images 前缀下。"""
    ext = _MIME_EXTENSION.get(mime_type or "") or os.path.splitext(filename)[1].lower()
    return f"upload_images/_tmp/{mid}/{new_id()}{ext}"


async def _record_delete_job(db: AsyncSession, storage_key: str, asset_id: str | None = None) -> None:
    await media_storage_jobs.enqueue_delete(
        db,
        idempotency_key=f"delete:{storage_key}",
        source_key=storage_key,
        asset_id=asset_id,
    )
    await db.commit()


async def _promote_storage(storage, source_key: str, final_key: str) -> str:
    promote = getattr(storage, "promote", None)
    if promote is not None:
        return await promote(source_key, final_key)
    if not hasattr(storage, "get"):
        return final_key
    data = await storage.get(source_key)
    await storage.put(final_key, data)
    await storage.delete(source_key)
    return final_key


async def _put_temp_object(storage, key: str, path: str) -> str:
    put_file = getattr(storage, "put_file", None)
    if put_file is not None:
        return await put_file(key, path)
    # 兼容旧测试替身和旧 provider；生产 provider 应实现 put_file。
    import asyncio

    with open(path, "rb") as file_obj:
        data = await asyncio.to_thread(file_obj.read)
    return await storage.put(key, data)


def _safe_download_filename(storage_key: str) -> str:
    name = os.path.basename(validate_key_prefix(storage_key))
    safe = "".join(
        char if char.isascii() and char.isprintable() and char not in {'"', "\\", "/", "\r", "\n"} else "_"
        for char in name
    ).strip(" ._")
    return safe or "download"


def _content_disposition(storage_key: str) -> str:
    filename = _safe_download_filename(storage_key)
    return f"inline; filename=\"{filename}\"; filename*=UTF-8''{quote(filename)}"


async def _run_promote_job(
    db: AsyncSession, storage, job_id: str, *, owner: str = "upload-request"
) -> None:
    claim = await media_storage_jobs.claim_job(db, job_id, owner=owner, lease_seconds=300)
    if claim is None:
        return
    try:
        if claim.job_type != "promote" or not claim.final_key:
            raise RuntimeError("invalid media storage job")
        await _promote_storage(storage, claim.source_key, claim.final_key)
        await media_storage_jobs.mark_succeeded(db, claim.id, claim.lease_token)
        await db.commit()
    except Exception as exc:
        await media_storage_jobs.mark_retry(db, claim.id, claim.lease_token, exc)
        await db.commit()
        raise


def _parse_range(range_header: str | None, total: int) -> tuple[int, int] | None:
    """解析 Range 请求头，返回 (start, end) 含端点；无 Range 返回 None。

    支持 ``bytes=start-end`` / ``bytes=start-`` / ``bytes=-suffix``。
    """
    if not range_header or not range_header.startswith("bytes="):
        return None
    spec = range_header[len("bytes="):].strip()
    if "," in spec:
        return None
    if "-" not in spec:
        return None
    start_s, end_s = spec.split("-", 1)
    start_s = start_s.strip()
    end_s = end_s.strip()
    try:
        if start_s == "":
            # 后缀范围: bytes=-N 取最后 N 字节
            suffix = int(end_s)
            if suffix <= 0:
                return None
            start = max(0, total - suffix)
            end = total - 1
        else:
            start = int(start_s)
            end = int(end_s) if end_s else total - 1
    except ValueError:
        return None
    if start < 0 or start >= total:
        return None
    if end >= total:
        end = total - 1
    if end < start:
        return None
    return start, end


def _key_matches(value: object, rel: str) -> bool:
    """在 JSON 输出中递归查找精确 storage_key，兼容前导斜杠。"""
    if isinstance(value, str):
        return value.lstrip("/") == rel
    if isinstance(value, dict):
        return any(_key_matches(item, rel) for item in value.values())
    if isinstance(value, list):
        return any(_key_matches(item, rel) for item in value)
    return False


async def _load_script(db: AsyncSession, script_id: str) -> Script:
    script = (
        await db.execute(select(Script).where(Script.id == script_id))
    ).scalar_one_or_none()
    if script is None:
        raise NotFoundError("剧本不存在")
    return script


async def _authorize_script(
    db: AsyncSession, script_id: str, user: User, require_write: bool
) -> None:
    script = await _load_script(db, script_id)
    await check_script_access(
        db, user, script, action=Action.EDIT if require_write else Action.READ
    )


async def _authorize_video_media_key(
    db: AsyncSession, rel: str, user: User, require_write: bool
) -> None:
    """授权 video worker 产物和末帧 key，必须精确绑定 DB 记录。"""
    video_jobs = list((await db.execute(select(VideoGenerationJob))).scalars().all())
    for job in video_jobs:
        if _key_matches(job.output_json, rel):
            await _authorize_script(db, job.script_id, user, require_write)
            return

    shot_groups = list((await db.execute(select(ShotGroup))).scalars().all())
    for group in shot_groups:
        if _key_matches(group.reference_frames, rel):
            await _authorize_script(db, group.script_id, user, require_write)
            return

    raise NotFoundError("媒体资源不存在")


async def _authorize_generation_image_key(
    db: AsyncSession,
    rel: str,
    material: Materials,
    user: User,
    require_write: bool,
) -> None:
    """授权普通生成图：必须绑定 AssetEntry 或 GenerationJob 输出。"""
    entry = (
        await db.execute(
            select(AssetEntry).where(
                AssetEntry.script_id == material.script_id,
                (AssetEntry.storage_key == f"/{rel}") | (AssetEntry.storage_key == rel),
            ).limit(1)
        )
    ).scalars().first()
    if entry is not None:
        await _authorize_script(db, material.script_id, user, require_write)
        return

    jobs = list(
        (
            await db.execute(
                select(GenerationJob).where(
                    GenerationJob.material_id == material.id,
                    GenerationJob.script_id == material.script_id,
                )
            )
        ).scalars().all()
    )
    if any(_key_matches(job.output_json, rel) for job in jobs):
        await _authorize_script(db, material.script_id, user, require_write)
        return

    raise NotFoundError("媒体资源不存在")


async def _authorize_upload_media_key(
    db: AsyncSession, rel: str, user: User, require_write: bool
) -> None:
    entry = (
        await db.execute(
            select(AssetEntry).where(
                (AssetEntry.storage_key == f"/{rel}") | (AssetEntry.storage_key == rel),
            ).limit(1)
        )
    ).scalars().first()
    if entry is None:
        raise NotFoundError("媒体资源不存在")
    await _authorize_script(db, entry.script_id, user, require_write)


async def _authorize_timeline_media_key(
    db: AsyncSession, rel: str, user: User, require_write: bool
) -> None:
    job = (
        await db.execute(
            select(TimelineRenderJob).where(
                TimelineRenderJob.output_storage_key == rel,
                TimelineRenderJob.status == "succeeded",
            )
        )
    ).scalar_one_or_none()
    if job is None or job.status != "succeeded":
        raise NotFoundError("媒体资源不存在")
    await _authorize_script(db, job.script_id, user, require_write)


async def _authorize_by_storage_key(
    db: AsyncSession, storage_key: str, user: User, require_write: bool = False
) -> None:
    """从 storage_key 解析归属资源，校验当前用户对其访问权限。

    两类鉴权路径：
    - generate_images/ 或 upload_images/ 前缀：解析 mid -> Materials -> Script -> check_script_access
    - generate_outputs/ 或 render_outputs/ 前缀：合成产物按 script_id 鉴权
      （第二段为 script_id，非 mid）-> Script -> check_script_access
    """
    try:
        rel = validate_key_prefix(storage_key)
    except ValueError:
        raise ForbiddenError("非法媒体路径")
    parts = rel.split("/")
    if parts[:2] == ["generate_images", "video"]:
        await _authorize_video_media_key(db, rel, user, require_write)
        return

    # 合成产物（时间线最终视频等）必须精确绑定 render job。
    if parts and parts[0] in ("generate_outputs", "render_outputs"):
        await _authorize_timeline_media_key(db, rel, user, require_write)
        return
    # 资源素材按 mid 鉴权
    mid = parse_mid_from_key(rel)
    if mid is None:
        raise ForbiddenError("无法从路径解析画布项目")
    material = await db.get(Materials, mid)
    if material is None:
        raise NotFoundError("画布项目不存在")
    if parts[0] == "generate_images":
        await _authorize_generation_image_key(db, rel, material, user, require_write)
        return
    if parts[0] == "upload_images":
        await _authorize_upload_media_key(db, rel, user, require_write)
        return
    await _authorize_script(db, material.script_id, user, require_write)


@router.get("/{storage_key:path}")
async def get_media(
    storage_key: str,
    request_range: str | None = Header(default=None, alias="Range"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> StreamingResponse:
    """鉴权代理：会话校验 + 资源级授权后流式返回媒体。

    等价 sapi.yangshipin.cn 的 ``/api/x/cctv/zhihui/flow-app/<storage_key>``。
    - Content-Type 按扩展名推断 (前端主要消费 png/jpg/mp4 等)。
    - 支持 HTTP Range 请求，大文件真流式分块返回（不整文件读入内存）。
    """
    # 资源级鉴权：解析 mid -> Materials -> Script -> check_script_access
    await _authorize_by_storage_key(db, storage_key, user, require_write=False)

    storage = get_storage()
    try:
        key = validate_key_prefix(storage_key)
    except ValueError:
        raise ForbiddenError("非法媒体路径")

    if not await storage.exists(key):
        raise HTTPException(status_code=404, detail="媒体资源不存在")

    total = await storage.get_size(key)
    if request_range is None and total > settings.max_media_read_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"媒体文件过大，上限 {settings.max_media_read_bytes} 字节",
        )
    ext = os.path.splitext(key)[1].lower()
    media_type = _EXT_MIME.get(ext, "application/octet-stream")

    # 优先复用存储侧已探测的真实 MIME（AssetEntry.mime_type）
    # storage_key 在 DB 中可能带前导 "/"，做两端兼容匹配；
    # 历史数据或重复上传可能使同 storage_key 存在多条记录，用 .first() 避免
    # scalar_one_or_none 命中多行时抛 MultipleResultsError → 500
    norm_key = key.lstrip("/")
    entry = (
        (
            await db.execute(
            select(AssetEntry).where(
                (AssetEntry.storage_key == f"/{norm_key}")
                | (AssetEntry.storage_key == norm_key)
            ).limit(1)
            )
        ).scalars().first()
    )
    if entry is not None and entry.mime_type:
        media_type = entry.mime_type

    range_span = _parse_range(request_range, total)
    if request_range is not None and range_span is None:
        raise HTTPException(
            status_code=416,
            detail="Range 不可满足",
            headers={"Content-Range": f"bytes */{total}"},
        )
    if range_span is not None:
        start, end = range_span
        chunk_iter: AsyncIterator[bytes] = storage.iter_chunks(
            key, chunk_size=65536, start=start, end=end
        )
        headers = {
            "Content-Range": f"bytes {start}-{end}/{total}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(end - start + 1),
            "Content-Disposition": _content_disposition(key),
        }
        return StreamingResponse(
            chunk_iter,
            media_type=media_type,
            status_code=206,
            headers=headers,
        )

    # 无 Range，整体流式返回
    chunk_iter = storage.iter_chunks(key, chunk_size=65536, start=0, end=None)
    return StreamingResponse(
        chunk_iter,
        media_type=media_type,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(total),
            "Content-Disposition": _content_disposition(key),
        },
    )


@router.post("/upload_images/{mid}/{filename}")
async def upload_image(
    mid: str,
    filename: str,
    file: UploadFile,
    subject_id: str | None = None,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
) -> dict:
    """上传媒体文件 —— 校验资源级写权限 + 真实 MIME 后存储。

    - 路径参数 mid 对应画布项目，校验当前用户对该 material 所属 script 的写权限。
    - 可选 subject_id：提供则创建 AssetEntry（候选资产，is_approved=False），
      使上传媒体出现在资产库可被主体挂载/审核/汇聚参考图。
    - 返回 ``{"storage_key": "...", "mime_type": "...", "size_bytes": N, "sha256": "...",
        "asset_id": "..."}``（无 subject_id 时 asset_id 为 null）。
    前端通过 ``media_proxy_prefix + storage_key`` 访问。
    """
    # 资源级写权限校验：mid -> Materials -> Script -> check_script_access(require_write=True)
    material = await db.get(Materials, mid)
    if material is None:
        raise NotFoundError("画布项目不存在")
    script = (
        await db.execute(select(Script).where(Script.id == material.script_id))
    ).scalar_one_or_none()
    if script is None:
        raise NotFoundError("剧本不存在")
    await check_script_access(db, user, script, action=Action.EDIT)

    subject = None
    if subject_id is not None:
        from app.models.subject import Subject

        subject = (
            await db.execute(select(Subject).where(Subject.id == subject_id))
        ).scalar_one_or_none()
        if subject is None:
            raise NotFoundError("主体不存在")
        if subject.script_id != script.id:
            raise ForbiddenError("主体不属于该剧本")

    # 分块落盘到临时文件：超过上限提前 413，不在内存中累积整文件
    # （SpooledTemporaryFile 小文件留内存、大文件自动落盘，内存占用恒为单块，
    #  避免 chunks 列表 + b"".join 在大文件上传时造成 worker OOM）
    import tempfile

    limit = settings.max_upload_bytes
    total_read = 0
    sha256 = hashlib.sha256()
    tmp = tempfile.NamedTemporaryFile(mode="w+b")
    try:
        while True:
            chunk = await file.read(1024 * 1024)  # 1MiB/块
            if not chunk:
                break
            total_read += len(chunk)
            if total_read > limit:
                raise HTTPException(
                    status_code=413,
                    detail=f"上传文件过大，上限 {limit} 字节",
                )
            sha256.update(chunk)
            tmp.write(chunk)
        if total_read == 0:
            raise HTTPException(status_code=400, detail="上传文件为空")
        tmp.flush()
        tmp.seek(0)

        # MIME 探测基于临时文件路径，不复制整文件到内存
        mime = _detect_mime(tmp.name, filename)
        if mime not in _ALLOWED_UPLOAD_MIME_TYPES:
            raise HTTPException(status_code=400, detail=f"不支持的媒体类型: {mime}")

        temp_storage_key = _temp_storage_key_for_upload(mid, filename, mime)
        final_storage_key = _storage_key_for_upload(mid, filename, mime)
        storage = get_storage()
        sha = sha256.hexdigest()
        size_bytes = total_read

        asset_id: str | None = None
        entry: AssetEntry | None = None
        # 仅图片/音频进入候选审核；视频只作为上传媒体保留，不得污染图片审核队列。
        if subject is not None and (mime.startswith("image/") or mime.startswith("audio/")):
            kind = "audio" if mime.startswith("audio/") else "image"
            entry = await add_candidate(
                db,
                script_id=script.id,
                subject_id=subject.id,
                kind=kind,
                storage_key=f"/{final_storage_key}",
                mime_type=mime,
                size_bytes=size_bytes,
                source="upload_images",
                sha256=sha,
                subject=subject,
            )
            asset_id = entry.id

        promote_job = await media_storage_jobs.enqueue_promote(
            db,
            idempotency_key=f"promote:{final_storage_key}",
            source_key=temp_storage_key,
            final_key=final_storage_key,
            asset_id=asset_id,
        )
        await db.commit()

        try:
            await _put_temp_object(storage, temp_storage_key, tmp.name)
        except Exception:
            await db.rollback()
            if entry is not None:
                entry.set_status("deleted")
            promote_job.job_type = "delete"
            promote_job.idempotency_key = f"delete:{temp_storage_key}"
            promote_job.final_key = None
            promote_job.asset_id = asset_id
            promote_job.status = "pending"
            promote_job.attempt = 0
            promote_job.lease_owner = None
            promote_job.lease_token = None
            promote_job.lease_until = None
            promote_job.last_error_type = None
            promote_job.last_error_message = None
            await db.commit()
            try:
                await storage.delete(temp_storage_key)
            except Exception:
                logger.exception("upload_temp_delete_failed")
            raise
    finally:
        tmp.close()

    await _run_promote_job(db, storage, promote_job.id)

    return {
        "storage_key": f"/{final_storage_key}",
        "mime_type": mime,
        "size_bytes": size_bytes,
        "sha256": sha,
        "asset_id": asset_id,
    }
