"""参考图/参考音频汇聚服务 — 解析 shot_group 主体名 → 已审核 image/audio entry。

视频生成 (T8) 调用：shot_group.materials {character[], background[], prop[], other[]}
→ 每主体名查 Subject (review_status==approved 且 image_entry_id 非空)
→ 汇集 reference_images[]{kind, storage_key, mime_type, size_bytes, sha256}
   + reference_image_names[] (一一对应)
→ 未通过主体不进参考图，返回缺失列表供 [检查素材] 标红。

参考音频 (doc 06 §9.6)：shot_group.audio_elements（需配音角色名列表）
→ 每角色名查 Subject + 其 is_approved 的 audio AssetEntry
→ 汇集 reference_audios[] + reference_audio_names[]，缺失角色记入 missing_audio。
"""

from dataclasses import dataclass, field

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset_entry import AssetEntry
from app.models.shot_group import ShotGroup
from app.models.subject import Subject
from app.models.video_generation_job import VideoGenerationJob


@dataclass
class AggregatedReferences:
    """参考图/参考音频汇聚结果。"""

    reference_images: list[dict] = field(default_factory=list)
    reference_image_names: list[str] = field(default_factory=list)
    # 未通过审核的主体名列表（供 [检查素材] 接口标红）
    missing_subjects: list[str] = field(default_factory=list)
    # 参考图主体名 → subject_id 映射（调试/审计用）
    subject_id_map: dict[str, str] = field(default_factory=dict)
    # 参考图类别，与 reference_images/reference_image_names 一一对应。
    reference_image_categories: list[str] = field(default_factory=list)
    # 最终图片顺序对应的稳定 token 绑定；与 reference_images 一一对应。
    reference_image_refs: list[dict[str, str | None]] = field(default_factory=list)
    # 参考音频（配音素材，结构同参考图）
    reference_audios: list[dict] = field(default_factory=list)
    reference_audio_names: list[str] = field(default_factory=list)
    # 缺少已审核音频的角色名（供 [检查素材] 标红）
    missing_audio: list[str] = field(default_factory=list)
    # 已成功视频任务作为动作/镜头运动参考，结构同参考图。
    reference_videos: list[dict] = field(default_factory=list)
    reference_video_names: list[str] = field(default_factory=list)
    # 失效、跨剧本或尚未成功的视频任务。
    missing_videos: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class MaterialReference:
    label: str
    category: str
    subject_id: str | None = None
    asset_entry_id: str | None = None
    reference_ref: str | None = None


@dataclass(frozen=True)
class AudioReference:
    label: str
    subject_id: str | None = None
    asset_entry_id: str | None = None
    item_type: str | None = None


@dataclass(frozen=True)
class VideoReference:
    """镜头组中引用的已完成视频生成任务。"""

    label: str
    video_job_id: str | None = None
    storage_key: str | None = None


def _parse_material_names(materials: dict | None) -> list[str]:
    """从 shot_group.materials 解析所有类别主体名（去重保序）。

    materials 结构: {character[], background[], prop[], other[]}
    """
    if not materials:
        return []
    seen: set[str] = set()
    names: list[str] = []
    for key in ("character", "background", "prop", "other"):
        group = materials.get(key) or []
        if not isinstance(group, list):
            continue
        for item in group:
            if isinstance(item, dict):
                name = str(item.get("name") or "")
            elif isinstance(item, str):
                name = item
            else:
                continue
            name = name.strip()
            if name and name not in seen:
                seen.add(name)
                names.append(name)
    return names


def _parse_material_references(materials: dict | None) -> list[MaterialReference]:
    """解析参考图主体，优先保留稳定 subject_id/asset_entry_id，旧字符串名兼容。"""
    if not materials:
        return []
    seen: set[tuple[str | None, str | None, str]] = set()
    refs: list[MaterialReference] = []
    for key in ("character", "background", "prop", "other"):
        group = materials.get(key) or []
        if not isinstance(group, list):
            continue
        for item in group:
            if isinstance(item, dict):
                subject_id = item.get("subject_id") or item.get("subjectId")
                asset_entry_id = item.get("asset_entry_id") or item.get("assetEntryId")
                reference_ref = item.get("ref") or item.get("reference_ref") or item.get("short_ref")
                label = str(item.get("name") or item.get("subject_name") or subject_id or "")
            elif isinstance(item, str):
                subject_id = None
                asset_entry_id = None
                reference_ref = None
                label = item
            else:
                continue
            label = label.strip()
            if not label:
                continue
            dedupe_key = (subject_id, asset_entry_id, label)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            refs.append(
                MaterialReference(
                    label=label,
                    category=key,
                    subject_id=subject_id,
                    asset_entry_id=asset_entry_id,
                    reference_ref=(
                        str(reference_ref).strip()
                        if isinstance(reference_ref, str) and reference_ref.strip()
                        else None
                    ),
                )
            )
    return refs


def _parse_audio_names(audio_elements) -> list[str]:
    """从 shot_group.audio_elements 解析需配音角色名（去重保序）。

    audio_elements 可为角色名列表 ["张三","李四"]，或对象列表 [{name/character/role}]。
    """
    if not audio_elements:
        return []
    if isinstance(audio_elements, dict):
        # 兼容 {characters:[...], sounds:[...]}，对白角色与音效都要汇聚。
        merged: list = []
        for key in ("character", "characters", "names", "roles", "sounds", "sound"):
            seq = audio_elements.get(key)
            if isinstance(seq, list):
                merged.extend(seq)
        if merged:
            audio_elements = merged
        else:
            return []
    if not isinstance(audio_elements, list):
        return []
    seen: set[str] = set()
    out: list[str] = []
    for item in audio_elements:
        name = ""
        if isinstance(item, str):
            name = item
        elif isinstance(item, dict):
            name = str(
                item.get("name") or item.get("character") or item.get("role") or ""
            )
        name = name.strip()
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def _parse_audio_references(audio_elements) -> list[AudioReference]:
    """解析音频引用，优先保留稳定 subject_id/asset_entry_id，兼容旧字符串。"""
    if not audio_elements:
        return []
    raw_items: list = []
    if isinstance(audio_elements, dict):
        for key in ("character", "characters", "names", "roles", "sounds", "sound"):
            seq = audio_elements.get(key)
            if isinstance(seq, list):
                raw_items.extend(seq)
    elif isinstance(audio_elements, list):
        raw_items = audio_elements
    else:
        return []

    seen: set[tuple[str | None, str | None, str]] = set()
    refs: list[AudioReference] = []
    for item in raw_items:
        if isinstance(item, str):
            label = item
            subject_id = None
            asset_entry_id = None
            item_type = None
        elif isinstance(item, dict):
            subject_id = item.get("subject_id") or item.get("subjectId")
            asset_entry_id = item.get("asset_entry_id") or item.get("assetEntryId")
            item_type = item.get("type")
            label = str(
                item.get("name")
                or item.get("character")
                or item.get("role")
                or subject_id
                or ""
            )
        else:
            continue
        label = str(label).strip()
        if not label:
            continue
        dedupe_key = (subject_id, asset_entry_id, label)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        refs.append(
            AudioReference(
                label=label,
                subject_id=subject_id,
                asset_entry_id=asset_entry_id,
                item_type=item_type,
            )
        )
    return refs


def _parse_video_references(materials: dict | None) -> list[VideoReference]:
    """解析 ``materials.video``，兼容历史 camelCase 键但只接受任务 ID 引用。"""

    if not materials or not isinstance(materials, dict):
        return []
    raw_items = materials.get("video")
    if not isinstance(raw_items, list):
        return []

    seen: set[str] = set()
    refs: list[VideoReference] = []
    for item in raw_items:
        if isinstance(item, dict):
            job_id = item.get("video_job_id") or item.get("videoJobId")
            label = str(item.get("name") or job_id or "").strip()
            storage_key = item.get("storage_key")
        else:
            job_id = None
            label = str(item or "").strip()
            storage_key = None
        normalized_job_id = str(job_id).strip() if job_id else ""
        normalized_storage_key = str(storage_key).strip() if storage_key else None
        if not normalized_job_id:
            if label:
                refs.append(VideoReference(label=label, storage_key=normalized_storage_key))
            continue
        if normalized_job_id in seen:
            continue
        seen.add(normalized_job_id)
        refs.append(
            VideoReference(
                label=label or normalized_job_id,
                video_job_id=normalized_job_id,
                storage_key=normalized_storage_key,
            )
        )
    return refs


async def _load_subjects_by_name(
    db: AsyncSession, script_id: str, names: list[str]
) -> dict[str, Subject]:
    """按 name 查同剧本 Subject，返回 {name: 第一个 approved 主体}。"""
    if not names:
        return {}
    sub_result = await db.execute(
        select(Subject).where(
            Subject.script_id == script_id,
            Subject.name.in_(names),
        )
    )
    subjects = sub_result.scalars().all()
    approved_by_name: dict[str, Subject] = {}
    for s in subjects:
        if s.review_status == "approved":
            approved_by_name.setdefault(s.name, s)
    return approved_by_name


async def _load_subjects_by_ids(
    db: AsyncSession, script_id: str, subject_ids: list[str]
) -> dict[str, Subject]:
    if not subject_ids:
        return {}
    result = await db.execute(
        select(Subject).where(
            Subject.script_id == script_id,
            Subject.id.in_(subject_ids),
        )
    )
    return {subject.id: subject for subject in result.scalars().all()}


async def _load_subjects_by_name_any_status(
    db: AsyncSession, script_id: str, names: list[str]
) -> dict[str, Subject]:
    if not names:
        return {}
    result = await db.execute(
        select(Subject).where(
            Subject.script_id == script_id,
            Subject.name.in_(names),
        )
    )
    subjects_by_name: dict[str, Subject] = {}
    for subject in result.scalars().all():
        subjects_by_name.setdefault(subject.name, subject)
    return subjects_by_name


async def aggregate_references(
    db: AsyncSession, shot_group_id: str
) -> AggregatedReferences:
    """汇聚 shot_group 的视频生成参考图 + 参考音频。

    Args:
        db: 异步会话。
        shot_group_id: 镜头组 ID。

    Returns:
        AggregatedReferences — reference_images / reference_image_names 一一对应，
        missing_subjects 为缺失主体名；reference_audios 同理。
    """
    result = AggregatedReferences()

    # 取 shot_group
    sg_result = await db.execute(
        select(ShotGroup).where(ShotGroup.shot_group_id == shot_group_id)
    )
    shot_group = sg_result.scalar_one_or_none()
    if shot_group is None:
        from fastapi import HTTPException, status

        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="镜头组不存在")

    script_id = shot_group.script_id

    # ===== 参考图：materials 主体名 → approved image_entry =====
    references = _parse_material_references(shot_group.materials)
    names = [ref.label for ref in references if not ref.subject_id]
    approved_by_name = await _load_subjects_by_name(db, script_id, names)
    subjects_by_id = await _load_subjects_by_ids(
        db,
        script_id,
        [ref.subject_id for ref in references if ref.subject_id],
    )

    # 收集需要加载的 image_entry_id；显式 asset_entry_id 与 subject 当前 image_entry_id
    # 都要加载，以便旧显式 asset 被 superseded 时回退到同主体当前 approved 图。
    entry_ids = []
    for ref in references:
        subject = subjects_by_id.get(ref.subject_id) if ref.subject_id else approved_by_name.get(ref.label)
        if ref.asset_entry_id:
            entry_ids.append(ref.asset_entry_id)
        if subject is not None and subject.image_entry_id:
            entry_ids.append(subject.image_entry_id)
    entries_map: dict[str, AssetEntry] = {}
    if entry_ids:
        entry_result = await db.execute(
            select(AssetEntry).where(AssetEntry.id.in_(entry_ids))
        )
        for ae in entry_result.scalars().all():
            entries_map[ae.id] = ae

    # 按原始 references 顺序输出，保持一一对应
    for ref in references:
        subject = subjects_by_id.get(ref.subject_id) if ref.subject_id else approved_by_name.get(ref.label)
        if subject is None or subject.review_status != "approved":
            result.missing_subjects.append(ref.label)
            continue
        entry: AssetEntry | None = None
        if ref.asset_entry_id:
            explicit = entries_map.get(ref.asset_entry_id)
            if (
                explicit is None
                or explicit.script_id != script_id
                or explicit.subject_id != subject.id
                or explicit.kind != "image"
            ):
                result.missing_subjects.append(ref.label)
                continue
            if explicit.is_approved and not getattr(explicit, "superseded", False):
                entry = explicit
            elif explicit.status == "superseded" and subject.image_entry_id:
                entry = entries_map.get(subject.image_entry_id)
            else:
                result.missing_subjects.append(ref.label)
                continue
        elif subject.image_entry_id:
            entry = entries_map.get(subject.image_entry_id)
        else:
            result.missing_subjects.append(ref.label)
            continue
        if (
            entry is None
            or entry.kind != "image"
            or entry.script_id != script_id
            or entry.subject_id != subject.id
            or not entry.is_approved
            or getattr(entry, "superseded", False)
            or not entry.sha256
        ):
            result.missing_subjects.append(f"{ref.category}:{ref.label}")
            continue
        result.reference_images.append({
            "kind": "storage",
            "storage_key": entry.storage_key,
            "mime_type": entry.mime_type,
            "size_bytes": entry.size_bytes,
            "sha256": entry.sha256 or "",
        })
        result.reference_image_names.append(ref.label)
        result.reference_image_categories.append(ref.category)
        result.reference_image_refs.append(
            {
                "token": f"图{len(result.reference_images)}",
                "ref": ref.reference_ref,
                "name": ref.label,
                "category": ref.category,
                "subject_id": subject.id,
                "asset_entry_id": entry.id,
            }
        )
        result.subject_id_map[f"{ref.category}:{ref.label}"] = subject.id

    # ===== 参考音频：audio_elements 角色名 → approved audio AssetEntry =====
    audio_refs = _parse_audio_references(shot_group.audio_elements)
    if audio_refs:
        await _aggregate_audio(db, script_id, audio_refs, result)

    # ===== 参考视频：materials.video → 当前剧本已完成的视频任务 =====
    video_refs = _parse_video_references(shot_group.materials)
    if video_refs:
        await _aggregate_videos(db, script_id, video_refs, result)

    return result


async def _aggregate_audio(
    db: AsyncSession,
    script_id: str,
    audio_refs: list[AudioReference],
    result: AggregatedReferences,
) -> None:
    """按角色名汇聚已审核音频 AssetEntry（doc 06 §9.6）。

    逻辑：每角色名查同剧本 Subject（approved）→ 其 subject_id 下 is_approved 的
    audio AssetEntry 取第一个 → 汇入 reference_audios[] + reference_audio_names[]。
    缺少已审核音频的角色记入 missing_audio。
    """
    names = [ref.label for ref in audio_refs if not ref.subject_id]
    subjects_by_name = await _load_subjects_by_name_any_status(db, script_id, names)
    subjects_by_id = await _load_subjects_by_ids(
        db,
        script_id,
        [ref.subject_id for ref in audio_refs if ref.subject_id],
    )
    subject_ids = [
        s.id
        for s in [*subjects_by_name.values(), *subjects_by_id.values()]
    ]
    if not subject_ids:
        result.missing_audio.extend(ref.label for ref in audio_refs)
        return

    # 查这些 subject 下已审核的音频条目
    ae_result = await db.execute(
        select(AssetEntry).where(
            AssetEntry.subject_id.in_(subject_ids),
            AssetEntry.kind == "audio",
            AssetEntry.is_approved.is_(True),
        )
    )
    audio_entries: dict[str, AssetEntry] = {}  # subject_id -> entry
    audio_entries_by_id: dict[str, AssetEntry] = {}
    for ae in ae_result.scalars().all():
        audio_entries.setdefault(ae.subject_id, ae)
        audio_entries_by_id[ae.id] = ae

    for ref in audio_refs:
        subject = subjects_by_id.get(ref.subject_id) if ref.subject_id else subjects_by_name.get(ref.label)
        if subject is None:
            result.missing_audio.append(ref.label)
            continue
        entry = audio_entries_by_id.get(ref.asset_entry_id) if ref.asset_entry_id else audio_entries.get(subject.id)
        if (
            entry is None
            or entry.subject_id != subject.id
            or entry.script_id != script_id
            or entry.kind != "audio"
            or not entry.is_approved
            or not entry.sha256
        ):
            result.missing_audio.append(ref.label)
            continue
        result.reference_audios.append({
            "kind": "storage",
            "storage_key": entry.storage_key,
            "mime_type": entry.mime_type,
            "size_bytes": entry.size_bytes,
            "sha256": entry.sha256 or "",
        })
        result.reference_audio_names.append(ref.label)


async def _aggregate_videos(
    db: AsyncSession,
    script_id: str,
    video_refs: list[VideoReference],
    result: AggregatedReferences,
) -> None:
    """汇聚已成功任务的视频产物，禁止跨剧本或未完成任务作为参考。"""

    job_ids = [
        ref.video_job_id
        for ref in video_refs
        if ref.video_job_id and not ref.storage_key
    ]
    jobs_by_id: dict[str, VideoGenerationJob] = {}
    if job_ids:
        job_result = await db.execute(
            select(VideoGenerationJob).where(
                VideoGenerationJob.script_id == script_id,
                VideoGenerationJob.job_id.in_(job_ids),
            )
        )
        jobs_by_id = {job.job_id: job for job in job_result.scalars().all()}

    for ref in video_refs:
        if ref.storage_key:
            result.reference_videos.append(
                {
                    "kind": "storage",
                    "storage_key": ref.storage_key,
                    "mime_type": "video/mp4",
                    "source_video_job_id": ref.video_job_id,
                }
            )
            result.reference_video_names.append(ref.label)
            continue
        job = jobs_by_id.get(ref.video_job_id or "")
        output = job.output_json if job is not None and isinstance(job.output_json, dict) else {}
        storage_key = output.get("video_storage_key") if isinstance(output, dict) else None
        if (
            job is None
            or job.status != "succeeded"
            or not isinstance(storage_key, str)
            or not storage_key.strip()
        ):
            result.missing_videos.append(ref.label)
            continue
        result.reference_videos.append(
            {
                "kind": "storage",
                "storage_key": storage_key,
                "mime_type": "video/mp4",
                "source_video_job_id": job.job_id,
            }
        )
        result.reference_video_names.append(ref.label)
