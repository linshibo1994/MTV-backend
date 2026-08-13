"""分镜输入的脱敏哈希快照与可解释新鲜度校验。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.episode import Episode
from app.models.storyboard_job import StoryboardJob
from app.models.subject import Subject

ProvenanceStatus = Literal["current", "stale", "legacy"]
_PROVENANCE_SCHEMA = "storyboard.provenance.v1"
_HASH_FIELDS = (
    "episode_script_sha256",
    "subject_catalog_sha256",
    "system_prompt_sha256",
    "user_prompt_sha256",
    "prompt_templates_sha256",
)


def _canonical_sha256(value: object) -> str:
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def storyboard_provenance_sha256(provenance: Mapping[str, object]) -> str:
    """生成可供产出实体引用的 provenance 整体锚点。"""
    return _canonical_sha256(dict(provenance))


def _template_metadata(
    templates: Mapping[str, Mapping[str, object]],
) -> dict[str, dict[str, object]]:
    """只保留不可变版本锚点，不保存模板正文。"""
    sanitized: dict[str, dict[str, object]] = {}
    for key in sorted(templates):
        raw = templates[key]
        version = raw.get("version")
        content_sha256 = raw.get("content_sha256")
        if type(version) is not int or version < 0:
            raise ValueError("提示词模板版本无效")
        if not isinstance(content_sha256, str) or len(content_sha256) != 64:
            raise ValueError("提示词模板哈希无效")
        sanitized[key] = {
            "version": version,
            "content_sha256": content_sha256,
            "source": raw.get("source") if raw.get("source") in {"database", "fallback"} else None,
        }
    return sanitized


def canonical_subject_catalog(subjects: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """生成与数据库顺序和主体展示名称无关的最小目录快照。"""
    catalog: list[dict[str, object]] = []
    for subject in subjects:
        subject_id = subject.get("subject_id") or subject.get("id")
        if not isinstance(subject_id, str) or not subject_id:
            raise ValueError("主体目录缺少 subject_id")
        episode_nos = subject.get("episode_nos")
        raw_subject_version = subject.get("subject_version")
        subject_version = (
            raw_subject_version
            if type(raw_subject_version) is int and raw_subject_version >= 1
            else 1
        )
        raw_enrich_version = subject.get("enrich_version")
        enrich_version = (
            raw_enrich_version
            if type(raw_enrich_version) is int and raw_enrich_version >= 1
            else 1
        )
        catalog.append(
            {
                "subject_id": subject_id,
                "type": str(subject.get("type") or ""),
                "subject_version": subject_version,
                "enrich_version": enrich_version,
                "is_active": bool(subject.get("is_active", True)),
                "episode_nos": sorted(
                    int(item)
                    for item in episode_nos
                    if type(item) is int
                )
                if isinstance(episode_nos, list)
                else [],
            }
        )
    return sorted(catalog, key=lambda item: str(item["subject_id"]))


def build_storyboard_provenance(
    *,
    episode_script: str,
    episode_version: int,
    client_save_seq: int,
    subject_catalog: Sequence[Mapping[str, object]],
    system_prompt: str,
    user_prompt: str,
    prompt_templates: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    """构造只含哈希、版本和最小重算元数据的持久化快照。"""
    subjects = canonical_subject_catalog(subject_catalog)
    templates = _template_metadata(prompt_templates)
    return {
        "schema": _PROVENANCE_SCHEMA,
        "episode_version": int(episode_version),
        "client_save_seq": int(client_save_seq),
        "episode_script_sha256": _canonical_sha256({"script_text": episode_script}),
        "subject_catalog_sha256": _canonical_sha256(subjects),
        "system_prompt_sha256": _canonical_sha256({"prompt": system_prompt}),
        "user_prompt_sha256": _canonical_sha256({"prompt": user_prompt}),
        "prompt_templates_sha256": _canonical_sha256(templates),
        "subject_catalog": subjects,
        "prompt_templates": templates,
    }


@dataclass(frozen=True)
class ProvenanceReport:
    job_id: str
    status: ProvenanceStatus
    mismatches: tuple[str, ...] = ()

    @property
    def all_ok(self) -> bool:
        return self.status == "current"

    @property
    def skipped(self) -> bool:
        return self.status == "legacy"

    def as_dict(self) -> dict[str, object]:
        return {
            "version": 1,
            "job_id": self.job_id,
            "status": self.status,
            "all_ok": self.all_ok,
            "skipped": self.skipped,
            "mismatches": list(self.mismatches),
        }


def compare_storyboard_provenance(
    job_id: str,
    stored: object,
    current: Mapping[str, object],
) -> ProvenanceReport:
    """比较两个脱敏快照；历史任务标 legacy，差异仅称 stale。"""
    if not isinstance(stored, Mapping) or stored.get("schema") != _PROVENANCE_SCHEMA:
        return ProvenanceReport(job_id=job_id, status="legacy")
    stored_templates = stored.get("prompt_templates")
    stored_subjects = stored.get("subject_catalog")
    invalid_anchors: list[str] = []
    if not isinstance(stored_templates, Mapping) or stored.get(
        "prompt_templates_sha256"
    ) != _canonical_sha256(stored_templates):
        invalid_anchors.append("prompt_templates_sha256")
    if not isinstance(stored_subjects, list) or stored.get(
        "subject_catalog_sha256"
    ) != _canonical_sha256(stored_subjects):
        invalid_anchors.append("subject_catalog_sha256")
    mismatches = tuple(
        dict.fromkeys(
            [
                *invalid_anchors,
                *(
                    field
                    for field in (*_HASH_FIELDS, "episode_version", "client_save_seq")
                    if stored.get(field) != current.get(field)
                ),
            ]
        )
    )
    return ProvenanceReport(
        job_id=job_id,
        status="stale" if mismatches else "current",
        mismatches=mismatches,
    )


async def validate_storyboard_provenance(
    db: AsyncSession,
    job_id: str,
) -> ProvenanceReport:
    """用当前剧本、主体、选案和模板重新渲染并校验全部输入锚点。"""
    job = (
        await db.execute(select(StoryboardJob).where(StoryboardJob.job_id == job_id))
    ).scalar_one_or_none()
    if job is None:
        raise ValueError("分镜任务不存在")
    stored = job.input_provenance
    if not isinstance(stored, Mapping) or stored.get("schema") != _PROVENANCE_SCHEMA:
        return ProvenanceReport(job_id=job_id, status="legacy")
    episode = (
        await db.execute(select(Episode).where(Episode.episode_id == job.episode_id))
    ).scalar_one_or_none()
    if episode is None:
        return ProvenanceReport(job_id=job_id, status="stale", mismatches=("episode_missing",))
    all_subjects = list(
        (
            await db.execute(select(Subject).where(Subject.script_id == job.script_id))
        )
        .scalars()
        .all()
    )
    # 延迟导入避免 storyboard 服务构造快照时形成模块循环。
    from app.services.storyboard import (
        _build_subject_reference_catalog,
        _resolve_storyboard_input,
    )
    subject_references = _build_subject_reference_catalog(
        all_subjects,
        episode.episode_no,
    )
    resolved = await _resolve_storyboard_input(
        db,
        script_id=job.script_id,
        episode=episode,
        subjects=all_subjects,
        subject_references=subject_references,
    )
    current = resolved.provenance
    return compare_storyboard_provenance(job_id, stored, current)


__all__ = [
    "ProvenanceReport",
    "build_storyboard_provenance",
    "canonical_subject_catalog",
    "compare_storyboard_provenance",
    "storyboard_provenance_sha256",
    "validate_storyboard_provenance",
]
