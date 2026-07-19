"""生命周期删除使用的全局对象存储引用判权。"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import cast, func, or_, select
from sqlalchemy.dialects.postgresql import JSONPATH

from app.core.errors import ConflictError
from app.models.asset_entry import AssetEntry
from app.models.canvas_edge import CanvasEdge
from app.models.canvas_node import CanvasNode
from app.models.episode import Episode
from app.models.generation_job import GenerationJob
from app.models.media_storage_job import MediaStorageJob
from app.models.media_upload import MediaUpload
from app.models.pipeline_job import PipelineJob
from app.models.shot_group import ShotGroup
from app.models.storyboard_job import StoryboardJob
from app.models.timeline_render_job import TimelineRenderJob
from app.models.video_generation_job import VideoGenerationJob
from app.providers.storage import validate_key_prefix

logger = logging.getLogger(__name__)

_ACTIVE_PROMOTE_STATUSES = frozenset({"pending", "retry", "running"})
_PROMOTE_QUERY_CHUNK_SIZE = 500
_MAX_PROMOTE_CLOSURE_KEYS = 10_000
_MAX_PROMOTE_CLOSURE_ROWS = 5_000
_MAX_PROMOTE_CLOSURE_ROUNDS = 128
_REFERENCE_QUERY_CHUNK_SIZE = 500
_MAX_REFERENCE_SCAN_ROWS = 10_000


@dataclass(slots=True)
class StorageReferenceExclusions:
    """本次事务会删除或失去所有权的记录主键。"""

    asset_entry_ids: set[str] = field(default_factory=set)
    media_upload_ids: set[str] = field(default_factory=set)
    episode_ids: set[str] = field(default_factory=set)
    generation_job_ids: set[str] = field(default_factory=set)
    video_generation_job_ids: set[str] = field(default_factory=set)
    timeline_render_job_ids: set[str] = field(default_factory=set)
    storyboard_job_ids: set[str] = field(default_factory=set)
    pipeline_job_ids: set[str] = field(default_factory=set)
    shot_group_ids: set[str] = field(default_factory=set)
    canvas_node_ids: set[str] = field(default_factory=set)
    canvas_edge_ids: set[str] = field(default_factory=set)


@dataclass(frozen=True, slots=True)
class StorageKeyDecision:
    """规范化候选 key 的全局保留与可删除集合。"""

    candidate_keys: frozenset[str]
    protected_keys: frozenset[str]
    deletable_keys: frozenset[str]


def collect_storage_keys(value: Any, destination: set[str]) -> None:
    """递归收集合法对象键；候选限定使未知 JSON 结构也不会漏判。"""
    if isinstance(value, str):
        try:
            destination.add(validate_key_prefix(value))
        except ValueError:
            return
    elif isinstance(value, dict):
        for item in value.values():
            collect_storage_keys(item, destination)
    elif isinstance(value, (list, tuple)):
        for item in value:
            collect_storage_keys(item, destination)


def normalize_storage_keys(values: set[str]) -> set[str]:
    normalized: set[str] = set()
    for value in values:
        try:
            normalized.add(validate_key_prefix(value))
        except ValueError:
            logger.warning("跳过不受支持的存量媒体键")
    return normalized


async def acquire_storage_reference_barrier(db: Any) -> None:
    """兼容旧调用点；全表屏障已由 per-key durable lifecycle 取代。"""
    del db


def _excluding(statement: Any, column: Any, identifiers: set[str]) -> Any:
    if not identifiers:
        return statement
    return statement.where(column.not_in(sorted(identifiers)))


def _collect_matching_keys(
    value: Any,
    *,
    candidates: set[str],
    destination: set[str],
) -> None:
    if isinstance(value, str):
        try:
            normalized = validate_key_prefix(value)
        except ValueError:
            return
        if normalized in candidates:
            destination.add(normalized)
    elif isinstance(value, dict):
        for item in value.values():
            _collect_matching_keys(
                item,
                candidates=candidates,
                destination=destination,
            )
    elif isinstance(value, (list, tuple)):
        for item in value:
            _collect_matching_keys(
                item,
                candidates=candidates,
                destination=destination,
            )


def _dialect_name(db: Any) -> str | None:
    try:
        return str(db.get_bind().dialect.name)
    except (AttributeError, RuntimeError):
        return None


def _reference_query_values(candidates: set[str]) -> list[str]:
    return sorted(candidates)


def _postgres_json_reference_path(query_values: list[str]) -> str:
    pattern = rf"^/*(?:{'|'.join(re.escape(value) for value in query_values)})$"
    return f"$.** ? (@ like_regex {json.dumps(pattern, ensure_ascii=False)})"


def _postgres_json_conditions(
    columns: tuple[Any, ...],
    query_values: list[str],
) -> list[Any]:
    path = cast(_postgres_json_reference_path(query_values), JSONPATH)
    return [func.jsonb_path_exists(column, path) for column in columns]


async def _scan_reference_table(
    db: Any,
    *,
    identifier: Any,
    scalar_columns: tuple[Any, ...],
    json_columns: tuple[Any, ...],
    excluded_ids: set[str],
    candidates: set[str],
    destination: set[str],
) -> None:
    dialect_name = _dialect_name(db)
    chunks = _chunks_with_size(candidates, _REFERENCE_QUERY_CHUNK_SIZE)
    if json_columns and dialect_name != "postgresql":
        chunks = [sorted(candidates)]

    seen_ids: set[str] = set()
    for chunk in chunks:
        query_values = _reference_query_values(set(chunk))
        conditions = [func.ltrim(column, "/").in_(query_values) for column in scalar_columns]
        if json_columns and dialect_name == "postgresql":
            conditions.extend(_postgres_json_conditions(json_columns, query_values))

        statement = select(identifier, *scalar_columns, *json_columns)
        statement = _excluding(statement, identifier, excluded_ids)
        if conditions and not (json_columns and dialect_name != "postgresql"):
            statement = statement.where(or_(*conditions))
        remaining = _MAX_REFERENCE_SCAN_ROWS - len(seen_ids)
        rows = (await db.execute(statement.limit(remaining + 1))).all()
        for row in rows:
            row_id, *values = row
            if row_id in seen_ids:
                continue
            seen_ids.add(row_id)
            if len(seen_ids) > _MAX_REFERENCE_SCAN_ROWS:
                raise ConflictError("对象存储引用结果过多，请缩小删除范围后重试")
            for value in values:
                _collect_matching_keys(
                    value,
                    candidates=candidates,
                    destination=destination,
                )


def _chunks_with_size(values: set[str], size: int) -> list[list[str]]:
    ordered = sorted(values)
    return [ordered[offset : offset + size] for offset in range(0, len(ordered), size)]


async def decide_storage_key_deletes(
    db: Any,
    candidate_keys: set[str],
    *,
    exclusions: StorageReferenceExclusions,
) -> StorageKeyDecision:
    """在写屏障内排除删除集合，扫描全局保留记录的对象键引用。"""
    candidates = normalize_storage_keys(candidate_keys)
    if not candidates:
        return StorageKeyDecision(frozenset(), frozenset(), frozenset())

    if len(candidates) > _MAX_PROMOTE_CLOSURE_KEYS:
        raise ConflictError("对象存储引用候选过多，请缩小删除范围后重试")
    protected: set[str] = set()
    scans = (
        (
            AssetEntry.id,
            (AssetEntry.storage_key,),
            (),
            exclusions.asset_entry_ids,
        ),
        (
            MediaUpload.id,
            (MediaUpload.temp_storage_key, MediaUpload.final_storage_key),
            (),
            exclusions.media_upload_ids,
        ),
        (
            Episode.episode_id,
            (Episode.script_file_storage_key,),
            (),
            exclusions.episode_ids,
        ),
        (
            GenerationJob.job_id,
            (),
            (GenerationJob.output_json, GenerationJob.metrics),
            exclusions.generation_job_ids,
        ),
        (
            VideoGenerationJob.job_id,
            (),
            (
                VideoGenerationJob.input_json,
                VideoGenerationJob.output_json,
                VideoGenerationJob.metrics,
            ),
            exclusions.video_generation_job_ids,
        ),
        (
            TimelineRenderJob.job_id,
            (TimelineRenderJob.output_storage_key,),
            (
                TimelineRenderJob.input_json,
                TimelineRenderJob.output_json,
            ),
            exclusions.timeline_render_job_ids,
        ),
        (
            StoryboardJob.job_id,
            (),
            (StoryboardJob.detail,),
            exclusions.storyboard_job_ids,
        ),
        (
            PipelineJob.id,
            (),
            (PipelineJob.output_json,),
            exclusions.pipeline_job_ids,
        ),
        (
            ShotGroup.shot_group_id,
            (),
            (
                ShotGroup.reference_frames,
                ShotGroup.materials,
                ShotGroup.audio_elements,
            ),
            exclusions.shot_group_ids,
        ),
        (
            CanvasNode.id,
            (),
            (CanvasNode.data,),
            exclusions.canvas_node_ids,
        ),
        (
            CanvasEdge.id,
            (),
            (CanvasEdge.data,),
            exclusions.canvas_edge_ids,
        ),
    )
    for identifier, scalar_columns, json_columns, excluded_ids in scans:
        await _scan_reference_table(
            db,
            identifier=identifier,
            scalar_columns=scalar_columns,
            json_columns=json_columns,
            excluded_ids=excluded_ids,
            candidates=candidates,
            destination=protected,
        )

    return StorageKeyDecision(
        candidate_keys=frozenset(candidates),
        protected_keys=frozenset(protected),
        deletable_keys=frozenset(candidates - protected),
    )


def _chunks(values: set[str]) -> list[list[str]]:
    return _chunks_with_size(values, _PROMOTE_QUERY_CHUNK_SIZE)


async def _load_promote_closure(
    db: Any,
    candidate_keys: set[str],
    *,
    asset_ids: set[str],
) -> tuple[set[str], list[MediaStorageJob]]:
    """在固定写屏障内按稳定行序锁定 promote 图的有界连通闭包。"""
    await acquire_storage_reference_barrier(db)
    closure = normalize_storage_keys(candidate_keys)
    if len(closure) > _MAX_PROMOTE_CLOSURE_KEYS:
        raise ConflictError("对象存储清理依赖过多，请缩小删除范围后重试")

    frontier = set(closure)
    pending_asset_ids = set(asset_ids)
    rows_by_id: dict[str, MediaStorageJob] = {}
    for _round in range(_MAX_PROMOTE_CLOSURE_ROUNDS):
        matched: dict[str, MediaStorageJob] = {}
        for chunk in _chunks(frontier):
            seen_ids = set(rows_by_id) | set(matched)
            remaining = _MAX_PROMOTE_CLOSURE_ROWS - len(seen_ids)
            statement = select(MediaStorageJob).where(
                MediaStorageJob.job_type == "promote",
                or_(
                    MediaStorageJob.source_key.in_(chunk),
                    MediaStorageJob.final_key.in_(chunk),
                ),
            )
            if seen_ids:
                statement = statement.where(MediaStorageJob.id.not_in(sorted(seen_ids)))
            rows = (
                (
                    await db.execute(
                        statement
                        .order_by(MediaStorageJob.id.asc())
                        .limit(remaining + 1)
                        .with_for_update()
                    )
                )
                .scalars()
                .all()
            )
            if len(rows) > remaining:
                raise ConflictError("对象存储清理任务过多，请缩小删除范围后重试")
            matched.update({row.id: row for row in rows})
        for chunk in _chunks(pending_asset_ids):
            seen_ids = set(rows_by_id) | set(matched)
            remaining = _MAX_PROMOTE_CLOSURE_ROWS - len(seen_ids)
            statement = select(MediaStorageJob).where(
                MediaStorageJob.job_type == "promote",
                MediaStorageJob.asset_id.in_(chunk),
            )
            if seen_ids:
                statement = statement.where(MediaStorageJob.id.not_in(sorted(seen_ids)))
            rows = (
                (
                    await db.execute(
                        statement
                        .order_by(MediaStorageJob.id.asc())
                        .limit(remaining + 1)
                        .with_for_update()
                    )
                )
                .scalars()
                .all()
            )
            if len(rows) > remaining:
                raise ConflictError("对象存储清理任务过多，请缩小删除范围后重试")
            matched.update({row.id: row for row in rows})
        pending_asset_ids.clear()

        rows_by_id.update(matched)
        if len(rows_by_id) > _MAX_PROMOTE_CLOSURE_ROWS:
            raise ConflictError("对象存储清理任务过多，请缩小删除范围后重试")
        expanded: set[str] = set()
        for row in matched.values():
            for value in (row.source_key, row.final_key):
                if value is None:
                    continue
                normalized = _try_normalize(value)
                if normalized is not None and normalized not in closure:
                    expanded.add(normalized)
        if not expanded:
            return closure, [rows_by_id[row_id] for row_id in sorted(rows_by_id)]
        closure.update(expanded)
        if len(closure) > _MAX_PROMOTE_CLOSURE_KEYS:
            raise ConflictError("对象存储清理依赖过多，请缩小删除范围后重试")
        frontier = expanded

    raise ConflictError("对象存储清理依赖链过深，请缩小删除范围后重试")


def _propagate_required_promote_sources(
    rows: list[MediaStorageJob],
    protected_keys: set[str],
) -> set[str]:
    """活动 promote 的 final 被保留时，其尚未消费的 source 也必须保留。"""
    protected = set(protected_keys)
    for _round in range(len(rows) + 1):
        expanded = False
        for row in rows:
            if row.status not in _ACTIVE_PROMOTE_STATUSES or row.final_key is None:
                continue
            final_key = _try_normalize(row.final_key)
            source_key = _try_normalize(row.source_key)
            if (
                final_key is not None
                and source_key is not None
                and final_key in protected
                and source_key not in protected
            ):
                protected.add(source_key)
                expanded = True
        if not expanded:
            return protected
    raise ConflictError("对象存储清理依赖保护链无法收敛")


async def enqueue_storage_deletes(
    db: Any,
    storage_keys: set[str],
    *,
    exclusions: StorageReferenceExclusions,
    asset_ids_by_key: dict[str, str] | None = None,
    upload_ids_by_key: dict[str, str] | None = None,
    protected_storage_keys: set[str] | None = None,
) -> None:
    """仅对判权后的 key 取消晋升，并幂等写入物理删除任务。"""
    from app.services.media_storage_jobs import cancel_promotes_for_delete, enqueue_delete

    explicitly_protected = normalize_storage_keys(protected_storage_keys or set())
    normalized_keys = normalize_storage_keys(storage_keys) - explicitly_protected
    normalized_asset_ids = {
        normalized: asset_id
        for key, asset_id in (asset_ids_by_key or {}).items()
        if (normalized := _try_normalize(key)) is not None
        and normalized in normalized_keys
    }
    normalized_upload_ids = {
        normalized: upload_id
        for key, upload_id in (upload_ids_by_key or {}).items()
        if (normalized := _try_normalize(key)) is not None
    }
    closure_keys, promote_rows = await _load_promote_closure(
        db,
        normalized_keys,
        asset_ids=set(normalized_asset_ids.values()),
    )
    decision = await decide_storage_key_deletes(
        db,
        closure_keys,
        exclusions=exclusions,
    )
    protected_keys = _propagate_required_promote_sources(
        promote_rows,
        set(decision.protected_keys) | explicitly_protected,
    )
    deletable_keys = set(decision.candidate_keys) - protected_keys
    cancellable_final_keys = {
        final_key
        for row in promote_rows
        if row.final_key is not None
        and (final_key := _try_normalize(row.final_key)) is not None
        and final_key in deletable_keys
    }
    await cancel_promotes_for_delete(
        db,
        asset_ids=set(),
        final_keys=cancellable_final_keys,
    )
    for key in sorted(deletable_keys):
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        await enqueue_delete(
            db,
            idempotency_key=f"lifecycle-delete:{digest}",
            source_key=key,
            asset_id=normalized_asset_ids.get(key),
            upload_id=normalized_upload_ids.get(key),
        )


def _try_normalize(value: str) -> str | None:
    try:
        return validate_key_prefix(value)
    except ValueError:
        logger.warning("跳过不受支持的存量媒体键")
        return None
