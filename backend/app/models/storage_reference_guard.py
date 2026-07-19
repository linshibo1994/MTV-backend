"""对象存储引用写入与物理删除 tombstone 的数据库协议。"""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import event, func, inspect, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import ORMExecuteState, Session
from sqlalchemy.sql.dml import Insert, Update
from sqlalchemy.sql.elements import BindParameter, TextClause

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

_REFERENCE_FIELDS: dict[type[Any], tuple[str, ...]] = {
    AssetEntry: ("storage_key",),
    MediaUpload: ("temp_storage_key", "final_storage_key"),
    Episode: ("script_file_storage_key",),
    GenerationJob: ("input_json", "output_json", "metrics"),
    VideoGenerationJob: ("input_json", "output_json", "metrics"),
    TimelineRenderJob: ("input_json", "output_json", "output_storage_key"),
    StoryboardJob: ("detail",),
    PipelineJob: ("output_json",),
    ShotGroup: ("reference_frames", "materials", "audio_elements"),
    CanvasNode: ("data",),
    CanvasEdge: ("data",),
}
_REFERENCE_FIELDS_BY_TABLE = {
    model.__tablename__: fields for model, fields in _REFERENCE_FIELDS.items()
}
_PRE_EFFECT_DELETE_STATUSES = frozenset({"pending", "retry", "running"})
DELETE_TOMBSTONE_STATUSES = frozenset({"deleting", "succeeded", "effect_failed"})
_RAW_REFERENCE_WRITE = re.compile(
    rf"\b(?:insert\s+into|update)\s+(?:\"?\w+\"?\.)?\"?(?:{'|'.join(map(re.escape, _REFERENCE_FIELDS_BY_TABLE))})\"?\b",
    re.IGNORECASE,
)
STORAGE_REFERENCE_GUARD_BYPASS = "storage_reference_guard_bypass"


def storage_key_lock_id(value: str) -> int:
    """生成跨进程稳定的 PostgreSQL advisory lock 有符号 64 位 ID。"""
    normalized = validate_key_prefix(value)
    digest = hashlib.sha256(normalized.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def _normalize_values(values: Any, destination: set[str]) -> None:
    if isinstance(values, str):
        try:
            destination.add(validate_key_prefix(values))
        except ValueError:
            return
    elif isinstance(values, dict):
        for value in values.values():
            _normalize_values(value, destination)
    elif isinstance(values, (list, tuple, set, frozenset)):
        for value in values:
            _normalize_values(value, destination)


def _canonicalize_values(values: Any) -> Any:
    """递归规范化引用字段中的合法对象键，保留其他业务值。"""
    if isinstance(values, str):
        try:
            return validate_key_prefix(values)
        except ValueError:
            return values
    if isinstance(values, dict):
        return {key: _canonicalize_values(value) for key, value in values.items()}
    if isinstance(values, list):
        return [_canonicalize_values(value) for value in values]
    if isinstance(values, tuple):
        return tuple(_canonicalize_values(value) for value in values)
    if isinstance(values, set):
        return {_canonicalize_values(value) for value in values}
    if isinstance(values, frozenset):
        return frozenset(_canonicalize_values(value) for value in values)
    return values


def normalize_reference_keys(values: Any) -> tuple[str, ...]:
    normalized: set[str] = set()
    _normalize_values(values, normalized)
    return tuple(sorted(normalized))


def _dialect_name(bind: Any) -> str | None:
    try:
        return str(bind.dialect.name)
    except (AttributeError, RuntimeError):
        return None


def _acquire_sync_key_locks(connection: Any, keys: tuple[str, ...]) -> None:
    if _dialect_name(connection) != "postgresql":
        return
    for key in keys:
        connection.execute(select(func.pg_advisory_xact_lock(storage_key_lock_id(key))))


async def acquire_storage_key_locks(db: Any, values: Any) -> tuple[str, ...]:
    """按规范化 key 稳定排序获取事务级 advisory lock。"""
    keys = normalize_reference_keys(values)
    try:
        bind = db.get_bind()
    except (AttributeError, RuntimeError):
        return keys
    if _dialect_name(bind) != "postgresql":
        return keys
    for key in keys:
        await db.execute(select(func.pg_advisory_xact_lock(storage_key_lock_id(key))))
    return keys


def _guard_reference_keys(connection: Any, values: Any) -> None:
    keys = normalize_reference_keys(values)
    if not keys:
        return
    # 部分 SQLite 单元测试只建业务表子集；生产 PostgreSQL 缺表仍必须直接失败。
    if _dialect_name(connection) == "sqlite" and not inspect(connection).has_table(
        MediaStorageJob.__tablename__
    ):
        return
    _acquire_sync_key_locks(connection, keys)
    rows = list(
        connection.execute(
            select(MediaStorageJob.id, MediaStorageJob.status)
            .where(
                MediaStorageJob.job_type == "delete",
                func.ltrim(MediaStorageJob.source_key, "/").in_(keys),
                MediaStorageJob.status.in_(
                    sorted(_PRE_EFFECT_DELETE_STATUSES | DELETE_TOMBSTONE_STATUSES)
                ),
            )
            .order_by(MediaStorageJob.id.asc())
            .with_for_update()
        ).all()
    )
    if any(status in DELETE_TOMBSTONE_STATUSES for _job_id, status in rows):
        raise ConflictError("媒体对象正在删除或已删除，拒绝新增引用")

    cancel_ids = [
        job_id for job_id, status in rows if status in _PRE_EFFECT_DELETE_STATUSES
    ]
    if cancel_ids:
        connection.execute(
            update(MediaStorageJob)
            .where(
                MediaStorageJob.id.in_(cancel_ids),
                MediaStorageJob.status.in_(sorted(_PRE_EFFECT_DELETE_STATUSES)),
            )
            .values(
                status="canceled",
                lease_owner=None,
                lease_token=None,
                lease_until=None,
                completed_at=datetime.now(UTC),
            )
        )


def _instance_reference_values(target: Any, *, inserting: bool) -> list[Any]:
    fields = _REFERENCE_FIELDS[type(target)]
    state = inspect(target)
    status_changed = "status" in state.attrs and state.attrs.status.history.has_changes()

    if isinstance(target, AssetEntry):
        active = (target.status or "candidate") != "deleted"
    elif isinstance(target, MediaUpload):
        status = target.status or "staging"
        active = status in {"staging", "ready"}
        fields = (
            ("temp_storage_key", "final_storage_key")
            if status == "staging"
            else ("final_storage_key",)
        )
    else:
        active = True
    if not active:
        return []

    if inserting or status_changed:
        selected_fields = fields
    else:
        selected_fields = tuple(
            field for field in fields if state.attrs[field].history.has_changes()
        )
    values: list[Any] = []
    for field in selected_fields:
        value = _canonicalize_values(getattr(target, field, None))
        setattr(target, field, value)
        values.append(value)
    return values


def _before_reference_insert(_mapper: Any, connection: Any, target: Any) -> None:
    _guard_reference_keys(
        connection,
        _instance_reference_values(target, inserting=True),
    )


def _before_reference_update(_mapper: Any, connection: Any, target: Any) -> None:
    _guard_reference_keys(
        connection,
        _instance_reference_values(target, inserting=False),
    )


def _cancel_same_flush_delete_jobs(session: Session, _context: Any, _instances: Any) -> None:
    keys: set[str] = set()
    for target in (*session.new, *session.dirty):
        if type(target) not in _REFERENCE_FIELDS:
            continue
        for value in _instance_reference_values(target, inserting=target in session.new):
            _normalize_values(value, keys)
    if not keys:
        return
    now = datetime.now(UTC)
    for job in (*session.new, *session.dirty):
        if not isinstance(job, MediaStorageJob) or job.job_type != "delete":
            continue
        try:
            source_key = validate_key_prefix(job.source_key)
        except ValueError:
            continue
        if source_key not in keys:
            continue
        if job.status in DELETE_TOMBSTONE_STATUSES:
            raise ConflictError("媒体对象正在删除或已删除，拒绝新增引用")
        if job.status in _PRE_EFFECT_DELETE_STATUSES:
            job.status = "canceled"
            job.lease_owner = None
            job.lease_token = None
            job.lease_until = None
            job.completed_at = now


def _mapping_key(value: Any) -> str:
    return str(getattr(value, "key", value))


def _canonicalize_parameter_mapping(
    mapping: dict[Any, Any],
    fields: tuple[str, ...],
) -> tuple[dict[Any, Any], list[Any], set[str]]:
    normalized = dict(mapping)
    values: list[Any] = []
    touched_fields: set[str] = set()
    for key, value in mapping.items():
        field = _mapping_key(key)
        if field not in fields:
            continue
        canonical = _canonicalize_values(value)
        normalized[key] = canonical
        values.append(canonical)
        touched_fields.add(field)
    return normalized, values, touched_fields


def _canonicalize_reference_dml(
    statement: Any,
    multiparams: Any,
    params: dict[Any, Any],
) -> tuple[Any, Any, dict[Any, Any], list[Any]]:
    table = getattr(statement, "table", None)
    table_name = getattr(table, "name", None)
    fields = _REFERENCE_FIELDS_BY_TABLE.get(table_name)
    if fields is None:
        return statement, multiparams, params, []
    if isinstance(statement, Insert) and getattr(statement, "select", None) is not None:
        raise ConflictError("对象存储引用 INSERT FROM SELECT 无法安全判定")

    assignments = getattr(statement, "_values", None) or {}
    values: list[Any] = []
    touched_fields: set[str] = set()
    replacements: dict[str, Any] = {}
    explicit_status_values: list[Any] = []
    for column, expression in assignments.items():
        field = _mapping_key(column)
        if field == "status":
            if not isinstance(expression, BindParameter):
                raise ConflictError("对象存储引用状态批量写入无法安全判定")
            explicit_status_values.append(expression.value)
        if field not in fields:
            continue
        if not isinstance(expression, BindParameter):
            raise ConflictError("对象存储引用批量写入无法安全判定")
        canonical = _canonicalize_values(expression.value)
        replacements[field] = canonical
        values.append(canonical)
        touched_fields.add(field)
    if replacements:
        statement = statement.values(**replacements)

    normalized_multiparams: Any = multiparams
    if isinstance(multiparams, (list, tuple)):
        normalized_items: list[Any] = []
        for item in multiparams:
            if not isinstance(item, dict):
                normalized_items.append(item)
                continue
            normalized, item_values, item_fields = _canonicalize_parameter_mapping(item, fields)
            normalized_items.append(normalized)
            values.extend(item_values)
            touched_fields.update(item_fields)
        normalized_multiparams = (
            tuple(normalized_items) if isinstance(multiparams, tuple) else normalized_items
        )

    normalized_params, parameter_values, parameter_fields = _canonicalize_parameter_mapping(
        params, fields
    )
    values.extend(parameter_values)
    touched_fields.update(parameter_fields)

    if table_name in {AssetEntry.__tablename__, MediaUpload.__tablename__}:
        active_status = any(
            status not in {None, "deleted", "failed"} for status in explicit_status_values
        )
        if active_status and not touched_fields:
            raise ConflictError("对象存储引用状态批量写入必须显式提供存储键")
    return statement, normalized_multiparams, normalized_params, values


def _guard_session_parameterized_status(state: ORMExecuteState) -> None:
    """ORM flush 不触发本事件；显式 Session.execute 参数写入在此 fail closed。"""
    statement = state.statement
    table = getattr(statement, "table", None)
    table_name = getattr(table, "name", None)
    if table_name not in {AssetEntry.__tablename__, MediaUpload.__tablename__}:
        return
    raw_parameters = state.parameters
    if isinstance(raw_parameters, dict):
        parameter_sets = [raw_parameters]
    elif isinstance(raw_parameters, (list, tuple)):
        parameter_sets = [item for item in raw_parameters if isinstance(item, dict)]
    else:
        parameter_sets = []
    fields = _REFERENCE_FIELDS_BY_TABLE[table_name]
    for parameters in parameter_sets:
        status = parameters.get("status")
        if status in {None, "deleted", "failed"}:
            continue
        if not any(field in parameters for field in fields):
            raise ConflictError("对象存储引用状态批量写入必须显式提供存储键")


def _guard_connection_reference_write(
    connection: Any,
    statement: Any,
    multiparams: Any,
    params: dict[Any, Any],
    execution_options: Any,
) -> tuple[Any, Any, dict[Any, Any]]:
    if execution_options.get(STORAGE_REFERENCE_GUARD_BYPASS):
        return statement, multiparams, params
    if isinstance(statement, TextClause):
        if _RAW_REFERENCE_WRITE.search(statement.text):
            raise ConflictError("禁止使用未受保护的原始 SQL 写入对象存储引用")
        return statement, multiparams, params
    if not isinstance(statement, (Insert, Update)):
        return statement, multiparams, params
    statement, multiparams, params, values = _canonicalize_reference_dml(
        statement,
        multiparams,
        params,
    )
    if values:
        _guard_reference_keys(connection, values)
    return statement, multiparams, params


def _guard_uncompiled_reference_sql(
    _connection: Any,
    _cursor: Any,
    statement: str,
    _parameters: Any,
    context: Any,
    _executemany: bool,
) -> None:
    if getattr(context, "compiled", None) is not None:
        return
    execution_options = getattr(context, "execution_options", {})
    if execution_options.get(STORAGE_REFERENCE_GUARD_BYPASS):
        return
    if _RAW_REFERENCE_WRITE.search(statement):
        raise ConflictError("禁止使用未受保护的原始 SQL 写入对象存储引用")


class StorageReferenceGuardedSession(Session):
    """显式封禁会绕过 ORM/Engine 写入协议的 legacy bulk API。"""

    @staticmethod
    def _reject_protected_mapper(mapper: Any) -> None:
        model = mapper if isinstance(mapper, type) else getattr(mapper, "class_", None)
        if model in _REFERENCE_FIELDS:
            raise ConflictError("禁止对对象存储引用模型使用 legacy bulk API")

    def bulk_save_objects(
        self,
        objects: Any,
        return_defaults: bool = False,
        update_changed_only: bool = True,
        preserve_order: bool = True,
    ) -> None:
        rows = list(objects)
        if any(type(row) in _REFERENCE_FIELDS for row in rows):
            raise ConflictError("禁止对对象存储引用模型使用 legacy bulk API")
        super().bulk_save_objects(
            rows,
            return_defaults=return_defaults,
            update_changed_only=update_changed_only,
            preserve_order=preserve_order,
        )

    def bulk_insert_mappings(
        self,
        mapper: Any,
        mappings: Any,
        return_defaults: bool = False,
        render_nulls: bool = False,
    ) -> None:
        self._reject_protected_mapper(mapper)
        super().bulk_insert_mappings(
            mapper,
            mappings,
            return_defaults=return_defaults,
            render_nulls=render_nulls,
        )

    def bulk_update_mappings(self, mapper: Any, mappings: Any) -> None:
        self._reject_protected_mapper(mapper)
        super().bulk_update_mappings(mapper, mappings)


for _model in _REFERENCE_FIELDS:
    event.listen(_model, "before_insert", _before_reference_insert)
    event.listen(_model, "before_update", _before_reference_update)
event.listen(Session, "before_flush", _cancel_same_flush_delete_jobs)
event.listen(Session, "do_orm_execute", _guard_session_parameterized_status)
event.listen(Engine, "before_execute", _guard_connection_reference_write, retval=True)
event.listen(Engine, "before_cursor_execute", _guard_uncompiled_reference_sql)
