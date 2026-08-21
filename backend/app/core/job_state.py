"""可测试的任务租约、CAS 与流水线错误状态辅助。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from secrets import token_urlsafe

TASK_HARD_TIME_LIMIT_SECONDS = 1320
TASK_SOFT_TIME_LIMIT_SECONDS = 1200
# 分镜 worker 在生成调用结束后还需要完成 lease CAS、积分结算和状态回写。
STORYBOARD_GENERATION_TIMEOUT_SECONDS = TASK_SOFT_TIME_LIMIT_SECONDS - 90
MEDIA_STORAGE_JOB_LEASE_SECONDS = TASK_HARD_TIME_LIMIT_SECONDS + 60

_SAFE_FAILURES: dict[str, tuple[str, bool]] = {
    "provider_contract_error": ("供应商响应不符合约定", False),
    "provider_timeout": ("上游服务超时", True),
    "infrastructure_unavailable": ("依赖服务暂不可用", True),
    "invalid_pipeline_data": ("流水线数据无效", False),
    "insufficient_credits": ("积分不足", False),
    "internal_error": ("任务执行失败", False),
}

_SAFE_DIAGNOSTIC_CATEGORIES = frozenset(
    {"invalid_json", "non_object_json", "schema_validation", "semantic_validation"}
)
_SAFE_DIAGNOSTIC_SCHEMAS = frozenset(
    {
        "SplitOutput",
        "ExtractOutput",
        "MergeOutput",
        "ParentsOutput",
        "StoryboardContract",
        "StoryboardV2Contract",
        "StoryboardSceneSplitContract",
    }
)
_SAFE_DIAGNOSTIC_RESPONSE_KINDS = frozenset(
    {"text", "tool_input", "empty", "list", "str", "int", "float", "bool", "NoneType"}
)
_SAFE_SEMANTIC_DIAGNOSTIC_CODES = frozenset(
    {
        "semantic_validation",
        "scene_no_not_consecutive",
        "group_no_not_consecutive",
        "group_duration_out_of_range",
        "shot_count_out_of_range",
        "display_no_not_consecutive",
        "shot_time_not_continuous",
        "shot_end_must_be_greater_than_start",
        "shot_duration_total_mismatch",
        "description_template_mismatch",
        "shot_line_format_invalid",
        "shot_line_time_mismatch",
        "reference_not_allowed",
        "reference_type_mismatch",
        "schema_version_unsupported",
        "scene_split_no_source_blocks",
        "scene_split_too_few_scenes",
        "scene_split_scene_no_not_consecutive",
        "scene_split_background_not_allowed",
        "scene_split_duplicate_block_ref",
        "scene_split_unknown_block_ref",
        "scene_split_block_assigned_twice",
        "scene_split_unknown_omitted_block_ref",
        "scene_split_duplicate_omitted_block_ref",
        "scene_split_covered_and_omitted",
        "scene_generation_requires_v2",
        "scene_generation_requires_one_scene",
        "scene_generation_scene_no_must_be_one",
        "scene_generation_background_mismatch",
        "scene_generation_group_budget_mismatch",
        "scene_generation_omitted_block_out_of_scope",
        "scene_generation_duplicate_omitted_block",
        "scene_generation_omitted_block_without_reason",
        "scene_generation_covered_block_out_of_scope",
        "scene_generation_covered_and_omitted",
        "scene_generation_block_coverage_incomplete",
        "merged_scene_missing_v2",
        "merged_scene_shape_invalid",
        "merged_omitted_item_invalid",
        "merged_omitted_block_ref_invalid",
        "merged_omitted_block_out_of_scope",
        "merged_omitted_block_without_reason",
        "merged_duplicate_omitted_block",
        "merged_covered_block_out_of_scope",
        "merged_covered_and_omitted",
        "merged_block_coverage_incomplete",
    }
)
_SAFE_DIAGNOSTIC_PATH_FIELDS = frozenset(
    {
        "schema_version",
        "episodes",
        "episode_no",
        "title",
        "script_text",
        "subjects",
        "merged_subjects",
        "supplemental_subjects",
        "total_count",
        "name",
        "type",
        "description",
        "is_child",
        "episode_nos",
        "evidence",
        "quote",
        "parent_subjects",
        "child_count",
        "child_ids",
        "scenes",
        "scene_no",
        "background",
        "background_ref",
        "shot_groups",
        "group_no",
        "narrative_purpose",
        "estimated_groups",
        "duration_seconds",
        "is_continuation",
        "shots",
        "display_no",
        "content",
        "shot_line",
        "start_seconds",
        "end_seconds",
        "characters",
        "character_refs",
        "dialogue_character",
        "dialogue",
        "speaker_ref",
        "text",
        "kind",
        "props",
        "prop_refs",
        "sounds",
        "sound_refs",
        "sound_design",
        "visual",
        "shot_size",
        "composition",
        "camera_angle",
        "camera_movement",
        "action",
        "frame_description",
        "lighting",
        "emotion",
        "source_block_refs",
        "coverage",
        "status",
        "source_range",
        "char_start",
        "char_end",
        "summary",
        "repeat_reason",
        "omitted_source_blocks",
        "block_ref",
        "reason",
        "start_boundary",
        "end_boundary",
        "positions",
        "character_positions",
        "ref",
        "position",
        "facing",
        "pose_balance",
        "gaze",
        "hands",
        "left",
        "right",
        "held_props",
        "visible_state",
        "audience_visibility",
        "fact",
        "permission",
        "evidence",
        "reveal_trigger",
        "protection_method",
        "rationale",
    }
)


def _normalize_diagnostic_path(value: object) -> str | None:
    """把诊断路径收敛为已知合约字段，未知片段不原样出站。"""
    if not isinstance(value, str) or len(value) > 255:
        return None
    if value == "<root>":
        return value
    normalized: list[str] = []
    for raw_segment in value.split("."):
        is_array = raw_segment.endswith("[]")
        segment = raw_segment[:-2] if is_array else raw_segment
        if segment not in _SAFE_DIAGNOSTIC_PATH_FIELDS and segment != "<unknown_field>":
            segment = "<unknown_field>"
        normalized.append(f"{segment}[]" if is_array else segment)
    return ".".join(normalized) if normalized else None


def _safe_diagnostic(value: object) -> dict[str, object] | None:
    """仅透传合约层定义的枚举和字段路径，禁止异常携带任意内容出站。"""
    if not isinstance(value, dict):
        return None
    category = value.get("category")
    schema = value.get("schema")
    if (
        category not in _SAFE_DIAGNOSTIC_CATEGORIES
        or schema not in _SAFE_DIAGNOSTIC_SCHEMAS
    ):
        return None
    if category in {"invalid_json", "non_object_json"}:
        response = value.get("response")
        if not isinstance(response, dict):
            return None
        kind = response.get("kind")
        length_bucket = response.get("length_bucket")
        top_level_kind = response.get("top_level_kind")
        if (
            not isinstance(kind, str)
            or kind not in _SAFE_DIAGNOSTIC_RESPONSE_KINDS
            or length_bucket not in {"0", "1_128", "129_2048", "over_2048"}
            or top_level_kind not in {"invalid_json", "list", "str", "int", "float", "bool", "NoneType"}
        ):
            return None
        return {
            "version": 1,
            "category": category,
            "schema": schema,
            "response": {
                "kind": kind,
                "length_bucket": length_bucket,
                "top_level_kind": top_level_kind,
            },
        }
    validation = value.get("validation")
    if not isinstance(validation, dict):
        return None
    raw_codes = validation.get("codes")
    raw_paths = validation.get("paths")
    if not isinstance(raw_codes, list) or not isinstance(raw_paths, list):
        return None
    codes = [
        item
        for item in raw_codes[:8]
        if isinstance(item, str)
        and (
            item in _SAFE_SEMANTIC_DIAGNOSTIC_CODES
            if category == "semantic_validation"
            else item.replace("_", "").isalnum()
        )
    ]
    paths = [
        path
        for item in raw_paths[:8]
        if (path := _normalize_diagnostic_path(item)) is not None
    ]
    if not codes or not paths:
        return None
    raw_error_count = validation.get("error_count")
    error_count = (
        raw_error_count
        if isinstance(raw_error_count, int) and raw_error_count >= 0
        else len(codes)
    )
    return {
        "version": 1,
        "category": category,
        "schema": schema,
        "validation": {
            "error_count": min(error_count, 8),
            "codes": codes,
            "paths": paths,
        },
    }


@dataclass
class LeaseState:
    """可由 ORM 行或内存测试替代的最小租约状态。"""

    status: str
    lease_owner: str | None = None
    lease_token: str | None = None
    lease_until: datetime | None = None
    heartbeat_at: datetime | None = None


@dataclass
class StageSnapshot:
    status: str
    detail: dict | None
    current: int | None
    total: int | None
    error: dict | None


@dataclass(frozen=True)
class PipelineFailure:
    error_type: str
    error_message: str
    retryable: bool
    diagnostic: dict[str, object] | None = None

    @classmethod
    def from_exception(cls, exc: Exception) -> PipelineFailure:
        declared = str(getattr(exc, "error_type", ""))
        if declared in _SAFE_FAILURES:
            error_type = declared
        elif isinstance(exc, TimeoutError):
            error_type = "provider_timeout"
        elif isinstance(exc, ConnectionError | OSError):
            error_type = "infrastructure_unavailable"
        elif isinstance(exc, ValueError):
            error_type = "invalid_pipeline_data"
        elif getattr(exc, "status_code", None) == 402:
            error_type = "insufficient_credits"
        else:
            error_type = "internal_error"
        message, default_retryable = _SAFE_FAILURES[error_type]
        retryable = bool(getattr(exc, "retryable", default_retryable))
        diagnostic = _safe_diagnostic(getattr(exc, "diagnostic", None))
        return cls(error_type, message, retryable, diagnostic)

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "error_type": self.error_type,
            "error_message": self.error_message,
            "retryable": self.retryable,
        }
        if self.diagnostic is not None:
            payload["diagnostic"] = self.diagnostic
        return payload


def claim_lease(
    state: LeaseState, owner: str, now: datetime, duration: timedelta
) -> str | None:
    """CAS 领取 pending/retry 或已过期的 running 项，返回不可伪造的 token。"""
    expired = state.lease_until is not None and state.lease_until <= now
    if state.status not in {"pending", "retry"} and not (
        state.status == "running" and expired
    ):
        return None
    token = token_urlsafe(24)
    state.status = "running"
    state.lease_owner = owner
    state.lease_token = token
    state.lease_until = now + duration
    state.heartbeat_at = now
    return token


def heartbeat_lease(
    state: LeaseState, token: str, now: datetime, duration: timedelta
) -> bool:
    if state.status != "running" or state.lease_token != token:
        return False
    state.lease_until = now + duration
    state.heartbeat_at = now
    return True


def complete_lease(state: LeaseState, token: str, terminal_status: str) -> bool:
    """仅当前租约 token 可以把运行态转换为终态。"""
    if terminal_status not in {"done", "failed", "published", "skipped"}:
        raise ValueError("terminal_status 必须是终态")
    if state.status != "running" or state.lease_token != token:
        return False
    state.status = terminal_status
    state.lease_owner = None
    state.lease_token = None
    state.lease_until = None
    return True


def reset_stage(stage: StageSnapshot) -> None:
    """新轮次必须清空上一轮的展示状态和错误。"""
    stage.status = "pending"
    stage.detail = None
    stage.current = None
    stage.total = None
    stage.error = None


def is_current_input_version(input_version: int, active_version: int) -> bool:
    """只有完全匹配的版本可写回，晚到或异常超前任务都必须放弃结果。"""
    return input_version == active_version
