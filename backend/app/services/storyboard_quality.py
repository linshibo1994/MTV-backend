"""storyboard.v2 的确定性质量评估。

本模块只读取已通过结构校验的合约并产出 ``QualityWarning``。连续性、覆盖度、
观众可见性等启发式规则首版一律 warn-only，不修改模型创作事实，也不抛业务异常。
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

from app.schemas.storyboard_contracts import (
    CharacterPosition,
    ShotBoundary,
    StoryboardV2Contract,
    StoryboardV2ShotContract,
    StoryboardV2ShotGroupContract,
)
from app.services.boundary_projection import boundary_snapshot
from app.services.prompt_builder import (
    build_director_command_summary,
    build_group_director_summaries,
)

WarningSeverity = Literal["warning"]

# 这些字段会被拼进交给视频模型的执行提示词，超长会挤掉后续镜头，
# 上限与 storyboard.system 提示词中的“字段精简”段保持一致。
_EXECUTION_FIELD_LIMITS: tuple[tuple[str, int], ...] = (
    ("frame_description", 30),
    ("action", 20),
    ("lighting", 16),
    ("emotion", 8),
)
_SOUND_DESIGN_LIMIT = 20
# 执行提示词的约束尾段要求画面无任何可读文字，画面字段不得反过来要求字幕。
# 屏幕文字检查只覆盖 storyboard.system 提示词里明确下禁令的四个画面字段；
# emotion 不描述画面内容，不在禁令内。
_ON_SCREEN_TEXT_FIELDS = frozenset({"frame_description", "action", "lighting", "sound_design"})
_ON_SCREEN_TEXT_WORDS = ("字幕", "片名", "预告文字", "logo", "水印", "标题卡")


@dataclass(frozen=True)
class ScriptBlock:
    """用于 Coverage 的稳定原文段。"""

    block_ref: str
    text: str
    char_start: int
    char_end: int

    def as_dict(self) -> dict[str, object]:
        return {
            "block_ref": self.block_ref,
            "text": self.text,
            "char_start": self.char_start,
            "char_end": self.char_end,
        }


@dataclass(frozen=True)
class QualityWarning:
    """不阻断生成的结构化质量告警。"""

    code: str
    path: str
    message: str
    severity: WarningSeverity = "warning"
    details: dict[str, object] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "path": self.path,
            "message": self.message,
            "severity": self.severity,
            "details": self.details,
        }


@dataclass(frozen=True)
class CoverageSummary:
    expected_block_refs: tuple[str, ...]
    covered_block_refs: tuple[str, ...]
    omitted_block_refs: tuple[str, ...]
    missing_block_refs: tuple[str, ...]
    unknown_block_refs: tuple[str, ...]

    @property
    def accounted_count(self) -> int:
        expected = set(self.expected_block_refs)
        return len(expected & (set(self.covered_block_refs) | set(self.omitted_block_refs)))

    @property
    def coverage_ratio(self) -> float:
        return self.accounted_count / len(self.expected_block_refs) if self.expected_block_refs else 1.0

    def as_dict(self) -> dict[str, object]:
        return {
            "expected_block_refs": list(self.expected_block_refs),
            "covered_block_refs": list(self.covered_block_refs),
            "omitted_block_refs": list(self.omitted_block_refs),
            "missing_block_refs": list(self.missing_block_refs),
            "unknown_block_refs": list(self.unknown_block_refs),
            "coverage_ratio": self.coverage_ratio,
        }


@dataclass(frozen=True)
class QualityReport:
    warnings: tuple[QualityWarning, ...]
    coverage: CoverageSummary
    shot_guidance: tuple[dict[str, object], ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "version": 1,
            "status": "warning" if self.warnings else "passed",
            "warnings": [warning.as_dict() for warning in self.warnings],
            "coverage": self.coverage.as_dict(),
            "shot_guidance": list(self.shot_guidance),
        }


_BLOCK_REF_RE = re.compile(r"^B0*([1-9]\d*)$")
_SCRIPT_BREAK_RE = re.compile(r"\n\s*\n+")


def canonical_source_block_ref(value: str) -> str:
    """把 ``B1``/``B001`` 收敛为 ``B01``；非法值原样返回供告警。"""

    normalized = value.strip().upper()
    match = _BLOCK_REF_RE.fullmatch(normalized)
    if match is None:
        return normalized
    number = int(match.group(1))
    return f"B{number:02d}"


def build_script_blocks(script_text: str) -> list[ScriptBlock]:
    """按空行优先、非空行兜底切分原文，并保留字符范围。

    该算法不依赖 LLM，输入不变时编号和范围稳定。连续非空行属于同一段；
    没有空行但存在多行时按行拆分，避免整集退化为一个 Coverage block。
    """

    if not script_text or not script_text.strip():
        return []
    chunks = [chunk.strip() for chunk in _SCRIPT_BREAK_RE.split(script_text) if chunk.strip()]
    if len(chunks) == 1:
        lines = [line.strip() for line in script_text.splitlines() if line.strip()]
        if len(lines) > 1:
            chunks = lines
    blocks: list[ScriptBlock] = []
    cursor = 0
    for index, chunk in enumerate(chunks, start=1):
        start = script_text.find(chunk, cursor)
        if start < 0:
            start = cursor
        end = start + len(chunk)
        cursor = end
        blocks.append(
            ScriptBlock(
                block_ref=f"B{index:02d}",
                text=chunk,
                char_start=start,
                char_end=end,
            )
        )
    return blocks


def _ordered_unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))


def _shot_path(scene_index: int, group_index: int, shot_index: int) -> str:
    return f"scenes[{scene_index}].shot_groups[{group_index}].shots[{shot_index}]"


def _iter_shots(
    contract: StoryboardV2Contract,
) -> Iterable[tuple[int, int, int, StoryboardV2ShotContract]]:
    for scene_index, scene in enumerate(contract.scenes):
        for group_index, group in enumerate(scene.shot_groups):
            for shot_index, shot in enumerate(group.shots):
                yield scene_index, group_index, shot_index, shot


def evaluate_coverage(
    contract: StoryboardV2Contract,
    expected_source_blocks: Sequence[str | ScriptBlock] = (),
) -> tuple[CoverageSummary, list[QualityWarning]]:
    """计算原文落实率；遗漏、未知与重复均只返回告警。"""

    expected = _ordered_unique(
        canonical_source_block_ref(
            block.block_ref if isinstance(block, ScriptBlock) else block
        )
        for block in expected_source_blocks
    )
    expected_set = set(expected)
    shot_refs: list[str] = []
    occurrences: dict[str, list[tuple[str, str | None]]] = {}
    warnings: list[QualityWarning] = []
    for scene_index, group_index, shot_index, shot in _iter_shots(contract):
        path = _shot_path(scene_index, group_index, shot_index)
        legacy_refs = [canonical_source_block_ref(ref) for ref in shot.source_block_refs]
        structured_refs = [canonical_source_block_ref(item.block_ref) for item in shot.coverage]
        structured_by_ref = dict(zip(structured_refs, shot.coverage, strict=True))
        normalized = _ordered_unique((*legacy_refs, *structured_by_ref))
        duplicates = sorted(
            {
                ref
                for refs in (legacy_refs, structured_refs)
                for ref, count in Counter(refs).items()
                if count > 1
            }
        )
        if duplicates:
            warnings.append(
                QualityWarning(
                    code="coverage_duplicate_shot_ref",
                    path=f"{path}.source_block_refs",
                    message="同一镜头重复引用了原文段落",
                    details={"block_refs": duplicates},
                )
            )
        shot_refs.extend(normalized)
        for ref in normalized:
            coverage_item = structured_by_ref.get(ref)
            occurrences.setdefault(ref, []).append(
                (
                    path,
                    coverage_item.repeat_reason
                    if coverage_item is not None
                    else None,
                )
            )

    for block_ref, block_occurrences in sorted(occurrences.items()):
        if len(block_occurrences) <= 1:
            continue
        missing_reasons = [path for path, reason in block_occurrences[1:] if not reason]
        warnings.append(
            QualityWarning(
                code=(
                    "coverage_cross_shot_duplicate_without_reason"
                    if missing_reasons
                    else "coverage_cross_shot_duplicate"
                ),
                path=block_occurrences[1][0],
                message="同一原文段被多个镜头落实，重复镜头应说明必要性",
                details={
                    "block_ref": block_ref,
                    "shot_paths": [path for path, _reason in block_occurrences],
                    "missing_reason_paths": missing_reasons,
                },
            )
        )

    omitted_refs = [
        canonical_source_block_ref(item.block_ref) for item in contract.omitted_source_blocks
    ]
    duplicate_omissions = sorted(
        ref for ref, count in Counter(omitted_refs).items() if count > 1
    )
    if duplicate_omissions:
        warnings.append(
            QualityWarning(
                code="coverage_duplicate_omission",
                path="omitted_source_blocks",
                message="同一原文段落存在重复省略声明",
                details={"block_refs": duplicate_omissions},
            )
        )
    for index, item in enumerate(contract.omitted_source_blocks):
        if not item.reason:
            warnings.append(
                QualityWarning(
                    code="coverage_omission_without_reason",
                    path=f"omitted_source_blocks[{index}].reason",
                    message="省略原文段落时应提供原因",
                    details={"block_ref": canonical_source_block_ref(item.block_ref)},
                )
            )

    covered = _ordered_unique(shot_refs)
    omitted = _ordered_unique(omitted_refs)
    accounted = set(covered) | set(omitted)
    unknown = _ordered_unique(ref for ref in (*covered, *omitted) if expected_set and ref not in expected_set)
    missing = tuple(ref for ref in expected if ref not in accounted)
    overlap = sorted(set(covered) & set(omitted))
    if overlap:
        warnings.append(
            QualityWarning(
                code="coverage_covered_and_omitted",
                path="omitted_source_blocks",
                message="原文段落不能同时标记为已落实和已省略",
                details={"block_refs": overlap},
            )
        )
    if unknown:
        warnings.append(
            QualityWarning(
                code="coverage_unknown_block",
                path="scenes",
                message="分镜引用了不在当前原文落实表中的段落",
                details={"block_refs": list(unknown)},
            )
        )
    if missing:
        warnings.append(
            QualityWarning(
                code="coverage_missing_block",
                path="scenes",
                message="部分原文段落既未落实到镜头，也未声明省略",
                details={"block_refs": list(missing)},
            )
        )
    summary = CoverageSummary(
        expected_block_refs=expected,
        covered_block_refs=covered,
        omitted_block_refs=omitted,
        missing_block_refs=missing,
        unknown_block_refs=unknown,
    )
    return summary, warnings


def _position_map(items: Sequence[CharacterPosition]) -> dict[str, CharacterPosition]:
    return {item.ref: item for item in items}


def _boundary_mismatches(previous: ShotBoundary, current: ShotBoundary) -> list[str]:
    mismatches: list[str] = []
    previous_positions = _position_map(previous.positions)
    current_positions = _position_map(current.positions)
    for ref in sorted(previous_positions.keys() & current_positions.keys()):
        previous_item = previous_positions[ref]
        current_item = current_positions[ref]
        if (
            previous_item.position
            and current_item.position
            and previous_item.position != current_item.position
        ):
            mismatches.append(f"positions.{ref}.position")
        if previous_item.facing and current_item.facing and previous_item.facing != current_item.facing:
            mismatches.append(f"positions.{ref}.facing")
    for field_name in ("pose_balance", "gaze", "hands"):
        left = getattr(previous, field_name)
        right = getattr(current, field_name)
        if left is not None and right is not None and left != right:
            mismatches.append(field_name)
    if previous.held_props and current.held_props and set(previous.held_props) != set(current.held_props):
        mismatches.append("held_props")
    if previous.visible_state and current.visible_state and set(previous.visible_state) != set(
        current.visible_state
    ):
        mismatches.append("visible_state")
    return mismatches


def evaluate_boundary_continuity(contract: StoryboardV2Contract) -> list[QualityWarning]:
    """比较同场景相邻镜头的 end/start boundary，不跨 Scene 强行对齐。"""

    warnings: list[QualityWarning] = []
    for scene_index, scene in enumerate(contract.scenes):
        ordered = [shot for group in scene.shot_groups for shot in group.shots]
        for current_index in range(1, len(ordered)):
            previous = ordered[current_index - 1]
            current = ordered[current_index]
            if previous.end_boundary is None or current.start_boundary is None:
                continue
            mismatches = _boundary_mismatches(previous.end_boundary, current.start_boundary)
            if mismatches:
                warnings.append(
                    QualityWarning(
                        code="boundary_discontinuity",
                        path=f"scenes[{scene_index}]",
                        message="相邻镜头的结束与开始边界存在可见事实跳变",
                        details={
                            "previous_shot_index": current_index - 1,
                            "current_shot_index": current_index,
                            "fields": mismatches,
                        },
                    )
                )
    return warnings


def _horizontal_side(position: str | None) -> str | None:
    if not position:
        return None
    if position.startswith("左"):
        return "left"
    if position.startswith("右"):
        return "right"
    return "center"


def _shot_character_positions(shot: StoryboardV2ShotContract) -> list[CharacterPosition]:
    # 连续性基准描述镜头结束后的状态；显式 end_boundary 因此优先于构图快照。
    if shot.end_boundary and shot.end_boundary.positions:
        return shot.end_boundary.positions
    if shot.character_positions:
        return shot.character_positions
    if shot.start_boundary and shot.start_boundary.positions:
        return shot.start_boundary.positions
    return []


_POSITION_MOVEMENT_WORDS = (
    "移动",
    "移至",
    "来到",
    "穿过",
    "横穿",
    "绕到",
    "换位",
    "走到",
    "跑到",
)
_TURN_WORDS = ("转身", "转向", "回身", "面向", "朝向")


def _action_mentions_character(shot: StoryboardV2ShotContract, ref: str) -> bool:
    """只把明确动作主体或单角色镜头视为当前角色的动作证据。"""

    explicit_refs = re.findall(
        r"(?<![A-Z0-9])[A-Z]{1,5}\d{2,10}(?![A-Z0-9])",
        shot.visual.action,
    )
    if explicit_refs:
        return explicit_refs[0] == ref
    return len(shot.character_refs) == 1 and shot.character_refs[0] == ref


def _boundary_movement_evidence(
    shot: StoryboardV2ShotContract,
    baseline: CharacterPosition,
    current: CharacterPosition,
) -> tuple[bool, bool]:
    """返回起止边界是否明确解释了位置变化与朝向变化。"""

    if shot.start_boundary is None or shot.end_boundary is None:
        return False, False
    start = _position_map(shot.start_boundary.positions).get(current.ref)
    end = _position_map(shot.end_boundary.positions).get(current.ref)
    if start is None or end is None:
        return False, False
    position_evidence = (
        baseline.position is not None
        and current.position is not None
        and start.position is not None
        and end.position is not None
        and start.position == baseline.position
        and end.position == current.position
        and start.position != end.position
    )
    facing_evidence = (
        baseline.facing is not None
        and current.facing is not None
        and start.facing is not None
        and end.facing is not None
        and start.facing == baseline.facing
        and end.facing == current.facing
        and start.facing != end.facing
    )
    return position_evidence, facing_evidence


def _action_movement_evidence(
    shot: StoryboardV2ShotContract,
    current: CharacterPosition,
) -> tuple[bool, bool]:
    """返回动作字段是否明确说明了跨侧移动与转向。"""

    action = shot.visual.action
    if not _action_mentions_character(shot, current.ref):
        return False, False
    side = _horizontal_side(current.position)
    side_marker = {"left": "左", "right": "右"}.get(side) if side is not None else None
    position_evidence = any(word in action for word in _POSITION_MOVEMENT_WORDS) and (
        side_marker is not None and side_marker in action
        or any(word in action for word in ("穿过", "横穿", "换位"))
    )
    facing_marker = (
        current.facing.removeprefix("面朝").removeprefix("侧身")
        if current.facing
        else None
    )
    facing_evidence = any(word in action for word in _TURN_WORDS) and (
        facing_marker in action if facing_marker in {"左", "右", "镜头"} else True
    )
    return position_evidence, facing_evidence


def evaluate_position_continuity(contract: StoryboardV2Contract) -> list[QualityWarning]:
    """以角色在场景内第一次明确位置为基准，提示左右跳侧和朝向反转。"""

    warnings: list[QualityWarning] = []
    for scene_index, scene in enumerate(contract.scenes):
        baseline: dict[str, CharacterPosition] = {}
        scene_shots = [shot for group in scene.shot_groups for shot in group.shots]
        for shot_index, shot in enumerate(scene_shots):
            for current in _shot_character_positions(shot):
                first = baseline.setdefault(current.ref, current)
                boundary_position, boundary_facing = _boundary_movement_evidence(
                    shot,
                    first,
                    current,
                )
                action_position, action_facing = _action_movement_evidence(shot, current)
                position_explained = boundary_position or action_position
                facing_explained = boundary_facing or action_facing
                first_side = _horizontal_side(first.position)
                current_side = _horizontal_side(current.position)
                crossed_axis = {first_side, current_side} == {"left", "right"}
                facing_changed = bool(
                    first.facing and current.facing and first.facing != current.facing
                )
                unsupported_position = crossed_axis and not position_explained
                unsupported_facing = facing_changed and not facing_explained
                if unsupported_position:
                    warnings.append(
                        QualityWarning(
                            code="position_axis_crossing",
                            path=f"scenes[{scene_index}].shots[{shot_index}].character_positions",
                            message="角色相对场景基准从画面一侧跳到另一侧，请核对 180 度线",
                            details={
                                "character_ref": current.ref,
                                "baseline_position": first.position,
                                "current_position": current.position,
                            },
                        )
                    )
                if unsupported_facing:
                    warnings.append(
                        QualityWarning(
                            code="facing_direction_changed",
                            path=f"scenes[{scene_index}].shots[{shot_index}].character_positions",
                            message="角色朝向相对场景基准发生变化，请确认是否有动作依据",
                            details={
                                "character_ref": current.ref,
                                "baseline_facing": first.facing,
                                "current_facing": current.facing,
                            },
                        )
                    )
                if (
                    (position_explained or facing_explained)
                    and not unsupported_position
                    and not unsupported_facing
                ):
                    baseline[current.ref] = current
    return warnings


def evaluate_audience_visibility(contract: StoryboardV2Contract) -> list[QualityWarning]:
    """检查保留揭示的本地完整性，并提示整集从未揭示的事实。"""

    warnings: list[QualityWarning] = []
    withheld: dict[str, tuple[str, str | None]] = {}
    revealed: set[str] = set()
    for scene_index, group_index, shot_index, shot in _iter_shots(contract):
        path = _shot_path(scene_index, group_index, shot_index)
        for visibility_index, item in enumerate(shot.audience_visibility):
            fact_key = item.fact.strip()
            if item.status == "withhold_now":
                if not item.reveal_trigger:
                    warnings.append(
                        QualityWarning(
                            code="audience_withhold_without_trigger",
                            path=f"{path}.audience_visibility[{visibility_index}].reveal_trigger",
                            message="保留揭示的事实缺少 reveal_trigger",
                            details={"fact": item.fact},
                        )
                    )
                withheld.setdefault(fact_key, (path, item.reveal_trigger))
            else:
                revealed.add(fact_key)
    for fact, (path, trigger) in withheld.items():
        if fact not in revealed:
            warnings.append(
                QualityWarning(
                    code="audience_fact_never_revealed",
                    path=path,
                    message="被保留的观众事实在本次分镜中没有后续揭示",
                    details={"fact": fact, "reveal_trigger": trigger},
                )
            )
    return warnings


def _execution_field_values(shot: StoryboardV2ShotContract) -> tuple[tuple[str, str, int], ...]:
    values = [
        (name, getattr(shot.visual, name), limit) for name, limit in _EXECUTION_FIELD_LIMITS
    ]
    values.append(("sound_design", shot.sound_design, _SOUND_DESIGN_LIMIT))
    return tuple(values)


def evaluate_execution_field_economy(contract: StoryboardV2Contract) -> list[QualityWarning]:
    """提示会进入执行提示词的字段超长或写入屏幕文字。"""

    warnings: list[QualityWarning] = []
    for scene_index, group_index, shot_index, shot in _iter_shots(contract):
        path = _shot_path(scene_index, group_index, shot_index)
        for name, value, limit in _execution_field_values(shot):
            field_path = f"{path}.sound_design" if name == "sound_design" else f"{path}.visual.{name}"
            if len(value) > limit:
                warnings.append(
                    QualityWarning(
                        code="execution_field_too_long",
                        path=field_path,
                        message=f"{name} 超出执行提示词建议长度，过长会挤掉后续镜头",
                        details={"field": name, "length": len(value), "limit": limit},
                    )
                )
            if name in _ON_SCREEN_TEXT_FIELDS:
                matched = [word for word in _ON_SCREEN_TEXT_WORDS if word in value.casefold()]
                if matched:
                    warnings.append(
                        QualityWarning(
                            code="execution_field_requests_on_screen_text",
                            path=field_path,
                            message=f"{name} 要求画面出现可读文字，与执行提示词的无字幕约束冲突",
                            details={"field": name, "matched": matched},
                        )
                    )
    return warnings


def build_shot_structured_data(
    shot: StoryboardV2ShotContract,
    *,
    subject_ids_by_ref: Mapping[str, str] | None = None,
    background_ref: str | None = None,
) -> dict[str, object]:
    """生成可写入 ``Shot.structured_data`` 的版本化快照。"""

    snapshot: dict[str, object] = {
        "schema_version": "storyboard.shot-structure.v1",
        **shot.model_dump(mode="json", exclude_none=True),
    }
    if subject_ids_by_ref:
        snapshot["subject_ids_by_ref"] = dict(sorted(subject_ids_by_ref.items()))
    if background_ref:
        snapshot["background_ref"] = background_ref
    return snapshot


def build_shot_group_quality_summary(
    group: StoryboardV2ShotGroupContract,
    *,
    warnings: Sequence[QualityWarning] = (),
) -> dict[str, object]:
    """生成可写入 ``ShotGroup.quality_summary`` 的确定性组级摘要。"""

    first_shot = group.shots[0]
    last_shot = group.shots[-1]
    boundary_snapshots: dict[str, object] = {}
    if first_shot.start_boundary is not None:
        boundary_snapshots["start"] = boundary_snapshot(
            first_shot.start_boundary,
            role="start",
        )
    if last_shot.end_boundary is not None:
        boundary_snapshots["end"] = boundary_snapshot(
            last_shot.end_boundary,
            role="end",
        )
    coverage_refs = _ordered_unique(
        canonical_source_block_ref(ref)
        for shot in group.shots
        for ref in (
            *shot.source_block_refs,
            *(item.block_ref for item in shot.coverage),
        )
    )
    return {
        "version": 1,
        "director_commands": [
            summary.as_dict() for summary in build_group_director_summaries(group)
        ],
        "coverage_block_refs": list(coverage_refs),
        "boundary_snapshots": boundary_snapshots,
        "warnings": [warning.as_dict() for warning in warnings],
    }


def evaluate_storyboard_quality(
    contract: StoryboardV2Contract,
    *,
    expected_source_blocks: Sequence[str | ScriptBlock] = (),
    check_execution_field_economy: bool = False,
) -> QualityReport:
    """运行全部首版纯规则，始终返回报告且不因质量问题抛异常。

    ``check_execution_field_economy`` 只在分镜生成时开启：字段长度由 Provider
    决定，重组既有镜头改不了它，合并路径开启只会重复刷同一批告警。
    """

    coverage, warnings = evaluate_coverage(contract, expected_source_blocks)
    warnings.extend(evaluate_boundary_continuity(contract))
    warnings.extend(evaluate_position_continuity(contract))
    warnings.extend(evaluate_audience_visibility(contract))
    if check_execution_field_economy:
        warnings.extend(evaluate_execution_field_economy(contract))
    guidance: list[dict[str, object]] = []
    for scene_index, scene in enumerate(contract.scenes):
        for group_index, group in enumerate(scene.shot_groups):
            for shot_index, shot in enumerate(group.shots):
                item = build_director_command_summary(shot, total_shots=len(group.shots)).as_dict()
                item["path"] = _shot_path(scene_index, group_index, shot_index)
                guidance.append(item)
    return QualityReport(
        warnings=tuple(warnings),
        coverage=coverage,
        shot_guidance=tuple(guidance),
    )


__all__ = [
    "CoverageSummary",
    "QualityReport",
    "QualityWarning",
    "ScriptBlock",
    "build_script_blocks",
    "build_shot_structured_data",
    "build_shot_group_quality_summary",
    "canonical_source_block_ref",
    "evaluate_audience_visibility",
    "evaluate_boundary_continuity",
    "evaluate_coverage",
    "evaluate_execution_field_economy",
    "evaluate_position_continuity",
    "evaluate_storyboard_quality",
]
