"""分镜组首尾帧链式回填与结构化断链诊断测试。"""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.crud.shot import _to_reference_frames
from app.services.storyboard import ChainFrameUpdateResult, update_chain_frames


class _ScalarRows:
    def __init__(self, values: list[SimpleNamespace]) -> None:
        self._values = values

    def scalars(self) -> _ScalarRows:
        return self

    def all(self) -> list[SimpleNamespace]:
        return self._values


def _group(
    *,
    scene_no: int,
    group_index: int,
    group_no: str,
    reference_frames: dict[str, object],
    is_continuation: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        scene_no=scene_no,
        group_index=group_index,
        group_no=group_no,
        reference_frames=reference_frames,
        is_continuation=is_continuation,
    )


@pytest.mark.asyncio
async def test_update_chain_frames_backfills_direct_predecessor_without_mixing_boundaries() -> None:
    first_frames = {
        "first_frame": "storage/start-one.png",
        "last_frame": "storage/tail-one.png",
        "boundary_projections": {
            "start": {"prompt_fragment": "第一组入画事实"},
            "end": {"prompt_fragment": "第一组出画事实"},
        },
        "future_extension": {"schema": 2},
        "chain_diagnostics": [
            {
                "code": "future_chain_signal",
                "severity": "info",
                "payload": {"kept": True},
            }
        ],
    }
    second_frames = {
        "last_frame": "storage/tail-two.png",
        "boundary_projections": {
            "start": {"prompt_fragment": "第二组入画事实"},
            "end": {"prompt_fragment": "第二组出画事实"},
        },
        "another_extension": ["must", "survive"],
    }
    first = _group(
        scene_no=1,
        group_index=1,
        group_no="G1",
        reference_frames=deepcopy(first_frames),
    )
    second = _group(
        scene_no=1,
        group_index=2,
        group_no="G2",
        reference_frames=deepcopy(second_frames),
    )
    db = SimpleNamespace(
        execute=AsyncMock(return_value=_ScalarRows([first, second])),
        flush=AsyncMock(),
    )

    result = await update_chain_frames(db, "episode-1")

    assert isinstance(result, ChainFrameUpdateResult)
    assert isinstance(result, int)
    assert result == result.updated == 1
    assert result.model_dump() == {"updated": 1, "diagnostics": []}
    assert second.reference_frames["first_frame"] == "storage/tail-one.png"
    assert second.reference_frames["boundary_projections"] == second_frames["boundary_projections"]
    assert first.reference_frames["boundary_projections"] == first_frames["boundary_projections"]
    assert first.reference_frames["future_extension"] == {"schema": 2}
    assert second.reference_frames["another_extension"] == ["must", "survive"]
    assert first.reference_frames["chain_diagnostics"] == first_frames["chain_diagnostics"]
    db.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_chain_frames_persists_safe_missing_and_broken_chain_diagnostics() -> None:
    predecessor = _group(
        scene_no=2,
        group_index=3,
        group_no="G3",
        reference_frames={
            "first_frame": "storage/predecessor-start.png",
            "boundary_projections": {
                "start": {"prompt_fragment": "只属于前驱首帧"},
                "end": {"prompt_fragment": "只属于前驱尾帧"},
            },
            "unknown_reference_role": "preserved",
        },
    )
    continuation = _group(
        scene_no=2,
        group_index=4,
        group_no="G4",
        reference_frames={
            "boundary_projections": {
                "start": {"prompt_fragment": "只属于续帧组首帧"},
                "end": {"prompt_fragment": "只属于续帧组尾帧"},
            },
            "unknown_reference_role": {"version": "future"},
        },
        is_continuation=True,
    )
    db = SimpleNamespace(
        execute=AsyncMock(return_value=_ScalarRows([predecessor, continuation])),
        flush=AsyncMock(),
    )

    result = await update_chain_frames(db, "episode-safe-diagnostic")

    assert result.updated == 0
    assert [item["code"] for item in result.diagnostics] == [
        "chain_last_frame_missing",
        "chain_last_frame_missing",
        "chain_link_broken",
        "continuation_predecessor_last_frame_unavailable",
    ]
    continuation_diagnostics = continuation.reference_frames["chain_diagnostics"]
    assert [item["code"] for item in continuation_diagnostics] == [
        "chain_last_frame_missing",
        "chain_link_broken",
        "continuation_predecessor_last_frame_unavailable",
    ]
    for diagnostic in result.diagnostics:
        assert diagnostic["severity"] == "warning"
        assert diagnostic["location"].keys() == {
            "scene_no",
            "group_no",
            "group_index",
        }
        serialized = repr(diagnostic)
        assert "storage/" not in serialized
        assert "episode-safe-diagnostic" not in serialized
    assert continuation.reference_frames["boundary_projections"]["start"] == {
        "prompt_fragment": "只属于续帧组首帧"
    }
    assert continuation.reference_frames["boundary_projections"]["end"] == {
        "prompt_fragment": "只属于续帧组尾帧"
    }
    assert continuation.reference_frames["unknown_reference_role"] == {
        "version": "future"
    }
    db.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_chain_frames_resets_per_scene_and_accepts_legacy_frames() -> None:
    legacy = SimpleNamespace(
        scene_no=1,
        group_index=1,
        group_no="legacy-1",
        reference_frames={
            "first_frame": None,
            "last_frame": "storage/legacy-tail.png",
            "reference_frame": "storage/legacy-reference.png",
            "legacy_extension": {"untouched": True},
        },
    )
    next_scene = SimpleNamespace(
        scene_no=2,
        group_index=2,
        group_no="legacy-2",
        reference_frames={
            "first_frame": None,
            "last_frame": "storage/new-scene-tail.png",
            "legacy_extension": "unchanged",
        },
    )
    original_next_scene = deepcopy(next_scene.reference_frames)
    db = SimpleNamespace(
        execute=AsyncMock(return_value=_ScalarRows([legacy, next_scene])),
        flush=AsyncMock(),
    )

    result = await update_chain_frames(db, "episode-history")

    assert result == 0
    assert result.diagnostics == []
    assert next_scene.reference_frames == original_next_scene
    assert legacy.reference_frames["legacy_extension"] == {"untouched": True}
    db.flush.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_chain_frames_replaces_owned_diagnostics_after_chain_recovers() -> None:
    predecessor = _group(
        scene_no=1,
        group_index=1,
        group_no="G1",
        reference_frames={"last_frame": "storage/recovered-tail.png"},
    )
    current = _group(
        scene_no=1,
        group_index=2,
        group_no="G2",
        reference_frames={
            "last_frame": "storage/current-tail.png",
            "chain_diagnostics": [
                {
                    "code": "chain_link_broken",
                    "severity": "warning",
                    "location": {"scene_no": 1, "group_no": "G2", "group_index": 2},
                },
                {"code": "future_chain_signal", "payload": {"preserved": True}},
            ],
        },
    )
    db = SimpleNamespace(
        execute=AsyncMock(return_value=_ScalarRows([predecessor, current])),
        flush=AsyncMock(),
    )

    first_result = await update_chain_frames(db, "episode-recovered")
    second_result = await update_chain_frames(db, "episode-recovered")

    assert first_result.updated == 1
    assert second_result.updated == 0
    assert first_result.diagnostics == second_result.diagnostics == []
    assert current.reference_frames["first_frame"] == "storage/recovered-tail.png"
    assert current.reference_frames["chain_diagnostics"] == [
        {"code": "future_chain_signal", "payload": {"preserved": True}}
    ]
    assert db.flush.await_count == 1


def test_reference_frames_output_exposes_diagnostics_and_degrades_unknown_history() -> None:
    projected = _to_reference_frames(
        {
            "first_frame": "storage/first.png",
            "last_frame": None,
            "chain_diagnostics": [
                {
                    "code": "chain_last_frame_missing",
                    "severity": "warning",
                    "location": {
                        "scene_no": 1,
                        "group_no": "G1",
                        "group_index": 1,
                    },
                }
            ],
        }
    )
    assert projected is not None
    assert projected.chain_diagnostics[0].code == "chain_last_frame_missing"
    assert projected.chain_diagnostics[0].location.scene_no == 1

    legacy = _to_reference_frames(
        {
            "first_frame": "storage/legacy.png",
            "future_extension": {"schema": 9},
            "chain_diagnostics": [{"unknown": "future-shape"}],
        }
    )
    assert legacy is not None
    assert legacy.first_frame == "storage/legacy.png"
    assert legacy.chain_diagnostics == []
