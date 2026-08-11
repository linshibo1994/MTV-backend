"""镜头组安全删除的专项验收。"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.api.v1 import shots as shots_api
from app.core.errors import ConflictError, ForbiddenError
from app.schemas.shot import VideoWorkbenchOut
from app.services.shot_group_delete import (
    _timeline_job_references_video_jobs,
    cancel_active_video_jobs_for_shot_group,
    delete_shot_group,
)
from app.services.video_cleanup_policy import has_retryable_video_cleanup_debt


class _Result:
    def __init__(self, *, scalar=None, scalars=None, rowcount: int | None = None):
        self.scalar_value = scalar
        self.scalar_values = list(scalars or [])
        self.rowcount = rowcount

    def scalar_one_or_none(self):
        return self.scalar_value

    def one_or_none(self):
        return self.scalar_value

    def scalars(self):
        return self

    def all(self):
        return list(self.scalar_values)

    def __iter__(self):
        return iter(self.scalar_values)


def test_video_cleanup_policy_preserves_manual_review_and_retryable_debts() -> None:
    assert has_retryable_video_cleanup_debt(
        "failed",
        {},
        "provider_submission_uncertain",
        False,
    )
    assert has_retryable_video_cleanup_debt(
        "canceled",
        {
            "provider_cancellation": {
                "provider_task_id": "provider-task-1",
                "outcome": "pending",
                "retryable": True,
            }
        },
        None,
        False,
    )
    assert has_retryable_video_cleanup_debt(
        "failed",
        {"attempt_output_cleanup": {"video": {"retryable": True}}},
        None,
        False,
    )
    assert not has_retryable_video_cleanup_debt(
        "succeeded",
        {"video_storage_key": "generate_outputs/video.mp4"},
        None,
        False,
    )


def test_active_timeline_snapshot_dependency_is_detected() -> None:
    assert _timeline_job_references_video_jobs(
        {"segments": [{"job_id": "video-1"}]},
        {"video-1"},
    )
    assert not _timeline_job_references_video_jobs(
        {"segments": [{"job_id": "video-other"}]},
        {"video-1"},
    )
    assert not _timeline_job_references_video_jobs({}, {"video-1"})


async def test_cancel_active_video_jobs_reuses_existing_safe_cancel_flow(monkeypatch) -> None:
    db = SimpleNamespace(
        execute=AsyncMock(return_value=_Result(scalars=["video-1", "video-2"]))
    )
    cancel = AsyncMock(
        side_effect=[
            SimpleNamespace(status="canceled"),
            SimpleNamespace(status="canceled"),
        ]
    )
    monkeypatch.setattr("app.services.video_generation.cancel_video_job", cancel)

    result = await cancel_active_video_jobs_for_shot_group(
        db,
        script_id="script-1",
        episode_id="episode-1",
        shot_group_id="group-1",
        actor_user_id="user-1",
    )

    assert result == ("video-1", "video-2")
    assert cancel.await_count == 2
    assert cancel.await_args_list[0].kwargs == {
        "script_id": "script-1",
        "job_id": "video-1",
        "actor_user_id": "user-1",
    }


async def test_delete_shot_group_removes_terminal_dependencies_and_updates_scene(monkeypatch) -> None:
    episode = SimpleNamespace(episode_version=3)
    scene = SimpleNamespace(scene_no=1, shot_group_count=2, pending_shot_count=4)
    group = SimpleNamespace(
        shot_group_id="group-1",
        scene_no=1,
        unit_id="group:group-1",
        reference_frames={"first_frame": "generate_outputs/frame.png"},
        materials={},
        audio_elements={},
    )
    video_job = SimpleNamespace(
        job_id="video-1",
        status="succeeded",
        input_json={},
        output_json={"video_storage_key": "generate_outputs/video.mp4"},
        metrics={},
        error_type=None,
        retryable=False,
        attempt=1,
    )
    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(scalar="script-1"),
                _Result(scalar=episode),
                _Result(scalar=("script-1", "episode-1", 1)),
                _Result(scalar=scene),
                _Result(scalar=group),
                _Result(scalars=[video_job]),
                _Result(scalars=[SimpleNamespace(status="captured")]),
                _Result(scalars=[]),
                _Result(scalars=[SimpleNamespace(shot_id="shot-1"), SimpleNamespace(shot_id="shot-2")]),
                _Result(rowcount=1),
                _Result(rowcount=1),
                _Result(rowcount=2),
                _Result(rowcount=1),
            ]
        ),
        flush=AsyncMock(),
    )
    acquire_barrier = AsyncMock()
    decide = AsyncMock(
        return_value=SimpleNamespace(
            deletable_keys=frozenset({"generate_outputs/video.mp4"}),
            protected_keys=frozenset({"generate_outputs/frame.png"}),
        )
    )
    enqueue = AsyncMock()
    cancel_outbox = AsyncMock(return_value=0)
    monkeypatch.setattr("app.services.shot_group_delete.acquire_storage_reference_barrier", acquire_barrier)
    monkeypatch.setattr("app.services.shot_group_delete.decide_storage_key_deletes", decide)
    monkeypatch.setattr("app.services.shot_group_delete.enqueue_storage_deletes", enqueue)
    monkeypatch.setattr("app.services.shot_group_delete.cancel_outbox_task", cancel_outbox)

    result = await delete_shot_group(
        db,
        script_id="script-1",
        episode_id="episode-1",
        shot_group_id="group-1",
        expected_episode_version=3,
    )

    assert result.episode_version == 4
    assert result.removed_shot_count == 2
    assert result.removed_video_job_ids == ("video-1",)
    assert result.removed_timeline_arrangement_count == 1
    assert result.scheduled_storage_delete_count == 1
    assert episode.episode_version == 4
    assert scene.shot_group_count == 1
    assert scene.pending_shot_count == 2
    cancel_outbox.assert_awaited_once_with(
        db,
        task_type="video.generate",
        job_id="video-1",
        attempt=1,
    )
    enqueue.assert_awaited_once()
    db.flush.assert_awaited_once()


async def test_delete_shot_group_rejects_active_video_job() -> None:
    episode = SimpleNamespace(episode_version=3)
    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(scalar="script-1"),
                _Result(scalar=episode),
                _Result(scalar=("script-1", "episode-1", 1)),
                _Result(scalar=SimpleNamespace(scene_no=1)),
                _Result(
                    scalar=SimpleNamespace(
                        shot_group_id="group-1",
                        scene_no=1,
                        unit_id="group:group-1",
                    )
                ),
                _Result(
                    scalars=[
                        SimpleNamespace(
                            status="processing",
                            output_json={},
                            error_type=None,
                            retryable=False,
                        )
                    ]
                ),
            ]
        ),
        flush=AsyncMock(),
    )

    with pytest.raises(ConflictError, match="正在生成"):
        await delete_shot_group(
            db,
            script_id="script-1",
            episode_id="episode-1",
            shot_group_id="group-1",
            expected_episode_version=3,
        )

    db.flush.assert_not_awaited()


async def test_delete_route_requires_confirmation(monkeypatch) -> None:
    validate = AsyncMock()
    delete = AsyncMock()
    monkeypatch.setattr(shots_api, "_validate_script_episode", validate)
    monkeypatch.setattr(shots_api, "delete_shot_group_safely", delete)

    with pytest.raises(ConflictError, match="confirm=true"):
        await shots_api.delete_shot_group(
            "script-1",
            "episode-1",
            "group-1",
            expected_episode_version=3,
            confirm=False,
            db=SimpleNamespace(),
            user=SimpleNamespace(user_id="user-1"),
        )

    delete.assert_not_awaited()
    validate.assert_awaited_once()


async def test_delete_route_returns_updated_workbench(monkeypatch) -> None:
    validate = AsyncMock()
    cancel = AsyncMock(return_value=("video-1",))
    delete = AsyncMock(
        return_value=SimpleNamespace(
            episode_version=4,
            removed_shot_count=2,
            removed_video_job_ids=("video-1",),
            removed_timeline_arrangement_count=1,
            scheduled_storage_delete_count=1,
        )
    )
    audit = AsyncMock()
    workbench = VideoWorkbenchOut(episode_id="episode-1", episode_version=4, scenes=[])
    get_workbench = AsyncMock(return_value=workbench)
    db = SimpleNamespace(commit=AsyncMock(), rollback=AsyncMock())
    monkeypatch.setattr(shots_api, "_validate_script_episode", validate)
    monkeypatch.setattr(shots_api, "cancel_active_video_jobs_for_shot_group", cancel)
    monkeypatch.setattr(shots_api, "delete_shot_group_safely", delete)
    monkeypatch.setattr(shots_api, "audit_log", audit)
    monkeypatch.setattr(shots_api, "get_video_workbench", get_workbench)

    response = await shots_api.delete_shot_group(
        "script-1",
        "episode-1",
        "group-1",
        expected_episode_version=3,
        confirm=True,
        db=db,
        user=SimpleNamespace(user_id="user-1"),
    )

    assert response == workbench
    cancel.assert_awaited_once()
    delete.assert_awaited_once()
    audit.assert_awaited_once()
    get_workbench.assert_awaited_once_with(db, "script-1", "episode-1")
    db.commit.assert_awaited_once()
    db.rollback.assert_not_awaited()


async def test_delete_service_rejects_cross_episode_group() -> None:
    db = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _Result(scalar="script-1"),
                _Result(scalar=SimpleNamespace(episode_version=3)),
                _Result(scalar=("script-1", "episode-other", 1)),
            ]
        )
    )

    with pytest.raises(ForbiddenError, match="不属于"):
        await delete_shot_group(
            db,
            script_id="script-1",
            episode_id="episode-1",
            shot_group_id="group-1",
            expected_episode_version=3,
        )
