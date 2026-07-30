"""跨模块 Celery 注册与持久化 outbox 发布契约。"""

from dataclasses import dataclass
from typing import Any

import pytest

from app.workers import outbox
from app.workers.celery_app import app as celery_app


@dataclass
class _Signature:
    task_name: str
    args: tuple[Any, ...]
    published: bool = False

    def apply_async(self) -> dict[str, Any]:
        self.published = True
        return {"task": self.task_name, "args": self.args}


class _Task:
    def __init__(self, name: str) -> None:
        self.name = name

    def s(self, *args: Any) -> _Signature:
        return _Signature(self.name, args)


def test_celery_registers_every_worker_that_consumes_durable_outbox() -> None:
    includes = set(celery_app.conf.include)
    assert {
        "app.workers.pipeline",
        "app.workers.generation",
        "app.workers.storyboard",
        "app.workers.video_generation",
        "app.workers.timeline_render",
        "app.workers.outbox",
    } <= includes


def test_celery_loader_registers_all_primary_business_tasks() -> None:
    celery_app.loader.import_default_modules()

    assert {
        "pipeline.split",
        "pipeline.extract",
        "pipeline.merge",
        "pipeline.enrich",
        "pipeline.parents",
        "pipeline.finalize",
        "app.workers.generation.generate_images_task",
        "app.workers.storyboard.generate_storyboard",
        "app.workers.video_generation.run_video_generation",
        "timeline.render",
        "outbox.dispatch",
        "generation.reconcile-financial-cleanup",
        "generation.reconcile-stale",
        "app.workers.storyboard.reconcile_storyboard_dispatches",
        "app.workers.storyboard.reconcile_storyboard_financial_cleanup",
        "app.workers.video_generation.recover_stale_jobs",
        "app.workers.video_generation.reconcile_video_financial_cleanup",
        "timeline.render.reconcile_stale",
        "timeline.render.financial_cleanup",
        "app.workers.media_storage.recover_media_storage_jobs",
        "app.workers.media_storage.reconcile_meizi_storage_objects",
    } <= set(celery_app.tasks)


def test_timeline_stale_reconciler_is_scheduled() -> None:
    schedule = celery_app.conf.beat_schedule["timeline-render-reconcile"]
    assert schedule["task"] == "timeline.render.reconcile_stale"
    assert schedule["schedule"] > 0


def test_storyboard_recovery_and_financial_cleanup_are_scheduled() -> None:
    schedule = celery_app.conf.beat_schedule
    assert schedule["storyboard-dispatch-reconcile"]["task"] == (
        "app.workers.storyboard.reconcile_storyboard_dispatches"
    )
    assert schedule["storyboard-financial-cleanup"]["task"] == (
        "app.workers.storyboard.reconcile_storyboard_financial_cleanup"
    )
    assert schedule["storyboard-dispatch-reconcile"]["schedule"] > 0
    assert schedule["storyboard-financial-cleanup"]["schedule"] > 0


def test_timeline_financial_cleanup_is_scheduled() -> None:
    schedule = celery_app.conf.beat_schedule["timeline-render-financial-cleanup"]
    assert schedule["task"] == "timeline.render.financial_cleanup"
    assert schedule["schedule"] > 0


def test_video_recovery_and_financial_cleanup_are_scheduled() -> None:
    schedule = celery_app.conf.beat_schedule
    assert schedule["video-stale-reconcile"]["task"] == (
        "app.workers.video_generation.recover_stale_jobs"
    )
    assert schedule["video-financial-cleanup"]["task"] == (
        "app.workers.video_generation.reconcile_video_financial_cleanup"
    )
    assert schedule["video-stale-reconcile"]["schedule"] > 0
    assert schedule["video-financial-cleanup"]["schedule"] > 0


def test_meizi_storage_cleanup_is_scheduled() -> None:
    schedule = celery_app.conf.beat_schedule["reconcile-meizi-storage-objects"]
    assert schedule["task"] == "app.workers.media_storage.reconcile_meizi_storage_objects"
    assert schedule["schedule"] > 0


@pytest.mark.parametrize(
    ("task_type", "payload", "worker_module", "task_attribute", "expected_args"),
    [
        (
            "generation.generate_images",
            {"job_id": "generation-1"},
            "generation",
            "generate_images_task",
            ("generation-1",),
        ),
        (
            "storyboard.generate",
            {
                "script_id": "script-1",
                "episode_id": "episode-1",
                "user_id": "user-1",
                "job_id": "storyboard-1",
            },
            "storyboard",
            "generate_storyboard_task",
            ("script-1", "episode-1", "user-1", "storyboard-1"),
        ),
        (
            "video.generate",
            {"job_id": "video-1"},
            "video_generation",
            "run_video_generation",
            ("video-1",),
        ),
        (
            "timeline.render",
            {"job_id": "timeline-1"},
            "timeline_render",
            "render_timeline_task",
            ("timeline-1",),
        ),
    ],
)
def test_shared_outbox_publishes_every_persisted_single_task_type(
    monkeypatch: pytest.MonkeyPatch,
    task_type: str,
    payload: dict[str, Any],
    worker_module: str,
    task_attribute: str,
    expected_args: tuple[Any, ...],
) -> None:
    module = __import__(f"app.workers.{worker_module}", fromlist=[task_attribute])
    fake_task = _Task(task_type)
    monkeypatch.setattr(module, task_attribute, fake_task)

    result = outbox.publish_outbox_payload(task_type, payload)

    assert result == {"task": task_type, "args": expected_args}


def test_shared_outbox_rejects_unknown_task_type() -> None:
    with pytest.raises(ValueError, match="未知 outbox task_type"):
        outbox.publish_outbox_payload("unknown.task", {"job_id": "job-1"})
