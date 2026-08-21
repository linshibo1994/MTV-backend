"""Celery outbox dispatcher worker。"""

import socket
from typing import Any, cast

from celery import chain, chord, group
from celery.canvas import Signature

from app.core.database import async_session_factory
from app.services.task_dispatch import dispatch_outbox_batch, reconcile_pipeline_dispatches
from app.workers.async_runtime import run_worker_async
from app.workers.celery_app import app as celery_app

_beat_schedule = dict(celery_app.conf.beat_schedule or {})
_beat_schedule.setdefault(
    "outbox-dispatch",
    {
        "task": "outbox.dispatch",
        "schedule": 5.0,
    },
)
_beat_schedule.setdefault(
    "pipeline-dispatch-reconcile",
    {
        "task": "outbox.reconcile-pipeline",
        "schedule": 30.0,
    },
)
_beat_schedule.setdefault(
    "generation-financial-cleanup",
    {
        "task": "generation.reconcile-financial-cleanup",
        "schedule": 60.0,
    },
)
_beat_schedule.setdefault(
    "generation-stale-reconcile",
    {
        "task": "generation.reconcile-stale",
        "schedule": 60.0,
    },
)
celery_app.conf.beat_schedule = _beat_schedule


def build_pipeline_canvas(
    payload: dict,
    split_task,
    extract_task,
    merge_task,
    enrich_task,
    parents_task,
    finalize_task,
    *,
    chain_factory=chain,
    group_factory=group,
    chord_factory=chord,
) -> object:
    """构造可由假签名测试的 DAG，不连接 broker。"""
    return chain_factory(
        split_task.s(
            payload["script_id"],
            payload["episode_id"],
            payload["input_version"],
            payload.get("reservation_id"),
            payload["estimated"],
            payload["dispatch_id"],
            payload["run_id"],
        ),
        extract_task.s(
            payload["episode_id"],
            payload["input_version"],
            payload.get("reservation_id"),
            payload["estimated"],
            payload["dispatch_id"],
            payload["run_id"],
        ),
        merge_task.s(
            payload["episode_id"],
            payload["input_version"],
            payload.get("reservation_id"),
            payload["estimated"],
            payload["dispatch_id"],
            payload["run_id"],
        ),
        chord_factory(
            group_factory(
                enrich_task.s(
                    payload["episode_id"],
                    payload["input_version"],
                    payload.get("reservation_id"),
                    payload["estimated"],
                    payload["dispatch_id"],
                    payload["run_id"],
                ),
                parents_task.s(
                    payload["episode_id"],
                    payload["input_version"],
                    payload.get("reservation_id"),
                    payload["estimated"],
                    payload["dispatch_id"],
                    payload["run_id"],
                ),
            ),
            finalize_task.s(
                payload["script_id"],
                payload["episode_id"],
                payload["input_version"],
                payload.get("reservation_id"),
                payload["estimated"],
                payload["dispatch_id"],
                payload["run_id"],
            ),
        ),
    )


def build_generation_canvas(payload: dict, generate_images_task) -> Any:
    """构造生图任务签名；只允许 outbox 持久化后发布。"""
    return generate_images_task.s(payload["job_id"])


def publish_outbox_payload(task_type: str, payload: dict) -> Any:
    """仅在 outbox 已持久化后构造合法 Celery canvas。"""
    if task_type == "generation.generate_images":
        from app.workers import generation as generation_worker

        workflow = build_generation_canvas(
            payload, generation_worker.generate_images_task
        )
        return workflow.apply_async()
    if task_type == "storyboard.generate":
        from app.workers import storyboard as storyboard_worker

        workflow = cast(
            Signature,
            storyboard_worker.build_storyboard_canvas(
                payload, storyboard_worker.generate_storyboard_task
            ),
        )
        return workflow.apply_async()
    if task_type == "video.generate":
        from app.workers import video_generation as video_worker

        return video_worker.run_video_generation.s(payload["job_id"]).apply_async()
    if task_type == "timeline.render":
        from app.workers import timeline_render as timeline_worker

        return timeline_worker.render_timeline_task.s(payload["job_id"]).apply_async()
    if task_type == "scene_audition.generate":
        from app.workers import scene_audition as scene_audition_worker

        return scene_audition_worker.generate_scene_audition_task.s(
            payload["job_id"],
            payload.get("attempt"),
        ).apply_async()
    if task_type != "pipeline.dag":
        raise ValueError("未知 outbox task_type")
    from app.workers.pipeline import (
        enrich_task,
        extract_task,
        finalize_task,
        merge_task,
        parents_task,
        split_task,
    )

    workflow = cast(
        Signature,
        build_pipeline_canvas(
            payload,
            split_task,
            extract_task,
            merge_task,
            enrich_task,
            parents_task,
            finalize_task,
        ),
    )
    return workflow.apply_async()


async def _dispatch() -> int:
    async with async_session_factory() as db:
        return await dispatch_outbox_batch(
            db, publish_outbox_payload, owner=f"{socket.gethostname()}:{__name__}"
        )


@celery_app.task(name="outbox.dispatch", bind=True, max_retries=0)
def dispatch_outbox_task(self) -> int:
    return run_worker_async(_dispatch())


async def _reconcile_pipeline() -> int:
    async with async_session_factory() as db:
        recovered = await reconcile_pipeline_dispatches(db)
        await db.commit()
        return recovered


@celery_app.task(name="outbox.reconcile-pipeline", bind=True, max_retries=0)
def reconcile_pipeline_task(self) -> int:
    return run_worker_async(_reconcile_pipeline())
