"""Celery 应用实例 — broker/result_backend 走 Redis。

worker 各模块 (pipeline/generation/video_generation/timeline_render) 共用本实例。
启动: celery -A app.workers.celery_app worker -l info
"""

from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown, worker_shutdown

from app.core.config import settings
from app.core.job_state import TASK_HARD_TIME_LIMIT_SECONDS, TASK_SOFT_TIME_LIMIT_SECONDS
from app.workers.async_runtime import (
    discard_inherited_database_pool,
    reset_worker_async_runtime_after_fork,
    shutdown_worker_async_runtime,
)

app = Celery(
    "flowapp",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=[
        "app.workers.pipeline",
        "app.workers.generation",
        "app.workers.storyboard",
        "app.workers.outbox",
        "app.workers.timeline_render",
        "app.workers.video_generation",
        "app.workers.media_storage",
        "app.workers.scene_audition",
    ],
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    # AI 任务链式编排：延迟确认 + 单任务预取，避免长任务阻塞短任务
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_track_started=True,
    # AI 调用可能较长，设置软/硬超时，超时即失败并释放积分
    task_time_limit=TASK_HARD_TIME_LIMIT_SECONDS,
    task_soft_time_limit=TASK_SOFT_TIME_LIMIT_SECONDS,
    # 失败不自动重试（AI 调用失败由用户手动重试创建新 attempt）
    task_autoretry_for=(),
    beat_schedule={
        "outbox-dispatch": {
            "task": "outbox.dispatch",
            "schedule": 5.0,
        },
        "pipeline-dispatch-reconcile": {
            "task": "outbox.reconcile-pipeline",
            "schedule": 30.0,
        },
        "generation-financial-cleanup": {
            "task": "generation.reconcile-financial-cleanup",
            "schedule": 60.0,
        },
        "generation-stale-reconcile": {
            "task": "generation.reconcile-stale",
            "schedule": 60.0,
        },
        "storyboard-dispatch-reconcile": {
            "task": "app.workers.storyboard.reconcile_storyboard_dispatches",
            "schedule": 60.0,
        },
        "storyboard-financial-cleanup": {
            "task": "app.workers.storyboard.reconcile_storyboard_financial_cleanup",
            "schedule": 60.0,
        },
        "timeline-render-reconcile": {
            "task": "timeline.render.reconcile_stale",
            "schedule": 60.0,
        },
        "timeline-render-financial-cleanup": {
            "task": "timeline.render.financial_cleanup",
            "schedule": 60.0,
        },
        "video-stale-reconcile": {
            "task": "app.workers.video_generation.recover_stale_jobs",
            "schedule": 60.0,
        },
        "video-financial-cleanup": {
            "task": "app.workers.video_generation.reconcile_video_financial_cleanup",
            "schedule": 60.0,
        },
        "video-remote-cancellation-cleanup": {
            "task": "app.workers.video_generation.reconcile_video_remote_cancellations",
            "schedule": 60.0,
        },
        "recover-media-storage-jobs": {
            "task": "app.workers.media_storage.recover_media_storage_jobs",
            "schedule": 60.0,
        },
        "recover-scene-auditions": {
            "task": "scene_audition.recover-expired",
            "schedule": 60.0,
        },
    },
)

# 兼容别名：部分 worker 通过 `from app.workers.celery_app import celery_app` 引用，
# 同时保留 `app` 以兼容 `celery -A app.workers.celery_app` 启动方式。
celery_app = app


def _prepare_worker_async_runtime(**_kwargs: object) -> None:
    """prefork 子进程必须丢弃父进程继承的循环和数据库连接池。"""

    reset_worker_async_runtime_after_fork()
    discard_inherited_database_pool()


def _shutdown_worker_async_runtime(**_kwargs: object) -> None:
    shutdown_worker_async_runtime()


worker_process_init.connect(_prepare_worker_async_runtime, weak=False)
worker_process_shutdown.connect(_shutdown_worker_async_runtime, weak=False)
worker_shutdown.connect(_shutdown_worker_async_runtime, weak=False)
