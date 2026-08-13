"""路由聚合 — 挂载到 /v3api 前缀。

各业务模块的 router 已在内部写全路径或自带 prefix（auth/credits/scripts/materials
自带 prefix；asset_library/shots/video_generation/timeline/generation_jobs 内部
路由已含完整路径）。因此此处仅 include，不再追加 prefix，避免路径嵌套重复。
"""

from fastapi import APIRouter

from app.api.v1 import (
    analysis_jobs,
    art_styles,
    asset_library,
    auth,
    credits,
    generation_jobs,
    materials,
    pipeline_runs,
    project_members,
    prompt_styles,
    prompt_templates,
    runtime_models,
    scene_auditions,
    scripts,
    shots,
    subject_candidates,
    timeline,
    video_generation,
)
from app.core.config import settings

api_router = APIRouter(prefix="/v3api")


@api_router.get("/health", tags=["meta"])
async def health() -> dict:
    return {"status": "ok", "env": settings.app_env}


# ===== 业务路由（include 时不加 prefix，路径由各模块内部定义）=====
# T1 鉴权 (router 自带 prefix=/auth)
api_router.include_router(auth.router, tags=["鉴权"])
# T2 积分 (router 自带 prefix=/credits)
api_router.include_router(credits.router, tags=["积分"])
# T3 剧本 (router 自带 prefix=/scripts)
api_router.include_router(scripts.router, tags=["剧本"])
api_router.include_router(pipeline_runs.router)
api_router.include_router(analysis_jobs.router)
# T3.1 团队项目成员（独立路由，共享剧本资源权限）
api_router.include_router(project_members.router, tags=["项目成员"])
# T4 资产库 (router 内部路由含 /scripts/{script_id}/asset-library/* 全路径)
api_router.include_router(asset_library.router, tags=["资产库"])
# T5 画布 (router 自带 prefix=/materials)
api_router.include_router(materials.router, tags=["画布"])
# T6 生成任务 (router 内部路由含 /generation-jobs 全路径)
api_router.include_router(generation_jobs.router, tags=["生成任务"])
# T7 分镜 (router 内部路由含 /scripts/{script_id}/episodes/* 与 /shots/* 全路径)
api_router.include_router(shots.router, tags=["分镜"])
# T8 视频生成 (router 内部路由含 /video-generation* 与 /video-generation-jobs 全路径)
api_router.include_router(video_generation.router, tags=["视频生成"])
# 当前部署实际使用的模型配置（不暴露密钥和服务地址）。
api_router.include_router(runtime_models.router, tags=["运行时配置"])
# 提示词风格目录与优化（router 内部包含两个独立完整路径）。
api_router.include_router(prompt_styles.router, tags=["提示词风格"])
api_router.include_router(art_styles.router, tags=["项目画风"])
api_router.include_router(prompt_templates.router, tags=["提示词模板"])
api_router.include_router(subject_candidates.router, tags=["主体候选"])
api_router.include_router(scene_auditions.router, tags=["导演方案"])
# T9 时间线 (router 内部路由含 /video-timeline-arrangements* 全路径)
api_router.include_router(timeline.router, tags=["时间线"])
# T10 媒体代理在 app.main 中单独挂载到 settings.media_proxy_prefix
