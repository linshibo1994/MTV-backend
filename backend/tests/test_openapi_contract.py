"""TASK.md 所列后端 API 的 OpenAPI 总契约。"""

from app.main import app

REQUIRED_OPERATIONS = {
    ("POST", "/v3api/auth/login"),
    ("POST", "/v3api/auth/logout"),
    ("GET", "/v3api/auth/me"),
    ("GET", "/v3api/credits/accounts"),
    ("GET", "/v3api/credits/logs"),
    ("POST", "/v3api/credits/grant"),
    ("GET", "/v3api/scripts/page"),
    ("POST", "/v3api/scripts"),
    ("GET", "/v3api/scripts/{script_id}"),
    ("GET", "/v3api/scripts/{script_id}/episodes"),
    ("POST", "/v3api/scripts/{script_id}/episodes"),
    ("PUT", "/v3api/scripts/{script_id}/episodes/{episode_id}"),
    ("GET", "/v3api/scripts/{script_id}/episodes/{episode_id}/breakdown"),
    ("GET", "/v3api/scripts/{script_id}/pipeline-status"),
    ("GET", "/v3api/scripts/{script_id}/extract-subjects-status"),
    ("GET", "/v3api/scripts/news-broadcast/jobs"),
    ("GET", "/v3api/scripts/{script_id}/asset/entries"),
    ("GET", "/v3api/scripts/{script_id}/asset-library/subjects"),
    ("GET", "/v3api/scripts/{script_id}/asset-library/parent-subjects"),
    ("PATCH", "/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}"),
    ("DELETE", "/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}"),
    ("GET", "/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/candidates"),
    ("POST", "/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/approve-image"),
    (
        "DELETE",
        "/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/candidates/{candidate_id}",
    ),
    ("POST", "/v3api/scripts/{script_id}/asset-library/subjects/{subject_id}/approve-audio"),
    ("GET", "/v3api/scripts/{script_id}/canvas-project/ensure"),
    ("GET", "/v3api/materials/{material_id}"),
    ("GET", "/v3api/materials/{material_id}/canvas"),
    ("POST", "/v3api/materials/{material_id}/canvas/delta"),
    ("GET", "/v3api/materials/{material_id}/image/{image_id}"),
    ("POST", "/v3api/generation-jobs"),
    ("GET", "/v3api/generation-jobs/{job_id}"),
    ("GET", "/v3api/generation-jobs"),
    ("POST", "/v3api/scripts/{script_id}/episodes/{episode_id}/generate-storyboard"),
    ("POST", "/v3api/scripts/{script_id}/upload-and-split"),
    ("GET", "/v3api/analysis-jobs/{analysis_job_id}"),
    ("GET", "/v3api/scripts/{script_id}/episodes/{episode_id}/shot-hierarchy"),
    (
        "POST",
        "/v3api/scripts/{script_id}/episodes/{episode_id}/shot-groups/merge",
    ),
    ("GET", "/v3api/shots/{script_id}/episodes/{episode_id}/video-workbench"),
    ("GET", "/v3api/shots/{script_id}/cover"),
    ("POST", "/v3api/video-generation-jobs"),
    ("GET", "/v3api/video-generation-jobs"),
    ("GET", "/v3api/video-generation/{job_id}"),
    ("GET", "/v3api/video-generation/active-provider"),
    ("POST", "/v3api/video-generation/active-provider"),
    ("GET", "/v3api/video-timeline-arrangements"),
    ("POST", "/v3api/video-timeline-arrangements"),
    ("PATCH", "/v3api/video-timeline-arrangements/{arrangement_id}"),
    ("PUT", "/v3api/video-timeline-arrangements/reorder"),
    ("POST", "/v3api/video-timeline-arrangements/render"),
    ("GET", "/api/x/cctv/zhihui/flow-app/{storage_key}"),
    ("POST", "/api/x/cctv/zhihui/flow-app/upload_images/{mid}/{filename}"),
}


def test_openapi_contains_every_task_required_backend_operation() -> None:
    schema = app.openapi()
    operations = {
        (method.upper(), path)
        for path, path_item in schema["paths"].items()
        for method in path_item
        if method in {"get", "post", "put", "patch", "delete"}
    }

    assert REQUIRED_OPERATIONS <= operations, sorted(REQUIRED_OPERATIONS - operations)


def test_openapi_operation_ids_are_unique() -> None:
    operation_ids = [
        operation["operationId"]
        for path_item in app.openapi()["paths"].values()
        for method, operation in path_item.items()
        if method in {"get", "post", "put", "patch", "delete"}
    ]

    assert len(operation_ids) == len(set(operation_ids))
