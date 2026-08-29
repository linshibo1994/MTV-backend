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
    ("POST", "/v3api/generation-jobs/batch"),
    ("POST", "/v3api/scripts/{script_id}/episodes/{episode_id}/generate-storyboard"),
    ("POST", "/v3api/scripts/{script_id}/upload-and-split"),
    ("GET", "/v3api/analysis-jobs/{analysis_job_id}"),
    ("GET", "/v3api/scripts/{script_id}/episodes/{episode_id}/shot-hierarchy"),
    (
        "POST",
        "/v3api/scripts/{script_id}/episodes/{episode_id}/shot-groups/merge",
    ),
    (
        "PATCH",
        "/v3api/scripts/{script_id}/episodes/{episode_id}/shot-groups/{shot_group_id}",
    ),
    (
        "DELETE",
        "/v3api/scripts/{script_id}/episodes/{episode_id}/shot-groups/{shot_group_id}",
    ),
    ("GET", "/v3api/shots/{script_id}/episodes/{episode_id}/video-workbench"),
    ("GET", "/v3api/shots/{script_id}/cover"),
    ("POST", "/v3api/video-generation-jobs"),
    ("GET", "/v3api/video-generation-jobs"),
    ("GET", "/v3api/video-generation/{job_id}"),
    ("GET", "/v3api/video-generation/active-provider"),
    ("POST", "/v3api/video-generation/active-provider"),
    ("GET", "/v3api/runtime-models"),
    ("GET", "/v3api/provider-catalog"),
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


def test_shot_group_merge_openapi_accepts_full_episode_shot_set() -> None:
    schema = app.openapi()
    shot_ids = schema["components"]["schemas"]["ShotGroupMergeIn"]["properties"]["shot_ids"]

    assert shot_ids["minItems"] == 1
    assert shot_ids["maxItems"] == 1000
    response = schema["paths"][
        "/v3api/scripts/{script_id}/episodes/{episode_id}/shot-groups/merge"
    ]["post"]["responses"]["200"]["content"]["application/json"]["schema"]
    assert response["$ref"].endswith("/VideoWorkbenchOut")


def test_shot_group_update_openapi_exposes_partial_edit_contract() -> None:
    schema = app.openapi()
    update = schema["components"]["schemas"]["ShotGroupUpdateIn"]
    workbench_group = schema["components"]["schemas"]["ShotGroupWorkbenchOut"]

    assert update["properties"]["duration_seconds"]["anyOf"][0] == {
        "type": "integer",
        "maximum": 15.0,
        "minimum": 3.0,
    }
    assert workbench_group["properties"]["content_hash"] == {
        "type": "string",
        "maxLength": 64,
        "minLength": 64,
        "title": "Content Hash",
    }
    response = schema["paths"][
        "/v3api/scripts/{script_id}/episodes/{episode_id}/shot-groups/{shot_group_id}"
    ]["patch"]["responses"]["200"]["content"]["application/json"]["schema"]
    assert response["$ref"].endswith("/VideoWorkbenchOut")


def test_shot_group_delete_openapi_requires_version_and_explicit_confirmation() -> None:
    schema = app.openapi()
    operation = schema["paths"][
        "/v3api/scripts/{script_id}/episodes/{episode_id}/shot-groups/{shot_group_id}"
    ]["delete"]
    parameters = {item["name"]: item for item in operation["parameters"]}

    assert parameters["expected_episode_version"]["required"] is True
    assert parameters["expected_episode_version"]["schema"]["minimum"] == 1.0
    assert parameters["confirm"]["schema"]["default"] is False
    response = operation["responses"]["200"]["content"]["application/json"]["schema"]
    assert response["$ref"].endswith("/VideoWorkbenchOut")


def test_image_and_video_generation_openapi_expose_prompt_overrides() -> None:
    schemas = app.openapi()["components"]["schemas"]

    image_single = schemas["CreateJobIn"]
    image_properties = image_single["properties"]
    assert image_properties["prompt"] == {
        "type": "string",
        "minLength": 1,
        "maxLength": 32000,
        "title": "Prompt",
        "description": "客户端确认的最终生图提示词",
    }
    assert image_properties["negative_prompt"]["default"] == ""
    assert "prompt" in image_single["required"]
    assert {"style_id", "style_ids", "art_style_id", "visual_style"}.isdisjoint(image_properties)

    image_batch = schemas["BatchCreateJobIn"]
    image_batch_properties = image_batch["properties"]
    assert image_batch_properties["prompts"] == {
        "type": "object",
        "minProperties": 1,
        "additionalProperties": {
            "type": "string",
            "minLength": 1,
            "maxLength": 32000,
        },
        "title": "Prompts",
        "description": "每个主体的客户端确认最终生图提示词，键为 subject_id",
    }
    assert "prompts" in image_batch["required"]
    assert {"style_id", "style_ids", "art_style_id", "visual_style"}.isdisjoint(
        image_batch_properties
    )

    for single_name in ("VideoJobCreateIn",):
        prompt = schemas[single_name]["properties"]["prompt"]["anyOf"][0]
        assert prompt == {"type": "string", "minLength": 1, "maxLength": 32000}

    for batch_name in ("VideoJobBatchCreateIn",):
        prompts = schemas[batch_name]["properties"]["prompts"]["anyOf"][0]
        assert prompts["type"] == "object"
        assert prompts["additionalProperties"] == {
            "type": "string",
            "minLength": 1,
            "maxLength": 32000,
        }


def test_batch_image_generation_openapi_exposes_client_prompt_mappings() -> None:
    schema = app.openapi()
    negative_prompts = schema["components"]["schemas"]["BatchCreateJobIn"]["properties"][
        "negative_prompts"
    ]

    assert negative_prompts["anyOf"][0] == {
        "type": "object",
        "additionalProperties": {
            "type": "string",
            "maxLength": 32000,
        },
    }
    request = schema["paths"]["/v3api/generation-jobs/batch"]["post"]["requestBody"][
        "content"
    ]["application/json"]["schema"]
    assert request["$ref"].endswith("/BatchCreateJobIn")

    response = schema["paths"]["/v3api/generation-jobs/batch"]["post"]["responses"][
        "201"
    ]["content"]["application/json"]["schema"]
    assert response["$ref"].endswith("/BatchJobOut")
    created = schema["components"]["schemas"]["BatchJobOut"]["properties"]["created"]
    assert created["items"]["$ref"].endswith("/GenerationJobOut")
