"""分镜输入 provenance 的确定性与兼容状态测试。"""

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from app.services import storyboard as storyboard_service
from app.services.prompt_template import fallback_snapshot
from app.services.storyboard_provenance import (
    build_storyboard_provenance,
    compare_storyboard_provenance,
    validate_storyboard_provenance,
)
from app.services.storyboard_quality import ScriptBlock


def _snapshot() -> dict[str, object]:
    return build_storyboard_provenance(
        episode_script="第一场。阿明入门。",
        episode_version=3,
        client_save_seq=8,
        subject_catalog=[
            {
                "subject_id": "subject-b",
                "type": "scene",
                "subject_version": 2,
                "is_active": True,
                "episode_nos": [2, 1],
            },
            {
                "subject_id": "subject-a",
                "type": "character",
                "subject_version": 4,
                "is_active": True,
                "episode_nos": [1],
            },
        ],
        system_prompt="系统约束",
        user_prompt="用户提示词",
        prompt_templates={
            "storyboard.system": {
                "version": 2,
                "content_sha256": "a" * 64,
                "source": "database",
                "content": "不得进入快照的正文",
            }
        },
    )


def test_storyboard_provenance_is_deterministic_and_never_stores_prompt_body() -> None:
    first = _snapshot()
    second = _snapshot()
    assert first == second
    serialized = repr(first)
    assert "第一场" not in serialized
    assert "系统约束" not in serialized
    assert "用户提示词" not in serialized
    assert "不得进入快照的正文" not in serialized
    assert first["schema"] == "storyboard.provenance.v3"
    assert "system_prompt_sha256" not in first
    assert "user_prompt_sha256" not in first
    assert isinstance(first["prompt_workflow_sha256"], str)
    assert first["subject_catalog"][0]["subject_id"] == "subject-a"
    assert "enrich_version" not in first["subject_catalog"][0]
    assert first["prompt_templates"] == {
        "storyboard.system": {
            "version": 2,
            "content_sha256": "a" * 64,
            "source": "database",
        }
    }


def test_storyboard_subject_snapshot_omits_retired_enrich_version() -> None:
    snapshot = storyboard_service._subject_provenance_snapshot(
        SimpleNamespace(
            id="subject-1",
            type="character",
            subject_version=3,
            enrich_version=99,
            is_active=True,
            episode_nos=[1],
        )
    )

    assert snapshot == {
        "subject_id": "subject-1",
        "type": "character",
        "subject_version": 3,
        "is_active": True,
        "episode_nos": [1],
    }


def test_provenance_reports_current_stale_and_legacy_without_claiming_tampering() -> None:
    stored = _snapshot()
    assert compare_storyboard_provenance("job-1", stored, stored).status == "current"

    changed = dict(stored)
    changed["episode_script_sha256"] = "b" * 64
    report = compare_storyboard_provenance("job-1", stored, changed)
    assert report.status == "stale"
    assert report.mismatches == ("episode_script_sha256",)
    assert "tamper" not in repr(report.as_dict()).lower()

    legacy = compare_storyboard_provenance("job-old", None, changed)
    assert legacy.status == "legacy"
    assert legacy.skipped is True
    assert legacy.all_ok is False

    v1 = dict(stored)
    v1["schema"] = "storyboard.provenance.v1"
    assert compare_storyboard_provenance("job-v1", v1, stored).status == "legacy"


def test_retired_enrich_version_v2_snapshot_is_legacy_not_stale() -> None:
    """下线 enrich 后，历史 v2 快照应跳过比对而非持续报告输入变化。"""
    stored = _snapshot()
    historical_catalog = [
        {
            "subject_id": "subject-a",
            "type": "character",
            "subject_version": 4,
            "enrich_version": 5,
            "is_active": True,
            "episode_nos": [1],
        },
        {
            "subject_id": "subject-b",
            "type": "scene",
            "subject_version": 2,
            "enrich_version": 1,
            "is_active": True,
            "episode_nos": [1, 2],
        },
    ]
    stored["schema"] = "storyboard.provenance.v2"
    stored["subject_catalog"] = historical_catalog
    stored["subject_catalog_sha256"] = hashlib.sha256(
        json.dumps(
            historical_catalog,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    report = compare_storyboard_provenance("job-retired-enrich", stored, _snapshot())

    assert report.status == "legacy"
    assert report.skipped is True


def test_subject_display_name_does_not_change_catalog_hash_but_version_does() -> None:
    original = _snapshot()
    renamed = build_storyboard_provenance(
        episode_script="第一场。阿明入门。",
        episode_version=3,
        client_save_seq=8,
        subject_catalog=[
            {
                "subject_id": "subject-a",
                "name": "新展示名",
                "type": "character",
                "subject_version": 4,
                "is_active": True,
                "episode_nos": [1],
            },
            {
                "subject_id": "subject-b",
                "name": "新场景名",
                "type": "scene",
                "subject_version": 2,
                "is_active": True,
                "episode_nos": [1, 2],
            },
        ],
        system_prompt="系统约束",
        user_prompt="用户提示词",
        prompt_templates={
            "storyboard.system": {
                "version": 2,
                "content_sha256": "a" * 64,
                "source": "database",
            }
        },
    )
    assert renamed["subject_catalog_sha256"] == original["subject_catalog_sha256"]

    revised = build_storyboard_provenance(
        episode_script="第一场。阿明入门。",
        episode_version=3,
        client_save_seq=8,
        subject_catalog=[
            {
                "subject_id": "subject-a",
                "type": "character",
                "subject_version": 6,
                "is_active": True,
                "episode_nos": [1],
            },
            {
                "subject_id": "subject-b",
                "type": "scene",
                "subject_version": 2,
                "is_active": True,
                "episode_nos": [1, 2],
            },
        ],
        system_prompt="系统约束",
        user_prompt="用户提示词",
        prompt_templates={
            "storyboard.system": {
                "version": 2,
                "content_sha256": "a" * 64,
                "source": "database",
            }
        },
    )
    assert revised["subject_catalog_sha256"] != original["subject_catalog_sha256"]


def test_provenance_detects_mutated_minimal_snapshot_anchor() -> None:
    stored = _snapshot()
    mutated = dict(stored)
    mutated["prompt_templates"] = {
        "storyboard.system": {
            "version": 99,
            "content_sha256": "a" * 64,
            "source": "database",
        }
    }
    report = compare_storyboard_provenance("job-1", mutated, mutated)
    assert report.status == "stale"
    assert report.mismatches == ("prompt_templates_sha256",)


def test_prompt_workflow_hash_tracks_explicit_workflow_without_storing_body() -> None:
    workflow = {
        "strategy": "scene_split",
        "split": {"system": "仅做场景划分", "user": "B01 私密剧本"},
        "scene": {"system": "仅生成单场", "user_template": "私密单场提示"},
    }
    first = build_storyboard_provenance(
        episode_script="第一场。阿明入门。",
        episode_version=3,
        client_save_seq=8,
        subject_catalog=[],
        system_prompt="已废弃的整集系统提示",
        user_prompt="已废弃的整集用户提示",
        prompt_templates={},
        prompt_workflow=workflow,
    )
    changed = build_storyboard_provenance(
        episode_script="第一场。阿明入门。",
        episode_version=3,
        client_save_seq=8,
        subject_catalog=[],
        system_prompt="不同的已废弃系统提示",
        user_prompt="不同的已废弃用户提示",
        prompt_templates={},
        prompt_workflow={
            **workflow,
            "scene": {"system": "仅生成单场", "user_template": "更新后的私密单场提示"},
        },
    )

    assert first["prompt_workflow_sha256"] != changed["prompt_workflow_sha256"]
    assert compare_storyboard_provenance("job-workflow", first, changed).mismatches == (
        "prompt_workflow_sha256",
    )
    serialized = repr(first)
    assert "私密剧本" not in serialized
    assert "私密单场提示" not in serialized
    assert "已废弃的整集系统提示" not in serialized


def test_scene_prompt_workflow_hash_tracks_provider_response_schemas(
    monkeypatch,
) -> None:
    """结构化输出 Schema 是实际 Provider 输入，必须进入工作流锚点。"""

    episode = SimpleNamespace(
        episode_no=1,
        title="第一集",
        script_text="阿明进入办公室。",
    )
    references = {
        "BG01": storyboard_service._SubjectReference(
            ref="BG01",
            name="办公室",
            subject_type="background",
            description="现代办公室",
            subject_id="private-id",
            asset_entry_id=None,
        )
    }
    prompt_context = storyboard_service._StoryboardPromptContext(
        system_prompt="单场系统提示",
        user_prompt="历史整集提示",
        user_template=fallback_snapshot("storyboard.user", "{{ default_prompt }}"),
        constraint_lock="",
        templates={},
    )
    blocks = [ScriptBlock("B01", "阿明进入办公室。", 0, 8)]
    workflow = storyboard_service._build_scene_split_prompt_workflow(
        episode,
        [],
        references,
        blocks,
        prompt_context,
        "",
    )
    original = build_storyboard_provenance(
        episode_script=episode.script_text,
        episode_version=1,
        client_save_seq=1,
        subject_catalog=[],
        system_prompt="unused",
        user_prompt="unused",
        prompt_templates={},
        prompt_workflow=workflow,
    )
    assert workflow["split_response_schema"] == storyboard_service._PROVIDER_SCENE_SPLIT_RESPONSE_SCHEMA
    assert workflow["scene_response_schema"] == storyboard_service._PROVIDER_RESPONSE_SCHEMA

    monkeypatch.setattr(
        storyboard_service,
        "_PROVIDER_SCENE_SPLIT_RESPONSE_SCHEMA",
        {**storyboard_service._PROVIDER_SCENE_SPLIT_RESPONSE_SCHEMA, "x-test-schema": True},
    )
    changed_workflow = storyboard_service._build_scene_split_prompt_workflow(
        episode,
        [],
        references,
        blocks,
        prompt_context,
        "",
    )
    changed = build_storyboard_provenance(
        episode_script=episode.script_text,
        episode_version=1,
        client_save_seq=1,
        subject_catalog=[],
        system_prompt="unused",
        user_prompt="unused",
        prompt_templates={},
        prompt_workflow=changed_workflow,
    )

    assert original["prompt_workflow_sha256"] != changed["prompt_workflow_sha256"]


def test_legacy_single_call_workflow_hash_uses_compatible_prompt_arguments() -> None:
    original = _snapshot()
    changed = build_storyboard_provenance(
        episode_script="第一场。阿明入门。",
        episode_version=3,
        client_save_seq=8,
        subject_catalog=[
            {
                "subject_id": "subject-a",
                "type": "character",
                "subject_version": 4,
                "is_active": True,
                "episode_nos": [1],
            },
            {
                "subject_id": "subject-b",
                "type": "scene",
                "subject_version": 2,
                "is_active": True,
                "episode_nos": [1, 2],
            },
        ],
        system_prompt="系统约束",
        user_prompt="不同用户提示词",
        prompt_templates={
            "storyboard.system": {
                "version": 2,
                "content_sha256": "a" * 64,
                "source": "database",
            }
        },
    )

    assert original["prompt_workflow_sha256"] != changed["prompt_workflow_sha256"]
    report = compare_storyboard_provenance("job-single-call", original, changed)
    assert report.status == "stale"
    assert report.mismatches == ("prompt_workflow_sha256",)


def test_provenance_marks_malformed_workflow_anchor_stale() -> None:
    stored = _snapshot()
    malformed = dict(stored)
    malformed["prompt_workflow_sha256"] = "not-a-sha"

    report = compare_storyboard_provenance("job-malformed-workflow", malformed, stored)

    assert report.status == "stale"
    assert report.mismatches == ("prompt_workflow_sha256",)


async def test_validate_storyboard_provenance_marks_v1_snapshot_legacy() -> None:
    class _Result:
        def scalar_one_or_none(self):
            return SimpleNamespace(input_provenance={"schema": "storyboard.provenance.v1"})

    db = SimpleNamespace(execute=AsyncMock(return_value=_Result()))

    report = await validate_storyboard_provenance(db, "job-v1")

    assert report.status == "legacy"
    assert db.execute.await_count == 1
