"""分镜输入 provenance 的确定性与兼容状态测试。"""

from app.services.storyboard_provenance import (
    build_storyboard_provenance,
    compare_storyboard_provenance,
)


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
                "enrich_version": 1,
                "is_active": True,
                "episode_nos": [2, 1],
            },
            {
                "subject_id": "subject-a",
                "type": "character",
                "subject_version": 4,
                "enrich_version": 5,
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
    assert first["subject_catalog"][0]["subject_id"] == "subject-a"
    assert first["prompt_templates"] == {
        "storyboard.system": {
            "version": 2,
            "content_sha256": "a" * 64,
            "source": "database",
        }
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
                "enrich_version": 5,
                "is_active": True,
                "episode_nos": [1],
            },
            {
                "subject_id": "subject-b",
                "name": "新场景名",
                "type": "scene",
                "subject_version": 2,
                "enrich_version": 1,
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
