"""项目动态画风下线后的注册边界。"""

from __future__ import annotations

import pytest

from app.main import create_app
from app.models.script import Script
from app.models.subject import Subject
from app.workers.celery_app import app as celery_app
from app.workers.outbox import publish_outbox_payload


def test_dynamic_art_style_api_and_tasks_are_unregistered() -> None:
    """动态画风既不公开 HTTP 入口，也不能再通过 Outbox 投递。"""
    paths = create_app().openapi()["paths"]

    assert not any("/art-styles" in path for path in paths)
    components = create_app().openapi()["components"]["schemas"]
    for schema_name in (
        "ScriptOut",
        "ScriptListItem",
        "ScriptCreateIn",
        "ScriptUpdateIn",
        "SubjectAssetOut",
        "SubjectCreateIn",
        "SubjectUpdateIn",
        "SubjectCandidateResolveIn",
    ):
        properties = components[schema_name].get("properties", {})
        assert "visual_style" not in properties
        assert "unify_style" not in properties

    assert "visual_style" not in Script.__table__.c
    assert "unify_style" not in Script.__table__.c
    assert "visual_style" not in Subject.__table__.c
    assert "app.workers.art_style_extraction" not in celery_app.conf.include
    assert "recover-art-style-extractions" not in celery_app.conf.beat_schedule
    with pytest.raises(ValueError, match="未知 outbox task_type"):
        publish_outbox_payload("art_style.extract", {"job_id": "removed", "attempt": 1})
