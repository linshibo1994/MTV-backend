"""neolink 视频供应商约束迁移的静态安全测试。"""

from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parents[1]
_MIGRATION_PATH = (
    _BACKEND_DIR
    / "alembic"
    / "versions"
    / "8c1f4a2e7b90_启用_neolink_视频供应商.py"
)


def _migration_sections() -> tuple[str, str, str, str]:
    source = _MIGRATION_PATH.read_text(encoding="utf-8")
    replacement = source[source.index("def _replace_check_constraint") : source.index("def upgrade")]
    upgrade = source[source.index("def upgrade") : source.index("def downgrade")]
    downgrade = source[source.index("def downgrade") :]
    return source, replacement, upgrade, downgrade


def test_neolink_migration_revises_current_head_and_updates_both_constraints() -> None:
    source, _, upgrade, downgrade = _migration_sections()

    assert source.count('revision: str = "8c1f4a2e7b90"') == 1
    assert source.count('down_revision: str | None = "50028cba1845"') == 1
    assert '"ck_video_provider_configs_provider_key"' in source
    assert '"ck_video_jobs_provider_key"' in source
    assert source.count("'neolink'") >= 3
    assert upgrade.count("condition=_NEOLINK_PROVIDER_CHECK") == 2
    assert downgrade.count("condition=_PREVIOUS_PROVIDER_CHECK") == 2


def test_neolink_migration_replaces_constraints_without_write_gap() -> None:
    _, replacement, _, _ = _migration_sections()

    add_position = replacement.index("ADD CONSTRAINT")
    validate_position = replacement.index("VALIDATE CONSTRAINT")
    drop_position = replacement.index("drop_constraint")
    rename_position = replacement.index("RENAME CONSTRAINT")

    assert "NOT VALID" in replacement
    assert add_position < validate_position < drop_position < rename_position


def test_neolink_downgrade_locks_before_checking_and_refuses_data_loss() -> None:
    _, _, _, downgrade = _migration_sections()

    lock_position = downgrade.index("LOCK TABLE video_provider_configs, video_generation_jobs")
    config_query_position = downgrade.index("SELECT count(*) INTO neolink_config_count")
    job_query_position = downgrade.index("SELECT count(*) INTO neolink_job_count")
    replacement_position = downgrade.index("_replace_check_constraint")

    assert "IN SHARE ROW EXCLUSIVE MODE" in downgrade
    assert lock_position < config_query_position < job_query_position < replacement_position
    assert "FROM video_provider_configs" in downgrade
    assert "FROM video_generation_jobs" in downgrade
    assert "IF neolink_config_count > 0 OR neolink_job_count > 0" in downgrade
    assert "RAISE EXCEPTION" in downgrade
    assert "拒绝降级以避免数据丢失" in downgrade
    assert "DELETE FROM" not in downgrade.upper()
    assert "UPDATE " not in downgrade.upper()
