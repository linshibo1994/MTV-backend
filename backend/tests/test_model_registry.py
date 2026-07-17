"""Alembic 自动发现所需的 ORM 模型聚合契约。"""

from sqlalchemy import UniqueConstraint, create_mock_engine

from app.models import Base

REQUIRED_TABLES = {
    "organizations",
    "departments",
    "users",
    "user_organizations",
    "project_members",
    "credit_accounts",
    "credit_logs",
    "credit_reservations",
    "scripts",
    "episodes",
    "subjects",
    "parent_subjects",
    "pipeline_stages",
    "pipeline_jobs",
    "materials",
    "canvas_nodes",
    "canvas_edges",
    "generation_jobs",
    "generation_batch_requests",
    "asset_entries",
    "media_storage_jobs",
    "scenes",
    "shot_groups",
    "shots",
    "storyboard_jobs",
    "video_generation_jobs",
    "video_provider_configs",
    "video_timeline_arrangements",
    "timeline_render_jobs",
    "task_dispatch_outbox",
    "audit_events",
}


def test_model_package_registers_every_migrated_table() -> None:
    assert REQUIRED_TABLES <= set(Base.metadata.tables)


def test_every_composite_foreign_key_targets_a_database_candidate_key() -> None:
    missing: list[tuple[str, tuple[str, ...], str, tuple[str, ...]]] = []

    for table in Base.metadata.tables.values():
        for foreign_key in table.foreign_key_constraints:
            if len(foreign_key.elements) < 2:
                continue
            target_table = foreign_key.elements[0].column.table
            target_columns = tuple(
                element.column.name for element in foreign_key.elements
            )
            candidate_keys = {
                tuple(column.name for column in target_table.primary_key.columns),
                *(
                    tuple(column.name for column in constraint.columns)
                    for constraint in target_table.constraints
                    if isinstance(constraint, UniqueConstraint)
                ),
            }
            if target_columns not in candidate_keys:
                missing.append(
                    (
                        table.name,
                        tuple(element.parent.name for element in foreign_key.elements),
                        target_table.name,
                        target_columns,
                    )
                )

    assert missing == []


def test_postgresql_ddl_can_sort_all_tables_for_create_and_drop() -> None:
    engine = create_mock_engine("postgresql://", lambda *_args, **_kwargs: None)

    Base.metadata.create_all(engine, checkfirst=False)
    Base.metadata.drop_all(engine, checkfirst=False)
