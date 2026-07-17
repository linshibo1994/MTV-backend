"""团队项目 owner 成员存量修复的 PostgreSQL 迁移测试。"""

from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

import asyncpg
import pytest

_BACKEND_DIR = Path(__file__).resolve().parents[1]
_ALEMBIC = _BACKEND_DIR / ".venv" / "bin" / "alembic"
_DATABASE_URL_ENV = "MTV_TEST_POSTGRES_URL"


def _database_url() -> str:
    url = os.environ.get(_DATABASE_URL_ENV)
    if not url:
        pytest.skip(f"未配置 {_DATABASE_URL_ENV}，跳过真实 PostgreSQL 迁移测试")
    return url


def _asyncpg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _run_alembic(url: str, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["DATABASE_URL"] = url
    result = subprocess.run(
        [str(_ALEMBIC), *arguments],
        cwd=_BACKEND_DIR,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        pytest.fail(f"Alembic {' '.join(arguments)} 失败:\n{result.stdout}\n{result.stderr}")
    return result


def _expected_owner_member_id(script_id: str) -> str:
    digest = hashlib.md5(
        f"mtv-team-owner:{script_id}".encode(),
        usedforsecurity=False,
    ).hexdigest()
    return f"own-{digest}"


def _reset_to_revision(url: str, revision: str) -> None:
    _run_alembic(url, "downgrade", "base")
    _run_alembic(url, "upgrade", revision)


async def _seed_owner_repair_cases(url: str) -> None:
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        await connection.execute(
            """
            INSERT INTO organizations (id, org_name, org_code)
            VALUES ('org-owner-migration', '迁移测试组织', 'owner-migration')
            """
        )
        await connection.execute(
            """
            INSERT INTO users (
                user_id, username, password_hash, enabled, is_admin, account_type,
                org_id, department_id, can_manage_accounts, management_scope
            )
            VALUES
                ('owner-missing', 'owner-missing', 'hash', true, false, 'team',
                 'org-owner-migration', NULL, false, 'none'),
                ('owner-demoted', 'owner-demoted', 'hash', true, false, 'team',
                 'org-owner-migration', NULL, false, 'none'),
                ('owner-real', 'owner-real', 'hash', false, false, 'team',
                 'org-owner-migration', NULL, false, 'none'),
                ('owner-fake', 'owner-fake', 'hash', true, false, 'team',
                 'org-owner-migration', NULL, false, 'none')
            """
        )
        await connection.execute(
            """
            INSERT INTO scripts (
                id, title, scope, owner_user_id, org_id, created_by_user_id
            )
            VALUES
                ('script-missing', '缺失 owner 成员', 'team', 'owner-missing',
                 'org-owner-migration', 'owner-missing'),
                ('script-demoted', '真实 owner 被降级', 'team', 'owner-demoted',
                 'org-owner-migration', 'owner-demoted'),
                ('script-fake-owner', '存在虚假 owner', 'team', 'owner-real',
                 'org-owner-migration', 'owner-real')
            """
        )
        await connection.execute(
            """
            INSERT INTO project_members (id, script_id, user_id, project_role)
            VALUES
                ('member-demoted', 'script-demoted', 'owner-demoted', 'viewer'),
                ('member-real', 'script-fake-owner', 'owner-real', 'editor'),
                ('member-fake', 'script-fake-owner', 'owner-fake', 'owner')
            """
        )
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_owner_member_migration_repairs_legacy_rows_and_round_trips() -> None:
    url = _database_url()
    _reset_to_revision(url, "d4b7f9c2a610")
    await _seed_owner_repair_cases(url)

    _run_alembic(url, "upgrade", "head")

    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        rows = await connection.fetch(
            """
            SELECT script.id AS script_id, member.id, member.user_id, member.project_role
            FROM scripts AS script
            JOIN project_members AS member ON member.script_id = script.id
            WHERE script.id IN ('script-missing', 'script-demoted', 'script-fake-owner')
            ORDER BY script.id, member.user_id
            """
        )
        by_script = {
            script_id: [row for row in rows if row["script_id"] == script_id]
            for script_id in {row["script_id"] for row in rows}
        }

        missing_members = by_script["script-missing"]
        assert len(missing_members) == 1
        assert dict(missing_members[0]) == {
            "script_id": "script-missing",
            "id": _expected_owner_member_id("script-missing"),
            "user_id": "owner-missing",
            "project_role": "owner",
        }
        assert by_script["script-demoted"][0]["project_role"] == "owner"
        fake_owner_roles = {
            row["user_id"]: row["project_role"] for row in by_script["script-fake-owner"]
        }
        assert fake_owner_roles == {"owner-fake": "manager", "owner-real": "owner"}
        assert all(len(row["id"]) <= 36 for row in rows)
    finally:
        await connection.close()

    # downgrade 明确为 no-op：数据保持修复后状态；再升级不得重复插入。
    _run_alembic(url, "downgrade", "d4b7f9c2a610")
    _run_alembic(url, "upgrade", "head")
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        owner_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM project_members
            WHERE script_id = 'script-missing'
              AND user_id = 'owner-missing'
              AND project_role = 'owner'
            """
        )
        assert owner_count == 1
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_owner_member_migration_fails_closed_for_missing_user() -> None:
    url = _database_url()
    _reset_to_revision(url, "d4b7f9c2a610")
    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        await connection.execute(
            """
            INSERT INTO scripts (id, title, scope, owner_user_id, org_id, created_by_user_id)
            VALUES (
                'script-invalid-owner', '无效 owner', 'team', 'missing-user', NULL, 'missing-user'
            )
            """
        )
    finally:
        await connection.close()

    result = _run_alembic(url, "upgrade", "head", check=False)
    assert result.returncode != 0
    assert "团队项目 owner_user_id 未对应有效 users 行，拒绝迁移" in result.stderr

    connection = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        revision = await connection.fetchval("SELECT version_num FROM alembic_version")
        assert revision == "d4b7f9c2a610"
    finally:
        await connection.close()
