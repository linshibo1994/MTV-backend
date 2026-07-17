"""团队项目所有者成员数据修复

Revision ID: f6c9a31e8b42
Revises: d4b7f9c2a610
Create Date: 2026-07-18 23:40:00
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f6c9a31e8b42"
down_revision: str | None = "d4b7f9c2a610"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 迁移期间禁止业务并发改写用户、项目和成员，避免校验后再出现脏数据。
    op.execute(
        sa.text(
            """
            LOCK TABLE users, scripts, project_members
            IN SHARE ROW EXCLUSIVE MODE
            """
        )
    )

    # Script.owner_user_id 没有外键：若对应用户不存在，不得猜测或转移所有权。
    op.execute(
        sa.text(
            """
            DO $$
            DECLARE
                invalid_script_id text;
                invalid_owner_user_id text;
            BEGIN
                SELECT script.id, script.owner_user_id
                INTO invalid_script_id, invalid_owner_user_id
                FROM scripts AS script
                LEFT JOIN users AS owner_user
                  ON owner_user.user_id = script.owner_user_id
                WHERE script.scope = 'team'
                  AND owner_user.user_id IS NULL
                ORDER BY script.id
                LIMIT 1;

                IF invalid_script_id IS NOT NULL THEN
                    RAISE EXCEPTION
                        '团队项目 owner_user_id 未对应有效 users 行，拒绝迁移: script_id=%, owner_user_id=%',
                        invalid_script_id,
                        invalid_owner_user_id;
                END IF;
            END
            $$
            """
        )
    )

    # 非真实所有者的 owner 无法保守地推断原角色，统一降为 manager。
    op.execute(
        sa.text(
            """
            UPDATE project_members AS member
            SET project_role = 'manager'
            FROM scripts AS script
            WHERE script.id = member.script_id
              AND script.scope = 'team'
              AND member.project_role = 'owner'
              AND member.user_id <> script.owner_user_id
            """
        )
    )

    # 真实所有者已在成员表时，无条件恢复 owner 角色。
    op.execute(
        sa.text(
            """
            UPDATE project_members AS member
            SET project_role = 'owner'
            FROM scripts AS script
            WHERE script.id = member.script_id
              AND script.scope = 'team'
              AND member.user_id = script.owner_user_id
              AND member.project_role <> 'owner'
            """
        )
    )

    # 缺失成员行时使用 "own-" + md5(script_id) 的 36 字符稳定 ID。
    # 在极小概率的主键碰撞下 fail closed，避免 ON CONFLICT 吞掉数据错误。
    op.execute(
        sa.text(
            """
            DO $$
            DECLARE
                collision_script_id text;
                collision_member_id text;
            BEGIN
                WITH missing_owner AS (
                    SELECT
                        script.id AS script_id,
                        script.owner_user_id,
                        'own-' || md5('mtv-team-owner:' || script.id) AS member_id
                    FROM scripts AS script
                    LEFT JOIN project_members AS actual_owner
                      ON actual_owner.script_id = script.id
                     AND actual_owner.user_id = script.owner_user_id
                    WHERE script.scope = 'team'
                      AND actual_owner.id IS NULL
                )
                SELECT missing_owner.script_id, missing_owner.member_id
                INTO collision_script_id, collision_member_id
                FROM missing_owner
                JOIN project_members AS occupied
                  ON occupied.id = missing_owner.member_id
                WHERE occupied.script_id <> missing_owner.script_id
                   OR occupied.user_id <> missing_owner.owner_user_id
                ORDER BY missing_owner.script_id
                LIMIT 1;

                IF collision_script_id IS NOT NULL THEN
                    RAISE EXCEPTION
                        '团队项目 owner 成员确定性 ID 冲突，拒绝迁移: script_id=%, member_id=%',
                        collision_script_id,
                        collision_member_id;
                END IF;
            END
            $$
            """
        )
    )
    op.execute(
        sa.text(
            """
            INSERT INTO project_members (id, script_id, user_id, project_role)
            SELECT
                'own-' || md5('mtv-team-owner:' || script.id),
                script.id,
                script.owner_user_id,
                'owner'
            FROM scripts AS script
            LEFT JOIN project_members AS actual_owner
              ON actual_owner.script_id = script.id
             AND actual_owner.user_id = script.owner_user_id
            WHERE script.scope = 'team'
              AND actual_owner.id IS NULL
            ORDER BY script.id
            """
        )
    )

    # 修复后再断言：每个团队项目必须恰好一个 owner，且必须是 Script.owner_user_id。
    op.execute(
        sa.text(
            """
            DO $$
            DECLARE
                invalid_script_id text;
            BEGIN
                SELECT script.id
                INTO invalid_script_id
                FROM scripts AS script
                WHERE script.scope = 'team'
                  AND (
                      NOT EXISTS (
                          SELECT 1
                          FROM project_members AS actual_owner
                          WHERE actual_owner.script_id = script.id
                            AND actual_owner.user_id = script.owner_user_id
                            AND actual_owner.project_role = 'owner'
                      )
                      OR (
                          SELECT count(*)
                          FROM project_members AS owner_member
                          WHERE owner_member.script_id = script.id
                            AND owner_member.project_role = 'owner'
                      ) <> 1
                  )
                ORDER BY script.id
                LIMIT 1;

                IF invalid_script_id IS NOT NULL THEN
                    RAISE EXCEPTION
                        '团队项目 owner 成员数据修复后仍不一致，拒绝迁移: script_id=%',
                        invalid_script_id;
                END IF;
            END
            $$
            """
        )
    )


def downgrade() -> None:
    """数据修复不可逆：无法可靠还原虚假 owner 的原角色或判定哪些成员由迁移补入。"""
