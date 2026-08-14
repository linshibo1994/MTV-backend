-- MTV 管理员账户 + 初始积分 创建脚本（幂等，可重复执行）。
--
-- 用途：部署到新服务器后，直接创建一个 is_admin=true 的用户并向其积分账户
-- 授予 100 万积分。与 deploy-remote.sh 的 DEPLOY_BOOTSTRAP_ADMIN 走相同表结构，
-- 但使用独立的幂等键（用户 user_id 本身）；因此若改用本 SQL 手动创建，
-- 请勿再设 DEPLOY_BOOTSTRAP_ADMIN=true，避免重复授予。
--
-- 执行前必须先生成密码的 bcrypt 哈希（应用使用 passlib bcrypt，明文密码无法直接写入）：
--   cd backend && .venv/bin/python -c "from app.core.security import hash_password; print(hash_password('你的密码'))"
-- 把输出的 $2b$... 哈希填入下方 v_password_hash。
--
-- 执行方式（在服务器上，通过生产 compose 的 postgres 容器）：
--   docker compose --env-file <生产env> -f backend/docker-compose.production.yml \
--     exec -T postgres psql -U <MTV_POSTGRES_USER> <MTV_POSTGRES_DB> -f scripts/create-admin.sql
-- 或直接连库：
--   psql "postgresql://<user>:<pwd>@<host>:5432/<db>" -f scripts/create-admin.sql
--
-- 修改下方 DECLARE 中 v_username / v_password_hash 两个值后执行。
-- 单引号在哈希串里无需转义（bcrypt 哈希不含单引号）。

DO $$
DECLARE
    v_username        text   := 'admin';                -- 登录用户名
    v_password_hash   text   := 'REPLACE_WITH_BCRYPT_HASH'; -- 见上文生成方法
    v_credits          int    := 1000000;               -- 初始积分
    v_user_id          text;
    v_account_id       text;
    v_grant_exists     boolean;
BEGIN
    -- 1. 按用户名查找或创建管理员（已存在则保留原密码与权限）
    SELECT user_id INTO v_user_id FROM users WHERE username = v_username;
    IF v_user_id IS NULL THEN
        v_user_id := gen_random_uuid()::text;
        INSERT INTO users
            (user_id, username, password_hash, enabled, is_admin,
             account_type, can_manage_accounts, management_scope)
        VALUES
            (v_user_id, v_username, v_password_hash, true, true,
             'personal', false, 'none');
        RAISE NOTICE '已创建管理员账号：%', v_username;
    ELSE
        RAISE NOTICE '管理员账号已存在，保留现有密码与权限：%', v_username;
    END IF;

    -- 2. 创建或获取该用户的积分账户
    SELECT id INTO v_account_id
    FROM credit_accounts
    WHERE owner_type = 'user' AND owner_id = v_user_id;
    IF v_account_id IS NULL THEN
        v_account_id := gen_random_uuid()::text;
        INSERT INTO credit_accounts
            (id, owner_type, owner_id, org_id, department_id,
             balance_points, frozen_points, status)
        VALUES
            (v_account_id, 'user', v_user_id, NULL, NULL,
             0, 0, 'active');
    END IF;

    -- 3. 幂等授予积分：以 user_id 作为幂等键，已授予则跳过
    SELECT TRUE INTO v_grant_exists
    FROM credit_logs
    WHERE idempotency_key = v_user_id AND log_type = 'grant';
    IF v_grant_exists IS NULL THEN
        UPDATE credit_accounts
        SET balance_points = balance_points + v_credits,
            updated_at = now()
        WHERE id = v_account_id;

        INSERT INTO credit_logs
            (id, account_id, actor_user_id, actor_username,
             target_user_id, target_username, target_owner_type, target_owner_id,
             source_type, source_id, idempotency_key, log_type,
             points_delta, frozen_delta, balance_after, frozen_after, note)
        SELECT
            gen_random_uuid()::text, v_account_id, v_user_id, v_username,
            v_user_id, v_username, 'user', v_user_id,
            'deployment_bootstrap', v_user_id, v_user_id, 'grant',
            v_credits, 0, balance_points, 0, 'SQL 手动初始化管理员积分'
        FROM credit_accounts
        WHERE id = v_account_id;

        RAISE NOTICE '已向管理员账户授予 % 积分', v_credits;
    ELSE
        RAISE NOTICE '管理员初始积分已授予过，跳过重复授予';
    END IF;
END
$$;
