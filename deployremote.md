# MTV 远程部署操作手册

本手册基于 `scripts/deploy-remote.sh`，覆盖本地构建→远端部署、重启服务、查看日志、创建管理员等日常操作。

> 所有命令在项目根目录 `/Users/linshibo/GithubProject/MTV` 下执行。

---

## 一、前置：部署配置文件（仅首次）

```bash
cp scripts/deploy.env.example scripts/deploy.env
```

编辑 `scripts/deploy.env`，至少填写：

| 配置项 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| `DEPLOY_HOST` | 是 | — | 服务器 IP 或域名 |
| `DEPLOY_USER` | 否 | root | SSH 用户名 |
| `DEPLOY_PORT` | 否 | 22 | SSH 端口 |
| `DEPLOY_PASSWORD` | 否 | — | SSH 密码；**推荐改用 SSH 公钥免密** |
| `DEPLOY_PATH` | 否 | /data/users/lsbo/MTV/mtv-api | 远端后端目录 |
| `REMOTE_ENV_FILE` | 否 | $DEPLOY_PATH/backend/.env | 远端生产环境文件 |
| `MTV_PRODUCTION_IMAGE` | 否 | mtv-backend:production | 镜像标签 |
| `DEPLOY_TARGET_PLATFORM` | 否 | linux/amd64 | 构建平台 |

配置文件仅本地使用，不会被上传。SSH 公钥免密优先：脚本检测到免密就用公钥，否则用密码（需本地装 `sshpass` 或 `expect`）。

---

## 二、命令一览

```bash
./scripts/deploy-remote.sh <命令> [参数]
```

| 命令 | 作用 |
|------|------|
| `deploy`（默认） | 本地构建镜像→上传→加载→迁移→启动，一步发布 |
| `build` | 只在本地构建并校验 linux/amd64 镜像，不部署 |
| `start` | 服务器用现有镜像启动并强制重建服务 |
| `restart` | 服务器用现有镜像重启并强制重建服务 |
| `sync-env` | 从本地 `backend/.env` 生成并安全同步服务器生产 `.env` |
| `migrate` | 服务器执行数据库迁移（需 `DEPLOY_ALLOW_DATABASE_MIGRATION=true`） |
| `logs` | 查看服务器日志 |
| `status` | 查看服务状态 |
| `stop` | 停止服务（保留数据库、Redis、媒体数据） |
| `help` | 显示帮助 |

---

## 三、完整发布流程（本地构建 → 部署到远端）

```bash
# 1. 本地改完代码并提交
git add -A && git commit -m "feat: xxx"

# 2. 一键部署（构建 + 上传 + 迁移 + 启动 + 健康检查）
./scripts/deploy-remote.sh deploy
```

`deploy` 内部依次执行：
1. 本地构建 `linux/amd64` 后端镜像（`mtv-backend:production`）
2. 打包 backend 源码 + 后端镜像 + PostgreSQL/Redis 镜像归档（带 SHA-256 校验）
3. SSH 上传归档到服务器
4. 远端校验归档完整性 → 加载镜像 → 同步代码（不覆盖服务器 `.env`）
5. 启动 PostgreSQL/Redis 并等待健康检查
6. **停止旧 API/worker/beat**（避免迁移期并发写入）
7. **自动执行数据库迁移**：`alembic upgrade head && python -m app.db.drift_repair` + `--check-only` 门禁
8. 启动新 API/worker/beat → 健康检查 → 完成

### 迁移行为说明
- 默认 `DEPLOY_RUN_MIGRATIONS=true`、`DEPLOY_ALLOW_DATABASE_MIGRATION=true`，**迁移自动执行**，无需手动选迁移文件。
- 迁移含幂等漂移修复：补建线上 stamp 漂移导致的缺表/缺列/缺约束 + 回填 owner 成员，已一致时为 no-op。
- 迁移**前**会 `pg_dump` 备份到服务器（`MTV_DB_BACKUP_DIR`，默认 postgres 数据目录同级 `backups/`）；设 `MTV_SKIP_DB_BACKUP=true` 可跳过。
- **迁移失败时应用保持停止**（不拉起新代码在半迁移 schema 上跑），看日志排查后重新 `deploy`。

### 只构建不部署 / 复用镜像
```bash
./scripts/deploy-remote.sh build                    # 只构建，不部署
# 镜像已构建过、想跳过重建只更新代码：
DEPLOY_REUSE_LOCAL_IMAGE=true ./scripts/deploy-remote.sh deploy
```

---

## 四、重启远端服务

```bash
./scripts/deploy-remote.sh restart
```

`restart`/`start` 只在服务器用**现有镜像**重建容器，不重新构建、不跑 alembic 迁移。适用：改了 `.env` 后刷新配置、或单纯重启进程。

### 改 `.env` 的标准流程
```bash
# 1. 本地改 backend/.env
# 2. 安全同步到服务器（自动备份旧文件）
./scripts/deploy-remote.sh sync-env
# 3. 重启使新配置生效
./scripts/deploy-remote.sh restart
```

> 注意：`start`/`restart` 启动应用前会跑 `drift_repair --check-only` 只读门禁。若检测到 schema 漂移或迁移未完成会**拒绝启动**，提示先 `deploy`/`migrate`。所以代码有数据库变更时务必用 `deploy`，不要只 `restart`。

---

## 五、查看远端日志

```bash
# 跟随（实时）
./scripts/deploy-remote.sh logs -f api
./scripts/deploy-remote.sh logs -f celery-worker

# 看最近 N 行
./scripts/deploy-remote.sh logs --tail 500 api
./scripts/deploy-remote.sh logs --tail all api

# 最近一段时间
./scripts/deploy-remote.sh logs --since 10m api
./scripts/deploy-remote.sh logs --since 1h --until 30m api

# 默认（200 行、全部服务）
./scripts/deploy-remote.sh logs

# 指定多个服务
./scripts/deploy-remote.sh logs postgres redis
```

- 服务名：`postgres`、`redis`、`api`、`celery-worker`、`celery-beat`、`migrate`
- 选项：`-f/--follow`、`--tail N|all`、`--since`/`--until`、`-t/--timestamps`
- 默认行数 `MTV_LOG_TAIL=200`，可在 `deploy.env` 改

辅助：
```bash
./scripts/deploy-remote.sh status    # 查看各服务运行状态
./scripts/deploy-remote.sh stop      # 停止应用（保留数据）
```

---

## 六、创建管理员账户 + 100 万积分

部署到新服务器后，创建一个 `is_admin=true` 的管理员并授予 100 万积分。有两种方式，**二选一，不要混用**。

### 方式 A：部署时自动创建（推荐）

在 `scripts/deploy.env` 设置：
```bash
DEPLOY_BOOTSTRAP_ADMIN=true
DEPLOY_BOOTSTRAP_ADMIN_USERNAME=admin
DEPLOY_BOOTSTRAP_ADMIN_PASSWORD=你的密码
DEPLOY_BOOTSTRAP_ADMIN_CREDITS=1000000
```
然后 `./scripts/deploy-remote.sh deploy`，迁移后会自动创建管理员并授予积分（幂等，重复部署不会重复授予）。

### 方式 B：直接执行 SQL（手动，已验证）

脚本：`scripts/create-admin.sql`（幂等，可重复执行）。

**步骤：**

1. 生成密码的 bcrypt 哈希（应用用 passlib bcrypt，明文无法直接写入）：
```bash
cd backend && .venv/bin/python -c "from app.core.security import hash_password; print(hash_password('你的密码'))"
# 输出形如 $2b$12$...，复制
```

2. 编辑 `scripts/create-admin.sql`，把 `v_password_hash` 的 `REPLACE_WITH_BCRYPT_HASH` 替换为上一步哈希；按需改 `v_username`（默认 admin）。

3. 在服务器上执行（通过生产 compose 的 postgres 容器）：
```bash
docker compose --env-file <生产env文件> -f backend/docker-compose.production.yml \
  exec -T postgres psql -U <MTV_POSTGRES_USER> <MTV_POSTGRES_DB> -f scripts/create-admin.sql
```
或直接连库：
```bash
psql "postgresql://<user>:<pwd>@<host>:5432/<db>" -f scripts/create-admin.sql
```

4. 执行后会输出 NOTICE：`已创建管理员账号` + `已向管理员账户授予 1000000 积分`。重复执行只会提示「已存在，跳过」。

> 该 SQL 用 `user_id` 作为幂等键，与应用 bootstrap 的幂等键不同，故**若用此 SQL，就不要再设 `DEPLOY_BOOTSTRAP_ADMIN=true`**，避免重复授予。

---

## 七、典型场景速查

```bash
# 发布新代码（构建+迁移+启动）
./scripts/deploy-remote.sh deploy
./scripts/deploy-remote.sh logs -f api

# 只改了 .env
./scripts/deploy-remote.sh sync-env && ./scripts/deploy-remote.sh restart

# 单纯重启进程
./scripts/deploy-remote.sh restart

# 排查问题
./scripts/deploy-remote.sh status
./scripts/deploy-remote.sh logs --tail 500 api

# 只手动跑迁移（不重启应用，需 ALLOW 标志）
DEPLOY_ALLOW_DATABASE_MIGRATION=true ./scripts/deploy-remote.sh migrate

# 停服保留数据
./scripts/deploy-remote.sh stop
```
