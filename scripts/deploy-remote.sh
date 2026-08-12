#!/usr/bin/env bash

# MTV 后端远程 Docker 部署脚本。
#
# deploy：在本地构建 linux/amd64 镜像，上传镜像归档和 backend 源码，
#         在服务器加载镜像并启动生产 Compose。
# start/restart：只在服务器重新创建容器，适用于服务器 .env 变更后的配置刷新。
# logs/status/stop：通过 SSH 操作服务器上的生产 Compose。

set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
BACKEND_DIR="$ROOT_DIR/backend"
CONFIG_FILE="${DEPLOY_CONFIG:-}"
SSH_BASE_OPTS=(-o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30 -o ConnectTimeout=15)

ACTION=""
ACTION_ARGS=()
AUTH_MODE=""
DEPLOY_HOST="${DEPLOY_HOST:-}"
DEPLOY_USER="${DEPLOY_USER:-}"
DEPLOY_PASSWORD="${DEPLOY_PASSWORD:-}"
DEPLOY_PORT="${DEPLOY_PORT:-}"
DEPLOY_PATH="${DEPLOY_PATH:-}"
REMOTE_ENV_FILE="${REMOTE_ENV_FILE:-}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-}"
IMAGE_NAME="${IMAGE_NAME:-}"
TARGET_PLATFORM="${TARGET_PLATFORM:-}"
INSTALL_ALIYUN_OSS="${INSTALL_ALIYUN_OSS:-}"
PUSH_DEPENDENCY_IMAGES="${PUSH_DEPENDENCY_IMAGES:-}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-}"
REDIS_IMAGE="${REDIS_IMAGE:-}"
RUN_MIGRATIONS="${RUN_MIGRATIONS:-}"
ALLOW_DATABASE_MIGRATION="${ALLOW_DATABASE_MIGRATION:-}"
LOG_TAIL="${LOG_TAIL:-}"

CODE_ARCHIVE=""
IMAGE_ARCHIVE=""
POSTGRES_ARCHIVE=""
REDIS_ARCHIVE=""
LOCAL_BOOT=""

info() { echo "[INFO] $*"; }
ok() { echo "[OK] $*"; }
warn() { echo "[WARN] $*" >&2; }
die() { echo "[ERROR] $*" >&2; exit 1; }

usage() {
    cat <<'EOF'
MTV 后端远程 Docker 部署

用法：
  ./scripts/deploy-remote.sh [--config <文件>] [命令] [参数]

命令：
  deploy                 本地构建镜像、同步 backend、上传并启动生产服务（默认）
  build                  只在本地构建并校验 linux/amd64 后端镜像
  start                  服务器使用现有镜像启动并强制重建服务
  restart                服务器使用现有镜像重启并强制重建服务
  migrate                服务器执行 Alembic 迁移（需显式允许并确认备份）
  logs [日志选项/服务名] 查看服务器日志，例如：logs -f api
  status                 查看服务器服务状态
  stop                   停止服务器服务，保留数据库、Redis 和媒体数据
  help                   显示本帮助

配置文件（不会被上传）：
  .deploy.env 或 scripts/deploy.env；也可用 --config 指定。
  可复制 scripts/deploy.env.example 创建配置文件。

关键配置：
  DEPLOY_HOST                 服务器 IP/域名（必填）
  DEPLOY_USER                 SSH 用户名，默认 root
  DEPLOY_PORT                 SSH 端口，默认 22
  DEPLOY_PASSWORD             SSH 密码；更推荐配置 SSH 公钥免密
  DEPLOY_PATH                 远端后端目录，默认 /data/users/lsbo/MTV/mtv-api
  REMOTE_ENV_FILE             服务器环境文件，默认 /data/users/lsbo/MTV/mtv-api/backend/.env
  MTV_PRODUCTION_IMAGE        镜像标签，默认 mtv-backend:production
  MTV_PRODUCTION_PROJECT_NAME Compose 项目名，默认 mtv-production
  DEPLOY_TARGET_PLATFORM      构建平台，默认 linux/amd64
  MTV_INSTALL_ALIYUN_OSS      Docker 构建参数，默认 true
  DEPLOY_PUSH_DEPENDENCY_IMAGES=true  同时上传 PostgreSQL/Redis 镜像归档
  DEPLOY_RUN_MIGRATIONS=false deploy 时是否执行 Alembic；需同时确认数据库备份
  DEPLOY_ALLOW_DATABASE_MIGRATION=true  明确允许 deploy 执行迁移
  MTV_LOG_TAIL                logs 默认行数，默认 200

示例：
  cp scripts/deploy.env.example scripts/deploy.env
  ./scripts/deploy-remote.sh deploy
  ./scripts/deploy-remote.sh restart
  ./scripts/deploy-remote.sh logs -f api
EOF
}

parse_options() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help)
                usage
                exit 0
                ;;
            --config)
                CONFIG_FILE="${2:-}"
                [[ -n "$CONFIG_FILE" ]] || die "--config 需要文件路径"
                shift 2
                ;;
            --config=*)
                CONFIG_FILE="${1#*=}"
                [[ -n "$CONFIG_FILE" ]] || die "--config 需要文件路径"
                shift
                ;;
            *)
                break
                ;;
        esac
    done

    ACTION="${1:-deploy}"
    if [[ $# -gt 0 ]]; then
        shift
    fi
    ACTION_ARGS=("$@")

    case "$ACTION" in
        deploy|build|start|restart|migrate|logs|status|stop|help) ;;
        *) die "未知命令：${ACTION}（使用 help 查看用法）" ;;
    esac
    [[ "$ACTION" != "help" ]] || { usage; exit 0; }
}

resolve_config_file() {
    if [[ -n "$CONFIG_FILE" ]]; then
        if [[ "$CONFIG_FILE" != /* ]]; then
            CONFIG_FILE="$ROOT_DIR/$CONFIG_FILE"
        fi
        [[ -f "$CONFIG_FILE" ]] || die "配置文件不存在：$CONFIG_FILE"
        return
    fi

    if [[ -f "$ROOT_DIR/.deploy.env" ]]; then
        CONFIG_FILE="$ROOT_DIR/.deploy.env"
    elif [[ -f "$ROOT_DIR/scripts/deploy.env" ]]; then
        CONFIG_FILE="$ROOT_DIR/scripts/deploy.env"
    fi
}

load_config_file() {
    local file="$1"
    local line key value

    [[ -n "$file" && -f "$file" ]] || return 0
    info "加载部署配置：$file"
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%$'\r'}"
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ "$line" =~ ^[[:space:]]*$ ]] && continue
        [[ "$line" =~ ^[[:space:]]*[A-Za-z_][A-Za-z0-9_]*= ]] || continue
        line="${line#"${line%%[![:space:]]*}"}"
        key="${line%%=*}"
        value="${line#*=}"
        if [[ "$value" =~ ^\".*\"$ ]]; then
            value="${value:1:${#value}-2}"
        elif [[ "$value" =~ ^\'.*\'$ ]]; then
            value="${value:1:${#value}-2}"
        fi
        case "$key" in
            DEPLOY_HOST|DEPLOY_USER|DEPLOY_PASSWORD|DEPLOY_PORT|DEPLOY_PATH|REMOTE_ENV_FILE|\
            MTV_PRODUCTION_PROJECT_NAME|COMPOSE_PROJECT_NAME|MTV_PRODUCTION_IMAGE|IMAGE_NAME|\
            DEPLOY_TARGET_PLATFORM|TARGET_PLATFORM|MTV_INSTALL_ALIYUN_OSS|INSTALL_ALIYUN_OSS|\
            DEPLOY_PUSH_DEPENDENCY_IMAGES|PUSH_DEPENDENCY_IMAGES|MTV_POSTGRES_IMAGE|POSTGRES_IMAGE|\
            MTV_REDIS_IMAGE|REDIS_IMAGE|DEPLOY_RUN_MIGRATIONS|RUN_MIGRATIONS|\
            DEPLOY_ALLOW_DATABASE_MIGRATION|ALLOW_DATABASE_MIGRATION|MTV_LOG_TAIL|LOG_TAIL) ;;
            *) continue ;;
        esac
        if [[ -z "${!key:-}" ]]; then
            printf -v "$key" '%s' "$value"
            export "$key"
        fi
    done < "$file"
}

set_defaults() {
    DEPLOY_USER="${DEPLOY_USER:-root}"
    DEPLOY_PORT="${DEPLOY_PORT:-22}"
    DEPLOY_PATH="${DEPLOY_PATH:-/data/users/lsbo/MTV/mtv-api}"
    REMOTE_ENV_FILE="${REMOTE_ENV_FILE:-$DEPLOY_PATH/backend/.env}"
    COMPOSE_PROJECT_NAME="${MTV_PRODUCTION_PROJECT_NAME:-${COMPOSE_PROJECT_NAME:-mtv-production}}"
    IMAGE_NAME="${MTV_PRODUCTION_IMAGE:-${IMAGE_NAME:-mtv-backend:production}}"
    TARGET_PLATFORM="${DEPLOY_TARGET_PLATFORM:-${TARGET_PLATFORM:-linux/amd64}}"
    INSTALL_ALIYUN_OSS="${MTV_INSTALL_ALIYUN_OSS:-${INSTALL_ALIYUN_OSS:-true}}"
    PUSH_DEPENDENCY_IMAGES="${DEPLOY_PUSH_DEPENDENCY_IMAGES:-${PUSH_DEPENDENCY_IMAGES:-true}}"
    POSTGRES_IMAGE="${MTV_POSTGRES_IMAGE:-${POSTGRES_IMAGE:-postgres:16-alpine}}"
    REDIS_IMAGE="${MTV_REDIS_IMAGE:-${REDIS_IMAGE:-redis:7-alpine}}"
    RUN_MIGRATIONS="${DEPLOY_RUN_MIGRATIONS:-${RUN_MIGRATIONS:-false}}"
    ALLOW_DATABASE_MIGRATION="${DEPLOY_ALLOW_DATABASE_MIGRATION:-${ALLOW_DATABASE_MIGRATION:-false}}"
    LOG_TAIL="${MTV_LOG_TAIL:-${LOG_TAIL:-200}}"
}

validate_scalar() {
    local name="$1"
    local value="$2"
    [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]] || die "$name 不能包含换行符"
}

validate_settings() {
    local value

    if [[ "$ACTION" != build ]]; then
        [[ -n "$DEPLOY_HOST" ]] || die "DEPLOY_HOST 不能为空，请在配置文件中填写服务器地址"
        [[ "$DEPLOY_HOST" =~ ^[A-Za-z0-9._:-]+$ ]] || die "DEPLOY_HOST 只能包含域名、IPv4/IPv6 地址和端口相关字符"
        [[ "$DEPLOY_USER" =~ ^[A-Za-z0-9._-]+$ ]] || die "DEPLOY_USER 只能包含字母、数字、点、下划线和连字符"
    fi
    [[ "$DEPLOY_PORT" =~ ^[1-9][0-9]*$ && "$DEPLOY_PORT" -le 65535 ]] || die "DEPLOY_PORT 必须是 1-65535 的数字"
    [[ "$DEPLOY_PATH" == /* && "$DEPLOY_PATH" != "/" ]] || die "DEPLOY_PATH 必须是非根绝对路径"
    [[ "$REMOTE_ENV_FILE" == /* ]] || die "REMOTE_ENV_FILE 必须是绝对路径"
    [[ "$COMPOSE_PROJECT_NAME" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || die "MTV_PRODUCTION_PROJECT_NAME 只能包含小写字母、数字、下划线和连字符"
    [[ "$IMAGE_NAME" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:-]*$ ]] || die "MTV_PRODUCTION_IMAGE 不是有效的镜像标签"
    [[ "$POSTGRES_IMAGE" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:-]*$ ]] || die "MTV_POSTGRES_IMAGE 不是有效的镜像标签"
    [[ "$REDIS_IMAGE" =~ ^[A-Za-z0-9][A-Za-z0-9._/@:-]*$ ]] || die "MTV_REDIS_IMAGE 不是有效的镜像标签"
    [[ "$TARGET_PLATFORM" =~ ^linux/[A-Za-z0-9._/-]+$ ]] || die "DEPLOY_TARGET_PLATFORM 必须形如 linux/amd64"
    for value in "$DEPLOY_HOST" "$DEPLOY_USER" "$DEPLOY_PATH" "$REMOTE_ENV_FILE" "$COMPOSE_PROJECT_NAME" "$IMAGE_NAME" "$TARGET_PLATFORM"; do
        validate_scalar "部署配置" "$value"
    done
    case "$INSTALL_ALIYUN_OSS" in true|false) ;; *) die "MTV_INSTALL_ALIYUN_OSS 只能是 true 或 false" ;; esac
    case "$PUSH_DEPENDENCY_IMAGES" in true|false) ;; *) die "DEPLOY_PUSH_DEPENDENCY_IMAGES 只能是 true 或 false" ;; esac
    case "$RUN_MIGRATIONS" in true|false) ;; *) die "DEPLOY_RUN_MIGRATIONS 只能是 true 或 false" ;; esac
    case "$ALLOW_DATABASE_MIGRATION" in true|false) ;; *) die "DEPLOY_ALLOW_DATABASE_MIGRATION 只能是 true 或 false" ;; esac
    [[ "$LOG_TAIL" =~ ^[0-9]+$ ]] || die "MTV_LOG_TAIL 必须是非负整数"
}

validate_logs_args() {
    local args=("$@")
    local index=0
    local arg value

    while (( index < ${#args[@]} )); do
        arg="${args[index]}"
        case "$arg" in
            -f|--follow|-t|--timestamps|--no-color|--no-log-prefix)
                ;;
            --tail)
                index=$((index + 1))
                (( index < ${#args[@]} )) || die "logs --tail 需要行数（数字或 all）"
                value="${args[index]}"
                [[ "$value" == all || "$value" =~ ^[0-9]+$ ]] || die "logs --tail 只能是数字或 all"
                ;;
            --tail=*)
                value="${arg#*=}"
                [[ "$value" == all || "$value" =~ ^[0-9]+$ ]] || die "logs --tail 只能是数字或 all"
                ;;
            --since|--until)
                index=$((index + 1))
                (( index < ${#args[@]} )) || die "logs $arg 需要时间值"
                value="${args[index]}"
                [[ "$value" != -* && "$value" != *$'\n'* && "$value" != *$'\r'* ]] || die "logs $arg 的值无效"
                ;;
            --since=*|--until=*)
                value="${arg#*=}"
                [[ -n "$value" && "$value" != -* && "$value" != *$'\n'* && "$value" != *$'\r'* ]] || die "logs 时间参数值无效"
                ;;
            postgres|redis|api|celery-worker|celery-beat|migrate)
                ;;
            *)
                die "logs 不支持参数：${arg}；仅允许日志选项和 Compose 服务名"
                ;;
        esac
        index=$((index + 1))
    done
}

require_transfer_tools() {
    local command_name
    for command_name in tar ssh scp; do
        command -v "$command_name" >/dev/null 2>&1 || die "本地缺少命令：$command_name"
    done
}

require_local_docker() {
    command -v docker >/dev/null 2>&1 || die "本地未找到 Docker"
    docker info >/dev/null 2>&1 || die "本地 Docker 守护进程不可用"
}

quote_arg() {
    printf -v QUOTED_ARG '%q' "$1"
}

sha256_file() {
    local file="$1"
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$file" | awk '{print $1}'
    elif command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$file" | awk '{print $1}'
    elif command -v openssl >/dev/null 2>&1; then
        openssl dgst -sha256 "$file" | awk '{print $NF}'
    else
        die "本地缺少 SHA-256 校验工具（shasum、sha256sum 或 openssl）"
    fi
}

validate_local_image() {
    local actual_platform
    actual_platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$IMAGE_NAME" 2>/dev/null || true)"
    [[ "$actual_platform" == "$TARGET_PLATFORM" ]] || die "本地镜像平台不匹配：${actual_platform:-未知}，需要 $TARGET_PLATFORM"
    ok "本地镜像校验通过：$IMAGE_NAME ($actual_platform)"
}

build_backend_image() {
    require_local_docker
    [[ -f "$BACKEND_DIR/Dockerfile" ]] || die "未找到后端 Dockerfile：$BACKEND_DIR/Dockerfile"
    info "本地构建后端镜像：$IMAGE_NAME ($TARGET_PLATFORM)"
    if docker buildx version >/dev/null 2>&1; then
        docker buildx build \
            --platform "$TARGET_PLATFORM" \
            --load \
            --tag "$IMAGE_NAME" \
            --build-arg "INSTALL_ALIYUN_OSS=$INSTALL_ALIYUN_OSS" \
            "$BACKEND_DIR"
    else
        warn "未找到 docker buildx，尝试使用 docker build --platform"
        docker build \
            --platform "$TARGET_PLATFORM" \
            --tag "$IMAGE_NAME" \
            --build-arg "INSTALL_ALIYUN_OSS=$INSTALL_ALIYUN_OSS" \
            "$BACKEND_DIR"
    fi
    validate_local_image
}

prepare_dependency_archive() {
    local image="$1"
    local output_file="$2"
    local platform

    if ! docker image inspect "$image" >/dev/null 2>&1 || [[ "$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image" 2>/dev/null || true)" != "$TARGET_PLATFORM" ]]; then
        info "本地拉取依赖镜像：$image ($TARGET_PLATFORM)"
        docker pull --platform "$TARGET_PLATFORM" "$image"
    fi
    platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image" 2>/dev/null || true)"
    [[ "$platform" == "$TARGET_PLATFORM" ]] || die "依赖镜像 $image 平台不匹配：${platform:-未知}"
    info "导出依赖镜像：$image"
    docker save --output "$output_file" "$image"
}

create_archives() {
    local stamp="$1"
    [[ -f "$BACKEND_DIR/docker-compose.production.yml" ]] || die "未找到生产 Compose 文件"
    require_transfer_tools

    CODE_ARCHIVE="/tmp/mtv-api-code-${stamp}.tar.gz"
    IMAGE_ARCHIVE="/tmp/mtv-api-image-${stamp}.tar"
    LOCAL_BOOT="/tmp/mtv-api-boot-${stamp}.sh"
    tar -czf "$CODE_ARCHIVE" \
        --exclude='backend/.git' \
        --exclude='backend/.venv' \
        --exclude='backend/storage' \
        --exclude='backend/.pytest_cache' \
        --exclude='backend/.mypy_cache' \
        --exclude='backend/.ruff_cache' \
        --exclude='backend/__pycache__' \
        --exclude='backend/*/__pycache__' \
        --exclude='backend/*.pyc' \
        --exclude='backend/*.pyo' \
        --exclude='backend/*.egg-info' \
        --exclude='*.env' \
        --exclude='*.env.*' \
        -C "$ROOT_DIR" backend
    if tar -tzf "$CODE_ARCHIVE" | awk '/(^|\/)\.env($|\.)|(^|\/)[^/]*\.env$/ { found=1 } END { exit found ? 0 : 1 }'; then
        die "代码归档疑似包含环境文件，已中止上传"
    fi

    info "导出后端镜像：$IMAGE_NAME"
    docker save --output "$IMAGE_ARCHIVE" "$IMAGE_NAME"

    if [[ "$PUSH_DEPENDENCY_IMAGES" == true ]]; then
        POSTGRES_ARCHIVE="/tmp/mtv-api-postgres-${stamp}.tar"
        REDIS_ARCHIVE="/tmp/mtv-api-redis-${stamp}.tar"
        prepare_dependency_archive "$POSTGRES_IMAGE" "$POSTGRES_ARCHIVE"
        prepare_dependency_archive "$REDIS_IMAGE" "$REDIS_ARCHIVE"
    fi

    info "代码归档大小：$(du -h "$CODE_ARCHIVE" | awk '{print $1}')；镜像归档大小：$(du -h "$IMAGE_ARCHIVE" | awk '{print $1}')"
}

cleanup_local_files() {
    local file
    for file in "$CODE_ARCHIVE" "$IMAGE_ARCHIVE" "$POSTGRES_ARCHIVE" "$REDIS_ARCHIVE" "$LOCAL_BOOT"; do
        [[ -n "$file" && -e "$file" ]] && rm -f -- "$file"
    done
}

prepare_auth() {
    local target="${DEPLOY_USER}@${DEPLOY_HOST}"
    [[ -n "$AUTH_MODE" ]] && return

    if ssh -p "$DEPLOY_PORT" "${SSH_BASE_OPTS[@]}" -o BatchMode=yes "$target" -- true >/dev/null 2>&1; then
        AUTH_MODE="key"
        ok "检测到 SSH 公钥免密登录"
        return
    fi

    if [[ -z "$DEPLOY_PASSWORD" ]]; then
        if [[ ! -t 0 ]]; then
            die "SSH 公钥登录不可用，且当前没有可交互读取密码的终端；请设置 DEPLOY_PASSWORD 或配置公钥"
        fi
        IFS= read -r -s -p "SSH 密码：" DEPLOY_PASSWORD || true
        echo
    fi
    [[ -n "$DEPLOY_PASSWORD" ]] || die "DEPLOY_PASSWORD 不能为空"
    if command -v sshpass >/dev/null 2>&1; then
        AUTH_MODE="sshpass"
    elif command -v expect >/dev/null 2>&1; then
        AUTH_MODE="expect"
    else
        die "密码登录需要 sshpass 或 expect；macOS 可安装 sshpass，或改用 SSH 公钥"
    fi
    info "认证方式：$AUTH_MODE"
}

run_ssh() {
    local remote_command="$1"
    local target="${DEPLOY_USER}@${DEPLOY_HOST}"

    case "$AUTH_MODE" in
        key)
            ssh -p "$DEPLOY_PORT" "${SSH_BASE_OPTS[@]}" "$target" -- "$remote_command"
            ;;
        sshpass)
            SSHPASS="$DEPLOY_PASSWORD" sshpass -e \
                ssh -p "$DEPLOY_PORT" "${SSH_BASE_OPTS[@]}" "$target" -- "$remote_command"
            ;;
        expect)
            EXPECT_REMOTE_COMMAND="$remote_command" \
            DEPLOY_EXPECT_PASSWORD="$DEPLOY_PASSWORD" \
            DEPLOY_EXPECT_USER="$DEPLOY_USER" \
            DEPLOY_EXPECT_HOST="$DEPLOY_HOST" \
            DEPLOY_EXPECT_PORT="$DEPLOY_PORT" \
            expect <<'EXPECT_EOF'
set timeout 1800
set ssh_args [list ssh -p $env(DEPLOY_EXPECT_PORT) -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30 -o ConnectTimeout=15]
lappend ssh_args "$env(DEPLOY_EXPECT_USER)@$env(DEPLOY_EXPECT_HOST)" -- $env(EXPECT_REMOTE_COMMAND)
spawn {*}$ssh_args
expect {
    -re {(?i)are you sure you want to continue connecting} { send "yes\r"; exp_continue }
    -re {(?i)(password|口令):} { send -- $env(DEPLOY_EXPECT_PASSWORD); send "\r"; exp_continue }
    eof
}
lassign [wait] pid spawnid os_error_flag value
if {$os_error_flag != 0} { exit 1 }
exit $value
EXPECT_EOF
            ;;
        *) die "内部错误：未知认证方式 $AUTH_MODE" ;;
    esac
}

run_scp() {
    local source_file="$1"
    local remote_path="$2"
    local remote_target="${DEPLOY_USER}@${DEPLOY_HOST}:${remote_path}"

    case "$AUTH_MODE" in
        key)
            scp -P "$DEPLOY_PORT" "${SSH_BASE_OPTS[@]}" "$source_file" "$remote_target"
            ;;
        sshpass)
            SSHPASS="$DEPLOY_PASSWORD" sshpass -e \
                scp -P "$DEPLOY_PORT" "${SSH_BASE_OPTS[@]}" "$source_file" "$remote_target"
            ;;
        expect)
            DEPLOY_EXPECT_SOURCE="$source_file" \
            DEPLOY_EXPECT_TARGET="$remote_target" \
            DEPLOY_EXPECT_PASSWORD="$DEPLOY_PASSWORD" \
            DEPLOY_EXPECT_PORT="$DEPLOY_PORT" \
            expect <<'EXPECT_EOF'
set timeout 1800
set scp_args [list scp -P $env(DEPLOY_EXPECT_PORT) -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30]
lappend scp_args $env(DEPLOY_EXPECT_SOURCE) $env(DEPLOY_EXPECT_TARGET)
spawn {*}$scp_args
expect {
    -re {(?i)are you sure you want to continue connecting} { send "yes\r"; exp_continue }
    -re {(?i)(password|口令):} { send -- $env(DEPLOY_EXPECT_PASSWORD); send "\r"; exp_continue }
    eof
}
lassign [wait] pid spawnid os_error_flag value
if {$os_error_flag != 0} { exit 1 }
exit $value
EXPECT_EOF
            ;;
        *) die "内部错误：未知认证方式 $AUTH_MODE" ;;
    esac
}

make_boot_script() {
    local remote_path_q env_file_q project_q compose_file_q image_q platform_q
    local code_archive_q image_archive_q postgres_archive_q redis_archive_q
    local code_sha_q image_sha_q postgres_sha_q redis_sha_q postgres_image_q redis_image_q
    local run_migrations_q allow_migration_q

    quote_arg "$DEPLOY_PATH"; remote_path_q="$QUOTED_ARG"
    quote_arg "$REMOTE_ENV_FILE"; env_file_q="$QUOTED_ARG"
    quote_arg "$COMPOSE_PROJECT_NAME"; project_q="$QUOTED_ARG"
    quote_arg "$DEPLOY_PATH/backend/docker-compose.production.yml"; compose_file_q="$QUOTED_ARG"
    quote_arg "$IMAGE_NAME"; image_q="$QUOTED_ARG"
    quote_arg "$TARGET_PLATFORM"; platform_q="$QUOTED_ARG"
    quote_arg "$CODE_ARCHIVE"; code_archive_q="$QUOTED_ARG"
    quote_arg "$IMAGE_ARCHIVE"; image_archive_q="$QUOTED_ARG"
    quote_arg "${POSTGRES_ARCHIVE:-}"; postgres_archive_q="$QUOTED_ARG"
    quote_arg "${REDIS_ARCHIVE:-}"; redis_archive_q="$QUOTED_ARG"
    quote_arg "$(sha256_file "$CODE_ARCHIVE")"; code_sha_q="$QUOTED_ARG"
    quote_arg "$(sha256_file "$IMAGE_ARCHIVE")"; image_sha_q="$QUOTED_ARG"
    if [[ -n "$POSTGRES_ARCHIVE" ]]; then
        quote_arg "$(sha256_file "$POSTGRES_ARCHIVE")"; postgres_sha_q="$QUOTED_ARG"
    else
        postgres_sha_q="''"
    fi
    if [[ -n "$REDIS_ARCHIVE" ]]; then
        quote_arg "$(sha256_file "$REDIS_ARCHIVE")"; redis_sha_q="$QUOTED_ARG"
    else
        redis_sha_q="''"
    fi
    quote_arg "$POSTGRES_IMAGE"; postgres_image_q="$QUOTED_ARG"
    quote_arg "$REDIS_IMAGE"; redis_image_q="$QUOTED_ARG"
    quote_arg "$RUN_MIGRATIONS"; run_migrations_q="$QUOTED_ARG"
    quote_arg "$ALLOW_DATABASE_MIGRATION"; allow_migration_q="$QUOTED_ARG"

    {
        printf '%s\n' '#!/usr/bin/env bash' 'set -Eeuo pipefail'
        printf 'REMOTE_PATH=%s\n' "$remote_path_q"
        printf 'REMOTE_ENV_FILE=%s\n' "$env_file_q"
        printf 'COMPOSE_PROJECT_NAME=%s\n' "$project_q"
        printf 'COMPOSE_FILE=%s\n' "$compose_file_q"
        printf 'IMAGE_NAME=%s\n' "$image_q"
        printf 'TARGET_PLATFORM=%s\n' "$platform_q"
        printf 'CODE_ARCHIVE=%s\n' "$code_archive_q"
        printf 'IMAGE_ARCHIVE=%s\n' "$image_archive_q"
        printf 'POSTGRES_ARCHIVE=%s\n' "$postgres_archive_q"
        printf 'REDIS_ARCHIVE=%s\n' "$redis_archive_q"
        printf 'CODE_SHA256=%s\n' "$code_sha_q"
        printf 'IMAGE_SHA256=%s\n' "$image_sha_q"
        printf 'POSTGRES_SHA256=%s\n' "$postgres_sha_q"
        printf 'REDIS_SHA256=%s\n' "$redis_sha_q"
        printf 'POSTGRES_IMAGE=%s\n' "$postgres_image_q"
        printf 'REDIS_IMAGE=%s\n' "$redis_image_q"
        printf 'RUN_MIGRATIONS=%s\n' "$run_migrations_q"
        printf 'ALLOW_DATABASE_MIGRATION=%s\n' "$allow_migration_q"
        cat <<'REMOTE_BOOT_BODY'

info() { echo "[INFO] $*"; }
ok() { echo "[OK] $*"; }
warn() { echo "[WARN] $*" >&2; }
die() { echo "[ERROR] $*" >&2; exit 1; }

sha256_file() {
    local file="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$file" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$file" | awk '{print $1}'
    elif command -v openssl >/dev/null 2>&1; then
        openssl dgst -sha256 "$file" | awk '{print $NF}'
    else
        die "远端缺少 SHA-256 校验工具"
    fi
}

cleanup() {
    local file
    for file in "$CODE_ARCHIVE" "$IMAGE_ARCHIVE" "$POSTGRES_ARCHIVE" "$REDIS_ARCHIVE" "$0"; do
        [[ -n "$file" && -e "$file" ]] && rm -f -- "$file"
    done
    if [[ -n "${STAGING_DIR:-}" && "$STAGING_DIR" == "${TMPDIR:-/tmp}"/mtv-api-staging.* && -d "$STAGING_DIR" ]]; then
        rm -rf -- "$STAGING_DIR"
    fi
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || die "远端未安装 Docker"
docker compose version >/dev/null 2>&1 || die "远端 docker compose 不可用（需要 Compose V2）"
docker info >/dev/null 2>&1 || die "远端 Docker 守护进程不可用"
[[ -r "$REMOTE_ENV_FILE" ]] || die "远端环境文件不可读：$REMOTE_ENV_FILE"
[[ -f "$CODE_ARCHIVE" && -f "$IMAGE_ARCHIVE" ]] || die "远端部署归档不完整"

[[ "$(sha256_file "$CODE_ARCHIVE")" == "$CODE_SHA256" ]] || die "代码归档 SHA-256 校验失败"
[[ "$(sha256_file "$IMAGE_ARCHIVE")" == "$IMAGE_SHA256" ]] || die "后端镜像归档 SHA-256 校验失败"
if [[ -n "$POSTGRES_ARCHIVE" ]]; then
    [[ -f "$POSTGRES_ARCHIVE" ]] || die "PostgreSQL 镜像归档不存在"
    [[ "$(sha256_file "$POSTGRES_ARCHIVE")" == "$POSTGRES_SHA256" ]] || die "PostgreSQL 镜像归档 SHA-256 校验失败"
fi
if [[ -n "$REDIS_ARCHIVE" ]]; then
    [[ -f "$REDIS_ARCHIVE" ]] || die "Redis 镜像归档不存在"
    [[ "$(sha256_file "$REDIS_ARCHIVE")" == "$REDIS_SHA256" ]] || die "Redis 镜像归档 SHA-256 校验失败"
fi

remote_platform="$(docker info --format '{{.OSType}}/{{.Architecture}}' 2>/dev/null || true)"
[[ "$remote_platform" != "linux/x86_64" ]] || remote_platform="linux/amd64"
[[ "$remote_platform" == "$TARGET_PLATFORM" ]] || die "远端 Docker 平台不匹配：${remote_platform:-未知}，需要 $TARGET_PLATFORM"

if [[ -L "$REMOTE_PATH" ]]; then
    die "远端代码目录不能是符号链接：$REMOTE_PATH"
fi
mkdir -p "$REMOTE_PATH"
if [[ -e "$REMOTE_PATH/backend" && -L "$REMOTE_PATH/backend" ]]; then
    die "远端 backend 目录不能是符号链接：$REMOTE_PATH/backend"
fi

# 先在受控临时目录解压并检查归档，避免异常条目越界写入远端目录。
archive_entries="$(tar -tzf "$CODE_ARCHIVE")" || die "代码归档无法读取"
while IFS= read -r entry; do
    entry="${entry#./}"
    [[ -n "$entry" ]] || continue
    [[ "$entry" == backend || "$entry" == backend/* ]] || die "代码归档包含非 backend 条目：$entry"
    case "$entry" in
        /*|..|../*|*/../*|*/..|*.env|*.env.*|*/.env|*/.env.*)
            die "代码归档包含禁止条目：$entry"
            ;;
    esac
done <<< "$archive_entries"
archive_details="$(tar -tvzf "$CODE_ARCHIVE")" || die "代码归档详情无法读取"
if awk '$1 ~ /^[lh]/ { found=1 } END { exit found ? 0 : 1 }' <<< "$archive_details"; then
    die "代码归档不能包含符号链接或硬链接"
fi
STAGING_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mtv-api-staging.XXXXXX")"
tar -xzf "$CODE_ARCHIVE" --no-same-owner --no-same-permissions --no-overwrite-dir -C "$STAGING_DIR"
[[ -d "$STAGING_DIR/backend" && ! -L "$STAGING_DIR/backend" ]] || die "代码归档缺少安全的 backend 目录"
mkdir -p "$REMOTE_PATH/backend"
info "同步 backend 代码到 ${REMOTE_PATH}（不会覆盖服务器环境文件）"
cp -a "$STAGING_DIR/backend/." "$REMOTE_PATH/backend/"
[[ -f "$COMPOSE_FILE" ]] || die "同步后缺少生产 Compose 文件：$COMPOSE_FILE"
[[ -f "$REMOTE_PATH/backend/Dockerfile" ]] || die "同步后缺少 Dockerfile"

info "加载后端镜像"
docker load --input "$IMAGE_ARCHIVE" >/dev/null
docker image inspect "$IMAGE_NAME" >/dev/null 2>&1 || die "加载后未找到后端镜像：$IMAGE_NAME"
loaded_platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$IMAGE_NAME" 2>/dev/null || true)"
[[ "$loaded_platform" == "$TARGET_PLATFORM" ]] || die "加载的后端镜像平台不匹配：${loaded_platform:-未知}"

if [[ -n "$POSTGRES_ARCHIVE" ]]; then
    info "加载 PostgreSQL 镜像"
    docker load --input "$POSTGRES_ARCHIVE" >/dev/null
    docker image inspect "$POSTGRES_IMAGE" >/dev/null 2>&1 || die "加载后未找到 PostgreSQL 镜像：$POSTGRES_IMAGE"
fi
if [[ -n "$REDIS_ARCHIVE" ]]; then
    info "加载 Redis 镜像"
    docker load --input "$REDIS_ARCHIVE" >/dev/null
    docker image inspect "$REDIS_IMAGE" >/dev/null 2>&1 || die "加载后未找到 Redis 镜像：$REDIS_IMAGE"
fi

export MTV_ENV_FILE="$REMOTE_ENV_FILE"
export MTV_PRODUCTION_IMAGE="$IMAGE_NAME"
export MTV_POSTGRES_IMAGE="$POSTGRES_IMAGE"
export MTV_REDIS_IMAGE="$REDIS_IMAGE"
COMPOSE=(docker compose --project-name "$COMPOSE_PROJECT_NAME" --env-file "$REMOTE_ENV_FILE" -f "$COMPOSE_FILE")
compose() { "${COMPOSE[@]}" "$@"; }
compose_up() { compose up --pull never "$@"; }
compose_run() { compose run --pull never "$@"; }
compose config --quiet

require_image() {
    local image="$1"
    local component="$2"
    docker image inspect "$image" >/dev/null 2>&1 || die "远端缺少 ${component} 镜像：${image}；请先执行 deploy 或上传匹配归档"
    local platform
    platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image" 2>/dev/null || true)"
    [[ "$platform" == "$TARGET_PLATFORM" ]] || die "$component 镜像平台不匹配：${platform:-未知}，需要 $TARGET_PLATFORM"
}

require_image "$IMAGE_NAME" "后端"
require_image "$POSTGRES_IMAGE" "PostgreSQL"
require_image "$REDIS_IMAGE" "Redis"

wait_for_health() {
    local service="$1"
    local started_at="$SECONDS"
    local container health
    while (( SECONDS - started_at < 180 )); do
        container="$(compose ps -q "$service" 2>/dev/null || true)"
        if [[ -n "$container" ]]; then
            health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}missing-healthcheck{{end}}' "$container" 2>/dev/null || true)"
            case "$health" in
                healthy) ok "$service 健康检查通过"; return 0 ;;
                unhealthy|exited|dead) compose logs --tail 100 "$service" >&2 || true; die "$service 健康检查失败：$health" ;;
            esac
        fi
        sleep 2
    done
    compose ps "$service" >&2 || true
    compose logs --tail 100 "$service" >&2 || true
    die "等待 $service 健康检查超时"
}

wait_for_api() {
    local attempt
    for attempt in $(seq 1 45); do
        if compose exec -T api python -c '
import json
from urllib.request import urlopen
payload = json.load(urlopen("http://127.0.0.1:8000/v3api/health", timeout=3))
assert payload.get("status") == "ok", payload
assert payload.get("env") in ("prod", "production"), payload
' >/dev/null 2>&1; then
            ok "API 健康检查通过"
            return 0
        fi
        sleep 2
    done
    compose logs --tail 120 api celery-worker celery-beat >&2 || true
    die "API 未通过健康检查"
}

assert_running() {
    local service="$1"
    compose ps --status running --services | grep -Fx -- "$service" >/dev/null || {
        compose logs --tail 100 "$service" >&2 || true
        die "服务未运行：$service"
    }
}

if [[ "$RUN_MIGRATIONS" == true ]]; then
    [[ "$ALLOW_DATABASE_MIGRATION" == true ]] || die "DEPLOY_RUN_MIGRATIONS=true 时必须同时设置 DEPLOY_ALLOW_DATABASE_MIGRATION=true，并确认已备份数据库"
    info "启动 PostgreSQL 与 Redis"
    compose_up -d --no-build --remove-orphans postgres redis
    wait_for_health postgres
    wait_for_health redis
    info "停止旧 API、worker、beat 后执行 Alembic 迁移"
    compose stop api celery-worker celery-beat >/dev/null 2>&1 || true
    compose_run --rm --no-deps migrate
else
    warn "已跳过数据库迁移（DEPLOY_RUN_MIGRATIONS=false）"
fi

    info "确保 PostgreSQL、Redis 运行（使用已加载镜像，不在服务器构建）"
    compose_up -d --no-build --remove-orphans postgres redis
    info "启动 API、worker、beat（强制重建以加载最新 .env）"
    compose_up -d --no-build --force-recreate --remove-orphans api celery-worker celery-beat
wait_for_health postgres
wait_for_health redis
wait_for_api
assert_running postgres
assert_running redis
assert_running api
assert_running celery-worker
assert_running celery-beat

compose ps postgres redis api celery-worker celery-beat
ok "远程部署完成：$REMOTE_PATH"
REMOTE_BOOT_BODY
    } > "$LOCAL_BOOT"
    chmod 700 "$LOCAL_BOOT"
}

upload_and_deploy() {
    local stamp="$1"
    local remote_code="/tmp/$(basename -- "$CODE_ARCHIVE")"
    local remote_image="/tmp/$(basename -- "$IMAGE_ARCHIVE")"
    local remote_boot="/tmp/$(basename -- "$LOCAL_BOOT")"
    local remote_postgres=""
    local remote_redis=""
    local remote_command

    if [[ -n "$POSTGRES_ARCHIVE" ]]; then
        remote_postgres="/tmp/$(basename -- "$POSTGRES_ARCHIVE")"
    fi
    if [[ -n "$REDIS_ARCHIVE" ]]; then
        remote_redis="/tmp/$(basename -- "$REDIS_ARCHIVE")"
    fi

    prepare_auth
    info "检查远端 Docker 与代码目录权限"
    quote_arg "$DEPLOY_PATH"; remote_path_q="$QUOTED_ARG"
    remote_command="command -v docker >/dev/null && docker compose version >/dev/null && mkdir -p $remote_path_q"
    run_ssh "$remote_command"

    info "上传代码归档"
    run_scp "$CODE_ARCHIVE" "$remote_code"
    info "上传后端镜像归档"
    run_scp "$IMAGE_ARCHIVE" "$remote_image"
    if [[ -n "$POSTGRES_ARCHIVE" ]]; then
        info "上传 PostgreSQL 镜像归档"
        run_scp "$POSTGRES_ARCHIVE" "$remote_postgres"
    fi
    if [[ -n "$REDIS_ARCHIVE" ]]; then
        info "上传 Redis 镜像归档"
        run_scp "$REDIS_ARCHIVE" "$remote_redis"
    fi
    info "上传远端部署启动脚本"
    run_scp "$LOCAL_BOOT" "$remote_boot"
    ok "文件上传完成，开始远端部署"
    quote_arg "$remote_boot"
    run_ssh "bash $QUOTED_ARG"
    ok "部署命令执行完成（时间戳：${stamp}）"
}

remote_compose_action() {
    local action="$1"
    shift
    local args=""
    local arg
    local compose_file="$DEPLOY_PATH/backend/docker-compose.production.yml"
    local prefix
    local remote_command

    prepare_auth
    quote_arg "$DEPLOY_PATH"; local path_q="$QUOTED_ARG"
    quote_arg "$REMOTE_ENV_FILE"; local env_q="$QUOTED_ARG"
    quote_arg "$COMPOSE_PROJECT_NAME"; local project_q="$QUOTED_ARG"
    quote_arg "$compose_file"; local compose_q="$QUOTED_ARG"
    quote_arg "$IMAGE_NAME"; local image_q="$QUOTED_ARG"
    quote_arg "$POSTGRES_IMAGE"; local postgres_image_q="$QUOTED_ARG"
    quote_arg "$REDIS_IMAGE"; local redis_image_q="$QUOTED_ARG"
    prefix="docker compose --project-name $project_q --env-file $env_q -f $compose_q"
    for arg in "$@"; do
        quote_arg "$arg"
        args+=" $QUOTED_ARG"
    done

    case "$action" in
        start|restart)
            remote_command="set -Eeuo pipefail; cd $path_q; test -r $env_q; export MTV_ENV_FILE=$env_q MTV_PRODUCTION_IMAGE=$image_q MTV_POSTGRES_IMAGE=$postgres_image_q MTV_REDIS_IMAGE=$redis_image_q; docker image inspect $image_q >/dev/null 2>&1 || { echo '远端缺少后端镜像，请先执行 deploy' >&2; exit 1; }; docker image inspect $postgres_image_q >/dev/null 2>&1 || { echo '远端缺少 PostgreSQL 镜像，请先执行 deploy 或设置正确的 MTV_POSTGRES_IMAGE' >&2; exit 1; }; docker image inspect $redis_image_q >/dev/null 2>&1 || { echo '远端缺少 Redis 镜像，请先执行 deploy 或设置正确的 MTV_REDIS_IMAGE' >&2; exit 1; }; $prefix config --quiet; $prefix up --pull never -d --no-build --remove-orphans postgres redis; $prefix up --pull never -d --no-build --force-recreate --remove-orphans api celery-worker celery-beat; $prefix ps postgres redis api celery-worker celery-beat"
            ;;
        status)
            remote_command="set -Eeuo pipefail; cd $path_q; test -r $env_q; export MTV_ENV_FILE=$env_q MTV_PRODUCTION_IMAGE=$image_q MTV_POSTGRES_IMAGE=$postgres_image_q MTV_REDIS_IMAGE=$redis_image_q; $prefix config --quiet; $prefix ps postgres redis api celery-worker celery-beat"
            ;;
        stop)
            remote_command="set -Eeuo pipefail; cd $path_q; test -r $env_q; export MTV_ENV_FILE=$env_q MTV_PRODUCTION_IMAGE=$image_q MTV_POSTGRES_IMAGE=$postgres_image_q MTV_REDIS_IMAGE=$redis_image_q; $prefix config --quiet; $prefix stop api celery-worker celery-beat postgres redis"
            ;;
        migrate)
            remote_command="set -Eeuo pipefail; cd $path_q; test -r $env_q; export MTV_ENV_FILE=$env_q MTV_PRODUCTION_IMAGE=$image_q MTV_POSTGRES_IMAGE=$postgres_image_q MTV_REDIS_IMAGE=$redis_image_q; test \"${ALLOW_DATABASE_MIGRATION}\" = true || { echo '请设置 DEPLOY_ALLOW_DATABASE_MIGRATION=true，并确认已完成数据库备份' >&2; exit 1; }; docker image inspect $postgres_image_q >/dev/null 2>&1; docker image inspect $redis_image_q >/dev/null 2>&1; $prefix config --quiet; $prefix up --pull never -d --no-build postgres redis; $prefix run --pull never --rm --no-deps migrate"
            ;;
        logs)
            quote_arg "$LOG_TAIL"; local log_tail_q="$QUOTED_ARG"
            validate_logs_args "$@"
            remote_command="set -Eeuo pipefail; cd $path_q; test -r $env_q; export MTV_ENV_FILE=$env_q MTV_PRODUCTION_IMAGE=$image_q MTV_POSTGRES_IMAGE=$postgres_image_q MTV_REDIS_IMAGE=$redis_image_q; $prefix config --quiet; $prefix logs --tail $log_tail_q$args"
            ;;
        *) die "内部错误：未知远程操作 $action" ;;
    esac
    run_ssh "$remote_command"
}

deploy() {
    local stamp
    stamp="$(date +%Y%m%d%H%M%S)-$$"
    trap cleanup_local_files EXIT
    [[ "$RUN_MIGRATIONS" != true || "$ALLOW_DATABASE_MIGRATION" == true ]] || {
        die "DEPLOY_RUN_MIGRATIONS=true 时必须同时设置 DEPLOY_ALLOW_DATABASE_MIGRATION=true，并确认已备份数据库"
    }
    build_backend_image
    create_archives "$stamp"
    make_boot_script
    upload_and_deploy "$stamp"
}

build_only() {
    build_backend_image
}

parse_options "$@"
resolve_config_file
load_config_file "$CONFIG_FILE"
set_defaults
validate_settings

case "$ACTION" in
    deploy)
        [[ ${#ACTION_ARGS[@]} -eq 0 ]] || die "deploy 不接受额外参数"
        deploy
        ;;
    build)
        [[ ${#ACTION_ARGS[@]} -eq 0 ]] || die "build 不接受额外参数"
        build_only
        ;;
    start|restart|migrate|status|stop)
        [[ ${#ACTION_ARGS[@]} -eq 0 ]] || die "$ACTION 不接受额外参数"
        remote_compose_action "$ACTION"
        ;;
    logs)
        remote_compose_action logs "${ACTION_ARGS[@]}"
        ;;
esac
