#!/usr/bin/env bash

# MTV 单机离线部署统一入口：本地构建一个发布包，服务器在 /opt 直接管理完整服务栈。

set -euo pipefail

umask 077

SCRIPT_PATH="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/$(basename -- "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(dirname -- "$SCRIPT_PATH")"
PACKAGE_PREFIX="mtv-offline"
PACKAGE_FORMAT_VERSION="1"
TARGET_PLATFORM="linux/amd64"
POSTGRES_IMAGE="postgres:16-alpine"
REDIS_IMAGE="redis:7-alpine"
PRODUCTION_PROJECT_NAME="mtv-production"

SERVER_ROOT="${MTV_HOME:-/opt/mtv}"
BUNDLE_DIR="${MTV_BUNDLE_DIR:-$SCRIPT_DIR}"
RELEASES_DIR="$SERVER_ROOT/releases"
CURRENT_LINK="$SERVER_ROOT/current"
ATTEMPTED_LINK="$SERVER_ROOT/attempted"
RUNTIME_ENV_FILE="$SERVER_ROOT/config/production.env"

TEMP_DIR=""
TEMP_FILE=""
TEMP_ARCHIVE=""
INSTALLED_RELEASE=""

ALLOWED_BUSINESS_ENV_KEYS="
CORS_ORIGINS CORS_METHODS CORS_HEADERS
SESSION_COOKIE_NAME SESSION_MAX_AGE SESSION_COOKIE_SECURE TGW_L7_ROUTE_COOKIE
ALIYUN_OSS_ENDPOINT ALIYUN_OSS_ACCESS_KEY_ID ALIYUN_OSS_ACCESS_KEY_SECRET
ALIYUN_OSS_BUCKET ALIYUN_OSS_REGION
OPENAI_API_KEY OPENAI_LLM_MODEL OPENAI_IMAGE_MODEL OPENAI_VIDEO_MODEL
GOOGLE_API_KEY GOOGLE_IMAGE_MODEL GOOGLE_VIDEO_MODEL
VOLCENGINE_ACCESS_KEY VOLCENGINE_SECRET_KEY VOLCENGINE_REGION
VOLCENGINE_API_KEY VOLCENGINE_ARK_BASE_URL SEEDANCE_MODEL VIDEO_DEFAULT_PROVIDER
VOLC_IMAGE_AUDIT_ENABLED VOLC_IMAGEX_ACCESS_KEY VOLC_IMAGEX_SECRET_KEY
VOLC_IMAGEX_SERVICE_ID VOLC_IMAGE_AUDIT_PUBLIC_BASE_URL
TEXT_LLM_PROVIDER TEXT_LLM_BASE_URL TEXT_LLM_API_KEY TEXT_LLM_MODEL
CHAT_LLM_PROVIDER CHAT_LLM_BASE_URL CHAT_LLM_API_KEY CHAT_LLM_MODEL
IMAGE_GEN_PROVIDER IMAGE_GEN_BASE_URL IMAGE_GEN_API_KEY IMAGE_GEN_MODEL
VIDEO_PROVIDER_SELECTION_MODE VIDEO_GEN_PROVIDER VIDEO_GEN_BASE_URL VIDEO_GEN_API_KEY VIDEO_GEN_MODEL
DASHSCOPE_API_KEY DASHSCOPE_BASE_URL DASHSCOPE_VIDEO_MODEL
ANTHROPIC_API_KEY ANTHROPIC_BASE_URL ANTHROPIC_MODEL
DEEPSEEK_API_KEY DEEPSEEK_MODEL GEMINI_API_KEY GEMINI_LLM_MODEL
PRICE_EXTRACT_PER_EPISODE PRICE_MERGE PRICE_ENRICH_PER_SUBJECT PRICE_IMAGE_GEN
PRICE_STORYBOARD_PER_EPISODE PRICE_VIDEO_GEN PRICE_VIDEO_GEN_FROZEN
PIPELINE_EXTRACT_CONCURRENCY PIPELINE_ENRICH_CONCURRENCY
MEDIA_PROXY_PREFIX MAX_UPLOAD_BYTES MAX_MEDIA_READ_BYTES EDITOR_CAN_REVIEW
MTV_PRODUCTION_API_PORT MTV_PRODUCTION_WORKER_CONCURRENCY
MTV_PRODUCTION_MAX_TASKS_PER_CHILD MTV_DEPENDENCY_HEALTH_TIMEOUT_SECONDS
"

cleanup() {
    if [[ -n "$TEMP_FILE" && -e "$TEMP_FILE" ]]; then
        rm -f -- "$TEMP_FILE"
    fi
    if [[ -n "$TEMP_ARCHIVE" && -e "$TEMP_ARCHIVE" ]]; then
        rm -f -- "$TEMP_ARCHIVE"
    fi
    if [[ -n "$TEMP_DIR" && -d "$TEMP_DIR" ]]; then
        rm -rf -- "$TEMP_DIR"
    fi
}
trap cleanup EXIT

die() {
    echo "错误：$*" >&2
    exit 1
}

usage() {
    cat <<'EOF'
用法：
  ./local.sh build [版本]       本地构建 linux/amd64 完整离线包
  ./local.sh start [离线包]    安装最新离线包并启动/更新全部服务
  ./local.sh restart [离线包]  使用最新离线包重新执行安全部署
  ./local.sh status            查看全部服务状态
  ./local.sh stop              停止服务，保留数据库、Redis 与 OSS 数据
  ./local.sh log [行数]        持续查看 API、worker、beat 后端日志

生产默认禁用跨域；跨源前端可通过 MTV_PRODUCTION_CORS_ORIGINS 显式配置。
服务器默认从 local.sh 所在目录选择最新的 mtv-offline-*.tar.gz。
EOF
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "缺少命令：$1"
}

sha256_file() {
    local file="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$file" | awk '{print $1}'
        return
    fi
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$file" | awk '{print $1}'
        return
    fi
    if command -v openssl >/dev/null 2>&1; then
        openssl dgst -sha256 "$file" | awk '{print $NF}'
        return
    fi
    die "缺少 SHA-256 校验工具（sha256sum、shasum 或 openssl）。"
}

read_env_value() {
    local file="$1"
    local requested_key="$2"
    awk -v requested_key="$requested_key" '
        function normalize(line) {
            sub(/^[[:space:]]+/, "", line)
            sub(/^export[[:space:]]+/, "", line)
            return line
        }
        {
            line = normalize($0)
            if (line !~ /^[A-Za-z_][A-Za-z0-9_]*=/) {
                next
            }
            equal_at = index(line, "=")
            key = substr(line, 1, equal_at - 1)
            if (key != requested_key) {
                next
            }
            found++
            value = substr(line, equal_at + 1)
        }
        END {
            if (found > 1) {
                print "环境文件存在重复配置：" requested_key > "/dev/stderr"
                exit 2
            }
            sub(/\r$/, "", value)
            sub(/^[[:space:]]+/, "", value)
            sub(/[[:space:]]+$/, "", value)
            if ((value ~ /^".*"$/) || (value ~ /^\047.*\047$/)) {
                value = substr(value, 2, length(value) - 2)
            }
            print value
        }
    ' "$file"
}

extract_env_keys() {
    local source_file="$1"
    local target_file="$2"
    local allowed_keys="$3"

    allowed_keys="$(printf '%s' "$allowed_keys" | tr '\n' ' ')"

    awk -v allowed_keys="$allowed_keys" '
        BEGIN {
            count = split(allowed_keys, keys, /[[:space:]]+/)
            for (position = 1; position <= count; position++) {
                if (keys[position] != "") {
                    allowed[keys[position]] = 1
                }
            }
        }
        {
            line = $0
            sub(/^[[:space:]]+/, "", line)
            sub(/^export[[:space:]]+/, "", line)
            if (line !~ /^[A-Za-z_][A-Za-z0-9_]*=/) {
                next
            }
            equal_at = index(line, "=")
            key = substr(line, 1, equal_at - 1)
            if (!(key in allowed)) {
                next
            }
            if (key in emitted) {
                print "环境文件存在重复配置：" key > "/dev/stderr"
                failed = 1
                next
            }
            emitted[key] = 1
            print line
        }
        END {
            if (failed) {
                exit 2
            }
        }
    ' "$source_file" > "$target_file"
}

merge_env_file() {
    local target_file="$1"
    local override_file="$2"
    local merged_file="${target_file}.merge.$$"

    awk '
        function assignment_key(line, normalized, equal_at) {
            normalized = line
            sub(/^[[:space:]]+/, "", normalized)
            sub(/^export[[:space:]]+/, "", normalized)
            if (normalized !~ /^[A-Za-z_][A-Za-z0-9_]*=/) {
                return ""
            }
            equal_at = index(normalized, "=")
            return substr(normalized, 1, equal_at - 1)
        }
        FNR == NR {
            key = assignment_key($0)
            if (key != "") {
                if (key in overrides) {
                    print "覆盖配置存在重复项：" key > "/dev/stderr"
                    failed = 1
                    next
                }
                overrides[key] = $0
                order[++override_count] = key
            }
            next
        }
        {
            key = assignment_key($0)
            if (key != "" && key in overrides) {
                if (!(key in emitted)) {
                    print overrides[key]
                    emitted[key] = 1
                }
                next
            }
            print
        }
        END {
            if (failed) {
                exit 2
            }
            for (position = 1; position <= override_count; position++) {
                key = order[position]
                if (!(key in emitted)) {
                    print overrides[key]
                }
            }
        }
    ' "$override_file" "$target_file" > "$merged_file"
    chmod 600 "$merged_file"
    mv -f -- "$merged_file" "$target_file"
}

random_hex_32() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 32
        return
    fi
    require_command od
    od -An -N32 -tx1 /dev/urandom | tr -d ' \n'
}

ensure_local_deploy_secrets() {
    local state_dir="$SCRIPT_DIR/.runtime"
    local secret_file="$state_dir/production-deploy-secrets.env"
    local secret_temp="${secret_file}.tmp.$$"
    local key value

    mkdir -p -m 700 -- "$state_dir"
    if [[ ! -f "$secret_file" ]]; then
        {
            echo "# 请保留此文件；删除后重新生成密码会导致既有 PostgreSQL/Redis 数据无法认证。"
            printf 'SESSION_SECRET=%s\n' "$(random_hex_32)"
            printf 'MTV_POSTGRES_PASSWORD=%s\n' "$(random_hex_32)"
            printf 'MTV_REDIS_PASSWORD=%s\n' "$(random_hex_32)"
        } > "$secret_temp"
        chmod 600 "$secret_temp"
        mv -f -- "$secret_temp" "$secret_file"
        echo "已生成并持久保存生产基础设施密钥：.runtime/production-deploy-secrets.env"
    fi
    chmod 600 "$secret_file"

    for key in SESSION_SECRET MTV_POSTGRES_PASSWORD MTV_REDIS_PASSWORD; do
        value="$(read_env_value "$secret_file" "$key")"
        [[ "$value" =~ ^[0-9a-f]{64}$ ]] || die "本地生产密钥文件中的 $key 无效；为保护既有数据，脚本不会自动覆盖。"
    done
    LOCAL_SECRET_FILE="$secret_file"
}

normalize_cors_value() {
    local value="$1"
    if [[ "$value" == http://* || "$value" == https://* ]]; then
        [[ "$value" != *'"'* && "$value" != *'\\'* && "$value" != *','* ]] || return 1
        value="[\"$value\"]"
    fi
    printf '%s' "$value"
}

cors_needs_replacement() {
    local value="$1"
    [[ -z "$value" || "$value" == *'*'* || "$value" == *'example.com'* || \
        "$value" == *'localhost'* || "$value" == *'127.0.0.1'* ]]
}

select_production_cors() {
    local cors="${MTV_PRODUCTION_CORS_ORIGINS:-[]}"

    [[ "$cors" != *$'\n'* && "$cors" != *$'\r'* ]] || die "CORS_ORIGINS 不能包含换行。"
    cors="$(normalize_cors_value "$cors")" || die "生产前端 Origin 格式无效。"
    case "$cors" in
        \[*\]) ;;
        *) die "MTV_PRODUCTION_CORS_ORIGINS 必须是合法 Origin 或 JSON 数组，Origin 不能包含路径。" ;;
    esac
    cors_needs_replacement "$cors" && die "CORS_ORIGINS 不能使用通配符、本机地址或占位域名。"
    PRODUCTION_CORS="$cors"
}

write_forced_production_env() {
    local target_file="$1"
    local version="$2"
    local backend_image="$3"
    local postgres_password redis_password session_secret session_cookie_secure

    postgres_password="$(read_env_value "$LOCAL_SECRET_FILE" MTV_POSTGRES_PASSWORD)"
    redis_password="$(read_env_value "$LOCAL_SECRET_FILE" MTV_REDIS_PASSWORD)"
    session_secret="$(read_env_value "$LOCAL_SECRET_FILE" SESSION_SECRET)"
    session_cookie_secure="${MTV_SESSION_COOKIE_SECURE:-false}"
    case "$session_cookie_secure" in
        true|false) ;;
        *) die "MTV_SESSION_COOKIE_SECURE 只能是 true 或 false。" ;;
    esac

    {
        echo "# 由 local.sh build 生成；包含真实密钥，禁止提交 Git 或公开传输。"
        echo "APP_ENV=production"
        echo "APP_HOST=0.0.0.0"
        echo "APP_PORT=8000"
        printf 'CORS_ORIGINS=%s\n' "$PRODUCTION_CORS"
        printf 'SESSION_SECRET=%s\n' "$session_secret"
        printf 'SESSION_COOKIE_SECURE=%s\n' "$session_cookie_secure"
        echo "STORAGE_DRIVER=oss"
        echo "STORAGE_ROOT=/var/lib/mtv/storage"
        echo "MTV_ALLOW_LOCAL_STORAGE=false"
        echo "MTV_INSTALL_ALIYUN_OSS=true"
        echo "ALIYUN_OSS_DATA_MIGRATION_CONFIRMED=true"
        echo "ALIYUN_OSS_CANARY_CONFIRMED=true"
        echo "MTV_BOOTSTRAP_ADMIN_USERNAME=admin"
        echo "MTV_BOOTSTRAP_ADMIN_PASSWORD=admin"
        echo "MTV_LEGACY_STORAGE_HOST_PATH=/opt/mtv/data/storage"
        echo "MTV_POSTGRES_USER=mtv_app"
        printf 'MTV_POSTGRES_PASSWORD=%s\n' "$postgres_password"
        echo "MTV_POSTGRES_DB=mtv"
        echo "MTV_POSTGRES_DATA_DIR=/opt/mtv/data/postgres"
        printf 'DATABASE_URL=postgresql+asyncpg://mtv_app:%s@postgres:5432/mtv\n' "$postgres_password"
        printf 'DATABASE_SYNC_URL=postgresql+psycopg2://mtv_app:%s@postgres:5432/mtv\n' "$postgres_password"
        printf 'MTV_REDIS_PASSWORD=%s\n' "$redis_password"
        echo "MTV_REDIS_DATA_DIR=/opt/mtv/data/redis"
        printf 'REDIS_URL=redis://:%s@redis:6379/0\n' "$redis_password"
        printf 'CELERY_BROKER_URL=redis://:%s@redis:6379/1\n' "$redis_password"
        printf 'CELERY_RESULT_BACKEND=redis://:%s@redis:6379/2\n' "$redis_password"
        printf 'MTV_PRODUCTION_IMAGE=%s\n' "$backend_image"
        printf 'MTV_POSTGRES_IMAGE=%s\n' "$POSTGRES_IMAGE"
        printf 'MTV_REDIS_IMAGE=%s\n' "$REDIS_IMAGE"
        printf 'MTV_RELEASE_VERSION=%s\n' "$version"
    } > "$target_file"
    chmod 600 "$target_file"
}

require_env_value() {
    local file="$1"
    local key="$2"
    local value
    value="$(read_env_value "$file" "$key")"
    [[ -n "$value" ]] || die "生产配置缺少非空值：$key"
    case "$value" in
        replace-*|change-me*|changeme*) die "生产配置仍包含占位值：$key" ;;
    esac
}

validate_generated_env_basics() {
    local env_file="$1"
    local key cors

    for key in \
        APP_ENV SESSION_SECRET SESSION_COOKIE_SECURE CORS_ORIGINS STORAGE_DRIVER \
        ALIYUN_OSS_ENDPOINT ALIYUN_OSS_ACCESS_KEY_ID ALIYUN_OSS_ACCESS_KEY_SECRET \
        ALIYUN_OSS_BUCKET ALIYUN_OSS_REGION \
        MTV_BOOTSTRAP_ADMIN_USERNAME MTV_BOOTSTRAP_ADMIN_PASSWORD \
        MTV_POSTGRES_USER MTV_POSTGRES_PASSWORD MTV_POSTGRES_DB \
        DATABASE_URL DATABASE_SYNC_URL MTV_REDIS_PASSWORD \
        REDIS_URL CELERY_BROKER_URL CELERY_RESULT_BACKEND; do
        require_env_value "$env_file" "$key"
    done

    [[ "$(read_env_value "$env_file" APP_ENV)" == "production" ]] || die "build 必须生成 APP_ENV=production。"
    [[ "$(read_env_value "$env_file" STORAGE_DRIVER)" == "oss" ]] || die "build 只允许生成 STORAGE_DRIVER=oss。"
    case "$(read_env_value "$env_file" SESSION_COOKIE_SECURE)" in
        true|false) ;;
        *) die "SESSION_COOKIE_SECURE 必须是 true 或 false。" ;;
    esac
    cors="$(read_env_value "$env_file" CORS_ORIGINS)"
    cors_needs_replacement "$cors" && die "CORS_ORIGINS 不符合生产要求。"

    if awk '
        /^[[:space:]]*(export[[:space:]]+)?(MEIZI_|S3_|MTV_INSTALL_S3)/ { found = 1 }
        END { exit(found ? 0 : 1) }
    ' "$env_file"; then
        die "生成的生产配置仍包含 MeiZi 或旧 S3 配置。"
    fi
}

require_local_build_environment() {
    [[ -f "$SCRIPT_DIR/backend/Dockerfile" ]] || die "build 必须使用 MTV 仓库根目录中的 local.sh。"
    [[ -f "$SCRIPT_DIR/backend/.env" ]] || die "缺少 backend/.env；请先填写 OSS 和业务 Provider 配置。"
    [[ -f "$SCRIPT_DIR/backend/.env.example" ]] || die "缺少 backend/.env.example。"
    [[ -f "$SCRIPT_DIR/backend/docker-compose.production.yml" ]] || die "缺少生产 Compose 文件。"
    [[ -f "$SCRIPT_DIR/scripts/deploy-production.sh" ]] || die "缺少底层生产部署脚本。"

    require_command docker
    docker info >/dev/null 2>&1 || die "Docker 守护进程不可用。"
    docker compose version >/dev/null 2>&1 || die "缺少 Docker Compose v2。"
    docker buildx version >/dev/null 2>&1 || die "缺少 docker buildx。"
}

validate_image_platform() {
    local image="$1"
    local platform
    platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image" 2>/dev/null || true)"
    [[ "$platform" == "$TARGET_PLATFORM" ]] || die "镜像平台错误：$image 为 ${platform:-未知}，要求 ${TARGET_PLATFORM}。"
}

validate_backend_image_config() {
    local image="$1"
    local env_file="$2"

    docker run --rm --platform "$TARGET_PLATFORM" --env-file "$env_file" \
        --entrypoint python "$image" -c '
import sys

try:
    from pydantic import ValidationError
    from pydantic_settings import SettingsError
    from app.core.config import settings
    from app.providers.storage import get_storage

    assert settings.is_prod
    assert settings.storage_driver == "oss"
    get_storage()
    import alibabacloud_oss_v2  # noqa: F401
except SettingsError as exc:
    print(f"生产配置解析失败：{exc}", file=sys.stderr)
    raise SystemExit(1) from None
except ValidationError as exc:
    for error in exc.errors(include_input=False, include_url=False):
        location = ".".join(str(part) for part in error.get("loc", ())) or "Settings"
        message = error.get("msg", "配置无效")
        print(f"生产配置错误 [{location}]：{message}", file=sys.stderr)
    raise SystemExit(1) from None
except Exception as exc:
    print(f"生产镜像配置校验失败：{type(exc).__name__}", file=sys.stderr)
    raise SystemExit(1) from None

print("生产镜像配置与阿里云 OSS SDK 校验通过")
'
}

write_manifest() {
    local release_root="$1"
    local manifest="$release_root/manifest.sha256"
    local relative_path digest
    local files=(
        "manifest.env"
        "config/production.env"
        "backend/docker-compose.production.yml"
        "scripts/deploy-production.sh"
        "images/backend.tar"
        "images/postgres.tar"
        "images/redis.tar"
    )

    : > "$manifest"
    for relative_path in "${files[@]}"; do
        digest="$(sha256_file "$release_root/$relative_path")"
        printf '%s  %s\n' "$digest" "$relative_path" >> "$manifest"
    done
    chmod 600 "$manifest"
}

build_bundle() {
    local requested_version="${1:-${MTV_RELEASE_VERSION:-}}"
    local version backend_image dist_dir release_name release_root archive git_commit git_dirty built_at
    local example_env local_env forced_env production_env compose_file deploy_script

    [[ $# -le 1 ]] || die "build 最多接受一个版本参数。"
    require_local_build_environment

    version="$requested_version"
    if [[ -z "$version" ]]; then
        version="$(date '+%Y%m%d-%H%M%S')"
    fi
    [[ "$version" =~ ^[a-z0-9][a-z0-9._-]*$ ]] || die "版本只能包含小写字母、数字、点、下划线和连字符。"

    backend_image="mtv-backend:$version"
    dist_dir="$SCRIPT_DIR/dist"
    release_name="$PACKAGE_PREFIX-$version"
    archive="$dist_dir/$release_name.tar.gz"
    [[ ! -e "$archive" ]] || die "离线包已存在，请使用新版本或先自行归档旧文件：$archive"

    mkdir -p -m 700 -- "$dist_dir"
    TEMP_DIR="$(mktemp -d "$dist_dir/.build.XXXXXX")"
    release_root="$TEMP_DIR/$release_name"
    mkdir -p -m 700 \
        "$release_root/config" "$release_root/backend" "$release_root/scripts" "$release_root/images"

    example_env="$TEMP_DIR/example.env"
    local_env="$TEMP_DIR/local.env"
    forced_env="$TEMP_DIR/forced.env"
    production_env="$release_root/config/production.env"
    compose_file="$release_root/backend/docker-compose.production.yml"
    deploy_script="$release_root/scripts/deploy-production.sh"

    extract_env_keys "$SCRIPT_DIR/backend/.env.example" "$example_env" "$ALLOWED_BUSINESS_ENV_KEYS"
    extract_env_keys "$SCRIPT_DIR/backend/.env" "$local_env" "$ALLOWED_BUSINESS_ENV_KEYS"
    cp -- "$example_env" "$production_env"
    merge_env_file "$production_env" "$local_env"

    ensure_local_deploy_secrets
    select_production_cors
    write_forced_production_env "$forced_env" "$version" "$backend_image"
    merge_env_file "$production_env" "$forced_env"
    chmod 600 "$production_env"
    validate_generated_env_basics "$production_env"

    echo "构建后端镜像：$backend_image ($TARGET_PLATFORM)"
    docker buildx build \
        --platform "$TARGET_PLATFORM" \
        --build-arg INSTALL_ALIYUN_OSS=true \
        --tag "$backend_image" \
        --load \
        "$SCRIPT_DIR/backend"
    validate_image_platform "$backend_image"
    validate_backend_image_config "$backend_image" "$production_env"

    echo "准备 PostgreSQL 与 Redis 离线镜像 ($TARGET_PLATFORM)"
    docker pull --platform "$TARGET_PLATFORM" "$POSTGRES_IMAGE"
    docker pull --platform "$TARGET_PLATFORM" "$REDIS_IMAGE"
    validate_image_platform "$POSTGRES_IMAGE"
    validate_image_platform "$REDIS_IMAGE"

    cp -- "$SCRIPT_DIR/backend/docker-compose.production.yml" "$compose_file"
    cp -- "$SCRIPT_DIR/scripts/deploy-production.sh" "$deploy_script"
    chmod 600 "$compose_file"
    chmod 700 "$deploy_script"

    MTV_ENV_FILE="$production_env" docker compose \
        --project-name "$PRODUCTION_PROJECT_NAME" \
        --env-file "$production_env" \
        -f "$compose_file" config --quiet

    echo "导出三个离线镜像..."
    docker save --output "$release_root/images/backend.tar" "$backend_image"
    docker save --output "$release_root/images/postgres.tar" "$POSTGRES_IMAGE"
    docker save --output "$release_root/images/redis.tar" "$REDIS_IMAGE"
    chmod 600 "$release_root/images/"*.tar

    git_commit="$(git -C "$SCRIPT_DIR" rev-parse --short=12 HEAD 2>/dev/null || echo unknown)"
    if [[ -n "$(git -C "$SCRIPT_DIR" status --short 2>/dev/null || true)" ]]; then
        git_dirty="true"
    else
        git_dirty="false"
    fi
    built_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    {
        printf 'PACKAGE_FORMAT_VERSION=%s\n' "$PACKAGE_FORMAT_VERSION"
        printf 'RELEASE_VERSION=%s\n' "$version"
        printf 'TARGET_PLATFORM=%s\n' "$TARGET_PLATFORM"
        printf 'BACKEND_IMAGE=%s\n' "$backend_image"
        printf 'POSTGRES_IMAGE=%s\n' "$POSTGRES_IMAGE"
        printf 'REDIS_IMAGE=%s\n' "$REDIS_IMAGE"
        printf 'GIT_COMMIT=%s\n' "$git_commit"
        printf 'GIT_DIRTY=%s\n' "$git_dirty"
        printf 'BUILT_AT=%s\n' "$built_at"
    } > "$release_root/manifest.env"
    chmod 600 "$release_root/manifest.env"
    write_manifest "$release_root"

    TEMP_ARCHIVE="$dist_dir/.${release_name}.tar.gz.tmp.$$"
    COPYFILE_DISABLE=1 tar --no-xattrs -czf "$TEMP_ARCHIVE" -C "$TEMP_DIR" "$release_name"
    chmod 600 "$TEMP_ARCHIVE"
    mv -f -- "$TEMP_ARCHIVE" "$archive"
    TEMP_ARCHIVE=""

    cp -- "$SCRIPT_PATH" "$dist_dir/local.sh"
    chmod 700 "$dist_dir/local.sh"

    echo
    echo "构建完成："
    echo "  离线包：$archive"
    echo "  部署脚本：$dist_dir/local.sh"
    echo "只需将这两个文件上传到服务器 /opt，然后执行：sudo /opt/local.sh start"
}

require_server_privileges() {
    local action="$1"
    shift
    if [[ "$SERVER_ROOT" == /opt/* && "${EUID:-$(id -u)}" -ne 0 ]]; then
        require_command sudo
        exec sudo bash "$SCRIPT_PATH" "$action" "$@"
    fi
}

require_server_environment() {
    local architecture
    [[ "$SERVER_ROOT" == /* && "$SERVER_ROOT" != "/" ]] || die "MTV_HOME 必须是非根目录的绝对路径。"
    [[ "$(uname -s)" == "Linux" ]] || die "start/restart/status/stop/log 只能在 Linux 服务器执行。"
    architecture="$(uname -m)"
    case "$architecture" in
        x86_64|amd64) ;;
        *) die "当前服务器架构为 ${architecture}，本离线包仅支持 linux/amd64。" ;;
    esac
    require_command docker
    require_command tar
    require_command cmp
    require_command find
    require_command awk
    require_command stat
    require_command readlink
    docker info >/dev/null 2>&1 || die "Docker 守护进程不可用，或当前用户无权限访问。"
    docker compose version >/dev/null 2>&1 || die "缺少 Docker Compose v2。"
}

select_latest_bundle() {
    local explicit_bundle="${1:-${MTV_BUNDLE:-}}"
    local candidate latest="" latest_mtime="0" candidate_mtime basename_value

    if [[ -n "$explicit_bundle" ]]; then
        candidate="$explicit_bundle"
        [[ "$candidate" == /* ]] || candidate="$PWD/$candidate"
        [[ -f "$candidate" && -r "$candidate" ]] || die "离线包不可读：$candidate"
        SELECTED_BUNDLE="$candidate"
    else
        for candidate in "$BUNDLE_DIR"/$PACKAGE_PREFIX-*.tar.gz; do
            [[ -f "$candidate" && -r "$candidate" ]] || continue
            candidate_mtime="$(stat -c '%Y' "$candidate" 2>/dev/null || echo 0)"
            if [[ "$candidate_mtime" -ge "$latest_mtime" ]]; then
                latest="$candidate"
                latest_mtime="$candidate_mtime"
            fi
        done
        [[ -n "$latest" ]] || return 1
        SELECTED_BUNDLE="$latest"
    fi

    basename_value="$(basename -- "$SELECTED_BUNDLE")"
    [[ "$basename_value" =~ ^mtv-offline-[a-z0-9][a-z0-9._-]*\.tar\.gz$ ]] || die "离线包文件名不符合约定：$basename_value"
    return 0
}

write_expected_archive_members() {
    local root_name="$1"
    local target_file="$2"
    {
        echo "$root_name/"
        echo "$root_name/backend/"
        echo "$root_name/backend/docker-compose.production.yml"
        echo "$root_name/config/"
        echo "$root_name/config/production.env"
        echo "$root_name/images/"
        echo "$root_name/images/backend.tar"
        echo "$root_name/images/postgres.tar"
        echo "$root_name/images/redis.tar"
        echo "$root_name/manifest.env"
        echo "$root_name/manifest.sha256"
        echo "$root_name/scripts/"
        echo "$root_name/scripts/deploy-production.sh"
    } | LC_ALL=C sort > "$target_file"
}

validate_archive_layout() {
    local bundle="$1"
    local work_dir="$2"
    local entries="$work_dir/archive.entries"
    local sorted_entries="$work_dir/archive.entries.sorted"
    local verbose_entries="$work_dir/archive.verbose"
    local expected_entries="$work_dir/archive.expected"
    local root_name first_character line_count bundle_version

    tar -tzf "$bundle" > "$entries" || die "无法读取离线包目录。"
    [[ -s "$entries" ]] || die "离线包为空。"
    root_name="$(awk -F/ 'NR == 1 { print $1 }' "$entries")"
    [[ "$root_name" =~ ^mtv-offline-[a-z0-9][a-z0-9._-]*$ ]] || die "离线包顶层目录不合法。"
    bundle_version="$(basename -- "$bundle")"
    bundle_version="${bundle_version#$PACKAGE_PREFIX-}"
    bundle_version="${bundle_version%.tar.gz}"
    [[ "$root_name" == "$PACKAGE_PREFIX-$bundle_version" ]] || die "离线包文件名与内部版本不一致。"

    LC_ALL=C sort "$entries" > "$sorted_entries"
    write_expected_archive_members "$root_name" "$expected_entries"
    cmp -s "$sorted_entries" "$expected_entries" || die "离线包包含缺失、重复或未授权成员。"

    tar -tvzf "$bundle" > "$verbose_entries" || die "无法检查离线包成员类型。"
    line_count="0"
    while IFS= read -r line; do
        first_character="${line:0:1}"
        case "$first_character" in
            d|-) ;;
            *) die "离线包包含链接或特殊文件，已拒绝解压。" ;;
        esac
        line_count=$((line_count + 1))
    done < "$verbose_entries"
    [[ "$line_count" -eq 13 ]] || die "离线包成员数量异常。"

    ARCHIVE_ROOT_NAME="$root_name"
    ARCHIVE_VERSION="${root_name#$PACKAGE_PREFIX-}"
}

verify_release_manifest() {
    local release_dir="$1"
    local manifest="$release_dir/manifest.sha256"
    local relative_path expected actual count
    local files=(
        "manifest.env"
        "config/production.env"
        "backend/docker-compose.production.yml"
        "scripts/deploy-production.sh"
        "images/backend.tar"
        "images/postgres.tar"
        "images/redis.tar"
    )

    [[ -f "$manifest" && -r "$manifest" ]] || die "release 缺少 manifest.sha256。"
    count="$(awk 'NF { count++ } END { print count + 0 }' "$manifest")"
    [[ "$count" -eq 7 ]] || die "manifest.sha256 条目数量异常。"
    for relative_path in "${files[@]}"; do
        expected="$(awk -v path="$relative_path" '$2 == path { value = $1; count++ } END { if (count != 1) exit 2; print value }' "$manifest")" \
            || die "manifest.sha256 缺少唯一条目：$relative_path"
        [[ "$expected" =~ ^[0-9a-f]{64}$ ]] || die "manifest.sha256 摘要格式无效：$relative_path"
        [[ -f "$release_dir/$relative_path" && ! -L "$release_dir/$relative_path" ]] || die "release 文件无效：$relative_path"
        actual="$(sha256_file "$release_dir/$relative_path")"
        [[ "$actual" == "$expected" ]] || die "release 文件校验失败：$relative_path"
    done
}

validate_release_metadata() {
    local release_dir="$1"
    local expected_version="$2"
    local manifest="$release_dir/manifest.env"
    local format version platform backend_image postgres_image redis_image

    format="$(read_env_value "$manifest" PACKAGE_FORMAT_VERSION)"
    version="$(read_env_value "$manifest" RELEASE_VERSION)"
    platform="$(read_env_value "$manifest" TARGET_PLATFORM)"
    backend_image="$(read_env_value "$manifest" BACKEND_IMAGE)"
    postgres_image="$(read_env_value "$manifest" POSTGRES_IMAGE)"
    redis_image="$(read_env_value "$manifest" REDIS_IMAGE)"

    [[ "$format" == "$PACKAGE_FORMAT_VERSION" ]] || die "不支持的离线包格式版本：$format"
    [[ "$version" == "$expected_version" ]] || die "manifest release 版本不一致。"
    [[ "$platform" == "$TARGET_PLATFORM" ]] || die "离线包目标平台不是 ${TARGET_PLATFORM}。"
    [[ "$backend_image" == "mtv-backend:$version" ]] || die "后端镜像标签与 release 版本不一致。"
    [[ "$postgres_image" == "$POSTGRES_IMAGE" ]] || die "PostgreSQL 镜像标签不符合约定。"
    [[ "$redis_image" == "$REDIS_IMAGE" ]] || die "Redis 镜像标签不符合约定。"
}

install_bundle() {
    local bundle="$1"
    local extraction_dir candidate_release destination existing_manifest candidate_manifest

    mkdir -p -m 700 -- "$RELEASES_DIR"
    TEMP_DIR="$(mktemp -d "$RELEASES_DIR/.incoming.XXXXXX")"
    validate_archive_layout "$bundle" "$TEMP_DIR"
    extraction_dir="$TEMP_DIR/extracted"
    mkdir -p -m 700 -- "$extraction_dir"
    tar --no-same-owner --no-same-permissions -xzf "$bundle" -C "$extraction_dir"
    candidate_release="$extraction_dir/$ARCHIVE_ROOT_NAME"

    [[ -d "$candidate_release" && ! -L "$candidate_release" ]] || die "离线包解压结果无效。"
    [[ -z "$(find "$candidate_release" -type l -print -quit)" ]] || die "release 中不允许符号链接。"
    verify_release_manifest "$candidate_release"
    validate_release_metadata "$candidate_release" "$ARCHIVE_VERSION"
    validate_generated_env_basics "$candidate_release/config/production.env"

    destination="$RELEASES_DIR/$ARCHIVE_VERSION"
    if [[ -e "$destination" || -L "$destination" ]]; then
        [[ -d "$destination" && ! -L "$destination" ]] || die "同名 release 路径不是普通目录：$destination"
        verify_release_manifest "$destination"
        existing_manifest="$(sha256_file "$destination/manifest.sha256")"
        candidate_manifest="$(sha256_file "$candidate_release/manifest.sha256")"
        [[ "$existing_manifest" == "$candidate_manifest" ]] || die "同版本 release 已存在但内容不同，请重新 build 新版本。"
        echo "release 已安装且校验一致，直接复用：$ARCHIVE_VERSION"
    else
        chmod -R go-rwx "$candidate_release"
        chmod 700 "$candidate_release/scripts/deploy-production.sh"
        mv -- "$candidate_release" "$destination"
        echo "已安装 release：$ARCHIVE_VERSION"
    fi

    INSTALLED_RELEASE="$destination"
    rm -rf -- "$(dirname -- "$extraction_dir")"
    TEMP_DIR=""
}

atomic_symlink() {
    local target="$1"
    local link_path="$2"
    local temporary_link="${link_path}.tmp.$$"

    if [[ -e "$link_path" && ! -L "$link_path" ]]; then
        die "release 指针被非符号链接占用：$link_path"
    fi
    rm -f -- "$temporary_link"
    ln -s "$target" "$temporary_link"
    mv -f -- "$temporary_link" "$link_path"
}

resolve_release_link() {
    local link_path="$1"
    local target
    [[ -L "$link_path" ]] || return 1
    target="$(readlink "$link_path")"
    [[ "$target" == "$RELEASES_DIR"/* && -d "$target" ]] || die "release 指针目标无效：$link_path"
    RESOLVED_RELEASE="$target"
}

resolve_active_release() {
    if resolve_release_link "$CURRENT_LINK"; then
        return
    fi
    if resolve_release_link "$ATTEMPTED_LINK"; then
        echo "尚无成功 release，使用最近一次部署尝试进行诊断。" >&2
        return
    fi
    die "服务器尚未安装 release，请先执行：$SCRIPT_PATH start"
}

prepare_data_directories() {
    mkdir -p -m 750 \
        "$SERVER_ROOT/data/postgres" \
        "$SERVER_ROOT/data/redis" \
        "$SERVER_ROOT/data/storage"
}

prepare_runtime_env() {
    local release_env="$1"
    local config_dir="$SERVER_ROOT/config"
    local selected="$config_dir/.stable-values.$$"
    local old_runtime_exists="false"
    local stable_keys=""
    local key existing_session_secret

    mkdir -p -m 700 -- "$config_dir"
    TEMP_FILE="$config_dir/.production.env.tmp.$$"
    cp -- "$release_env" "$TEMP_FILE"
    chmod 600 "$TEMP_FILE"

    if [[ ! -f "$RUNTIME_ENV_FILE" && -f "$SERVER_ROOT/data/postgres/PG_VERSION" ]]; then
        die "检测到既有 PostgreSQL 数据，但缺少 ${RUNTIME_ENV_FILE}，无法确认原数据库凭据。"
    fi
    if [[ ! -f "$RUNTIME_ENV_FILE" && \
        -n "$(find "$SERVER_ROOT/data/redis" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
        die "检测到既有 Redis 数据，但缺少 ${RUNTIME_ENV_FILE}，无法确认原 Redis 凭据。"
    fi

    if [[ -f "$RUNTIME_ENV_FILE" ]]; then
        old_runtime_exists="true"
        existing_session_secret="$(read_env_value "$RUNTIME_ENV_FILE" SESSION_SECRET)"
        if [[ "$existing_session_secret" =~ ^[0-9a-f]{64}$ ]]; then
            stable_keys="SESSION_SECRET"
        else
            echo "旧 SESSION_SECRET 不是有效强随机密钥，改用新发布包中的密钥。"
        fi
        if [[ -f "$SERVER_ROOT/data/postgres/PG_VERSION" ]]; then
            for key in MTV_POSTGRES_USER MTV_POSTGRES_PASSWORD MTV_POSTGRES_DB DATABASE_URL DATABASE_SYNC_URL; do
                require_env_value "$RUNTIME_ENV_FILE" "$key"
            done
            stable_keys="$stable_keys MTV_POSTGRES_USER MTV_POSTGRES_PASSWORD MTV_POSTGRES_DB DATABASE_URL DATABASE_SYNC_URL"
        fi
        if [[ -n "$(find "$SERVER_ROOT/data/redis" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
            for key in MTV_REDIS_PASSWORD REDIS_URL CELERY_BROKER_URL CELERY_RESULT_BACKEND; do
                require_env_value "$RUNTIME_ENV_FILE" "$key"
            done
            stable_keys="$stable_keys MTV_REDIS_PASSWORD REDIS_URL CELERY_BROKER_URL CELERY_RESULT_BACKEND"
        fi
        extract_env_keys "$RUNTIME_ENV_FILE" "$selected" "$stable_keys"
        merge_env_file "$TEMP_FILE" "$selected"
        rm -f -- "$selected"
    fi

    validate_generated_env_basics "$TEMP_FILE"
    mv -f -- "$TEMP_FILE" "$RUNTIME_ENV_FILE"
    TEMP_FILE=""
    chmod 600 "$RUNTIME_ENV_FILE"

    if [[ "$old_runtime_exists" == "true" ]]; then
        echo "已更新生产配置，并保留可安全复用的既有基础设施凭据。"
    else
        echo "已安装生产配置：$RUNTIME_ENV_FILE"
    fi
}

run_release_action() {
    local release_dir="$1"
    local action="$2"
    local manifest="$release_dir/manifest.env"
    local backend_image postgres_image redis_image

    verify_release_manifest "$release_dir"
    backend_image="$(read_env_value "$manifest" BACKEND_IMAGE)"
    postgres_image="$(read_env_value "$manifest" POSTGRES_IMAGE)"
    redis_image="$(read_env_value "$manifest" REDIS_IMAGE)"

    env \
        MTV_USE_PREBUILT_IMAGE=true \
        MTV_IMAGE_ARCHIVE="$release_dir/images/backend.tar" \
        MTV_PRODUCTION_IMAGE="$backend_image" \
        MTV_POSTGRES_IMAGE_ARCHIVE="$release_dir/images/postgres.tar" \
        MTV_POSTGRES_IMAGE="$postgres_image" \
        MTV_REDIS_IMAGE_ARCHIVE="$release_dir/images/redis.tar" \
        MTV_REDIS_IMAGE="$redis_image" \
        MTV_PRODUCTION_ENV_FILE="$RUNTIME_ENV_FILE" \
        MTV_PRODUCTION_PROJECT_NAME="$PRODUCTION_PROJECT_NAME" \
        "$release_dir/scripts/deploy-production.sh" "$action"
}

deploy_server() {
    local requested_bundle="${1:-}"
    local release_dir api_port
    [[ $# -le 1 ]] || die "start/restart 最多接受一个离线包路径。"

    require_server_environment
    mkdir -p -m 700 -- "$SERVER_ROOT"
    prepare_data_directories

    if select_latest_bundle "$requested_bundle"; then
        echo "使用离线包：$SELECTED_BUNDLE"
        install_bundle "$SELECTED_BUNDLE"
        release_dir="$INSTALLED_RELEASE"
    else
        resolve_active_release
        release_dir="$RESOLVED_RELEASE"
        echo "未发现新离线包，复用已安装 release：$(basename -- "$release_dir")"
    fi

    prepare_runtime_env "$release_dir/config/production.env"
    atomic_symlink "$release_dir" "$ATTEMPTED_LINK"
    echo "开始部署 release：$(basename -- "$release_dir")"
    run_release_action "$release_dir" deploy
    atomic_symlink "$release_dir" "$CURRENT_LINK"
    api_port="$(read_env_value "$RUNTIME_ENV_FILE" MTV_PRODUCTION_API_PORT)"
    echo "当前生效 release：$(basename -- "$release_dir")"
    echo "本机健康检查：http://127.0.0.1:${api_port:-8000}/v3api/health"
}

show_status() {
    require_server_environment
    resolve_active_release
    echo "当前 release：$(basename -- "$RESOLVED_RELEASE")"
    run_release_action "$RESOLVED_RELEASE" status
}

stop_server() {
    require_server_environment
    resolve_active_release
    run_release_action "$RESOLVED_RELEASE" stop
}

follow_backend_logs() {
    local tail_lines="${1:-${MTV_LOG_TAIL:-200}}"
    [[ $# -le 1 ]] || die "log 最多接受一个日志行数。"
    [[ "$tail_lines" =~ ^[1-9][0-9]*$ ]] || die "日志行数必须是正整数。"
    require_server_environment
    resolve_active_release
    [[ -f "$RUNTIME_ENV_FILE" ]] || die "生产配置不存在：$RUNTIME_ENV_FILE"

    echo "持续查看后端日志，按 Ctrl+C 退出。release：$(basename -- "$RESOLVED_RELEASE")"
    MTV_ENV_FILE="$RUNTIME_ENV_FILE" docker compose \
        --project-name "$PRODUCTION_PROJECT_NAME" \
        --env-file "$RUNTIME_ENV_FILE" \
        -f "$RESOLVED_RELEASE/backend/docker-compose.production.yml" \
        logs --follow --tail "$tail_lines" api celery-worker celery-beat
}

main() {
    local action="${1:-help}"
    if [[ $# -gt 0 ]]; then
        shift
    fi

    case "$action" in
        build)
            build_bundle "$@"
            ;;
        start|restart)
            require_server_privileges "$action" "$@"
            deploy_server "$@"
            ;;
        status)
            [[ $# -eq 0 ]] || die "status 不接受额外参数。"
            require_server_privileges "$action"
            show_status
            ;;
        stop)
            [[ $# -eq 0 ]] || die "stop 不接受额外参数。"
            require_server_privileges "$action"
            stop_server
            ;;
        log|logs)
            require_server_privileges "$action" "$@"
            follow_backend_logs "$@"
            ;;
        help|-h|--help)
            usage
            ;;
        *)
            usage >&2
            exit 2
            ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
