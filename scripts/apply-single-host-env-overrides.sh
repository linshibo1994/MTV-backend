#!/usr/bin/env bash

# 将单机基础设施和存储覆盖配置安全合并到已有生产环境文件，保留现有 CORS、Provider 密钥和其他配置。

set -euo pipefail

TARGET_ENV_FILE="${1:-}"
OVERRIDE_ENV_FILE="${2:-}"

usage() {
    echo "用法：$0 <现有 production.env> <single-host-overrides.env>" >&2
    exit 2
}

[[ -n "$TARGET_ENV_FILE" && -n "$OVERRIDE_ENV_FILE" ]] || usage
[[ -f "$TARGET_ENV_FILE" && -r "$TARGET_ENV_FILE" && -w "$TARGET_ENV_FILE" ]] || {
    echo "目标环境文件不可读写：$TARGET_ENV_FILE" >&2
    exit 1
}
[[ -f "$OVERRIDE_ENV_FILE" && -r "$OVERRIDE_ENV_FILE" ]] || {
    echo "覆盖配置文件不可读：$OVERRIDE_ENV_FILE" >&2
    exit 1
}

umask 077
TEMP_ENV_FILE="$(mktemp "${TARGET_ENV_FILE}.tmp.XXXXXX")"
cleanup() {
    [[ -e "$TEMP_ENV_FILE" ]] && rm -f -- "$TEMP_ENV_FILE"
}
trap cleanup EXIT

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
            print "覆盖配置中存在重复项：" key > "/dev/stderr"
            exit 2
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
    for (position = 1; position <= override_count; position++) {
        key = order[position]
        if (!(key in emitted)) {
            print overrides[key]
        }
    }
}
' "$OVERRIDE_ENV_FILE" "$TARGET_ENV_FILE" > "$TEMP_ENV_FILE"

chmod 600 "$TEMP_ENV_FILE"
mv -f -- "$TEMP_ENV_FILE" "$TARGET_ENV_FILE"
trap - EXIT

echo "单机部署覆盖配置已合并；现有 CORS、Provider 密钥和其他配置已保留。"
