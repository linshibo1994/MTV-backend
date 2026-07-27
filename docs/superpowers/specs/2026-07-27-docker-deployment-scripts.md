# Docker 测试与生产部署脚本说明

## 目标

新增两份可重复执行的服务器部署脚本：测试脚本使用 Docker 内 PostgreSQL、Redis 与宿主机本地媒体目录；生产脚本使用服务器既有的外部 PostgreSQL、Redis、S3 配置。

## 约束

- 不读取、复制或提交真实 `.env`、数据库密码、S3 凭据或 AI 密钥。
- 测试部署可以使用非敏感默认值，但允许服务器受保护的环境文件覆盖。
- 生产部署只读取服务器 `/etc/mtv/production.env`（或显式指定的文件），不创建或覆盖它。
- API、worker、beat 使用同一镜像和同一环境文件；迁移必须单独、串行执行；beat 仅一实例。
- 保留本地 `start.sh` 和原开发 Compose 的行为。

## 验收标准

- 两个脚本均完成 Docker 可用性、配置校验、构建、迁移、启动与 HTTP 健康检查。
- 测试 Compose 包含 API、PostgreSQL、Redis、worker、beat 和持久化本地媒体卷。
- 生产 Compose 不创建 PostgreSQL/Redis，不挂载本地媒体，且明确要求 `APP_ENV=prod` 与 S3 配置。
- 脚本可通过 shell 静态检查；Compose 可在不访问真实依赖时完成配置展开与语法验证。
