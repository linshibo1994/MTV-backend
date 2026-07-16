# 后端功能核验 Handoff

## 目标与边界

- 仅核验并补全 `backend/`，不验证或修改前端。
- 需求基线：`01` 至 `09`、`TASK.md`、`PLAN.md` 及 `exploration/` 全部文档和截图。
- 结论必须区分 E1/E2 兼容事实、D 级推荐设计与 U 级待验证项。
- 不读取 `.env*` 或密钥；不调用真实收费 AI；不自动推送远端。

## 任务开始基线

- 仓库：`/Users/linshibo/GithubProject/MTV`
- 分支：`main`
- 任务开始前已有大量已跟踪文档删除，且 `backend/`、`docs/Claude/` 整体未跟踪；均视为用户现有改动，不回滚、不混入无关提交。
- 已在会话内保存任务开始时的 Git 状态和非敏感文件 SHA-256 清单，用于最终识别本次增量。
- 相关后端源文件约 17,058 行；任务开始时仅发现 `tests/test_auth.py` 一组测试。

## 已完成

- 已调用 Sequential Thinking 收敛后端验收 workflow。
- 已创建主 `/goal` 与六阶段执行计划。
- 已完整阅读 `docs/Claude/` 下所有 Markdown、探索证据和截图；最后单独阅读 `TASK.md`、`PLAN.md`。
- 第一批 CLI 审计曾按复杂度选择 `gpt-5.6-terra`（T0/T1）与
  `gpt-5.6-sol`（T2/T3）；因内部代理连接故障未形成报告，随后由三个
  原生干净上下文代理完成独立核验。
- Python 3.12 基线测试：`42 passed, 34 failed`；34 个失败均在 `tests/test_auth.py`。
- 静态基线：Ruff 83 项，Mypy 37 项；应用可导入，OpenAPI 共 53 条路径。
- 第一批独立核验已完成：
  - T0/T1：`DONE_WITH_CONCERNS`。31 个失败为组织成员查询新增后测试夹具过期；3 个失败直接暴露拒绝审计绕过依赖注入、可能将预期 403 覆盖为 500。另确认生产默认会话密钥、活动租户未贯穿、3 处跨资源授权风险及无 Alembic revision。
  - T2：`DONE_WITH_CONCERNS`。确认锁查询未强制刷新 identity map、结算失败仍写业务成功两个 P0；部门下发、禁用账户清算、任务投递补偿等存在 P1。
  - T3：`BLOCKED`。确认 `celery_app.chain/group` 不存在、ParentSubject 外键与写入互斥、旧 split 任务可覆盖新文本三个 P0；另有输出契约、attempt 并发和迁移缺口。
- 第二批独立核验已完成：
  - T4：无 P0，确认首次审核并发覆盖、ensure 竞态、上传候选未切 pending、改名后替换影响漏报和缺迁移等 P1。
  - T5：无 P0，确认 ensure 竞态、可绕过/秒级画布版本、节点 data 未规范化、孤儿边和缺迁移/测试。
  - T10：不通过。确认本地存储路径逃逸、伪造图片扩展名绕过 MIME、视频/抽帧 key 无法授权、上传失败/候选删除遗留对象、同步大文件探测及 StorageProvider 缺 signed_url 等问题。
- 第三批独立核验已完成：
  - T6：不通过。`gpt-5.6-sol` 因容量不足未启动，降级 `gpt-5.6-terra` 完成；确认生图 worker 未加入 Celery include 的 P0，以及幂等/取消、任务领取、prompt 来源和返回数量计费缺口。
  - T7：无 P0；确认 check-materials IDOR、异步任务未绑定输入版本、并发重复投递、无积分账户仍调用 LLM、输出模板未校验等 P1/P2。
  - T8：无 P0；确认动态 `{job_id}` 路由遮蔽两个静态 GET、结算失败仍成功、视频 key 不可播放、投递失败冻结、并发幂等与取消竞争等 P1/P2。
  - T9：不通过。确认渲染 worker 调用不存在的 storage 工厂及同步 API 的 P0；另有关联完整性、row_order 并发、取消授权、trim 参数和 Redis-only job 缺口。
- 首次 CLI 审计因代理内部再次拆分及 MCP 连接故障长时间等待，已安全终止；重启时禁用嵌套代理。三路并发使用 `--ignore-user-config` 时模型路由超时，随后改为保留 custom provider、关闭 `multi_agent/apps/remote_plugin`，单代理连接恢复。

## 当前状态

- T0-T10、全端点机械清点、跨模块安全/事务复审和静态质量分诊均已完成。
- 尚未修改生产代码。按 brainstorming 流程等待用户批准分阶段修复设计；
  获批后先为确定性 P0/P1 写失败测试，再按互不重叠的文件边界并发修复。
- 数据库初始迁移仍等待单独授权，未获授权前不创建或执行。

## 下一步必读

- `docs/Claude/TASK.md`
- `docs/Claude/PLAN.md`
- `backend/pyproject.toml`
- 第一批代理报告：`/tmp/mtv-audit-*.md`
- 第二批代理报告：`/tmp/mtv-audit-t4.md`、`/tmp/mtv-audit-t5.md`、`/tmp/mtv-audit-t10.md`
- 第三批代理报告：`/tmp/mtv-audit-t6.md`、`/tmp/mtv-audit-t7.md`、`/tmp/mtv-audit-t8.md`
- T9 与横向报告：`/tmp/mtv-audit-t9.md`、`/tmp/mtv-audit-coverage-luna.md`、`/tmp/mtv-audit-horizontal-sol.md`、`/tmp/mtv-audit-static-terra.md`

## 待用户确认的高风险项

- 全仓库没有 Alembic revision。创建覆盖全部 ORM 的初始迁移属于数据库迁移操作，已请求用户单独授权；未获明确确认前不创建或执行迁移。

## 服务与资源记录

- 尚未启动本地服务、容器或临时监听端口。
- 本机既有 `127.0.0.1:18888` 服务（PID 73900）为用户原有进程，不得停止。
