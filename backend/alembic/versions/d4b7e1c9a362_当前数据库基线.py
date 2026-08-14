"""当前数据库基线（唯一迁移文件）

Revision ID: d4b7e1c9a362
Revises:
Create Date: 2026-08-13 16:34:15.031028

合并说明
--------
本文件是 versions 目录下唯一的迁移文件，含全部业务表的完整 schema 定义，
与 backend/app/models 下全部模型元数据一致。

- 全新数据库：``alembic upgrade head`` 直接创建全部 schema。
- 已有线上数据库：曾被 stamp 到本基线 ``d4b7e1c9a362``，但 stamp 不执行 DDL，
  可能存在 schema 漂移（缺表/缺列）。``alembic upgrade head`` 因已盖章到 head 而
  no-op，漂移由 ``app.db.drift_repair`` 在 migrate 流程中幂等修复。
- 后续 schema 变更：用 ``alembic revision`` 新建迁移文件追加到链尾
  （down_revision 指向当前 head），保持标准 alembic 增量模式。

downgrade 受保护：默认拒绝 DROP 全部业务表，避免 ``alembic downgrade base``
造成灾难性数据丢失；确需重置 schema 时设置 ``MTV_ALLOW_BASELINE_DOWNGRADE=true``。

"""

import hashlib
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "d4b7e1c9a362"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_DEFAULT_PROMPT_TEMPLATES: tuple[tuple[str, str, dict[str, object]], ...] = (
    (
        "storyboard.system",
        """你是一位专业的短剧分镜导演与摄影指导。请把剧本转换为可拍摄、可验证的 storyboard.v2 JSON。

## 层级
Episode -> Scene（连续时空段落）-> Shot Group（3~15 秒叙事单元）-> Shot（单一视觉节拍）。

## 导演规则
1. 场景、地点、日夜、时间或连续叙事段落变化时新建 Scene；离开后再回到同一地点也要新建 Scene。
2. 每个新场景最多用 1~2 个镜头完成定场；每镜只承载一个主要动作或信息变化，避免无叙事作用的装饰镜头。
3. 景别随叙事逐步聚焦或释放，避免连续三镜使用相同景别；运镜必须服务于发现、强调、跟随或情绪变化。
4. 相邻镜头保持人物位置、朝向、视线与动作进度连续；用反应镜、动作桥或声音桥承接情绪和转场。
5. frame_description 只写可见的主体、动作状态与前中后景空间，不写无法拍摄的心理判断。
6. 剧本中的对白、旁白和内心独白必须逐字保留；不得增删或改写。无语言内容时 dialogue 必须为 null。
7. 只可引用输入目录中的短引用；不得输出主体名称、subject_id、asset_entry_id，也不得创造新资产。
8. 不要输出 content、description 或 shot_line；这些派生字段由服务端生成。
9. 每镜用 source_block_refs 指明落实的 Bxx 原文段；无法落实的段只能在根级 omitted_source_blocks 声明并给出原因。
10. 用 start_boundary/end_boundary 记录镜头起止时刻可见的人物位置、朝向、视线、手部、道具与状态；不可见事实留空。
11. character_positions 使用九宫格位置与受控朝向；audience_visibility 明确事实现在展示或暂缓揭示，暂缓时给出触发条件与保护方法。

## 受控摄影词表
- shot_size: 极远景/大远景/远景/全景/中全景/中景/中近景/近景/特写/大特写/俯拍特写
- composition: 单人/双人/多人/过肩/对称/三分法/中央构图/框景/前景遮挡/留白/纵深构图（每镜 1~3 项）
- camera_angle: 平视/俯拍/仰拍/侧拍/顶拍/斜侧/主观
- camera_movement: 固定/推镜/拉镜/摇镜/移镜/跟拍/升降/环绕/手持

## 结构与时码
- schema_version 必须是 storyboard.v2。
- scene_no 从 1 连续递增；每个 Scene 内 group_no 从 G1 连续递增。
- 每组 1~5 镜，duration_seconds 为 3~15 的整数秒。单镜头组仅允许用于定场、单一反应镜或硬切转场三类非主体叙事用途，且全集单镜头组数量不得超过镜头组总数的 1/3；凡承载对白、冲突展开、动作推进或情绪转折的叙事主体单元必须使用 2~5 镜的连续镜头组，不得用 1 镜带过。
- 每组 display_no 必须严格为 1, 2, ..., N；首镜 start_seconds=0，相邻区间首尾相接。
- end_seconds 必须大于 start_seconds，末镜 end_seconds 必须等于 duration_seconds；所有时码只能是整数。
- background_ref 只能引用场景；character_refs 只能引用角色；prop_refs 只能引用道具；sound_refs 只能引用声音。
- kind=dialogue/inner_monologue 时 speaker_ref 必须是当前镜头可见角色；kind=voice_over 时 speaker_ref 可为角色、声音主体或 null。
- narrative_purpose 要说明该镜头让观众获得什么信息、感受什么变化，不能只写“推进剧情”。
- sound_design 写可执行的环境音/动作音/声音衔接，无声音时写“无”。

## 覆盖度（最高优先级，强制，优先于一切结构规则）
1. 必须覆盖剧本从开头到结尾的全部叙事节拍：每一处场景切换、每一次台词交锋、每一个动作与情绪转折都要落到具体镜头，不得合并、跳过或用一句话带过。
2. 不得把整场戏压缩进一个镜头组；除非该场戏本身只有一句定场旁白，否则每场戏至少拆为 2 个镜头组。
3. 剧本中的全部对白、旁白、内心独白必须逐字保留在对应镜头的 dialogue.text 中；遗漏台词视为覆盖失败。
4. 用户提示会给出由剧本字数推导的“本集覆盖度下限”（最少镜头数/组数/场数/总秒数），这是下限不是目标；实际产出应达到或超过该下限，低于下限会被判定为不合格并要求重做。
5. 禁止用“一个全景镜 + 长 duration_seconds”敷衍整场戏；duration_seconds 上限 15 秒，长戏必须拆成多个 3~15 秒的镜头组。
6. 不得为凑数量注水无叙事作用的装饰镜头；每镜只承载一个主要动作或信息变化。
7. 用户提示中的每个 Bxx 必须出现在至少一个镜头的 source_block_refs，或出现在根级 omitted_source_blocks 并说明原因。

只输出符合 JSON Schema 的 JSON 对象，不要 markdown 或解释文字。
""",
        {},
    ),
    (
        "storyboard.user",
        "{{ default_prompt }}",
        {"default_prompt": {"type": "string"}},
    ),
    (
        "storyboard.description",
        "在【{{ background }}】中采用【{{ camera }}】拍摄视角，画面主体【{{ characters }}】"
        "的关键动作是【{{ action }}】，整体表现的画面是【{{ scene }}】，表达了【{{ emotion }}】，"
        "整体画面的光影效果是【{{ lighting }}】，整个镜头的叙事目的是【{{ narrative_purpose }}】。",
        {
            key: {"type": "string"}
            for key in (
                "background",
                "camera",
                "characters",
                "action",
                "scene",
                "emotion",
                "lighting",
                "narrative_purpose",
            )
        },
    ),
    (
        "storyboard.shot",
        "【镜头{{ n }}:{{ start }}s～{{ end }}s】场景是{{ scene }}，{{ shot_scale }}·{{ camera }}，"
        "{{ camera_movement }}，{{ lighting }}，{{ action }}，{{ emotion }}，{{ characters }}:{{ dialogue }}，"
        "构图是【{{ composition }}】，声音设计是【{{ sound_design }}】，台词类型是【{{ dialogue_kind }}】",
        {
            key: {"type": "string"}
            for key in (
                "n",
                "start",
                "end",
                "scene",
                "shot_scale",
                "camera",
                "camera_movement",
                "lighting",
                "action",
                "emotion",
                "characters",
                "dialogue",
                "composition",
                "sound_design",
                "dialogue_kind",
            )
        },
    ),
    (
        "storyboard.group",
        "{{ shot_lines }}\n【角色】{{ referenced_names }}\n【叙事目的】{{ narrative_purpose }}\n"
        "{{ director_commands }}{{ constraint_lock }}",
        {
            key: {"type": "string"}
            for key in (
                "shot_lines",
                "referenced_names",
                "narrative_purpose",
                "director_commands",
                "constraint_lock",
            )
        },
    ),
    (
        "storyboard.constraint",
        "【约束】\n1. 人物特征高度一致\n2. 角色不脸崩\n3. 肢体不畸形\n"
        "4. 多镜头切镜丝滑平顺\n5. 画面质量4K稳定\n6. 语音清晰\n"
        "7. 情感表达贴合角色性格\n8. 台词与唇形/动作精准对位\n"
        "9. 禁止出现任何形式的字幕",
        {},
    ),
    (
        "video.constraint_lock",
        "【约束】人物特征高度一致，角色不脸崩、肢体不畸形，多镜头切镜丝滑平顺，"
        "画面质量4K稳定。语音清晰，情感表达贴合角色性格，台词与唇形/动作精准对位。"
        "禁止出现任何形式的字幕。",
        {},
    ),
)



def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('audit_events',
    sa.Column('actor_user_id', sa.String(length=36), nullable=True, comment='操作者用户 ID'),
    sa.Column('actor_username', sa.String(length=128), nullable=True, comment='操作者用户名（冗余便于查询）'),
    sa.Column('action', sa.String(length=64), nullable=False, comment='审计动作类型'),
    sa.Column('resource_type', sa.String(length=64), nullable=False, comment='资源类型: script/subject/job/credit_account/...'),
    sa.Column('resource_id', sa.String(length=36), nullable=True, comment='资源 ID'),
    sa.Column('before', postgresql.JSONB(astext_type=sa.Text()), nullable=True, comment='变更前状态快照'),
    sa.Column('after', postgresql.JSONB(astext_type=sa.Text()), nullable=True, comment='变更后状态快照'),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_audit_events_actor_user_id'), 'audit_events', ['actor_user_id'], unique=False)
    op.create_index(op.f('ix_audit_events_resource_id'), 'audit_events', ['resource_id'], unique=False)
    op.create_table('credit_accounts',
    sa.Column('owner_type', sa.String(length=32), nullable=False),
    sa.Column('owner_id', sa.String(length=36), nullable=False),
    sa.Column('org_id', sa.String(length=36), nullable=True),
    sa.Column('department_id', sa.String(length=36), nullable=True),
    sa.Column('balance_points', sa.Integer(), nullable=False),
    sa.Column('frozen_points', sa.Integer(), nullable=False),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('disabled_reason', sa.String(length=512), nullable=True),
    sa.Column('disabled_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("owner_type = 'user' OR (owner_type = 'org' AND org_id IS NOT NULL AND org_id = owner_id AND department_id IS NULL) OR (owner_type = 'department' AND org_id IS NOT NULL AND department_id IS NOT NULL AND department_id = owner_id)", name='ck_credit_accounts_owner_scope'),
    sa.CheckConstraint("owner_type IN ('user', 'org', 'department')", name='ck_credit_accounts_owner_type'),
    sa.CheckConstraint("status IN ('active', 'disabled')", name='ck_credit_accounts_status'),
    sa.CheckConstraint('balance_points >= 0', name='ck_credit_accounts_balance_nonnegative'),
    sa.CheckConstraint('frozen_points <= balance_points', name='ck_credit_accounts_frozen_within_balance'),
    sa.CheckConstraint('frozen_points >= 0', name='ck_credit_accounts_frozen_nonnegative'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('owner_type', 'owner_id', name='uq_credit_accounts_owner')
    )
    op.create_index(op.f('ix_credit_accounts_department_id'), 'credit_accounts', ['department_id'], unique=False)
    op.create_index(op.f('ix_credit_accounts_org_id'), 'credit_accounts', ['org_id'], unique=False)
    op.create_index(op.f('ix_credit_accounts_owner_id'), 'credit_accounts', ['owner_id'], unique=False)
    op.create_table('generation_batch_requests',
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('owner_user_id', sa.String(length=36), nullable=False),
    sa.Column('idempotency_key', sa.String(length=256), nullable=False),
    sa.Column('request_hash', sa.String(length=64), nullable=False),
    sa.Column('target_subject_ids', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('job_ids', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('skipped_subject_ids', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('failed_items', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("trim(idempotency_key) <> ''", name='ck_generation_batch_requests_idempotency_key_not_blank'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('owner_user_id', 'idempotency_key', name='uq_generation_batch_requests_owner_idempotency_key')
    )
    op.create_index(op.f('ix_generation_batch_requests_owner_user_id'), 'generation_batch_requests', ['owner_user_id'], unique=False)
    op.create_table(
        "meizi_storage_objects",
        sa.Column("storage_key", sa.String(length=512), nullable=False),
        sa.Column("media_id", sa.BigInteger(), nullable=False),
        sa.Column("info_id", sa.BigInteger(), nullable=False),
        sa.Column("lib_type", sa.Integer(), nullable=False),
        sa.Column("media_kind", sa.String(length=16), nullable=False),
        sa.Column("request_id", sa.String(length=64), nullable=False),
        sa.Column("public_url", sa.Text(), nullable=True),
        sa.Column("mime_type", sa.String(length=128), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("delete_namespace", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "media_kind IN ('img', 'audio', 'video', 'file')",
            name="ck_meizi_storage_objects_media_kind",
        ),
        sa.CheckConstraint(
            "status IN ('uploading', 'active', 'deleting', 'cleanup_pending', 'deleted')",
            name="ck_meizi_storage_objects_status",
        ),
        sa.CheckConstraint("media_id > 0", name="ck_meizi_storage_objects_media_id"),
        sa.CheckConstraint("info_id > 0", name="ck_meizi_storage_objects_info_id"),
        sa.CheckConstraint("lib_type > 0", name="ck_meizi_storage_objects_lib_type"),
        sa.CheckConstraint("size_bytes >= 0", name="ck_meizi_storage_objects_size_bytes"),
        sa.PrimaryKeyConstraint("storage_key"),
        sa.UniqueConstraint("media_id", "lib_type", name="uq_meizi_storage_objects_media_id_lib_type"),
    )
    op.create_index(
        op.f("ix_meizi_storage_objects_status"),
        "meizi_storage_objects",
        ["status"],
        unique=False,
    )

    op.create_table('organizations',
    sa.Column('org_name', sa.String(length=255), nullable=False, comment='企业名称'),
    sa.Column('org_code', sa.String(length=128), nullable=False, comment='企业编码'),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('org_code')
    )
    op.create_table('prompt_templates',
    sa.Column('key', sa.String(length=128), nullable=False),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('content', sa.Text(), nullable=False),
    sa.Column('content_sha256', sa.String(length=64), nullable=False),
    sa.Column('variables_schema', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('is_active', sa.Boolean(), nullable=False),
    sa.Column('created_by_user_id', sa.String(length=36), nullable=True),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("trim(content) <> ''", name='ck_prompt_templates_content_not_blank'),
    sa.CheckConstraint("trim(key) <> ''", name='ck_prompt_templates_key_not_blank'),
    sa.CheckConstraint('version >= 1', name='ck_prompt_templates_version_positive'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('key', 'version', name='uq_prompt_templates_key_version')
    )
    op.create_index(op.f('ix_prompt_templates_content_sha256'), 'prompt_templates', ['content_sha256'], unique=False)
    op.create_index(op.f('ix_prompt_templates_created_by_user_id'), 'prompt_templates', ['created_by_user_id'], unique=False)
    op.create_index(op.f('ix_prompt_templates_is_active'), 'prompt_templates', ['is_active'], unique=False)
    op.create_index(op.f('ix_prompt_templates_key'), 'prompt_templates', ['key'], unique=False)
    op.create_index('uq_prompt_templates_active_key', 'prompt_templates', ['key'], unique=True, postgresql_where=sa.text('is_active = true'), sqlite_where=sa.text('is_active = 1'))
    prompt_templates = sa.table(
        "prompt_templates",
        sa.column("id", sa.String(length=36)),
        sa.column("key", sa.String(length=128)),
        sa.column("version", sa.Integer()),
        sa.column("content", sa.Text()),
        sa.column("content_sha256", sa.String(length=64)),
        sa.column("variables_schema", postgresql.JSONB()),
        sa.column("is_active", sa.Boolean()),
        sa.column("created_by_user_id", sa.String(length=36)),
    )
    op.bulk_insert(
        prompt_templates,
        [
            {
                "id": f"prompt-template-default-{index:02d}",
                "key": key,
                "version": 1,
                "content": content,
                "content_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
                "variables_schema": variables,
                "is_active": True,
                "created_by_user_id": None,
            }
            for index, (key, content, variables) in enumerate(
                _DEFAULT_PROMPT_TEMPLATES,
                start=1,
            )
        ],
    )

    op.create_table('scripts',
    sa.Column('title', sa.String(length=512), nullable=False),
    sa.Column('scope', sa.String(length=32), nullable=False),
    sa.Column('owner_user_id', sa.String(length=36), nullable=False),
    sa.Column('org_id', sa.String(length=36), nullable=True),
    sa.Column('created_by_user_id', sa.String(length=36), nullable=False),
    sa.Column('visual_style', sa.String(length=32), nullable=True),
    sa.Column('unify_style', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_scripts_org_id'), 'scripts', ['org_id'], unique=False)
    op.create_index(op.f('ix_scripts_owner_user_id'), 'scripts', ['owner_user_id'], unique=False)
    op.create_table('task_dispatch_outbox',
    sa.Column('task_type', sa.String(length=128), nullable=False),
    sa.Column('job_id', sa.String(length=36), nullable=False),
    sa.Column('attempt', sa.Integer(), nullable=False),
    sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('available_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('published_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('publish_attempts', sa.Integer(), nullable=False),
    sa.Column('last_error', sa.Text(), nullable=True),
    sa.Column('lease_owner', sa.String(length=128), nullable=True),
    sa.Column('lease_token', sa.String(length=128), nullable=True),
    sa.Column('lease_until', sa.DateTime(timezone=True), nullable=True),
    sa.Column('heartbeat_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('task_type', 'job_id', 'attempt', name='uq_dispatch_outbox_task_job_attempt')
    )
    op.create_index(op.f('ix_task_dispatch_outbox_available_at'), 'task_dispatch_outbox', ['available_at'], unique=False)
    op.create_index(op.f('ix_task_dispatch_outbox_job_id'), 'task_dispatch_outbox', ['job_id'], unique=False)
    op.create_index(op.f('ix_task_dispatch_outbox_lease_owner'), 'task_dispatch_outbox', ['lease_owner'], unique=False)
    op.create_index(op.f('ix_task_dispatch_outbox_lease_token'), 'task_dispatch_outbox', ['lease_token'], unique=False)
    op.create_index(op.f('ix_task_dispatch_outbox_lease_until'), 'task_dispatch_outbox', ['lease_until'], unique=False)
    op.create_index(op.f('ix_task_dispatch_outbox_status'), 'task_dispatch_outbox', ['status'], unique=False)
    op.create_index(op.f('ix_task_dispatch_outbox_task_type'), 'task_dispatch_outbox', ['task_type'], unique=False)
    op.create_table('video_provider_configs',
    sa.Column('provider_key', sa.String(length=32), nullable=False),
    sa.Column('job_type', sa.String(length=32), nullable=False),
    sa.Column('model', sa.String(length=128), nullable=False),
    sa.Column('is_active', sa.Boolean(), nullable=False),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.CheckConstraint("job_type IN ('all_reference')", name='ck_video_provider_configs_job_type'),
    sa.CheckConstraint("provider_key IN ('jianyu', 'modelsai', 'volcengine', 'doubao', 'bailian', 'openai', 'gemini', 'mgrouter', 'neolink')", name='ck_video_provider_configs_provider_key'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('provider_key', 'job_type', name='uq_video_provider_key_job')
    )
    op.create_table('art_styles',
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('prompt_zh', sa.Text(), nullable=False),
    sa.Column('prompt_en', sa.Text(), nullable=False),
    sa.Column('negative_prompt', sa.Text(), nullable=False),
    sa.Column('visual_style', sa.String(length=32), nullable=True),
    sa.Column('source_asset_ids', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('source_hashes', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('thumbnail_storage_key', sa.String(length=512), nullable=True),
    sa.Column('extraction_provider', sa.String(length=64), nullable=True),
    sa.Column('extraction_model', sa.String(length=255), nullable=True),
    sa.Column('extraction_version', sa.Integer(), nullable=False),
    sa.Column('is_active', sa.Boolean(), nullable=False),
    sa.Column('created_by_user_id', sa.String(length=36), nullable=False),
    sa.Column('updated_by_user_id', sa.String(length=36), nullable=False),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint('extraction_version >= 1', name='ck_art_styles_extraction_version_positive'),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('script_id', 'name', name='uq_art_styles_script_name')
    )
    op.create_index(op.f('ix_art_styles_created_by_user_id'), 'art_styles', ['created_by_user_id'], unique=False)
    op.create_index(op.f('ix_art_styles_is_active'), 'art_styles', ['is_active'], unique=False)
    op.create_index('ix_art_styles_script_active', 'art_styles', ['script_id', 'is_active'], unique=False)
    op.create_index(op.f('ix_art_styles_script_id'), 'art_styles', ['script_id'], unique=False)
    op.create_index(op.f('ix_art_styles_updated_by_user_id'), 'art_styles', ['updated_by_user_id'], unique=False)
    op.create_table('credit_logs',
    sa.Column('account_id', sa.String(length=36), nullable=False),
    sa.Column('actor_user_id', sa.String(length=36), nullable=True),
    sa.Column('actor_username', sa.String(length=128), nullable=True),
    sa.Column('target_user_id', sa.String(length=36), nullable=True),
    sa.Column('target_username', sa.String(length=128), nullable=True),
    sa.Column('target_owner_type', sa.String(length=32), nullable=True),
    sa.Column('target_owner_id', sa.String(length=36), nullable=True),
    sa.Column('reservation_id', sa.String(length=36), nullable=True),
    sa.Column('source_type', sa.String(length=64), nullable=True),
    sa.Column('source_id', sa.String(length=36), nullable=True),
    sa.Column('idempotency_key', sa.String(length=36), nullable=True),
    sa.Column('log_type', sa.String(length=32), nullable=False),
    sa.Column('points_delta', sa.Integer(), nullable=False),
    sa.Column('frozen_delta', sa.Integer(), nullable=False),
    sa.Column('balance_after', sa.Integer(), nullable=False),
    sa.Column('frozen_after', sa.Integer(), nullable=False),
    sa.Column('note', sa.String(length=1024), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.CheckConstraint("(log_type = 'reserve' AND points_delta = 0 AND frozen_delta > 0 AND reservation_id IS NOT NULL) OR (log_type = 'settle' AND points_delta <= 0 AND frozen_delta < 0 AND reservation_id IS NOT NULL) OR (log_type = 'release' AND points_delta = 0 AND frozen_delta < 0 AND reservation_id IS NOT NULL) OR (log_type = 'refund' AND points_delta >= 0 AND frozen_delta = 0 AND reservation_id IS NOT NULL) OR (log_type = 'grant' AND points_delta != 0 AND frozen_delta = 0 AND reservation_id IS NULL)", name='ck_credit_logs_effect_shape'),
    sa.CheckConstraint("log_type IN ('reserve', 'settle', 'release', 'grant', 'refund')", name='ck_credit_logs_log_type'),
    sa.CheckConstraint('balance_after >= 0', name='ck_credit_logs_balance_nonnegative'),
    sa.CheckConstraint('frozen_after >= 0 AND frozen_after <= balance_after', name='ck_credit_logs_frozen_valid'),
    sa.ForeignKeyConstraint(['account_id'], ['credit_accounts.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('account_id', 'source_type', 'source_id', 'log_type', name='uq_credit_logs_idempotent_source'),
    sa.UniqueConstraint('idempotency_key', name='uq_credit_logs_grant_idempotency_key')
    )
    op.create_index('ix_credit_logs_account_created', 'credit_logs', ['account_id', 'created_at'], unique=False)
    op.create_index(op.f('ix_credit_logs_account_id'), 'credit_logs', ['account_id'], unique=False)
    op.create_index('ix_credit_logs_reservation', 'credit_logs', ['reservation_id'], unique=False)
    op.create_table('credit_reservations',
    sa.Column('account_id', sa.String(length=36), nullable=False),
    sa.Column('source_type', sa.String(length=64), nullable=False),
    sa.Column('source_id', sa.String(length=36), nullable=False),
    sa.Column('reserved_points', sa.Integer(), nullable=False),
    sa.Column('actual_points', sa.Integer(), nullable=True),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('released_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.CheckConstraint("(status IN ('reserved', 'released') AND actual_points IS NULL) OR (status IN ('captured', 'refunded') AND actual_points IS NOT NULL)", name='ck_credit_reservations_actual_by_status'),
    sa.CheckConstraint("status IN ('reserved', 'captured', 'released', 'refunded')", name='ck_credit_reservations_status'),
    sa.CheckConstraint('actual_points IS NULL OR actual_points >= 0', name='ck_credit_reservations_actual_nonnegative'),
    sa.CheckConstraint('reserved_points > 0', name='ck_credit_reservations_points_positive'),
    sa.ForeignKeyConstraint(['account_id'], ['credit_accounts.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('source_type', 'source_id', name='uq_credit_reservations_source')
    )
    op.create_index(op.f('ix_credit_reservations_account_id'), 'credit_reservations', ['account_id'], unique=False)
    op.create_index('ix_credit_reservations_source', 'credit_reservations', ['source_type', 'source_id'], unique=False)
    op.create_table('departments',
    sa.Column('org_id', sa.String(length=36), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False, comment='部门名称'),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.ForeignKeyConstraint(['org_id'], ['organizations.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_departments_org_id'), 'departments', ['org_id'], unique=False)
    op.create_table('episodes',
    sa.Column('episode_id', sa.String(length=36), nullable=False),
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('episode_no', sa.Integer(), nullable=False),
    sa.Column('title', sa.String(length=255), nullable=False),
    sa.Column('script_text', sa.Text(), nullable=True),
    sa.Column('script_file_storage_key', sa.String(length=512), nullable=True),
    sa.Column('schema_version', sa.Integer(), nullable=False),
    sa.Column('episode_version', sa.Integer(), nullable=False),
    sa.Column('client_save_seq', sa.Integer(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.PrimaryKeyConstraint('episode_id'),
    sa.UniqueConstraint('script_id', 'episode_id', name='uq_episodes_script_id_episode_id'),
    sa.UniqueConstraint('script_id', 'episode_no', name='uq_episodes_script_no')
    )
    op.create_index(op.f('ix_episodes_script_id'), 'episodes', ['script_id'], unique=False)
    op.create_table('materials',
    sa.Column('name', sa.String(length=512), nullable=False),
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('script_id'),
    sa.UniqueConstraint('script_id', 'id', name='uq_materials_script_id_id')
    )
    op.create_table('parent_subjects',
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('type', sa.String(length=32), nullable=False),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('child_count', sa.Integer(), nullable=False),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_parent_subjects_script_id'), 'parent_subjects', ['script_id'], unique=False)
    op.create_table('pipeline_stages',
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('stage', sa.String(length=32), nullable=False),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('detail', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('current', sa.Integer(), nullable=True),
    sa.Column('total', sa.Integer(), nullable=True),
    sa.Column('error', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('script_id', 'stage', name='uq_pipeline_stages_script_stage')
    )
    op.create_index(op.f('ix_pipeline_stages_script_id'), 'pipeline_stages', ['script_id'], unique=False)
    op.create_table('art_style_extraction_jobs',
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('actor_user_id', sa.String(length=36), nullable=False),
    sa.Column('idempotency_key', sa.String(length=128), nullable=False),
    sa.Column('request_hash', sa.String(length=64), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('source_asset_ids', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('source_hashes', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('input_snapshot', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('input_version_hash', sa.String(length=64), nullable=False),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('attempt_count', sa.Integer(), server_default=sa.text('1'), nullable=False),
    sa.Column('lease_owner', sa.String(length=128), nullable=True),
    sa.Column('lease_token', sa.String(length=128), nullable=True),
    sa.Column('leased_until', sa.DateTime(timezone=True), nullable=True),
    sa.Column('heartbeat_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('error_code', sa.String(length=64), nullable=True),
    sa.Column('error_message', sa.Text(), nullable=True),
    sa.Column('extraction_provider', sa.String(length=64), nullable=True),
    sa.Column('extraction_model', sa.String(length=255), nullable=True),
    sa.Column('art_style_id', sa.String(length=36), nullable=True),
    sa.Column('billing_mode', sa.String(length=32), server_default=sa.text("'unmetered'"), nullable=False),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("billing_mode = 'unmetered'", name='ck_art_style_extraction_jobs_unmetered'),
    sa.CheckConstraint("status IN ('queued', 'running', 'succeeded', 'failed')", name='ck_art_style_extraction_jobs_status'),
    sa.CheckConstraint('attempt_count >= 1', name='ck_art_style_extraction_jobs_attempt_positive'),
    sa.ForeignKeyConstraint(['art_style_id'], ['art_styles.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('script_id', 'actor_user_id', 'idempotency_key', name='uq_art_style_extraction_jobs_actor_idempotency')
    )
    op.create_index(op.f('ix_art_style_extraction_jobs_actor_user_id'), 'art_style_extraction_jobs', ['actor_user_id'], unique=False)
    op.create_index(op.f('ix_art_style_extraction_jobs_art_style_id'), 'art_style_extraction_jobs', ['art_style_id'], unique=False)
    op.create_index(op.f('ix_art_style_extraction_jobs_lease_owner'), 'art_style_extraction_jobs', ['lease_owner'], unique=False)
    op.create_index(op.f('ix_art_style_extraction_jobs_lease_token'), 'art_style_extraction_jobs', ['lease_token'], unique=False)
    op.create_index(op.f('ix_art_style_extraction_jobs_leased_until'), 'art_style_extraction_jobs', ['leased_until'], unique=False)
    op.create_index(op.f('ix_art_style_extraction_jobs_script_id'), 'art_style_extraction_jobs', ['script_id'], unique=False)
    op.create_index('ix_art_style_extraction_jobs_script_status', 'art_style_extraction_jobs', ['script_id', 'status'], unique=False)
    op.create_index(op.f('ix_art_style_extraction_jobs_status'), 'art_style_extraction_jobs', ['status'], unique=False)
    op.create_index('uq_art_style_extraction_jobs_active_name', 'art_style_extraction_jobs', ['script_id', 'name'], unique=True, postgresql_where=sa.text("status IN ('queued', 'running')"), sqlite_where=sa.text("status IN ('queued', 'running')"))
    op.create_table('canvas_nodes',
    sa.Column('id', sa.String(length=255), nullable=False),
    sa.Column('material_id', sa.String(length=36), nullable=False),
    sa.Column('type', sa.String(length=32), nullable=False),
    sa.Column('position', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['material_id'], ['materials.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('material_id', 'id', name='uq_canvas_nodes_material_id_id')
    )
    op.create_index(op.f('ix_canvas_nodes_material_id'), 'canvas_nodes', ['material_id'], unique=False)
    op.create_table('pipeline_jobs',
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('stage', sa.String(length=32), nullable=False),
    sa.Column('dispatch_id', sa.String(length=36), nullable=False),
    sa.Column('run_id', sa.String(length=36), nullable=False),
    sa.Column('episode_id', sa.String(length=36), nullable=True),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('input_version', sa.Integer(), nullable=False),
    sa.Column('input_client_save_seq', sa.Integer(), nullable=True),
    sa.Column('attempt', sa.Integer(), nullable=False),
    sa.Column('error_type', sa.String(length=128), nullable=True),
    sa.Column('error_message', sa.Text(), nullable=True),
    sa.Column('retryable', sa.Boolean(), nullable=True),
    sa.Column('output_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('lease_owner', sa.String(length=128), nullable=True),
    sa.Column('lease_token', sa.String(length=128), nullable=True),
    sa.Column('lease_until', sa.DateTime(timezone=True), nullable=True),
    sa.Column('heartbeat_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['episode_id'], ['episodes.episode_id'], ),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('run_id', 'stage', 'attempt', name='uq_pipeline_jobs_run_stage_attempt')
    )
    op.create_index(op.f('ix_pipeline_jobs_dispatch_id'), 'pipeline_jobs', ['dispatch_id'], unique=False)
    op.create_index(op.f('ix_pipeline_jobs_episode_id'), 'pipeline_jobs', ['episode_id'], unique=False)
    op.create_index(op.f('ix_pipeline_jobs_run_id'), 'pipeline_jobs', ['run_id'], unique=False)
    op.create_index(op.f('ix_pipeline_jobs_script_id'), 'pipeline_jobs', ['script_id'], unique=False)
    op.create_table('scene_auditions',
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('episode_id', sa.String(length=36), nullable=False),
    sa.Column('scene_no', sa.Integer(), nullable=False),
    sa.Column('input_version', sa.Integer(), nullable=False),
    sa.Column('scene_type', sa.String(length=64), nullable=False),
    sa.Column('source_hash', sa.String(length=64), nullable=False),
    sa.Column('plans', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('selected_plan_id', sa.String(length=64), nullable=True),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('attempt_count', sa.Integer(), server_default=sa.text('0'), nullable=False),
    sa.Column('error_code', sa.String(length=64), nullable=True),
    sa.Column('error_message', sa.String(length=255), nullable=True),
    sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('selected_by_user_id', sa.String(length=36), nullable=True),
    sa.Column('selected_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("scene_type IN ('character_introduction', 'identity_reveal', 'relationship_reversal', 'climax_ending')", name='ck_scene_auditions_key_scene_type'),
    sa.CheckConstraint("status IN ('draft', 'running', 'ready', 'selected', 'stale', 'failed')", name='ck_scene_auditions_status'),
    sa.CheckConstraint('attempt_count >= 0', name='ck_scene_auditions_attempt_count_non_negative'),
    sa.CheckConstraint('input_version >= 1', name='ck_scene_auditions_input_version_positive'),
    sa.CheckConstraint('scene_no >= 1', name='ck_scene_auditions_scene_no_positive'),
    sa.ForeignKeyConstraint(['episode_id'], ['episodes.episode_id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('episode_id', 'scene_no', 'input_version', name='uq_scene_auditions_episode_scene_version')
    )
    op.create_index(op.f('ix_scene_auditions_episode_id'), 'scene_auditions', ['episode_id'], unique=False)
    op.create_index(op.f('ix_scene_auditions_script_id'), 'scene_auditions', ['script_id'], unique=False)
    op.create_index(op.f('ix_scene_auditions_status'), 'scene_auditions', ['status'], unique=False)
    op.create_table('scenes',
    sa.Column('episode_id', sa.String(length=36), nullable=False),
    sa.Column('scene_no', sa.Integer(), nullable=False),
    sa.Column('scene_key', sa.String(length=255), nullable=False),
    sa.Column('background', sa.String(length=255), nullable=False),
    sa.Column('label', sa.String(length=255), nullable=True),
    sa.Column('shot_group_count', sa.Integer(), nullable=False),
    sa.Column('pending_shot_count', sa.Integer(), nullable=False),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.CheckConstraint('pending_shot_count >= 0', name='ck_scenes_pending_count_nonnegative'),
    sa.CheckConstraint('scene_no >= 1', name='ck_scenes_scene_no_positive'),
    sa.CheckConstraint('shot_group_count >= 0', name='ck_scenes_group_count_nonnegative'),
    sa.ForeignKeyConstraint(['episode_id'], ['episodes.episode_id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('episode_id', 'scene_no', name='uq_scenes_episode_scene_no')
    )
    op.create_index(op.f('ix_scenes_episode_id'), 'scenes', ['episode_id'], unique=False)
    op.create_table('storyboard_jobs',
    sa.Column('job_id', sa.String(length=36), nullable=False),
    sa.Column('job_type', sa.String(length=32), nullable=False),
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('episode_id', sa.String(length=36), nullable=False),
    sa.Column('task_id', sa.String(length=64), nullable=True),
    sa.Column('triggered_by_user_id', sa.String(length=36), nullable=True),
    sa.Column('owner_user_id', sa.String(length=36), nullable=True),
    sa.Column('episode_version', sa.Integer(), nullable=False),
    sa.Column('input_version', sa.Integer(), nullable=False),
    sa.Column('input_client_save_seq', sa.Integer(), nullable=True),
    sa.Column('attempt', sa.Integer(), nullable=False),
    sa.Column('idempotency_key', sa.String(length=128), nullable=False),
    sa.Column('request_hash', sa.String(length=64), nullable=False),
    sa.Column('lease_owner', sa.String(length=128), nullable=True),
    sa.Column('lease_token', sa.String(length=128), nullable=True),
    sa.Column('lease_until', sa.DateTime(timezone=True), nullable=True),
    sa.Column('heartbeat_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('canceled_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('reservation_id', sa.String(length=36), nullable=True),
    sa.Column('financial_cleanup_status', sa.String(length=32), nullable=True),
    sa.Column('financial_cleanup_error', sa.String(length=128), nullable=True),
    sa.Column('error_type', sa.String(length=64), nullable=True),
    sa.Column('retryable', sa.Boolean(), nullable=False),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('progress', sa.Integer(), nullable=False),
    sa.Column('progress_stage', sa.String(length=64), nullable=True),
    sa.Column('progress_message', sa.String(length=512), nullable=True),
    sa.Column('error_message', sa.String(length=1024), nullable=True),
    sa.Column('detail', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('input_provenance', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("job_type = 'storyboard'", name='ck_storyboard_jobs_job_type'),
    sa.CheckConstraint("status in ('pending', 'running', 'succeeded', 'failed', 'canceled', 'retry')", name='ck_storyboard_jobs_status'),
    sa.CheckConstraint('attempt >= 1', name='ck_storyboard_jobs_attempt'),
    sa.CheckConstraint('input_version >= 1', name='ck_storyboard_jobs_input_version'),
    sa.CheckConstraint('progress >= 0 and progress <= 100', name='ck_storyboard_jobs_progress'),
    sa.ForeignKeyConstraint(['episode_id'], ['episodes.episode_id'], ),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.PrimaryKeyConstraint('job_id'),
    sa.UniqueConstraint('episode_id', 'attempt', name='uq_storyboard_jobs_episode_attempt'),
    sa.UniqueConstraint('episode_id', 'idempotency_key', name='uq_storyboard_jobs_episode_idempotency_key')
    )
    op.create_index(op.f('ix_storyboard_jobs_episode_id'), 'storyboard_jobs', ['episode_id'], unique=False)
    op.create_index(op.f('ix_storyboard_jobs_financial_cleanup_status'), 'storyboard_jobs', ['financial_cleanup_status'], unique=False)
    op.create_index(op.f('ix_storyboard_jobs_idempotency_key'), 'storyboard_jobs', ['idempotency_key'], unique=False)
    op.create_index(op.f('ix_storyboard_jobs_lease_owner'), 'storyboard_jobs', ['lease_owner'], unique=False)
    op.create_index(op.f('ix_storyboard_jobs_lease_token'), 'storyboard_jobs', ['lease_token'], unique=False)
    op.create_index(op.f('ix_storyboard_jobs_lease_until'), 'storyboard_jobs', ['lease_until'], unique=False)
    op.create_index(op.f('ix_storyboard_jobs_owner_user_id'), 'storyboard_jobs', ['owner_user_id'], unique=False)
    op.create_index(op.f('ix_storyboard_jobs_request_hash'), 'storyboard_jobs', ['request_hash'], unique=False)
    op.create_index(op.f('ix_storyboard_jobs_reservation_id'), 'storyboard_jobs', ['reservation_id'], unique=False)
    op.create_index(op.f('ix_storyboard_jobs_script_id'), 'storyboard_jobs', ['script_id'], unique=False)
    op.create_index(op.f('ix_storyboard_jobs_task_id'), 'storyboard_jobs', ['task_id'], unique=False)
    op.create_index(op.f('ix_storyboard_jobs_triggered_by_user_id'), 'storyboard_jobs', ['triggered_by_user_id'], unique=False)
    op.create_table('subjects',
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('type', sa.String(length=32), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('pipeline_name', sa.String(length=255), nullable=False),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('visual_style', sa.String(length=32), nullable=True),
    sa.Column('subject_version', sa.Integer(), nullable=False),
    sa.Column('enrich_version', sa.Integer(), nullable=False),
    sa.Column('name_locked', sa.Boolean(), nullable=False),
    sa.Column('description_locked', sa.Boolean(), nullable=False),
    sa.Column('is_child', sa.Boolean(), nullable=False),
    sa.Column('is_parent', sa.Boolean(), nullable=False),
    sa.Column('parent_id', sa.String(length=36), nullable=True),
    sa.Column('episode_nos', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('is_active', sa.Boolean(), nullable=False),
    sa.Column('review_status', sa.String(length=32), nullable=False),
    sa.Column('image_entry_id', sa.String(length=36), nullable=True),
    sa.Column('whitelist_status', sa.String(length=64), nullable=True),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['parent_id'], ['parent_subjects.id'], ),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('script_id', 'id', name='uq_subjects_script_id_id'),
    sa.UniqueConstraint('script_id', 'type', 'pipeline_name', name='uq_subjects_script_type_pipeline_name')
    )
    op.create_index(op.f('ix_subjects_is_active'), 'subjects', ['is_active'], unique=False)
    op.create_index(op.f('ix_subjects_parent_id'), 'subjects', ['parent_id'], unique=False)
    op.create_index(op.f('ix_subjects_script_id'), 'subjects', ['script_id'], unique=False)
    op.create_index('ix_subjects_script_review_type', 'subjects', ['script_id', 'review_status', 'type'], unique=False)
    op.create_table('users',
    sa.Column('user_id', sa.String(length=36), nullable=False),
    sa.Column('username', sa.String(length=128), nullable=False, comment='登录用户名'),
    sa.Column('password_hash', sa.String(length=255), nullable=False),
    sa.Column('enabled', sa.Boolean(), nullable=False),
    sa.Column('is_admin', sa.Boolean(), nullable=False),
    sa.Column('account_type', sa.String(length=32), nullable=False),
    sa.Column('org_id', sa.String(length=36), nullable=True),
    sa.Column('department_id', sa.String(length=36), nullable=True),
    sa.Column('can_manage_accounts', sa.Boolean(), nullable=False),
    sa.Column('management_scope', sa.String(length=32), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['department_id'], ['departments.id'], ),
    sa.ForeignKeyConstraint(['org_id'], ['organizations.id'], ),
    sa.PrimaryKeyConstraint('user_id'),
    sa.UniqueConstraint('username')
    )
    op.create_index(op.f('ix_users_department_id'), 'users', ['department_id'], unique=False)
    op.create_index(op.f('ix_users_org_id'), 'users', ['org_id'], unique=False)
    op.create_table('asset_entries',
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('subject_id', sa.String(length=36), nullable=False),
    sa.Column('kind', sa.String(length=16), nullable=False),
    sa.Column('storage_key', sa.String(length=512), nullable=False),
    sa.Column('mime_type', sa.String(length=128), nullable=False),
    sa.Column('size_bytes', sa.Integer(), nullable=False),
    sa.Column('source', sa.String(length=32), nullable=False),
    sa.Column('sha256', sa.String(length=64), nullable=True),
    sa.Column('status', sa.String(length=16), nullable=False),
    sa.Column('is_approved', sa.Boolean(), nullable=False),
    sa.Column('superseded_by_id', sa.String(length=36), nullable=True),
    sa.Column('superseded', sa.Boolean(), nullable=False),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("(status = 'candidate' AND is_approved = false AND superseded = false AND superseded_by_id IS NULL) OR (status = 'approved' AND is_approved = true AND superseded = false AND superseded_by_id IS NULL) OR (status = 'superseded' AND is_approved = false AND superseded = true AND superseded_by_id IS NOT NULL) OR (status = 'deleted' AND is_approved = false AND superseded = false AND superseded_by_id IS NULL)", name='ck_asset_entries_lifecycle_consistent'),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.ForeignKeyConstraint(['subject_id'], ['subjects.id'], ),
    sa.ForeignKeyConstraint(['superseded_by_id'], ['asset_entries.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_asset_entries_script_id'), 'asset_entries', ['script_id'], unique=False)
    op.create_index(op.f('ix_asset_entries_status'), 'asset_entries', ['status'], unique=False)
    op.create_index(op.f('ix_asset_entries_subject_id'), 'asset_entries', ['subject_id'], unique=False)
    op.create_foreign_key(
        'fk_subjects_image_entry_id_asset_entries',
        'subjects',
        'asset_entries',
        ['image_entry_id'],
        ['id'],
    )
    op.create_table('canvas_edges',
    sa.Column('id', sa.String(length=255), nullable=False),
    sa.Column('material_id', sa.String(length=36), nullable=False),
    sa.Column('source', sa.String(length=255), nullable=False),
    sa.Column('target', sa.String(length=255), nullable=False),
    sa.Column('data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['material_id', 'source'], ['canvas_nodes.material_id', 'canvas_nodes.id'], name='fk_canvas_edges_source_same_material'),
    sa.ForeignKeyConstraint(['material_id', 'target'], ['canvas_nodes.material_id', 'canvas_nodes.id'], name='fk_canvas_edges_target_same_material'),
    sa.ForeignKeyConstraint(['material_id'], ['materials.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_canvas_edges_material_id'), 'canvas_edges', ['material_id'], unique=False)
    op.create_table('generation_jobs',
    sa.Column('job_id', sa.String(length=36), nullable=False),
    sa.Column('material_id', sa.String(length=36), nullable=False),
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('subject_id', sa.String(length=36), nullable=False),
    sa.Column('owner_user_id', sa.String(length=36), nullable=False),
    sa.Column('idempotency_key', sa.String(length=256), nullable=False),
    sa.Column('request_hash', sa.String(length=64), nullable=False),
    sa.Column('input_version', sa.Integer(), nullable=False),
    sa.Column('attempt', sa.Integer(), nullable=False),
    sa.Column('episode_id', sa.String(length=36), nullable=True),
    sa.Column('scene_no', sa.Integer(), nullable=True),
    sa.Column('job_type', sa.String(length=32), nullable=False),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('progress', sa.Integer(), nullable=False),
    sa.Column('progress_stage', sa.String(length=64), nullable=True),
    sa.Column('progress_message', sa.String(length=512), nullable=True),
    sa.Column('error_type', sa.String(length=64), nullable=True),
    sa.Column('error_message', sa.String(length=1024), nullable=True),
    sa.Column('retryable', sa.Boolean(), nullable=False),
    sa.Column('canceled_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('lease_owner', sa.String(length=128), nullable=True),
    sa.Column('lease_token', sa.String(length=128), nullable=True),
    sa.Column('lease_until', sa.DateTime(timezone=True), nullable=True),
    sa.Column('heartbeat_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('input_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('output_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('metrics', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("job_type = 'generate_images'", name='ck_generation_jobs_job_type'),
    sa.CheckConstraint("status in ('starting','processing','succeeded','failed','canceled')", name='ck_generation_jobs_status'),
    sa.CheckConstraint("trim(idempotency_key) <> ''", name='ck_generation_jobs_idempotency_key_not_blank'),
    sa.CheckConstraint('attempt >= 1', name='ck_generation_jobs_attempt'),
    sa.CheckConstraint('input_version > 0', name='ck_generation_jobs_input_version_positive'),
    sa.CheckConstraint('progress >= 0 and progress <= 100', name='ck_generation_jobs_progress'),
    sa.ForeignKeyConstraint(['episode_id'], ['episodes.episode_id'], ),
    sa.ForeignKeyConstraint(['material_id'], ['materials.id'], ),
    sa.ForeignKeyConstraint(['owner_user_id'], ['users.user_id'], ),
    sa.ForeignKeyConstraint(['script_id', 'episode_id'], ['episodes.script_id', 'episodes.episode_id'], name='fk_generation_jobs_episode_same_script'),
    sa.ForeignKeyConstraint(['script_id', 'material_id'], ['materials.script_id', 'materials.id'], name='fk_generation_jobs_material_same_script'),
    sa.ForeignKeyConstraint(['script_id', 'subject_id'], ['subjects.script_id', 'subjects.id'], name='fk_generation_jobs_subject_same_script'),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.ForeignKeyConstraint(['subject_id'], ['subjects.id'], ),
    sa.PrimaryKeyConstraint('job_id'),
    sa.UniqueConstraint('owner_user_id', 'idempotency_key', name='uq_generation_jobs_owner_idempotency_key')
    )
    op.create_index(op.f('ix_generation_jobs_lease_owner'), 'generation_jobs', ['lease_owner'], unique=False)
    op.create_index(op.f('ix_generation_jobs_lease_token'), 'generation_jobs', ['lease_token'], unique=False)
    op.create_index('ix_generation_jobs_material_created', 'generation_jobs', ['material_id', 'created_at'], unique=False)
    op.create_index(op.f('ix_generation_jobs_material_id'), 'generation_jobs', ['material_id'], unique=False)
    op.create_index(op.f('ix_generation_jobs_owner_user_id'), 'generation_jobs', ['owner_user_id'], unique=False)
    op.create_index(op.f('ix_generation_jobs_script_id'), 'generation_jobs', ['script_id'], unique=False)
    op.create_index('ix_generation_jobs_status_created', 'generation_jobs', ['status', 'created_at'], unique=False)
    op.create_index(op.f('ix_generation_jobs_subject_id'), 'generation_jobs', ['subject_id'], unique=False)
    op.create_table('project_members',
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('user_id', sa.String(length=36), nullable=False),
    sa.Column('project_role', sa.String(length=32), nullable=False),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.CheckConstraint("project_role IN ('owner', 'manager', 'editor', 'reviewer', 'viewer')", name='ck_project_members_role'),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.ForeignKeyConstraint(['user_id'], ['users.user_id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('script_id', 'user_id', name='uq_project_members_script_user')
    )
    op.create_index(op.f('ix_project_members_script_id'), 'project_members', ['script_id'], unique=False)
    op.create_index(op.f('ix_project_members_user_id'), 'project_members', ['user_id'], unique=False)
    op.create_table('shot_groups',
    sa.Column('shot_group_id', sa.String(length=36), nullable=False),
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('episode_id', sa.String(length=36), nullable=False),
    sa.Column('scene_no', sa.Integer(), nullable=False),
    sa.Column('group_index', sa.Integer(), nullable=False),
    sa.Column('group_no', sa.String(length=32), nullable=False),
    sa.Column('prompt', sa.Text(), nullable=False),
    sa.Column('reference_frames', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('reference_image_audit', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('duration_seconds', sa.Integer(), nullable=False),
    sa.Column('video_duration_seconds', sa.Integer(), nullable=True),
    sa.Column('materials', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('audio_elements', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('description_text', sa.Text(), nullable=False),
    sa.Column('quality_summary', postgresql.JSONB(astext_type=sa.Text()), server_default='{}', nullable=False),
    sa.Column('prompt_context', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('is_continuation', sa.Boolean(), server_default='false', nullable=False),
    sa.Column('source_storyboard_job_id', sa.String(length=36), nullable=True),
    sa.Column('unit_id', sa.String(length=64), nullable=False),
    sa.Column('unit_runtime_key', sa.String(length=128), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint('duration_seconds >= 3 and duration_seconds <= 15', name='ck_shot_groups_duration_range'),
    sa.CheckConstraint('group_index >= 1', name='ck_shot_groups_group_index_positive'),
    sa.CheckConstraint('scene_no >= 1', name='ck_shot_groups_scene_no_positive'),
    sa.CheckConstraint('video_duration_seconds is null or video_duration_seconds >= 0', name='ck_shot_groups_video_duration_nonnegative'),
    sa.ForeignKeyConstraint(['episode_id'], ['episodes.episode_id'], ),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.ForeignKeyConstraint(['source_storyboard_job_id'], ['storyboard_jobs.job_id'], name='fk_shot_groups_source_storyboard_job_id_storyboard_jobs', ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('shot_group_id'),
    sa.UniqueConstraint('episode_id', 'group_index', name='uq_shot_groups_episode_group_index'),
    sa.UniqueConstraint('episode_id', 'scene_no', 'group_no', name='uq_shot_groups_episode_scene_group_no')
    )
    op.create_index(op.f('ix_shot_groups_episode_id'), 'shot_groups', ['episode_id'], unique=False)
    op.create_index('ix_shot_groups_reference_frames_first_frame', 'shot_groups', [sa.literal_column("(CAST(reference_frames ->> 'first_frame' AS VARCHAR))")], unique=False)
    op.create_index('ix_shot_groups_reference_frames_last_frame', 'shot_groups', [sa.literal_column("(CAST(reference_frames ->> 'last_frame' AS VARCHAR))")], unique=False)
    op.create_index('ix_shot_groups_reference_frames_reference_frame', 'shot_groups', [sa.literal_column("(CAST(reference_frames ->> 'reference_frame' AS VARCHAR))")], unique=False)
    op.create_index(op.f('ix_shot_groups_script_id'), 'shot_groups', ['script_id'], unique=False)
    op.create_index(op.f('ix_shot_groups_source_storyboard_job_id'), 'shot_groups', ['source_storyboard_job_id'], unique=False)
    op.create_index(op.f('ix_shot_groups_unit_runtime_key'), 'shot_groups', ['unit_runtime_key'], unique=False)
    op.create_table('subject_candidates',
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('source_run_id', sa.String(length=36), nullable=True),
    sa.Column('subject_type', sa.String(length=32), nullable=False),
    sa.Column('name', sa.String(length=255), nullable=False),
    sa.Column('pipeline_name', sa.String(length=255), nullable=False),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('aliases', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('episode_nos', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('evidence', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('linked_subject_id', sa.String(length=36), nullable=True),
    sa.Column('resolved_by_user_id', sa.String(length=36), nullable=True),
    sa.Column('resolution_action', sa.String(length=32), nullable=True),
    sa.Column('resolution_request_hash', sa.String(length=64), nullable=True),
    sa.Column('resolved_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("resolution_action IS NULL OR resolution_action IN ('link_existing', 'create_subject', 'ignore')", name='ck_subject_candidates_resolution_action'),
    sa.CheckConstraint("status IN ('pending', 'linked', 'ignored')", name='ck_subject_candidates_status'),
    sa.CheckConstraint("subject_type IN ('character', 'scene', 'prop', 'sound')", name='ck_subject_candidates_type'),
    sa.ForeignKeyConstraint(['linked_subject_id'], ['subjects.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_subject_candidates_linked_subject_id'), 'subject_candidates', ['linked_subject_id'], unique=False)
    op.create_index(op.f('ix_subject_candidates_resolved_by_user_id'), 'subject_candidates', ['resolved_by_user_id'], unique=False)
    op.create_index(op.f('ix_subject_candidates_script_id'), 'subject_candidates', ['script_id'], unique=False)
    op.create_index(op.f('ix_subject_candidates_source_run_id'), 'subject_candidates', ['source_run_id'], unique=False)
    op.create_index(op.f('ix_subject_candidates_status'), 'subject_candidates', ['status'], unique=False)
    op.create_index('uq_subject_candidates_pending_identity', 'subject_candidates', ['script_id', 'subject_type', 'pipeline_name'], unique=True, postgresql_where=sa.text("status = 'pending'"), sqlite_where=sa.text("status = 'pending'"))
    op.create_table('subject_change_suggestions',
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('subject_id', sa.String(length=36), nullable=False),
    sa.Column('field', sa.String(length=32), nullable=False),
    sa.Column('current_value', sa.Text(), nullable=True),
    sa.Column('suggested_value', sa.Text(), nullable=True),
    sa.Column('base_subject_version', sa.Integer(), nullable=False),
    sa.Column('source_run_id', sa.String(length=36), nullable=True),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('resolved_by_user_id', sa.String(length=36), nullable=True),
    sa.Column('resolved_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("field IN ('name', 'description')", name='ck_subject_change_suggestions_field'),
    sa.CheckConstraint("status IN ('pending', 'accepted', 'ignored')", name='ck_subject_change_suggestions_status'),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['subject_id'], ['subjects.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_subject_change_suggestions_resolved_by_user_id'), 'subject_change_suggestions', ['resolved_by_user_id'], unique=False)
    op.create_index(op.f('ix_subject_change_suggestions_script_id'), 'subject_change_suggestions', ['script_id'], unique=False)
    op.create_index(op.f('ix_subject_change_suggestions_source_run_id'), 'subject_change_suggestions', ['source_run_id'], unique=False)
    op.create_index(op.f('ix_subject_change_suggestions_status'), 'subject_change_suggestions', ['status'], unique=False)
    op.create_index(op.f('ix_subject_change_suggestions_subject_id'), 'subject_change_suggestions', ['subject_id'], unique=False)
    op.create_index('uq_subject_change_suggestions_pending_version', 'subject_change_suggestions', ['subject_id', 'field', 'base_subject_version'], unique=True, postgresql_where=sa.text("status = 'pending'"), sqlite_where=sa.text("status = 'pending'"))
    op.create_table('timeline_render_jobs',
    sa.Column('job_id', sa.String(length=36), nullable=False),
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('episode_id', sa.String(length=36), nullable=False),
    sa.Column('owner_user_id', sa.String(length=36), nullable=False),
    sa.Column('idempotency_key', sa.String(length=256), nullable=False),
    sa.Column('request_hash', sa.String(length=64), nullable=False),
    sa.Column('input_version', sa.Integer(), nullable=False),
    sa.Column('attempt', sa.Integer(), nullable=False),
    sa.Column('reservation_id', sa.String(length=36), nullable=True),
    sa.Column('dispatch_id', sa.String(length=36), nullable=True),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('progress', sa.Integer(), nullable=False),
    sa.Column('progress_stage', sa.String(length=64), nullable=True),
    sa.Column('progress_message', sa.String(length=512), nullable=True),
    sa.Column('error_type', sa.String(length=64), nullable=True),
    sa.Column('error_message', sa.Text(), nullable=True),
    sa.Column('retryable', sa.Boolean(), nullable=False),
    sa.Column('input_json', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('output_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('output_storage_key', sa.String(length=512), nullable=False),
    sa.Column('lease_owner', sa.String(length=128), nullable=True),
    sa.Column('lease_token', sa.String(length=128), nullable=True),
    sa.Column('leased_until', sa.DateTime(timezone=True), nullable=True),
    sa.Column('heartbeat_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('canceled_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['episode_id'], ['episodes.episode_id'], ),
    sa.ForeignKeyConstraint(['owner_user_id'], ['users.user_id'], ),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.PrimaryKeyConstraint('job_id'),
    sa.UniqueConstraint('output_storage_key', name='uq_timeline_render_jobs_output_storage_key'),
    sa.UniqueConstraint('owner_user_id', 'idempotency_key', name='uq_timeline_render_jobs_owner_idempotency_key')
    )
    op.create_index(op.f('ix_timeline_render_jobs_dispatch_id'), 'timeline_render_jobs', ['dispatch_id'], unique=False)
    op.create_index(op.f('ix_timeline_render_jobs_episode_id'), 'timeline_render_jobs', ['episode_id'], unique=False)
    op.create_index(op.f('ix_timeline_render_jobs_lease_owner'), 'timeline_render_jobs', ['lease_owner'], unique=False)
    op.create_index(op.f('ix_timeline_render_jobs_lease_token'), 'timeline_render_jobs', ['lease_token'], unique=False)
    op.create_index(op.f('ix_timeline_render_jobs_leased_until'), 'timeline_render_jobs', ['leased_until'], unique=False)
    op.create_index(op.f('ix_timeline_render_jobs_owner_user_id'), 'timeline_render_jobs', ['owner_user_id'], unique=False)
    op.create_index(op.f('ix_timeline_render_jobs_reservation_id'), 'timeline_render_jobs', ['reservation_id'], unique=False)
    op.create_index(op.f('ix_timeline_render_jobs_script_id'), 'timeline_render_jobs', ['script_id'], unique=False)
    op.create_table('user_organizations',
    sa.Column('user_id', sa.String(length=36), nullable=False),
    sa.Column('org_id', sa.String(length=36), nullable=False),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.ForeignKeyConstraint(['org_id'], ['organizations.id'], ),
    sa.ForeignKeyConstraint(['user_id'], ['users.user_id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('user_id', 'org_id', name='uq_user_organizations_user_org')
    )
    op.create_index(op.f('ix_user_organizations_org_id'), 'user_organizations', ['org_id'], unique=False)
    op.create_index(op.f('ix_user_organizations_user_id'), 'user_organizations', ['user_id'], unique=False)
    op.create_table('media_uploads',
    sa.Column('owner_script_id', sa.String(length=36), nullable=False),
    sa.Column('script_id', sa.String(length=36), nullable=True),
    sa.Column('material_id', sa.String(length=36), nullable=True),
    sa.Column('subject_id', sa.String(length=36), nullable=True),
    sa.Column('asset_id', sa.String(length=36), nullable=True),
    sa.Column('temp_storage_key', sa.String(length=512), nullable=False),
    sa.Column('final_storage_key', sa.String(length=512), nullable=False),
    sa.Column('original_filename', sa.String(length=512), nullable=False),
    sa.Column('mime_type', sa.String(length=128), nullable=False),
    sa.Column('size_bytes', sa.Integer(), nullable=False),
    sa.Column('sha256', sa.String(length=64), nullable=False),
    sa.Column('status', sa.String(length=16), nullable=False),
    sa.Column('last_error_type', sa.String(length=128), nullable=True),
    sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("status IN ('staging', 'ready', 'failed', 'deleted')", name='ck_media_uploads_status'),
    sa.ForeignKeyConstraint(['asset_id'], ['asset_entries.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['material_id'], ['materials.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['subject_id'], ['subjects.id'], ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('asset_id', name='uq_media_uploads_asset_id'),
    sa.UniqueConstraint('final_storage_key', name='uq_media_uploads_final_storage_key'),
    sa.UniqueConstraint('temp_storage_key', name='uq_media_uploads_temp_storage_key')
    )
    op.create_index(op.f('ix_media_uploads_asset_id'), 'media_uploads', ['asset_id'], unique=False)
    op.create_index(op.f('ix_media_uploads_material_id'), 'media_uploads', ['material_id'], unique=False)
    op.create_index(op.f('ix_media_uploads_owner_script_id'), 'media_uploads', ['owner_script_id'], unique=False)
    op.create_index(op.f('ix_media_uploads_script_id'), 'media_uploads', ['script_id'], unique=False)
    op.create_index(op.f('ix_media_uploads_status'), 'media_uploads', ['status'], unique=False)
    op.create_index(op.f('ix_media_uploads_subject_id'), 'media_uploads', ['subject_id'], unique=False)
    op.create_table('shots',
    sa.Column('shot_id', sa.String(length=36), nullable=False),
    sa.Column('scene_no', sa.Integer(), nullable=False),
    sa.Column('shot_group_id', sa.String(length=36), nullable=False),
    sa.Column('group_no', sa.String(length=32), nullable=False),
    sa.Column('shot_no', sa.String(length=64), nullable=False),
    sa.Column('display_no', sa.Integer(), nullable=False),
    sa.Column('content', sa.Text(), nullable=False),
    sa.Column('description', sa.Text(), nullable=False),
    sa.Column('shot_line', sa.Text(), nullable=False),
    sa.Column('start_seconds', sa.Integer(), nullable=False),
    sa.Column('end_seconds', sa.Integer(), nullable=False),
    sa.Column('characters', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('props', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('sounds', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('dialogue_character', sa.String(length=255), nullable=False),
    sa.Column('structured_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.CheckConstraint('display_no >= 1', name='ck_shots_display_no_positive'),
    sa.CheckConstraint('end_seconds > start_seconds', name='ck_shots_time_range'),
    sa.CheckConstraint('scene_no >= 1', name='ck_shots_scene_no_positive'),
    sa.CheckConstraint('start_seconds >= 0', name='ck_shots_start_nonnegative'),
    sa.ForeignKeyConstraint(['shot_group_id'], ['shot_groups.shot_group_id'], ),
    sa.PrimaryKeyConstraint('shot_id'),
    sa.UniqueConstraint('shot_group_id', 'display_no', name='uq_shots_group_display_no'),
    sa.UniqueConstraint('shot_group_id', 'shot_no', name='uq_shots_group_shot_no')
    )
    op.create_index(op.f('ix_shots_shot_group_id'), 'shots', ['shot_group_id'], unique=False)
    op.create_table('video_generation_jobs',
    sa.Column('job_id', sa.String(length=36), nullable=False),
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('episode_id', sa.String(length=36), nullable=False),
    sa.Column('shot_group_id', sa.String(length=36), nullable=False),
    sa.Column('owner_user_id', sa.String(length=36), nullable=True),
    sa.Column('idempotency_key', sa.String(length=256), nullable=True),
    sa.Column('request_hash', sa.String(length=64), nullable=True),
    sa.Column('input_version', sa.Integer(), nullable=False),
    sa.Column('attempt', sa.Integer(), nullable=False),
    sa.Column('reservation_id', sa.String(length=36), nullable=True),
    sa.Column('dispatch_id', sa.String(length=36), nullable=True),
    sa.Column('predecessor_job_id', sa.String(length=36), nullable=True),
    sa.Column('scene_no', sa.Integer(), nullable=False),
    sa.Column('unit_id', sa.String(length=64), nullable=False),
    sa.Column('unit_runtime_key', sa.String(length=128), nullable=False),
    sa.Column('provider_key', sa.String(length=32), nullable=False),
    sa.Column('job_type', sa.String(length=32), nullable=False),
    sa.Column('status', sa.String(length=32), nullable=False),
    sa.Column('progress', sa.Integer(), nullable=False),
    sa.Column('progress_stage', sa.String(length=64), nullable=True),
    sa.Column('progress_message', sa.String(length=512), nullable=True),
    sa.Column('error_type', sa.String(length=64), nullable=True),
    sa.Column('error_message', sa.String(length=1024), nullable=True),
    sa.Column('retryable', sa.Boolean(), nullable=False),
    sa.Column('input_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('output_json', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('metrics', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('lease_owner', sa.String(length=128), nullable=True),
    sa.Column('lease_token', sa.String(length=128), nullable=True),
    sa.Column('lease_until', sa.DateTime(timezone=True), nullable=True),
    sa.Column('heartbeat_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('canceled_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("job_type IN ('all_reference')", name='ck_video_jobs_job_type'),
    sa.CheckConstraint("provider_key IN ('jianyu', 'modelsai', 'volcengine', 'doubao', 'bailian', 'openai', 'gemini', 'mgrouter', 'neolink')", name='ck_video_jobs_provider_key'),
    sa.CheckConstraint("status IN ('waiting_dependency', 'starting', 'processing', 'succeeded', 'failed', 'canceled')", name='ck_video_jobs_status'),
    sa.CheckConstraint('attempt > 0', name='ck_video_jobs_attempt_positive'),
    sa.CheckConstraint('progress >= 0 AND progress <= 100', name='ck_video_jobs_progress_range'),
    sa.ForeignKeyConstraint(['episode_id'], ['episodes.episode_id'], ),
    sa.ForeignKeyConstraint(['predecessor_job_id'], ['video_generation_jobs.job_id'], name='fk_video_jobs_predecessor_job', ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.ForeignKeyConstraint(['shot_group_id'], ['shot_groups.shot_group_id'], ),
    sa.PrimaryKeyConstraint('job_id'),
    sa.UniqueConstraint('script_id', 'idempotency_key', name='uq_video_generation_jobs_script_idempotency_key'),
    sa.UniqueConstraint('unit_id', 'attempt', name='uq_video_generation_jobs_unit_attempt')
    )
    op.create_index(op.f('ix_video_generation_jobs_dispatch_id'), 'video_generation_jobs', ['dispatch_id'], unique=False)
    op.create_index(op.f('ix_video_generation_jobs_episode_id'), 'video_generation_jobs', ['episode_id'], unique=False)
    op.create_index(op.f('ix_video_generation_jobs_lease_owner'), 'video_generation_jobs', ['lease_owner'], unique=False)
    op.create_index(op.f('ix_video_generation_jobs_lease_token'), 'video_generation_jobs', ['lease_token'], unique=False)
    op.create_index(op.f('ix_video_generation_jobs_lease_until'), 'video_generation_jobs', ['lease_until'], unique=False)
    op.create_index('ix_video_generation_jobs_output_video_storage_key', 'video_generation_jobs', [sa.literal_column("(CAST(output_json ->> 'video_storage_key' AS VARCHAR))")], unique=False)
    op.create_index(op.f('ix_video_generation_jobs_owner_user_id'), 'video_generation_jobs', ['owner_user_id'], unique=False)
    op.create_index(op.f('ix_video_generation_jobs_predecessor_job_id'), 'video_generation_jobs', ['predecessor_job_id'], unique=False)
    op.create_index(op.f('ix_video_generation_jobs_reservation_id'), 'video_generation_jobs', ['reservation_id'], unique=False)
    op.create_index(op.f('ix_video_generation_jobs_script_id'), 'video_generation_jobs', ['script_id'], unique=False)
    op.create_index(op.f('ix_video_generation_jobs_shot_group_id'), 'video_generation_jobs', ['shot_group_id'], unique=False)
    op.create_index(op.f('ix_video_generation_jobs_unit_runtime_key'), 'video_generation_jobs', ['unit_runtime_key'], unique=False)
    op.create_table('media_storage_jobs',
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('idempotency_key', sa.String(length=256), nullable=False),
    sa.Column('job_type', sa.String(length=16), nullable=False),
    sa.Column('source_key', sa.String(length=512), nullable=False),
    sa.Column('final_key', sa.String(length=512), nullable=True),
    sa.Column('asset_id', sa.String(length=36), nullable=True),
    sa.Column('upload_id', sa.String(length=36), nullable=True),
    sa.Column('expected_sha256', sa.String(length=64), nullable=True),
    sa.Column('expected_size_bytes', sa.Integer(), nullable=True),
    sa.Column('status', sa.String(length=16), nullable=False),
    sa.Column('attempt', sa.Integer(), nullable=False),
    sa.Column('max_attempts', sa.Integer(), nullable=False),
    sa.Column('available_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('lease_owner', sa.String(length=128), nullable=True),
    sa.Column('lease_token', sa.String(length=128), nullable=True),
    sa.Column('lease_until', sa.DateTime(timezone=True), nullable=True),
    sa.Column('last_error_type', sa.String(length=128), nullable=True),
    sa.Column('last_error_message', sa.String(length=256), nullable=True),
    sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.ForeignKeyConstraint(['upload_id'], ['media_uploads.id'], name='fk_media_storage_jobs_upload_id_media_uploads', ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('idempotency_key', name='uq_media_storage_jobs_idempotency_key')
    )
    op.create_index(op.f('ix_media_storage_jobs_asset_id'), 'media_storage_jobs', ['asset_id'], unique=False)
    op.create_index(op.f('ix_media_storage_jobs_available_at'), 'media_storage_jobs', ['available_at'], unique=False)
    op.create_index(op.f('ix_media_storage_jobs_lease_owner'), 'media_storage_jobs', ['lease_owner'], unique=False)
    op.create_index(op.f('ix_media_storage_jobs_lease_token'), 'media_storage_jobs', ['lease_token'], unique=False)
    op.create_index(op.f('ix_media_storage_jobs_lease_until'), 'media_storage_jobs', ['lease_until'], unique=False)
    op.create_index(op.f('ix_media_storage_jobs_status'), 'media_storage_jobs', ['status'], unique=False)
    op.create_index(op.f('ix_media_storage_jobs_upload_id'), 'media_storage_jobs', ['upload_id'], unique=False)
    op.create_table('video_generation_idempotency_requests',
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('idempotency_key', sa.String(length=256), nullable=False),
    sa.Column('request_hash', sa.String(length=64), nullable=False),
    sa.Column('job_id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint("trim(idempotency_key) <> ''", name='ck_video_generation_idempotency_requests_key_not_blank'),
    sa.ForeignKeyConstraint(['job_id'], ['video_generation_jobs.job_id'], name='fk_video_idem_request_job', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], name='fk_video_idem_request_script', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('script_id', 'idempotency_key', name='pk_video_generation_idempotency_requests')
    )
    op.create_index(op.f('ix_video_generation_idempotency_requests_job_id'), 'video_generation_idempotency_requests', ['job_id'], unique=False)
    op.create_table('video_timeline_arrangements',
    sa.Column('script_id', sa.String(length=36), nullable=False),
    sa.Column('episode_id', sa.String(length=36), nullable=False),
    sa.Column('unit_runtime_key', sa.String(length=128), nullable=False),
    sa.Column('job_id', sa.String(length=36), nullable=False),
    sa.Column('row_order', sa.Integer(), nullable=False),
    sa.Column('is_selected', sa.Boolean(), nullable=False),
    sa.Column('trim_start_seconds', sa.Float(), nullable=False),
    sa.Column('trim_end_seconds', sa.Float(), nullable=True),
    sa.Column('version', sa.Integer(), nullable=False),
    sa.Column('id', sa.String(length=36), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.CheckConstraint('row_order >= 0', name='ck_timeline_row_order_non_negative'),
    sa.CheckConstraint('trim_end_seconds IS NULL OR trim_end_seconds > trim_start_seconds', name='ck_timeline_trim_end_after_start'),
    sa.CheckConstraint('trim_start_seconds >= 0', name='ck_timeline_trim_start_non_negative'),
    sa.ForeignKeyConstraint(['episode_id'], ['episodes.episode_id'], ),
    sa.ForeignKeyConstraint(['job_id'], ['video_generation_jobs.job_id'], ),
    sa.ForeignKeyConstraint(['script_id'], ['scripts.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('script_id', 'episode_id', 'row_order', name='uq_video_timeline_arrangements_script_episode_row_order')
    )
    op.create_index(op.f('ix_video_timeline_arrangements_episode_id'), 'video_timeline_arrangements', ['episode_id'], unique=False)
    op.create_index(op.f('ix_video_timeline_arrangements_job_id'), 'video_timeline_arrangements', ['job_id'], unique=False)
    op.create_index(op.f('ix_video_timeline_arrangements_script_id'), 'video_timeline_arrangements', ['script_id'], unique=False)
    op.create_index(op.f('ix_video_timeline_arrangements_unit_runtime_key'), 'video_timeline_arrangements', ['unit_runtime_key'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # 基线是 versions 目录唯一迁移文件，downgrade 到 base 等价于清空全部业务表。
    # 该操作对生产数据库是灾难性的，且 DROP SCHEMA 还会连带删除 alembic_version
    # 表导致 alembic 版本簿记失败，因此本迁移始终拒绝 downgrade。
    # 如需重置 schema：开发环境用 ``docker compose down -v`` 清库后重新 upgrade；
    # 生产环境必须先 pg_dump 备份，再人工 DROP 并重建 schema，切勿通过 alembic downgrade。
    raise RuntimeError(
        "基线迁移不支持 downgrade：该操作会删除全部业务表，可能造成不可逆数据丢失。"
        "重置 schema 请用 docker compose down -v（开发）或人工 DROP SCHEMA 后重建（生产，需先备份）。"
    )
