"""补齐提示词模板与续帧任务迁移

Revision ID: d4b7e1c9a362
Revises: 99ac501e725c
Create Date: 2026-08-12 20:35:00
"""

import hashlib
from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "d4b7e1c9a362"
down_revision: str | None = "99ac501e725c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_VIDEO_JOB_TABLE = "video_generation_jobs"
_VIDEO_STATUS_CONSTRAINT = "ck_video_jobs_status"
_VIDEO_STATUS_TEMP_CONSTRAINT = f"{_VIDEO_STATUS_CONSTRAINT}_replacement"
_CURRENT_VIDEO_STATUS_CHECK = (
    "status IN ('starting', 'processing', 'succeeded', 'failed', 'canceled')"
)
_CONTINUATION_VIDEO_STATUS_CHECK = (
    "status IN ('waiting_dependency', 'starting', 'processing', 'succeeded', 'failed', 'canceled')"
)

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


def _replace_video_status_constraint(condition: str) -> None:
    """无空窗替换视频任务状态约束，避免迁移期间放宽写入校验。"""
    op.execute(
        sa.text(
            f"ALTER TABLE {_VIDEO_JOB_TABLE} "
            f"ADD CONSTRAINT {_VIDEO_STATUS_TEMP_CONSTRAINT} CHECK ({condition}) NOT VALID"
        )
    )
    op.execute(
        sa.text(
            f"ALTER TABLE {_VIDEO_JOB_TABLE} "
            f"VALIDATE CONSTRAINT {_VIDEO_STATUS_TEMP_CONSTRAINT}"
        )
    )
    op.drop_constraint(_VIDEO_STATUS_CONSTRAINT, _VIDEO_JOB_TABLE, type_="check")
    op.execute(
        sa.text(
            f"ALTER TABLE {_VIDEO_JOB_TABLE} "
            f"RENAME CONSTRAINT {_VIDEO_STATUS_TEMP_CONSTRAINT} TO {_VIDEO_STATUS_CONSTRAINT}"
        )
    )


def upgrade() -> None:
    op.create_table(
        "prompt_templates",
        sa.Column("key", sa.String(length=128), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("variables_schema", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("created_by_user_id", sa.String(length=36), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("version >= 1", name="ck_prompt_templates_version_positive"),
        sa.CheckConstraint("trim(key) <> ''", name="ck_prompt_templates_key_not_blank"),
        sa.CheckConstraint("trim(content) <> ''", name="ck_prompt_templates_content_not_blank"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key", "version", name="uq_prompt_templates_key_version"),
    )
    op.create_index(
        op.f("ix_prompt_templates_content_sha256"),
        "prompt_templates",
        ["content_sha256"],
        unique=False,
    )
    op.create_index(
        op.f("ix_prompt_templates_created_by_user_id"),
        "prompt_templates",
        ["created_by_user_id"],
        unique=False,
    )
    op.create_index(op.f("ix_prompt_templates_is_active"), "prompt_templates", ["is_active"], unique=False)
    op.create_index(op.f("ix_prompt_templates_key"), "prompt_templates", ["key"], unique=False)
    op.create_index(
        "uq_prompt_templates_active_key",
        "prompt_templates",
        ["key"],
        unique=True,
        postgresql_where=sa.text("is_active = true"),
        sqlite_where=sa.text("is_active = 1"),
    )
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

    op.add_column(_VIDEO_JOB_TABLE, sa.Column("predecessor_job_id", sa.String(length=36), nullable=True))
    op.create_index(
        op.f("ix_video_generation_jobs_predecessor_job_id"),
        _VIDEO_JOB_TABLE,
        ["predecessor_job_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_video_jobs_predecessor_job",
        _VIDEO_JOB_TABLE,
        _VIDEO_JOB_TABLE,
        ["predecessor_job_id"],
        ["job_id"],
        ondelete="SET NULL",
    )
    _replace_video_status_constraint(_CONTINUATION_VIDEO_STATUS_CHECK)


def downgrade() -> None:
    # 旧状态约束无法表达等待前驱任务，拒绝丢失这类任务状态。
    op.execute(sa.text(f"LOCK TABLE {_VIDEO_JOB_TABLE} IN SHARE ROW EXCLUSIVE MODE"))
    op.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM video_generation_jobs
                    WHERE status = 'waiting_dependency'
                ) THEN
                    RAISE EXCEPTION '存在等待前驱的视频任务，拒绝降级以避免数据丢失';
                END IF;
            END
            $$
            """
        )
    )
    _replace_video_status_constraint(_CURRENT_VIDEO_STATUS_CHECK)
    op.drop_constraint(
        "fk_video_jobs_predecessor_job",
        _VIDEO_JOB_TABLE,
        type_="foreignkey",
    )
    op.drop_index(op.f("ix_video_generation_jobs_predecessor_job_id"), table_name=_VIDEO_JOB_TABLE)
    op.drop_column(_VIDEO_JOB_TABLE, "predecessor_job_id")

    op.drop_index("uq_prompt_templates_active_key", table_name="prompt_templates")
    op.drop_index(op.f("ix_prompt_templates_key"), table_name="prompt_templates")
    op.drop_index(op.f("ix_prompt_templates_is_active"), table_name="prompt_templates")
    op.drop_index(op.f("ix_prompt_templates_created_by_user_id"), table_name="prompt_templates")
    op.drop_index(op.f("ix_prompt_templates_content_sha256"), table_name="prompt_templates")
    op.drop_table("prompt_templates")
