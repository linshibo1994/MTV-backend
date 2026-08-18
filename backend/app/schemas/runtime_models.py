"""面向前端公开的运行时模型配置契约。"""

from pydantic import BaseModel


class RuntimeModelOut(BaseModel):
    """一项模型能力当前实际使用的供应商与模型。"""

    provider: str
    model: str


class RuntimeVideoModelOut(RuntimeModelOut):
    """视频模型配置及其参考图审核要求。"""

    reference_image_audit_required: bool
    # 仅 Seedance/NeoLink 当前支持把参考视频真实传给上游模型。
    reference_video_supported: bool
    reference_video_limit: int


class RuntimeModelsOut(BaseModel):
    """当前运行时模型配置；不包含任何密钥、地址或鉴权状态。"""

    text_llm: RuntimeModelOut
    chat_llm: RuntimeModelOut
    image_gen: RuntimeModelOut
    video_gen: RuntimeVideoModelOut
