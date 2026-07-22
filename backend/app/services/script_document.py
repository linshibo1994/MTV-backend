"""剧本文档上传与安全文本解析。"""

from __future__ import annotations

import hashlib
import os
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree

from fastapi import HTTPException, UploadFile, status

SUPPORTED_SCRIPT_SUFFIXES = frozenset({".txt", ".docx"})
_CHUNK_SIZE = 1024 * 1024
_MAX_SCRIPT_TEXT_CHARS = 2_000_000
_MAX_SCRIPT_TEXT_BYTES = 8 * 1024 * 1024
_MAX_DOCX_MEMBERS = 2_000
_MAX_DOCX_UNCOMPRESSED_BYTES = 32 * 1024 * 1024
_MAX_DOCX_COMPRESSION_RATIO = 100
_WORDPROCESSING_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"


@dataclass(frozen=True)
class StagedScriptDocument:
    """已落盘、已解析的上传剧本；调用方负责完成后清理临时文件。"""

    original_filename: str
    suffix: str
    content_type: str | None
    size_bytes: int
    sha256: str
    script_text: str
    temp_path: str


def _normalized_filename(filename: str | None) -> tuple[str, str]:
    name = Path(filename or "").name.strip()
    if not name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="缺少剧本文档文件名")
    suffix = Path(name).suffix.lower()
    if suffix not in SUPPORTED_SCRIPT_SUFFIXES:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="仅支持 .txt 和 .docx 格式的剧本文档",
        )
    return name, suffix


def _validate_script_text(value: str) -> str:
    if "\x00" in value:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="剧本文档包含非法二进制内容")
    text = value.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="剧本文档没有可分析的文本")
    if len(text) > _MAX_SCRIPT_TEXT_CHARS:
        raise HTTPException(status_code=status.HTTP_413_CONTENT_TOO_LARGE, detail="剧本文本过长")
    return text


def _parse_txt(path: str) -> str:
    if os.path.getsize(path) > _MAX_SCRIPT_TEXT_BYTES:
        raise HTTPException(status_code=status.HTTP_413_CONTENT_TOO_LARGE, detail="剧本文本过长")
    raw = Path(path).read_bytes()
    for encoding in ("utf-8-sig", "utf-16", "gb18030"):
        try:
            return _validate_script_text(raw.decode(encoding))
        except UnicodeDecodeError:
            continue
    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="无法识别 .txt 文档编码")


def _parse_docx(path: str) -> str:
    try:
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
            if not members or len(members) > _MAX_DOCX_MEMBERS:
                raise ValueError("DOCX 压缩包成员数量异常")
            total_size = sum(member.file_size for member in members)
            if total_size > _MAX_DOCX_UNCOMPRESSED_BYTES:
                raise ValueError("DOCX 解压后内容过大")
            for member in members:
                if member.flag_bits & 0x1:
                    raise ValueError("不支持加密的 DOCX 文档")
                if member.compress_size and member.file_size > member.compress_size * _MAX_DOCX_COMPRESSION_RATIO:
                    raise ValueError("DOCX 压缩比异常")
            try:
                xml_data = archive.read("word/document.xml")
            except KeyError as exc:
                raise ValueError("DOCX 缺少正文内容") from exc
    except (OSError, zipfile.BadZipFile, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"无法解析 DOCX 文档: {exc}") from exc

    try:
        root = ElementTree.fromstring(xml_data)
    except ElementTree.ParseError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="DOCX 正文 XML 损坏") from exc

    paragraphs: list[str] = []
    text_length = 0
    for paragraph in root.findall(f".//{{{_WORDPROCESSING_NS}}}p"):
        value = "".join(node.text or "" for node in paragraph.findall(f".//{{{_WORDPROCESSING_NS}}}t"))
        if value.strip():
            text_length += len(value) + 1
            if text_length > _MAX_SCRIPT_TEXT_CHARS:
                raise HTTPException(
                    status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                    detail="剧本文本过长",
                )
            paragraphs.append(value)
    return _validate_script_text("\n".join(paragraphs))


def parse_script_document(path: str, suffix: str) -> str:
    """从已完成大小限制的临时文件解析正文。"""
    if suffix == ".txt":
        return _parse_txt(path)
    if suffix == ".docx":
        return _parse_docx(path)
    raise HTTPException(status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, detail="不支持的剧本文档格式")


async def stage_script_document(file: UploadFile, *, max_bytes: int) -> StagedScriptDocument:
    """流式接收、限制大小并解析上传文件，避免把文件整体留在内存。"""
    filename, suffix = _normalized_filename(file.filename)
    total = 0
    digest = hashlib.sha256()
    tmp = tempfile.NamedTemporaryFile(mode="w+b", suffix=suffix, delete=False)
    temp_path = tmp.name
    try:
        while True:
            chunk = await file.read(_CHUNK_SIZE)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise HTTPException(
                    status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                    detail=f"上传文件过大，上限 {max_bytes} 字节",
                )
            digest.update(chunk)
            tmp.write(chunk)
        if total == 0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="上传文件为空")
        tmp.flush()
        script_text = parse_script_document(temp_path, suffix)
        return StagedScriptDocument(
            original_filename=filename,
            suffix=suffix,
            content_type=file.content_type,
            size_bytes=total,
            sha256=digest.hexdigest(),
            script_text=script_text,
            temp_path=temp_path,
        )
    except Exception:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass
        raise
    finally:
        tmp.close()


def cleanup_staged_script_document(document: StagedScriptDocument) -> None:
    """尽力删除上传解析产生的临时文件。"""
    try:
        os.unlink(document.temp_path)
    except FileNotFoundError:
        pass
