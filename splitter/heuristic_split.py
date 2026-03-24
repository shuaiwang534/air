# semantic_pipeline/splitter/heuristic_split.py
from dataclasses import dataclass
from typing import List
import re


@dataclass(frozen=True)
class Candidate:
    text: str
    order: int
    source: str


# a） b） c) / 1） / （1）
_ENUM_PATTERN = re.compile(
    r"(?m)^\s*(?:[a-zA-Z]\s*[）\)]|\d+\s*[）\)]|[（(]\d+[）)])\s*"
)

_FIGURE_PATTERN = re.compile(r"^\s*(图|表)\s*\d+")

_TABLE_PATTERN = re.compile(r"^\s*\|(.+\|)+\s*$", re.MULTILINE)


def _split_by_enum(text: str) -> List[str]:
    matches = list(_ENUM_PATTERN.finditer(text))
    if len(matches) <= 1:
        return [text]

    blocks = []
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        part = text[start:end].strip()
        if part:
            blocks.append(part)

    return blocks if blocks else [text]


def _split_by_semicolon(text: str) -> List[str]:
    if "；" not in text or len(text) < 40:
        return [text]

    parts = [p.strip() for p in text.split("；") if p.strip()]
    if len(parts) <= 1:
        return [text]

    merged = []
    for p in parts:
        if merged and len(p) < 12:
            merged[-1] += "；" + p
        else:
            merged.append(p)

    return merged if merged else [text]


def _is_table(text: str) -> bool:
    """
    检测文本是否为表格格式（使用 | 作为列分隔符）
    """
    lines = text.strip().split("\n")
    
    # 表格标记：以 [表格] 开头
    if lines and "[表格]" in lines[0]:
        return True
    
    # 表格特征：多行且至少80%的行包含 | 符号
    if len(lines) < 2:
        return False
    
    table_lines = [line for line in lines if "|" in line]
    return len(table_lines) >= 2 and len(table_lines) >= len(lines) * 0.8


def heuristic_split(paragraph_text: str) -> List[Candidate]:
    base = paragraph_text
    if not base:
        return []
    if _FIGURE_PATTERN.match(base):
        return [Candidate(text=base, order=1, source="figure")]
    if _is_table(base):
        return [Candidate(text=base, order=1, source="table")]
    if "|" in base and not ";" in base and base.count("|") >= 1:
        return [Candidate(text=base, order=1, source="table")]
    return [Candidate(text=base, order=1, source="para")]
