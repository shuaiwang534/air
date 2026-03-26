# semantic_pipeline/io_utils.py
import json
from typing import List
from semantic_block.models import Section


def load_sections(jsonl_path: str) -> List[Section]:
    """
    读取 JSONL 格式的 section 输入
    """
    sections: List[Section] = []

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            obj = json.loads(line)

            sections.append(
                Section(
                    section_id=str(obj.get("section_id", "") or ""),
                    title=obj.get("title", ""),
                    path=obj.get("path", []),
                    content=obj.get("content", "") or ""
                )
            )

    return sections
