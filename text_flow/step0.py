# semantic_pipeline/step0.py
import json
import os
import re
import sys
from typing import Any, Dict, List

from io_utils import load_sections
from token_utils import estimate_tokens


def _normalize_budget(token_budget: int, fallback: int = 240) -> int:
    value = int(token_budget or 0)
    if value <= 0:
        return int(fallback)
    return value


def _safe_print(text: str) -> None:
    try:
        print(text)
    except UnicodeEncodeError:
        encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
        safe = str(text).encode(encoding, errors="replace").decode(encoding, errors="replace")
        print(safe)


def _split_section_into_paragraphs(text: str) -> List[str]:
    if text is None:
        return []

    normalized = str(text).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return []

    # Prefer blank-line paragraph boundaries.
    if re.search(r"\n\s*\n", normalized):
        raw_parts = re.split(r"\n\s*\n+", normalized)
    else:
        raw_parts = normalized.split("\n")

    return [p.strip() for p in raw_parts if p and p.strip()]


def _split_text_by_token_budget(text: str, token_budget: int) -> List[str]:
    t = str(text or "").strip()
    if not t:
        return []

    budget = _normalize_budget(token_budget)
    parts: List[str] = []
    buf = ""

    for ch in t:
        trial = buf + ch
        if buf and estimate_tokens(trial) > budget:
            part = buf.strip()
            if part:
                parts.append(part)
            buf = ch
        else:
            buf = trial

    if buf.strip():
        parts.append(buf.strip())

    return parts if parts else [t]


def _split_by_sentence_delimiters(text: str) -> List[str]:
    if not text:
        return []

    # Use Unicode escapes to avoid encoding issues on different environments.
    end_marks = set([
        u"\u3002",  # 。
        u"\uff01",  # ！
        u"\uff1f",  # ？
        u"\uff1b",  # ；
        u"\uff1a",  # ：
        "!",
        "?",
        ";",
        ":",
    ])
    parts: List[str] = []
    buf: List[str] = []

    for ch in text:
        buf.append(ch)
        if ch in end_marks:
            seg = "".join(buf).strip()
            if seg:
                parts.append(seg)
            buf = []

    if buf:
        tail = "".join(buf).strip()
        if tail:
            parts.append(tail)

    return parts


def _split_oversize_paragraph(paragraph: str, token_budget: int) -> List[str]:
    text = str(paragraph or "").strip()
    if not text:
        return []

    budget = _normalize_budget(token_budget)
    if estimate_tokens(text) <= budget:
        return [text]

    # First try sentence-like boundaries.
    sentences = _split_by_sentence_delimiters(text)
    if len(sentences) <= 1:
        return _split_text_by_token_budget(text, budget)

    units: List[str] = []
    for sent in sentences:
        if estimate_tokens(sent) <= budget:
            units.append(sent)
        else:
            units.extend(_split_text_by_token_budget(sent, budget))

    packed: List[str] = []
    buf = ""
    for unit in units:
        if not buf:
            buf = unit
            continue
        trial = buf + unit
        if estimate_tokens(trial) <= budget:
            buf = trial
        else:
            if buf.strip():
                packed.append(buf.strip())
            buf = unit

    if buf.strip():
        packed.append(buf.strip())

    return packed if packed else _split_text_by_token_budget(text, budget)


def _pack_section_content(section: Any, token_budget: int) -> List[Dict[str, Any]]:
    budget = _normalize_budget(token_budget)
    content = str(getattr(section, "content", "") or "").strip()
    if not content:
        return []

    section_id = getattr(section, "section_id", "")
    title = getattr(section, "title", "")
    path = getattr(section, "path", [])
    if not isinstance(path, list):
        path = []

    full_section_token_estimate = estimate_tokens(content)
    paragraphs = _split_section_into_paragraphs(content)
    if not paragraphs:
        paragraphs = [content]

    if full_section_token_estimate <= budget:
        text = "\n\n".join(paragraphs).strip()
        return [
            {
                "section_id": section_id,
                "title": title,
                "path": path,
                "order": 1,
                "chunk_index": 1,
                "text": text,
                "source": "section_pack",
                "token_estimate": estimate_tokens(text),
                "token_budget": budget,
                "part_count": len(paragraphs) if paragraphs else 1,
                "full_section_token_estimate": full_section_token_estimate,
                "para_start": 1,
                "para_end": len(paragraphs) if paragraphs else 1,
                "is_whole_section": True,
            }
        ]

    units: List[Dict[str, Any]] = []
    for idx, para in enumerate(paragraphs, start=1):
        para_text = str(para or "").strip()
        if not para_text:
            continue

        if estimate_tokens(para_text) <= budget:
            units.append({"text": para_text, "para_index": idx})
            continue

        split_parts = _split_oversize_paragraph(para_text, budget)
        for part in split_parts:
            part_text = str(part or "").strip()
            if part_text:
                units.append({"text": part_text, "para_index": idx})

    if not units:
        return []

    def build_chunk_text(chunk_units: List[Dict[str, Any]]) -> str:
        return "\n\n".join([x["text"] for x in chunk_units if x.get("text")]).strip()

    chunk_units_list: List[List[Dict[str, Any]]] = []
    buf_units: List[Dict[str, Any]] = []

    for unit in units:
        if not buf_units:
            buf_units = [unit]
            continue

        trial_units = buf_units + [unit]
        trial_text = build_chunk_text(trial_units)
        if estimate_tokens(trial_text) <= budget:
            buf_units = trial_units
        else:
            chunk_units_list.append(buf_units)
            buf_units = [unit]

    if buf_units:
        chunk_units_list.append(buf_units)

    packed: List[Dict[str, Any]] = []
    for idx, chunk_units in enumerate(chunk_units_list, start=1):
        text = build_chunk_text(chunk_units)
        para_indexes = [int(x.get("para_index", 0) or 0) for x in chunk_units]
        para_indexes = [x for x in para_indexes if x > 0]
        packed.append(
            {
                "section_id": section_id,
                "title": title,
                "path": path,
                "order": idx,
                "chunk_index": idx,
                "text": text,
                "source": "section_pack",
                "token_estimate": estimate_tokens(text),
                "token_budget": budget,
                "part_count": len(chunk_units),
                "full_section_token_estimate": full_section_token_estimate,
                "para_start": min(para_indexes) if para_indexes else 0,
                "para_end": max(para_indexes) if para_indexes else 0,
                "is_whole_section": False,
            }
        )

    return packed


def run_step0(
    input_jsonl: str = "section_chunks.jsonl",
    output_file: str = "output/paragraph_blocks.json",
    token_budget: int = 240,
) -> List[Dict[str, Any]]:
    sections = load_sections(input_jsonl)
    use_budget = _normalize_budget(token_budget)

    output_data: List[Dict[str, Any]] = []
    for sec in sections:
        _safe_print("\n=== Section {0} | {1} ===".format(sec.section_id, sec.title))

        packed_items = _pack_section_content(sec, token_budget=use_budget)
        if not packed_items:
            _safe_print("(no content)")
            continue

        full_section_token = int(packed_items[0].get("full_section_token_estimate", 0) or 0)
        chunk_tokens = [int(x.get("token_estimate", 0) or 0) for x in packed_items]
        _safe_print(
            "full_token={0} chunks={1} budget={2}".format(
                full_section_token, len(packed_items), use_budget
            )
        )
        _safe_print("chunk_tokens={0}".format(chunk_tokens))

        for item in packed_items:
            preview = str(item.get("text", "")).replace("\n", " | ").strip()
            if len(preview) > 180:
                preview = preview[:177] + "..."
            _safe_print(
                "[{0:02d}] ({1}) tk={2} parts={3} {4}".format(
                    int(item.get("order", 0) or 0),
                    item.get("source"),
                    int(item.get("token_estimate", 0) or 0),
                    int(item.get("part_count", 1) or 1),
                    preview,
                )
            )
            output_data.append({
                "section_id": item["section_id"],
                "title": item["title"],
                "path": item["path"],
                "order": item["order"],
                "text": item["text"],
                "source": item["source"],
                "token_estimate": item["token_estimate"],
                "part_count": item["part_count"],
                "token_budget": item["token_budget"],
                "full_section_token_estimate": item["full_section_token_estimate"],
                "chunk_index": item["chunk_index"],
                "para_start": item.get("para_start", 0),
                "para_end": item.get("para_end", 0),
                "is_whole_section": bool(item.get("is_whole_section", False)),
            })

    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    return output_data


def main():
    run_step0()


if __name__ == "__main__":
    main()

