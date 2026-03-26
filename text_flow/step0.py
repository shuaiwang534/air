# semantic_pipeline/step0.py
import json
import os
import sys
from typing import Any, Dict, List

from io_utils import load_sections
from semantic_block.builder import build_candidates
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


def _merge_candidates_within_section(
    raw_items: List[Dict[str, Any]],
    token_budget: int,
) -> List[Dict[str, Any]]:
    if not raw_items:
        return []

    budget = _normalize_budget(token_budget)
    merged: List[Dict[str, Any]] = []
    buf: List[Dict[str, Any]] = []
    next_order = 1

    def flush_buffer() -> None:
        nonlocal buf, next_order
        if not buf:
            return

        first = buf[0]
        text = "\n".join([x["text"] for x in buf if x["text"]]).strip()
        if not text:
            buf = []
            return

        merged.append(
            {
                "section_id": first["section_id"],
                "title": first["title"],
                "path": first["path"],
                "order": next_order,
                "text": text,
                "source": first["source"] if len(set([x["source"] for x in buf])) == 1 else "merged",
                "token_estimate": estimate_tokens(text),
                "part_count": len(buf),
                "token_budget": budget,
            }
        )
        next_order += 1
        buf = []

    for item in raw_items:
        source = str(item.get("source") or "").strip().lower()
        item_text = str(item.get("text") or "").strip()
        if not item_text:
            continue

        # Keep figure/table boundaries intact.
        if source in ("figure", "table"):
            flush_buffer()
            merged.append(
                {
                    "section_id": item["section_id"],
                    "title": item["title"],
                    "path": item["path"],
                    "order": next_order,
                    "text": item_text,
                    "source": item["source"],
                    "token_estimate": estimate_tokens(item_text),
                    "part_count": 1,
                    "token_budget": budget,
                }
            )
            next_order += 1
            continue

        if not buf:
            buf = [item]
            continue

        same_source = str(buf[0].get("source")) == str(item.get("source"))
        candidate_text = ("\n".join([x["text"] for x in buf] + [item_text])).strip()
        if same_source and estimate_tokens(candidate_text) <= budget:
            buf.append(item)
        else:
            flush_buffer()
            buf = [item]

    flush_buffer()
    return merged


def run_step0(
    input_jsonl: str = "section_chunks.jsonl",
    output_file: str = "output/paragraph_blocks.json",
    token_budget: int = 240,
) -> List[Dict[str, Any]]:
    sections = load_sections(input_jsonl)
    use_budget = _normalize_budget(token_budget)

    output_data = []
    for sec in sections:
        _safe_print("\n=== Section {0} | {1} ===".format(sec.section_id, sec.title))

        cands = build_candidates(sec)
        if not cands:
            _safe_print("(no candidates)")
            continue

        raw_items: List[Dict[str, Any]] = []
        for c in cands:
            raw_items.append({
                "section_id": c.section_id,
                "title": c.title,
                "path": c.path,
                "order": c.order,
                "text": c.text,
                "source": c.source,
            })

        merged_items = _merge_candidates_within_section(raw_items, token_budget=use_budget)
        _safe_print(
            "raw={0} merged={1} budget={2}".format(
                len(raw_items), len(merged_items), use_budget
            )
        )

        for item in merged_items:
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
            })

    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    return output_data


def main():
    run_step0()


if __name__ == "__main__":
    main()
