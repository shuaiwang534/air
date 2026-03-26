#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Table row pipeline:
1) Read output/table_rows.jsonl
2) Rewrite each row with LLM (faithful, short Chinese sentence)
3) Write output/table_sentences.jsonl
4) Build output/table_candidates.json for downstream merge
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

try:
    from llm.client import create_client
except Exception:
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from llm.client import create_client

try:
    from token_utils import estimate_tokens
except Exception:
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from token_utils import estimate_tokens


INPUT_TABLE_ROWS = "output/table_rows.jsonl"
OUTPUT_TABLE_SENTENCES = "output/table_sentences.jsonl"
OUTPUT_TABLE_CANDIDATES = "output/table_candidates.json"

DEFAULT_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_MODEL = "Qwen/Qwen2.5-72B-Instruct"
DEFAULT_GROUP_TOKEN_BUDGET = 220

SYSTEM_PROMPT_TABLE_ROW = """
你是“工程表格单行忠实改写器”。

输入是单条表格行 JSON，包含 table_id、header、cells、row_map 及章节信息。

请严格输出 JSON，不要输出其他文字。
要求：
1) faithful_text：仅基于本行内容，输出一句简短中文工程描述；
2) 必须忠实，不补充、不猜测、不跨行合并；
3) confidence：0.0~1.0；
4) insufficient_info：布尔值；
5) notes：可选说明，尽量简短。

输出格式必须是：
{
  "faithful_text": "string",
  "confidence": 0.0,
  "insufficient_info": false,
  "notes": "string"
}
""".strip()


def _clean_text(value: Any) -> str:
    text = str(value or "")
    return text.strip()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        num = float(value)
    except Exception:
        return default
    if num < 0.0:
        return 0.0
    if num > 1.0:
        return 1.0
    return num


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("true", "1", "yes", "y"):
            return True
        if low in ("false", "0", "no", "n"):
            return False
    return default


def _ensure_path(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    return [_clean_text(x) for x in value if _clean_text(x)]


def _fallback_faithful_text(header: List[Any], row_map: Dict[str, Any]) -> str:
    ordered_keys: List[str] = []
    seen = set()

    for raw_key in header:
        key = _clean_text(raw_key)
        if not key or key in seen:
            continue
        seen.add(key)
        ordered_keys.append(key)

    for raw_key in row_map.keys():
        key = _clean_text(raw_key)
        if not key or key in seen:
            continue
        seen.add(key)
        ordered_keys.append(key)

    parts = []
    for key in ordered_keys:
        val = _clean_text(row_map.get(key, ""))
        if not val:
            continue
        parts.append("{0}：{1}".format(key, val))

    if parts:
        return "；".join(parts) + "。"
    return "无有效表格信息。"


def _normalize_row_map(value: Any) -> Dict[str, str]:
    if not isinstance(value, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in value.items():
        key = _clean_text(k)
        if not key:
            continue
        out[key] = _clean_text(v)
    return out


def _load_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            try:
                obj = json.loads(text)
            except Exception:
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def _dump_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    out_dir = os.path.dirname(path) or "."
    os.makedirs(out_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _dump_json(path: str, payload: Any) -> None:
    out_dir = os.path.dirname(path) or "."
    os.makedirs(out_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _derive_system_tag(path: List[str], title: str) -> str:
    clean_path = [x for x in path if _clean_text(x)]
    if clean_path:
        return clean_path[-1]
    clean_title = _clean_text(title)
    if clean_title:
        return clean_title
    return "未分类"


def _rewrite_one_row(client, row: Dict[str, Any], use_llm: bool = True) -> Dict[str, Any]:
    row_map = _normalize_row_map(row.get("row_map", {}))
    header = row.get("header", [])
    if not isinstance(header, list):
        header = []
    payload = {
        "table_id": _clean_text(row.get("table_id")),
        "section_id": _clean_text(row.get("section_id")),
        "title": _clean_text(row.get("title")),
        "path": _ensure_path(row.get("path")),
        "row_index": int(row.get("row_index", 0) or 0),
        "header": header,
        "cells": row.get("cells", []),
        "row_map": row_map,
    }

    faithful_text = ""
    confidence = 0.0
    insufficient_info = False
    notes = ""

    if use_llm and client is not None:
        try:
            llm_ret = client.rewrite_table_row(
                row_payload=payload,
                system_prompt=SYSTEM_PROMPT_TABLE_ROW,
                temperature=0.1,
            )
            faithful_text = _clean_text(llm_ret.get("faithful_text"))
            confidence = _safe_float(llm_ret.get("confidence", 0.0), default=0.0)
            insufficient_info = _safe_bool(llm_ret.get("insufficient_info", False), default=False)
            notes = _clean_text(llm_ret.get("notes", ""))
        except Exception as e:
            notes = "llm_error: {0}".format(e)

    if not faithful_text:
        faithful_text = _fallback_faithful_text(payload.get("header", []), row_map)

    row_token_estimate = estimate_tokens(faithful_text)

    return {
        "table_id": payload["table_id"],
        "section_id": payload["section_id"],
        "title": payload["title"],
        "path": payload["path"],
        "row_index": payload["row_index"],
        "header": payload["header"],
        "cells": payload["cells"],
        "row_map": row_map,
        "faithful_text": faithful_text,
        "row_token_estimate": row_token_estimate,
        "confidence": confidence,
        "insufficient_info": insufficient_info,
        "notes": notes,
    }


def _extract_group_header(rows: List[Dict[str, Any]]) -> List[str]:
    header: List[str] = []
    seen = set()
    for row in rows:
        raw_header = row.get("header", [])
        if not isinstance(raw_header, list):
            continue
        for item in raw_header:
            key = _clean_text(item)
            if not key or key in seen:
                continue
            seen.add(key)
            header.append(key)
    for row in rows:
        row_map = _normalize_row_map(row.get("row_map", {}))
        for k in row_map.keys():
            key = _clean_text(k)
            if not key or key in seen:
                continue
            seen.add(key)
            header.append(key)
    return header


def _build_group_faithful_text(header: List[str], rows: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    if header:
        lines.append("字段：" + "｜".join(header))
    else:
        lines.append("字段：")

    for row in rows:
        row_map = _normalize_row_map(row.get("row_map", {}))
        row_index = int(row.get("row_index", 0) or 0)
        values = []
        for key in header:
            values.append(_clean_text(row_map.get(key, "")))
        if any(values):
            lines.append("[{0}] {1}".format(row_index, "｜".join(values)))
        else:
            fallback = _clean_text(row.get("faithful_text", ""))
            if fallback:
                lines.append("[{0}] {1}".format(row_index, fallback))
            else:
                lines.append("[{0}]".format(row_index))

    return "\n".join(lines).strip()


def _group_rows_by_table(
    rows: List[Dict[str, Any]],
    group_token_budget: int,
) -> List[Dict[str, Any]]:
    token_budget = int(group_token_budget or 0)
    if token_budget <= 0:
        token_budget = int(DEFAULT_GROUP_TOKEN_BUDGET)

    groups: List[Dict[str, Any]] = []
    table_rows: Dict[str, List[Dict[str, Any]]] = {}
    table_order: List[str] = []

    for row in rows:
        table_id = _clean_text(row.get("table_id", ""))
        if not table_id:
            table_id = "UNKNOWN_TABLE"
        if table_id not in table_rows:
            table_rows[table_id] = []
            table_order.append(table_id)

        # Ensure every row carries its own token estimate.
        row_copy = dict(row)
        row_text = _clean_text(row_copy.get("faithful_text"))
        row_copy["row_token_estimate"] = int(
            row_copy.get("row_token_estimate", estimate_tokens(row_text)) or estimate_tokens(row_text)
        )
        table_rows[table_id].append(row_copy)

    def build_group_record(table_id: str, chunk_index: int, chunk_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        first = chunk_rows[0]
        header = _extract_group_header(chunk_rows)
        row_indices = [int(x.get("row_index", 0) or 0) for x in chunk_rows]
        row_maps = [_normalize_row_map(x.get("row_map", {})) for x in chunk_rows]
        row_token_estimates = [int(x.get("row_token_estimate", 0) or 0) for x in chunk_rows]
        faithful_text = _build_group_faithful_text(header, chunk_rows)
        token_estimate = estimate_tokens(faithful_text)
        has_oversize_row = any([x > token_budget for x in row_token_estimates])
        return {
            "table_id": table_id,
            "section_id": _clean_text(first.get("section_id", "")),
            "title": _clean_text(first.get("title", "")),
            "path": _ensure_path(first.get("path", [])),
            "chunk_index": chunk_index,
            "row_start": min(row_indices) if row_indices else 0,
            "row_end": max(row_indices) if row_indices else 0,
            "row_count": len(chunk_rows),
            "row_indices": row_indices,
            "header": header,
            "row_maps": row_maps,
            "row_token_estimates": row_token_estimates,
            "row_token_sum": sum(row_token_estimates),
            "faithful_text": faithful_text,
            "token_estimate": token_estimate,
            "token_budget": token_budget,
            "has_oversize_row": has_oversize_row,
        }

    for table_id in table_order:
        items = sorted(
            table_rows.get(table_id, []),
            key=lambda x: int(x.get("row_index", 0) or 0),
        )
        chunk_index = 0

        buf: List[Dict[str, Any]] = []
        buf_token_sum = 0
        for row in items:
            row_tokens = int(row.get("row_token_estimate", 0) or 0)

            # Oversize row should occupy a dedicated chunk.
            if row_tokens > token_budget:
                if buf:
                    chunk_index += 1
                    groups.append(build_group_record(table_id, chunk_index, buf))
                    buf = []
                    buf_token_sum = 0
                chunk_index += 1
                groups.append(build_group_record(table_id, chunk_index, [row]))
                continue

            if not buf:
                buf = [row]
                buf_token_sum = row_tokens
                continue

            if buf_token_sum + row_tokens <= token_budget:
                buf.append(row)
                buf_token_sum += row_tokens
            else:
                chunk_index += 1
                groups.append(build_group_record(table_id, chunk_index, buf))
                buf = [row]
                buf_token_sum = row_tokens

        if buf:
            chunk_index += 1
            groups.append(build_group_record(table_id, chunk_index, buf))

    return groups


def _chunk_sentence_to_candidate(chunk_item: Dict[str, Any]) -> Dict[str, Any]:
    path = _ensure_path(chunk_item.get("path", []))
    title = _clean_text(chunk_item.get("title"))
    return {
        "section_id": _clean_text(chunk_item.get("section_id")),
        "title": title,
        "path": path,
        "content": _clean_text(chunk_item.get("faithful_text")),
        "system_tag": _derive_system_tag(path=path, title=title),
        "components": [],
        "interfaces": [],
        "functions": [],
        "logic_rules": [],
        "table_id": _clean_text(chunk_item.get("table_id")),
        "chunk_index": int(chunk_item.get("chunk_index", 0) or 0),
        "row_start": int(chunk_item.get("row_start", 0) or 0),
        "row_end": int(chunk_item.get("row_end", 0) or 0),
        "row_count": int(chunk_item.get("row_count", 0) or 0),
        "row_indices": chunk_item.get("row_indices", []),
        "row_maps": chunk_item.get("row_maps", []),
        "row_token_estimates": chunk_item.get("row_token_estimates", []),
        "row_token_sum": int(chunk_item.get("row_token_sum", 0) or 0),
        "token_estimate": int(chunk_item.get("token_estimate", 0) or 0),
        "token_budget": int(chunk_item.get("token_budget", 0) or 0),
        "has_oversize_row": bool(chunk_item.get("has_oversize_row", False)),
    }


def run_table_pipeline(
    input_file: str = INPUT_TABLE_ROWS,
    sentences_output: str = OUTPUT_TABLE_SENTENCES,
    candidates_output: str = OUTPUT_TABLE_CANDIDATES,
    base_url: str = DEFAULT_BASE_URL,
    model: str = DEFAULT_MODEL,
    api_key: Optional[str] = None,
    use_llm: bool = False,
    group_token_budget: int = DEFAULT_GROUP_TOKEN_BUDGET,
) -> List[Dict[str, Any]]:
    rows = _load_jsonl(input_file)
    print("table rows loaded: {0}".format(len(rows)))

    client = None
    if use_llm and rows:
        client = create_client(base_url=base_url, model=model, api_key=api_key)

    rewritten_rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        print("[table row {0}/{1}] table_id={2} row_index={3}".format(
            idx, len(rows), row.get("table_id"), row.get("row_index")
        ))
        rewritten_rows.append(_rewrite_one_row(client=client, row=row, use_llm=use_llm))

    sentence_rows = _group_rows_by_table(
        rewritten_rows,
        group_token_budget=group_token_budget,
    )

    _dump_jsonl(sentences_output, sentence_rows)

    candidates = [_chunk_sentence_to_candidate(x) for x in sentence_rows if _clean_text(x.get("faithful_text"))]
    _dump_json(candidates_output, candidates)

    print("table groups: {0}".format(len(sentence_rows)))
    print("table candidates: {0}".format(len(candidates)))
    print("group mode: token_budget={0}".format(int(group_token_budget or 0)))
    print("[OUT] sentences : {0}".format(os.path.abspath(sentences_output)))
    print("[OUT] candidates: {0}".format(os.path.abspath(candidates_output)))
    return candidates


def main() -> None:
    parser = argparse.ArgumentParser(description="Run row-wise table rewrite pipeline.")
    parser.add_argument("--input", default=INPUT_TABLE_ROWS, help="Input table_rows.jsonl")
    parser.add_argument("--sentences-output", default=OUTPUT_TABLE_SENTENCES, help="Output table_sentences.jsonl")
    parser.add_argument("--candidates-output", default=OUTPUT_TABLE_CANDIDATES, help="Output table_candidates.json")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="LLM base URL")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="LLM model")
    parser.add_argument("--api-key", default=None, help="API key")
    parser.add_argument("--group-token-budget", type=int, default=DEFAULT_GROUP_TOKEN_BUDGET, help="Token budget per merged group")
    parser.add_argument("--use-llm", action="store_true", help="Enable LLM rewrite (default: disabled)")
    parser.add_argument("--no-llm", action="store_true", help="Force disable LLM rewrite")
    args = parser.parse_args()

    run_table_pipeline(
        input_file=args.input,
        sentences_output=args.sentences_output,
        candidates_output=args.candidates_output,
        base_url=args.base_url,
        model=args.model,
        api_key=args.api_key,
        use_llm=(args.use_llm and not args.no_llm),
        group_token_budget=args.group_token_budget,
    )


if __name__ == "__main__":
    main()
