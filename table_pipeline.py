#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Table-only pipeline:
1) Load output/tables_raw.jsonl
2) Rewrite each table row into faithful engineering text
3) Build output/table_summary.json
4) Build output/table_candidates.json
"""

import argparse
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from llm.client import create_client


INPUT_TABLES_RAW = "output/tables_raw.jsonl"
OUTPUT_TABLE_SUMMARY = "output/table_summary.json"
OUTPUT_TABLE_CANDIDATES = "output/table_candidates.json"

DEFAULT_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DEFAULT_MODEL = "Qwen/Qwen2.5-72B-Instruct"

SYSTEM_PROMPT_TABLE_REWRITE = """
你是一个“工程表格单行忠实改写器”。

输入是某张表格中的单行数据（含 header、row_map、弱提示）。
请输出严格 JSON，不要输出解释性文字，不要输出 Markdown。

目标：
1. faithful_text：把该“单行”改写成1~2句简短工程文字。
2. 必须忠实于该行，不补充、不猜测、不跨行合并、不引入外部知识。
3. 如果该行信息不足，仍需输出，insufficient_info 设为 true，并在 notes 说明不足点。
4. table_role_hint 只能从以下枚举中选择一个：
   - Component
   - Interface
   - Function
   - LogicRule
   - Other
5. confidence 范围 0.0~1.0。

输出 JSON 结构（必须严格遵守）：
{
  "faithful_text": string,
  "table_role_hint": "Component | Interface | Function | LogicRule | Other",
  "confidence": number,
  "insufficient_info": boolean,
  "notes": string
}
""".strip()

ROLE_KEYWORDS = {
    "Component": ["部件", "组件", "模块", "单元", "设备", "构成", "组成", "component", "unit", "module"],
    "Interface": ["接口", "信号", "输入", "输出", "发送", "接收", "通信", "总线", "interface", "signal", "source", "target"],
    "Function": ["功能", "用途", "能力", "作用", "function", "capability"],
    "LogicRule": ["条件", "规则", "逻辑", "模式", "阈值", "触发", "限制", "logic", "rule", "condition"],
}

NAME_KEYWORDS = ["名称", "名", "name", "组件", "部件", "单元", "模块", "设备", "功能", "规则"]
SOURCE_KEYWORDS = ["源", "发送", "source", "from", "输入", "input"]
TARGET_KEYWORDS = ["目标", "接收", "target", "to", "输出", "output"]
TRIGGER_KEYWORDS = ["触发", "条件", "前提", "when", "if", "condition"]
ACTION_KEYWORDS = ["动作", "行为", "执行", "处理", "action", "do"]
LOGIC_TARGET_KEYWORDS = ["对象", "目标", "作用于", "target"]


def _ensure_list_str(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    out: List[str] = []
    for v in values:
        if isinstance(v, str):
            out.append(v)
        elif v is None:
            out.append("")
        else:
            out.append(str(v))
    return out


def _clean_text(text: Any) -> str:
    s = str(text or "")
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()


def _normalize_header(raw_header: List[str], width: int) -> List[str]:
    header = [_clean_text(x) for x in raw_header[:width]]
    while len(header) < width:
        header.append("")
    result: List[str] = []
    for i, name in enumerate(header, start=1):
        result.append(name if name else "col_{0}".format(i))
    return result


def _normalize_row(raw_row: List[str], width: int) -> List[str]:
    row = [_clean_text(x) for x in raw_row[:width]]
    while len(row) < width:
        row.append("")
    return row


def _calc_width(header: List[str], rows: List[List[str]]) -> int:
    width = len(header)
    for row in rows:
        width = max(width, len(row))
    return width


def _build_row_map(header: List[str], row: List[str]) -> Dict[str, str]:
    return {header[i]: row[i] for i in range(min(len(header), len(row)))}


def _contains_any(text: str, keywords: List[str]) -> bool:
    low = text.lower()
    for kw in keywords:
        if kw.lower() in low:
            return True
    return False


def _build_weak_role_hints(header: List[str], row_map: Dict[str, str]) -> List[str]:
    text = " ".join(header + list(row_map.keys()) + list(row_map.values()))
    hints: List[str] = []
    for role, keywords in ROLE_KEYWORDS.items():
        if _contains_any(text, keywords):
            hints.append(role)
    return hints


def _safe_confidence(value: Any, default: float = 0.0) -> float:
    try:
        score = float(value)
    except Exception:
        return default
    if score < 0.0:
        return 0.0
    if score > 1.0:
        return 1.0
    return score


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("true", "1", "yes"):
            return True
        if low in ("false", "0", "no"):
            return False
    return default


def _normalize_role_hint(role: Any, weak_hints: List[str]) -> str:
    role_text = _clean_text(role)
    canonical = {
        "component": "Component",
        "interface": "Interface",
        "function": "Function",
        "logicrule": "LogicRule",
        "logic": "LogicRule",
        "other": "Other",
    }
    key = role_text.lower().replace(" ", "")
    if key in canonical:
        return canonical[key]
    if weak_hints:
        return weak_hints[0]
    return "Other"


def _fallback_faithful_text(row_map: Dict[str, str]) -> str:
    parts = []
    for k, v in row_map.items():
        if v:
            parts.append("{0}: {1}".format(k, v))
    return "；".join(parts)


def _normalize_rewrite(raw: Dict[str, Any], row_map: Dict[str, str], weak_hints: List[str]) -> Dict[str, Any]:
    faithful_text = _clean_text(raw.get("faithful_text", ""))
    if not faithful_text:
        faithful_text = _fallback_faithful_text(row_map)

    return {
        "faithful_text": faithful_text,
        "table_role_hint": _normalize_role_hint(raw.get("table_role_hint"), weak_hints),
        "confidence": _safe_confidence(raw.get("confidence", 0.0), default=0.0),
        "insufficient_info": _safe_bool(raw.get("insufficient_info", False), default=False),
        "notes": _clean_text(raw.get("notes", "")),
    }


def _derive_system_tag(path: List[str], title: str) -> str:
    for item in reversed(path):
        clean = _clean_text(item)
        if re.search(r"(系统|控制|装置|模块|单元)", clean):
            return clean
    title_clean = _clean_text(title)
    if re.search(r"(系统|控制|装置|模块|单元)", title_clean):
        return title_clean
    if path:
        for item in reversed(path):
            clean = _clean_text(item)
            if clean:
                return clean
    return "未分类"


def _pick_by_key(row_map: Dict[str, str], keywords: List[str]) -> str:
    for k, v in row_map.items():
        if not v:
            continue
        if _contains_any(k, keywords):
            return v
    return ""


def _pick_name(row_map: Dict[str, str], fallback_text: str) -> str:
    name = _pick_by_key(row_map, NAME_KEYWORDS)
    if name:
        return name
    for v in row_map.values():
        clean = _clean_text(v)
        if clean:
            return clean
    return _clean_text(fallback_text)


def _build_interface(row_map: Dict[str, str]) -> Dict[str, Optional[str]]:
    source = _pick_by_key(row_map, SOURCE_KEYWORDS)
    target = _pick_by_key(row_map, TARGET_KEYWORDS)
    signal = ""
    medium = ""

    for k, v in row_map.items():
        if not v:
            continue
        if not signal and _contains_any(k, ["信号", "signal", "数据", "指令"]):
            signal = v
        if not medium and _contains_any(k, ["媒介", "介质", "总线", "通道", "medium", "bus"]):
            medium = v

    if not source or not target:
        values = [_clean_text(v) for v in row_map.values() if _clean_text(v)]
        if len(values) >= 2:
            if not source:
                source = values[0]
            if not target:
                target = values[1]

    return {
        "source": source,
        "target": target,
        "signal": signal or None,
        "medium": medium or None,
    }


def _build_logic_rule(row_map: Dict[str, str], faithful_text: str) -> Dict[str, Optional[str]]:
    trigger = _pick_by_key(row_map, TRIGGER_KEYWORDS)
    action = _pick_by_key(row_map, ACTION_KEYWORDS)
    target = _pick_by_key(row_map, LOGIC_TARGET_KEYWORDS)

    if not action:
        action = _clean_text(faithful_text) or None

    return {
        "trigger": trigger or None,
        "action": action or None,
        "target": target or None,
    }


def _rewrite_to_candidate(
    table_item: Dict[str, Any],
    row_index: int,
    row_map: Dict[str, str],
    rewrite: Dict[str, Any],
) -> Dict[str, Any]:
    content = rewrite.get("faithful_text", "")
    role = rewrite.get("table_role_hint", "Other")
    system_tag = _derive_system_tag(_ensure_list_str(table_item.get("path", [])), _clean_text(table_item.get("title", "")))

    components: List[Dict[str, str]] = []
    interfaces: List[Dict[str, Any]] = []
    functions: List[Dict[str, str]] = []
    logic_rules: List[Dict[str, Optional[str]]] = []

    if role == "Component":
        name = _pick_name(row_map, content)
        if name:
            components.append({"name": name})
    elif role == "Interface":
        interface = _build_interface(row_map)
        if interface["source"] or interface["target"]:
            interfaces.append(interface)
    elif role == "Function":
        name = _pick_name(row_map, content)
        if name:
            functions.append({"name": name})
    elif role == "LogicRule":
        rule = _build_logic_rule(row_map, content)
        if rule["trigger"] or rule["action"] or rule["target"]:
            logic_rules.append(rule)

    return {
        "section_id": _clean_text(table_item.get("section_id", "")),
        "title": _clean_text(table_item.get("title", "")),
        "path": _ensure_list_str(table_item.get("path", [])),
        "content": content,
        "system_tag": system_tag,
        "components": components,
        "interfaces": interfaces,
        "functions": functions,
        "logic_rules": logic_rules,
        "table_id": _clean_text(table_item.get("table_id", "")),
        "table_row_index": row_index,
        "table_role_hint": role,
        "table_rewrite_confidence": rewrite.get("confidence", 0.0),
        "table_insufficient_info": rewrite.get("insufficient_info", False),
        "table_notes": rewrite.get("notes", ""),
    }


def _default_rewrite(row_map: Dict[str, str], weak_hints: List[str]) -> Dict[str, Any]:
    text = _fallback_faithful_text(row_map)
    return {
        "faithful_text": text,
        "table_role_hint": weak_hints[0] if weak_hints else "Other",
        "confidence": 0.0,
        "insufficient_info": True,
        "notes": "dry_run_or_llm_unavailable",
    }


def load_tables_raw(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []

    tables: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8-sig") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception as e:
                print("[WARN] skip invalid json line {0}: {1}".format(line_no, e))
                continue
            if isinstance(obj, dict):
                tables.append(obj)
    return tables


def _dump_json(path: str, payload: Any) -> None:
    out_dir = os.path.dirname(path) or "."
    os.makedirs(out_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def run_table_pipeline(
    input_file: str = INPUT_TABLES_RAW,
    summary_output: str = OUTPUT_TABLE_SUMMARY,
    candidates_output: str = OUTPUT_TABLE_CANDIDATES,
    base_url: str = DEFAULT_BASE_URL,
    model: str = DEFAULT_MODEL,
    api_key: Optional[str] = None,
    use_llm: bool = True,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if api_key:
        os.environ["DASHSCOPE_API_KEY"] = api_key

    tables = load_tables_raw(input_file)
    print("[TABLE] input tables: {0}".format(len(tables)))

    if use_llm and tables:
        client = create_client(base_url=base_url, model=model, api_key=api_key)
    else:
        client = None

    table_summaries: List[Dict[str, Any]] = []
    table_candidates: List[Dict[str, Any]] = []

    for t_idx, table_item in enumerate(tables, start=1):
        raw_header = _ensure_list_str(table_item.get("header", []))
        raw_rows = table_item.get("rows", [])
        rows: List[List[str]] = []
        if isinstance(raw_rows, list):
            for row in raw_rows:
                rows.append(_ensure_list_str(row) if isinstance(row, list) else [str(row)])

        width = _calc_width(raw_header, rows)
        header = _normalize_header(raw_header, width)

        row_summaries: List[Dict[str, Any]] = []
        for row_index, raw_row in enumerate(rows, start=1):
            row = _normalize_row(raw_row, width)
            row_map = _build_row_map(header, row)
            weak_hints = _build_weak_role_hints(header, row_map)

            row_payload = {
                "table_id": table_item.get("table_id", ""),
                "section_id": table_item.get("section_id", ""),
                "title": table_item.get("title", ""),
                "path": table_item.get("path", []),
                "header": header,
                "row_index": row_index,
                "row": row,
                "row_map": row_map,
                "weak_role_hints": weak_hints,
            }

            if client is not None:
                try:
                    raw_rewrite = client.rewrite_table_row(
                        row_payload=row_payload,
                        system_prompt=SYSTEM_PROMPT_TABLE_REWRITE,
                        temperature=0.1,
                    )
                except Exception as e:
                    raw_rewrite = {
                        "faithful_text": _fallback_faithful_text(row_map),
                        "table_role_hint": weak_hints[0] if weak_hints else "Other",
                        "confidence": 0.0,
                        "insufficient_info": True,
                        "notes": "llm_error:{0}".format(e),
                    }
            else:
                raw_rewrite = _default_rewrite(row_map, weak_hints)

            rewrite = _normalize_rewrite(raw_rewrite, row_map, weak_hints)
            row_summary = {
                "row_index": row_index,
                "row_map": row_map,
                "weak_role_hints": weak_hints,
                "rewrite": rewrite,
            }
            row_summaries.append(row_summary)

            if rewrite.get("faithful_text"):
                table_candidates.append(
                    _rewrite_to_candidate(
                        table_item=table_item,
                        row_index=row_index,
                        row_map=row_map,
                        rewrite=rewrite,
                    )
                )

        table_summary = {
            "table_id": _clean_text(table_item.get("table_id", "")),
            "section_id": _clean_text(table_item.get("section_id", "")),
            "title": _clean_text(table_item.get("title", "")),
            "path": _ensure_list_str(table_item.get("path", [])),
            "header": header,
            "row_count": len(rows),
            "row_summaries": row_summaries,
            "raw_text": _clean_text(table_item.get("raw_text", "")),
        }
        table_summaries.append(table_summary)
        print(
            "[TABLE {0}/{1}] {2} rows={3}".format(
                t_idx,
                len(tables),
                table_summary["table_id"] or "UNKNOWN_TABLE",
                len(rows),
            )
        )

    _dump_json(summary_output, table_summaries)
    _dump_json(candidates_output, table_candidates)

    print("[TABLE] summary   : {0}".format(os.path.abspath(summary_output)))
    print("[TABLE] candidates: {0}".format(os.path.abspath(candidates_output)))
    print("[TABLE] candidate rows: {0}".format(len(table_candidates)))

    return table_summaries, table_candidates


def main():
    parser = argparse.ArgumentParser(description="Run table-only pipeline.")
    parser.add_argument("--input", default=INPUT_TABLES_RAW, help="Input tables_raw.jsonl")
    parser.add_argument("--summary-output", default=OUTPUT_TABLE_SUMMARY, help="Output table_summary.json")
    parser.add_argument("--candidates-output", default=OUTPUT_TABLE_CANDIDATES, help="Output table_candidates.json")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="LLM base URL")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="LLM model")
    parser.add_argument("--api-key", default=None, help="API key")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not call LLM; use deterministic fallback rewrite.",
    )
    args = parser.parse_args()

    run_table_pipeline(
        input_file=args.input,
        summary_output=args.summary_output,
        candidates_output=args.candidates_output,
        base_url=args.base_url,
        model=args.model,
        api_key=args.api_key,
        use_llm=not args.dry_run,
    )


if __name__ == "__main__":
    main()
