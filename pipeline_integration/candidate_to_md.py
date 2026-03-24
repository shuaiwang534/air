#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Convert candidate_blocks JSON into RAGFlow-friendly evidence Markdown.

Input JSON array element example:
{
  "section_id": "9.4.1",
  "title": "副翼控制功能概述",
  "path": ["系统运行描述", "副翼控制", "副翼控制功能概述"],
    "system_tag": "飞行控制系统",
  "content": "......",
  "components": [{"name": "..."}, ...],
  "interfaces": [{"source": "...", "target": "...", "signal": null, "medium": null}, ...],
  "functions": [{"name": "..."}, ...],
  "logic_rules": [{"trigger": null, "action": "...", "target": null}, ...]
}

Outputs:
1) One combined Markdown file: ragflow_evidence_candidates/evidence_all.md
2) Or one Markdown per chunk: ragflow_evidence_candidates/chunks/<chunk_id_sanitized>.md

ChunkID strategy (stable):
chunk_id = f"{doc_id}:{section_id}:idx{index}"
"""

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List


def safe_str(x: Any) -> str:
    return x if isinstance(x, str) else ""


def safe_list(x: Any) -> List:
    return x if isinstance(x, list) else []


def safe_list_str(x: Any) -> List[str]:
    if isinstance(x, list):
        return [safe_str(i) for i in x if isinstance(i, str)]
    return []


def sanitize_filename(name: str, max_len: int = 120) -> str:
    # Windows-friendly filename sanitize
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name or "untitled"


def load_candidates(path: Path) -> List[Dict[str, Any]]:
    """Load candidate_blocks JSON array"""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
        if isinstance(data, list):
            return data
        else:
            print("[WARN] Input JSON is not an array, treating as single object.")
            return [data] if isinstance(data, dict) else []


def build_chunk_id(doc_id: str, section_id: str, index: int) -> str:
    # Stable, deterministic
    doc_id = safe_str(doc_id).strip() or "DOC"
    section_id = safe_str(section_id).strip() or "UNKNOWN"
    return f"{doc_id}:{section_id}:idx{index:04d}"


def render_markdown(
    doc_id: str,
    section_id: str,
    title: str,
    path_list: List[str],
    chunk_id: str,
    system_tag: str,
    content: str,
    components: List[Dict[str, Any]],
    interfaces: List[Dict[str, Any]],
    functions: List[Dict[str, Any]],
    logic_rules: List[Dict[str, Any]],
) -> str:
    """Render candidate block as Markdown with object extraction results"""
    path_str = " > ".join([p for p in path_list if p]) if path_list else ""
    title_line = title.strip() if title.strip() else (path_list[-1].strip() if path_list else section_id)

    md = []
    md.append(f"# {title_line}")
    md.append("")
    md.append(f"- Doc: {doc_id}")
    md.append(f"- Section: {section_id}")
    if path_str:
        md.append(f"- Path: {path_str}")
    md.append(f"- SystemTag: {system_tag.strip() or '未分类'}")
    md.append(f"- ChunkID: {chunk_id}")
    md.append("")
    
    # Content
    md.append("正文内容")
    md.append("")
    md.append(content.rstrip() if isinstance(content, str) else "")
    md.append("")
    
    # Object extraction results
    md.append("对象抽取结果")
    md.append("")
    
    # Components
    if components:
        md.append("组件 (Components)")
        md.append("")
        for comp in components:
            name = safe_str(comp.get("name", "")).strip()
            if name:
                md.append(f"- {name}")
        md.append("")
    
    # Interfaces
    if interfaces:
        md.append("接口 (Interfaces)")
        md.append("")
        for intf in interfaces:
            source = safe_str(intf.get("source", "")).strip()
            target = safe_str(intf.get("target", "")).strip()
            signal = safe_str(intf.get("signal", "")).strip() or "N/A"
            medium = safe_str(intf.get("medium", "")).strip() or "N/A"
            if source or target:
                md.append(f"- **{source}** → **{target}**")
                if signal != "N/A":
                    md.append(f"  - 信号: {signal}")
                if medium != "N/A":
                    md.append(f"  - 媒介: {medium}")
        md.append("")
    
    # Functions
    if functions:
        md.append("功能 (Functions)")
        md.append("")
        for func in functions:
            name = safe_str(func.get("name", "")).strip()
            if name:
                md.append(f"- {name}")
        md.append("")
    
    # Logic Rules
    if logic_rules:
        md.append("逻辑规则 (Logic Rules)")
        md.append("")
        for rule in logic_rules:
            trigger = safe_str(rule.get("trigger", "")).strip() or "N/A"
            action = safe_str(rule.get("action", "")).strip() or "N/A"
            target = safe_str(rule.get("target", "")).strip() or "N/A"
            parts = []
            if trigger != "N/A":
                parts.append(f"触发: {trigger}")
            if action != "N/A":
                parts.append(f"动作: {action}")
            if target != "N/A":
                parts.append(f"目标: {target}")
            if parts:
                md.append(f"- {' | '.join(parts)}")
        md.append("")
    
    if not any([components, interfaces, functions, logic_rules]):
        md.append("*无抽取结果*")
        md.append("")
    
    return "\n".join(md)


def main() -> None:
    parser = argparse.ArgumentParser(description="Candidate blocks JSON -> RAGFlow evidence Markdown converter")
    parser.add_argument("--in", dest="in_path", default="output/candidate_blocks.json", help="Input JSON path")
    parser.add_argument("--out", dest="out_dir", default="ragflow_evidence_candidates", help="Output directory")
    parser.add_argument("--doc-id", dest="doc_id", default=None, help="Doc ID to embed into ChunkID (e.g., A320_v1)")
    parser.add_argument(
        "--mode",
        dest="mode",
        choices=["single", "multi", "both"],
        default="both",
        help="Output mode: single file / multi files / both",
    )
    args = parser.parse_args()

    in_path = Path(args.in_path)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise FileNotFoundError(f"Input JSON not found: {in_path}")

    # Default doc_id: use filename stem if not provided
    doc_id = args.doc_id or in_path.stem

    candidates = load_candidates(in_path)
    
    combined_md_parts: List[str] = []
    multi_dir = out_dir / "chunks"
    if args.mode in ("multi", "both"):
        multi_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for idx, rec in enumerate(candidates, start=1):
        section_id = safe_str(rec.get("section_id", "")).strip()
        title = safe_str(rec.get("title", "")).strip()
        path_list = safe_list_str(rec.get("path", []))
        system_tag = safe_str(rec.get("system_tag", "")).strip() or "未分类"
        content = safe_str(rec.get("content", ""))
        
        components = safe_list(rec.get("components", []))
        interfaces = safe_list(rec.get("interfaces", []))
        functions = safe_list(rec.get("functions", []))
        logic_rules = safe_list(rec.get("logic_rules", []))

        chunk_id = build_chunk_id(doc_id, section_id, idx)

        md = render_markdown(
            doc_id=doc_id,
            section_id=section_id or "UNKNOWN",
            title=title,
            path_list=path_list,
            chunk_id=chunk_id,
            system_tag=system_tag,
            content=content,
            components=components,
            interfaces=interfaces,
            functions=functions,
            logic_rules=logic_rules,
        )

        # single file
        if args.mode in ("single", "both"):
            combined_md_parts.append(md)
            combined_md_parts.append("\n---\n")

        # multi files
        if args.mode in ("multi", "both"):
            # filename includes section_id + title + idx for readability
            base = f"{chunk_id}__{title or (path_list[-1] if path_list else '')}"
            fname = sanitize_filename(base) + ".md"
            (multi_dir / fname).write_text(md, encoding="utf-8")

        count += 1

    if args.mode in ("single", "both"):
        combined_path = out_dir / "要素提取.md"
        # Remove trailing separator
        if combined_md_parts and combined_md_parts[-1].strip() == "---":
            combined_md_parts = combined_md_parts[:-1]
        combined_path.write_text("\n".join(combined_md_parts).strip() + "\n", encoding="utf-8")

    print(f"[OK] Converted {count} candidate blocks.")
    print(f"[OUT] {out_dir.resolve()}")
    if args.mode in ("single", "both"):
        print(f"[OUT] single: {(out_dir / 'evidence_all.md').resolve()}")
    if args.mode in ("multi", "both"):
        print(f"[OUT] multi : {(out_dir / 'chunks').resolve()}")


if __name__ == "__main__":
    main()
