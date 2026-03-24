#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Convert section-level JSONL chunks into RAGFlow-friendly evidence Markdown.
"""

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def safe_str(x: Any) -> str:
    return x if isinstance(x, str) else ""


def safe_list_str(x: Any) -> List[str]:
    if isinstance(x, list):
        return [safe_str(i) for i in x if isinstance(i, str)]
    return []


def sanitize_filename(name: str, max_len: int = 120) -> str:
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name or "untitled"


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for idx, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
                else:
                    print("[WARN] Line {0}: not a JSON object, skipped.".format(idx))
            except json.JSONDecodeError as e:
                print("[WARN] Line {0}: JSON decode error: {1}. Skipped.".format(idx, e))


def build_chunk_id(doc_id: str, section_id: str) -> str:
    doc_id = safe_str(doc_id).strip() or "DOC"
    section_id = safe_str(section_id).strip() or "UNKNOWN"
    return "{0}:{1}".format(doc_id, section_id)


def render_markdown(
    doc_id: str,
    section_id: str,
    title: str,
    path_list: List[str],
    chunk_id: str,
    content: str,
) -> str:
    path_str = " > ".join([p for p in path_list if p]) if path_list else ""
    title_line = title.strip() if title.strip() else (path_list[-1].strip() if path_list else section_id)

    md = []
    md.append("# {0}".format(title_line))
    md.append("")
    md.append("- Doc: {0}".format(doc_id))
    md.append("- Section: {0}".format(section_id))
    if path_str:
        md.append("- Path: {0}".format(path_str))
    md.append("- ChunkID: {0}".format(chunk_id))
    md.append("")
    md.append("正文内容")
    md.append("")
    md.append(content.rstrip() if isinstance(content, str) else "")
    md.append("")
    return "\n".join(md)


def run_section_build(
    in_path: str = "data/full.jsonl",
    out_dir: str = "ragflow_evidence",
    doc_id: Optional[str] = None,
    mode: str = "both",
) -> int:
    in_path_obj = Path(in_path)
    out_dir_obj = Path(out_dir)
    out_dir_obj.mkdir(parents=True, exist_ok=True)

    if not in_path_obj.exists():
        raise FileNotFoundError("Input JSONL not found: {0}".format(in_path_obj))

    use_doc_id = doc_id or in_path_obj.stem

    combined_md_parts = []
    multi_dir = out_dir_obj / "chunks"
    if mode in ("multi", "both"):
        multi_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for rec in iter_jsonl(in_path_obj):
        section_id = safe_str(rec.get("section_id", "")).strip()
        title = safe_str(rec.get("title", "")).strip()
        path_list = safe_list_str(rec.get("path", []))
        content = safe_str(rec.get("content", ""))

        chunk_id = build_chunk_id(use_doc_id, section_id)
        md = render_markdown(
            doc_id=use_doc_id,
            section_id=section_id or "UNKNOWN",
            title=title,
            path_list=path_list,
            chunk_id=chunk_id,
            content=content,
        )

        if mode in ("single", "both"):
            combined_md_parts.append(md)
            combined_md_parts.append("\n---\n")

        if mode in ("multi", "both"):
            base = "{0}__{1}".format(chunk_id, title or (path_list[-1] if path_list else ""))
            fname = sanitize_filename(base) + ".md"
            (multi_dir / fname).write_text(md, encoding="utf-8")

        count += 1

    if mode in ("single", "both"):
        combined_path = out_dir_obj / "evidence_all.md"
        if combined_md_parts and combined_md_parts[-1].strip() == "---":
            combined_md_parts = combined_md_parts[:-1]
        combined_path.write_text("\n".join(combined_md_parts).strip() + "\n", encoding="utf-8")

    print("[OK] Converted {0} chunks.".format(count))
    print("[OUT] {0}".format(out_dir_obj.resolve()))
    if mode in ("single", "both"):
        print("[OUT] single: {0}".format((out_dir_obj / "evidence_all.md").resolve()))
    if mode in ("multi", "both"):
        print("[OUT] multi : {0}".format((out_dir_obj / "chunks").resolve()))
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="JSONL -> RAGFlow evidence Markdown converter")
    parser.add_argument("--in", dest="in_path", default="data/full.jsonl", help="Input JSONL path")
    parser.add_argument("--out", dest="out_dir", default="ragflow_evidence", help="Output directory")
    parser.add_argument("--doc-id", dest="doc_id", default=None, help="Doc ID to embed into ChunkID")
    parser.add_argument(
        "--mode",
        dest="mode",
        choices=["single", "multi", "both"],
        default="both",
        help="Output mode: single file / multi files / both",
    )
    args = parser.parse_args()

    run_section_build(
        in_path=args.in_path,
        out_dir=args.out_dir,
        doc_id=args.doc_id,
        mode=args.mode,
    )


if __name__ == "__main__":
    main()
