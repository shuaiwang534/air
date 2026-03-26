#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Run full MBSE pipeline in one command:
1) text_flow/paragraph_chunks.py
2) text_flow/step0.py
3) text_flow/step1.py
4) text_flow/step2.py
5) table_flow/pipeline.py
6) pipeline_integration/section_build.py
7) pipeline_integration/candidate_to_md.py
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from text_flow import step0
from text_flow import step1
from text_flow import step2
from table_flow import pipeline as table_pipeline
from pipeline_integration import candidate_to_md
from pipeline_integration import section_build


def _run_module_main(module, argv):
    old_argv = sys.argv
    try:
        sys.argv = argv
        module.main()
    finally:
        sys.argv = old_argv


def _derive_doc_id(input_doc: str, doc_id: Optional[str]) -> str:
    if doc_id:
        return doc_id
    return Path(input_doc).stem or "DOC"


def _auto_pick_input_doc(preferred: str) -> str:
    """
    Pick a usable input doc path.
    1) Use preferred path if exists.
    2) Try same stem .docx/.doc in cwd.
    3) Fallback to first .docx then first .doc in cwd.
    """
    p = Path(preferred)
    if p.exists():
        return str(p)

    parent = p.parent if str(p.parent) else Path(".")
    stem = p.stem
    for c in (parent / "{0}.docx".format(stem), parent / "{0}.doc".format(stem)):
        if c.exists():
            return str(c)

    for c in parent.glob("*.docx"):
        return str(c)
    for c in parent.glob("*.doc"):
        return str(c)

    return preferred


def _load_json_array(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def _dump_json(path: str, payload: Any) -> None:
    out_dir = os.path.dirname(path) or "."
    os.makedirs(out_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def run_pipeline(
    input_doc: Optional[str] = None,
    section_jsonl: Optional[str] = None,
    output_dir: str = "output",
    base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1",
    model: str = "Qwen/Qwen2.5-72B-Instruct",
    api_key: Optional[str] = None,
    doc_id: Optional[str] = None,
    build_section_md: bool = True,
    build_candidate_md: bool = True,
    section_md_dir: str = "ragflow_evidence",
    candidate_md_dir: str = "ragflow_evidence_candidates",
    chunk_token_budget: int = 10000,
) -> str:
    chunk_token_budget = int(chunk_token_budget or 0)
    if chunk_token_budget <= 0:
        raise ValueError("chunk_token_budget must be > 0. Please set it in main() config.")

    if api_key:
        os.environ["DASHSCOPE_API_KEY"] = api_key

    os.makedirs(output_dir, exist_ok=True)

    paragraph_file = os.path.join(output_dir, "paragraph_blocks.json")
    table_blocks_file = os.path.join(output_dir, "table_blocks.json")
    unified_blocks_file = os.path.join(output_dir, "unified_blocks.json")
    semantic_file = os.path.join(output_dir, "semantic_blocks.json")
    candidate_file = os.path.join(output_dir, "candidate_blocks.json")
    table_rows_file = os.path.join(output_dir, "table_rows.jsonl")
    table_sentences_file = os.path.join(output_dir, "table_sentences.jsonl")

    print("\n========== PARAGRAPH_CHUNKS ==========")
    from text_flow import paragraph_chunks

    if not input_doc:
        input_doc = getattr(paragraph_chunks, "INPUT_DOCX", "input.docx")
    input_doc = _auto_pick_input_doc(input_doc)
    if not section_jsonl:
        section_jsonl = getattr(paragraph_chunks, "OUTPUT_JSONL", "section_chunks.jsonl")

    use_doc_id = _derive_doc_id(input_doc=input_doc, doc_id=doc_id)
    paragraph_chunks.INPUT_DOCX = input_doc
    paragraph_chunks.OUTPUT_JSONL = section_jsonl
    paragraph_chunks.OUTPUT_TABLES_JSONL = table_rows_file
    paragraph_chunks.main()

    print("\n========== STEP 0 ==========")
    step0.run_step0(
        input_jsonl=section_jsonl,
        output_file=paragraph_file,
        token_budget=chunk_token_budget,
    )

    print("\n========== TABLE PIPELINE ==========")
    table_pipeline.run_table_pipeline(
        input_file=table_rows_file,
        sentences_output=table_sentences_file,
        blocks_output=table_blocks_file,
        base_url=base_url,
        model=model,
        api_key=api_key,
        use_llm=False,
        group_token_budget=chunk_token_budget,
    )

    print("\n========== MERGE BLOCKS ==========")
    paragraph_blocks = _load_json_array(paragraph_file)
    table_blocks = _load_json_array(table_blocks_file)
    unified_blocks = paragraph_blocks + table_blocks
    unified_blocks.sort(
        key=lambda x: (
            str(x.get("section_id", "")),
            str(x.get("source", "")),
            int(x.get("order", 0) or 0),
            int(x.get("chunk_index", 0) or 0),
        )
    )
    _dump_json(unified_blocks_file, unified_blocks)
    print("  paragraph blocks: {0}".format(len(paragraph_blocks)))
    print("  table blocks    : {0}".format(len(table_blocks)))
    print("  unified total   : {0}".format(len(unified_blocks)))

    print("\n========== STEP 1 ==========")
    step1.INPUT_FILE = unified_blocks_file
    step1.OUTPUT_FILE = semantic_file
    step1.OLLAMA_URL = base_url
    step1.MODEL_NAME = model
    step1.main()

    print("\n========== STEP 2 ==========")
    step2.INPUT_FILE = semantic_file
    step2.OUTPUT_FILE = candidate_file
    step2.OLLAMA_URL = base_url
    step2.MODEL_NAME = model
    step2.main()

    if build_section_md:
        print("\n========== SECTION_BUILD ==========")
        _run_module_main(
            section_build,
            [
                "section_build.py",
                "--in",
                section_jsonl,
                "--out",
                section_md_dir,
                "--mode",
                "both",
                "--doc-id",
                use_doc_id,
            ],
        )

    if build_candidate_md:
        print("\n========== CANDIDATE_TO_MD ==========")
        _run_module_main(
            candidate_to_md,
            [
                "candidate_to_md.py",
                "--in",
                candidate_file,
                "--out",
                candidate_md_dir,
                "--mode",
                "both",
                "--doc-id",
                use_doc_id,
            ],
        )

    print("\n========== DONE ==========")
    print("[DOC_ID] {0}".format(use_doc_id))
    print("[OUT] section_jsonl : {0}".format(Path(section_jsonl).resolve()))
    print("[OUT] paragraph     : {0}".format(Path(paragraph_file).resolve()))
    print("[OUT] table blocks  : {0}".format(Path(table_blocks_file).resolve()))
    print("[OUT] unified blocks: {0}".format(Path(unified_blocks_file).resolve()))
    print("[OUT] semantic      : {0}".format(Path(semantic_file).resolve()))
    print("[OUT] candidate     : {0}".format(Path(candidate_file).resolve()))
    print("[OUT] table rows    : {0}".format(Path(table_rows_file).resolve()))
    print("[OUT] table sent    : {0}".format(Path(table_sentences_file).resolve()))
    if build_section_md:
        print("[OUT] section md    : {0}".format(Path(section_md_dir).resolve()))
    if build_candidate_md:
        print("[OUT] candidate md  : {0}".format(Path(candidate_md_dir).resolve()))

    return candidate_file


def main():
    # ================================
    # Single editable config entry
    # Edit values here for local fixed defaults.
    # ================================
    MAIN_CONFIG: Dict[str, Any] = {
        "input_doc": "A320文字版.docx",  # set to None to auto-pick from cwd
        "section_jsonl": None,  # None -> paragraph_chunks.OUTPUT_JSONL
        "output_dir": "output",
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "model": "Qwen/Qwen2.5-72B-Instruct",
        "api_key": None,
        "doc_id": None,
        "skip_section_md": False,
        "skip_candidate_md": False,
        "section_md_dir": "ragflow_evidence",
        "candidate_md_dir": "ragflow_evidence_candidates",
        "chunk_token_budget": 10000,
    }

    parser = argparse.ArgumentParser(description="Run full MBSE pipeline in one command.")
    parser.add_argument(
        "--input-doc",
        "-i",
        default=MAIN_CONFIG["input_doc"],
        help="Input .doc/.docx file path; default from main() fixed config",
    )
    parser.add_argument("--section-jsonl", default=MAIN_CONFIG["section_jsonl"], help="Output JSONL path; default from main() fixed config")
    parser.add_argument("--output-dir", default=MAIN_CONFIG["output_dir"], help="Output directory for step0/1/2 JSONs")
    parser.add_argument("--base-url", default=MAIN_CONFIG["base_url"], help="LLM base URL")
    parser.add_argument("--model", default=MAIN_CONFIG["model"], help="LLM model")
    parser.add_argument("--api-key", default=MAIN_CONFIG["api_key"], help="DashScope API key")
    parser.add_argument("--doc-id", default=MAIN_CONFIG["doc_id"], help="Doc ID for markdown chunk ids; default from input doc name")
    parser.add_argument("--skip-section-md", action="store_true", default=MAIN_CONFIG["skip_section_md"], help="Skip section JSONL -> markdown")
    parser.add_argument("--skip-candidate-md", action="store_true", default=MAIN_CONFIG["skip_candidate_md"], help="Skip candidate JSON -> markdown")
    parser.add_argument("--section-md-dir", default=MAIN_CONFIG["section_md_dir"], help="Output dir for section markdown")
    parser.add_argument("--candidate-md-dir", default=MAIN_CONFIG["candidate_md_dir"], help="Output dir for candidate markdown")
    parser.add_argument("--chunk-token-budget", type=int, default=MAIN_CONFIG["chunk_token_budget"], help="Unified token upper bound for both text and table chunking")
    args = parser.parse_args()

    use_chunk_token_budget = int(args.chunk_token_budget or 0)
    if use_chunk_token_budget <= 0:
        use_chunk_token_budget = int(MAIN_CONFIG["chunk_token_budget"] or 0)
    if use_chunk_token_budget <= 0:
        raise ValueError("chunk_token_budget in main() must be > 0")

    run_pipeline(
        input_doc=args.input_doc,
        section_jsonl=args.section_jsonl,
        output_dir=args.output_dir,
        base_url=args.base_url,
        model=args.model,
        api_key=args.api_key,
        doc_id=args.doc_id,
        build_section_md=not args.skip_section_md,
        build_candidate_md=not args.skip_candidate_md,
        section_md_dir=args.section_md_dir,
        candidate_md_dir=args.candidate_md_dir,
        chunk_token_budget=use_chunk_token_budget,
    )


if __name__ == "__main__":
    main()
