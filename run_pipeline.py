#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Run full MBSE pipeline in one command:
1) paragraph_chunks.py
2) step0.py
3) step1.py
4) step2.py
5) pipeline_integration/section_build.py
6) pipeline_integration/candidate_to_md.py
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import step0
import step1
import step2
import table_pipeline
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
) -> str:
    if api_key:
        os.environ["DASHSCOPE_API_KEY"] = api_key

    os.makedirs(output_dir, exist_ok=True)

    paragraph_file = os.path.join(output_dir, "paragraph_blocks.json")
    semantic_file = os.path.join(output_dir, "semantic_blocks.json")
    candidate_file = os.path.join(output_dir, "candidate_blocks.json")
    tables_raw_file = os.path.join(output_dir, "tables_raw.jsonl")
    table_summary_file = os.path.join(output_dir, "table_summary.json")
    table_candidates_file = os.path.join(output_dir, "table_candidates.json")
    merged_candidate_file = os.path.join(output_dir, "candidate_blocks_merged.json")

    print("\n========== PARAGRAPH_CHUNKS ==========")
    import paragraph_chunks

    if not input_doc:
        input_doc = getattr(paragraph_chunks, "INPUT_DOCX", "校验部分.doc")
    if not section_jsonl:
        section_jsonl = getattr(paragraph_chunks, "OUTPUT_JSONL", "section_chunks.jsonl")

    use_doc_id = _derive_doc_id(input_doc=input_doc, doc_id=doc_id)
    paragraph_chunks.INPUT_DOCX = input_doc
    paragraph_chunks.OUTPUT_JSONL = section_jsonl
    paragraph_chunks.OUTPUT_TABLES_JSONL = tables_raw_file
    paragraph_chunks.main()

    print("\n========== STEP 0 ==========")
    step0.run_step0(input_jsonl=section_jsonl, output_file=paragraph_file)

    print("\n========== STEP 1 ==========")
    step1.INPUT_FILE = paragraph_file
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

    print("\n========== TABLE PIPELINE ==========")
    table_pipeline.run_table_pipeline(
        input_file=tables_raw_file,
        summary_output=table_summary_file,
        candidates_output=table_candidates_file,
        base_url=base_url,
        model=model,
        api_key=api_key,
        use_llm=True,
    )

    print("\n========== MERGE CANDIDATES ==========")
    text_candidates = _load_json_array(candidate_file)
    table_candidates = _load_json_array(table_candidates_file)
    merged_candidates = text_candidates + table_candidates
    _dump_json(merged_candidate_file, merged_candidates)
    print("  text candidates : {0}".format(len(text_candidates)))
    print("  table candidates: {0}".format(len(table_candidates)))
    print("  merged total    : {0}".format(len(merged_candidates)))

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
                merged_candidate_file,
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
    print("[OUT] semantic      : {0}".format(Path(semantic_file).resolve()))
    print("[OUT] candidate     : {0}".format(Path(candidate_file).resolve()))
    print("[OUT] tables raw    : {0}".format(Path(tables_raw_file).resolve()))
    print("[OUT] table summary : {0}".format(Path(table_summary_file).resolve()))
    print("[OUT] table cand    : {0}".format(Path(table_candidates_file).resolve()))
    print("[OUT] merged cand   : {0}".format(Path(merged_candidate_file).resolve()))
    if build_section_md:
        print("[OUT] section md    : {0}".format(Path(section_md_dir).resolve()))
    if build_candidate_md:
        print("[OUT] candidate md  : {0}".format(Path(candidate_md_dir).resolve()))

    return merged_candidate_file


def main():
    parser = argparse.ArgumentParser(description="Run full MBSE pipeline in one command.")
    parser.add_argument("--input-doc", default=None, help="Input .doc/.docx file path; default from paragraph_chunks.INPUT_DOCX")
    parser.add_argument("--section-jsonl", default=None, help="Output JSONL path; default from paragraph_chunks.OUTPUT_JSONL")
    parser.add_argument("--output-dir", default="output", help="Output directory for step0/1/2 JSONs")
    parser.add_argument("--base-url", default="https://dashscope.aliyuncs.com/compatible-mode/v1", help="LLM base URL")
    parser.add_argument("--model", default="Qwen/Qwen2.5-72B-Instruct", help="LLM model")
    parser.add_argument("--api-key", default=None, help="DashScope API key")
    parser.add_argument("--doc-id", default=None, help="Doc ID for markdown chunk ids; default from input doc name")
    parser.add_argument("--skip-section-md", action="store_true", help="Skip section JSONL -> markdown")
    parser.add_argument("--skip-candidate-md", action="store_true", help="Skip candidate JSON -> markdown")
    parser.add_argument("--section-md-dir", default="ragflow_evidence", help="Output dir for section markdown")
    parser.add_argument("--candidate-md-dir", default="ragflow_evidence_candidates", help="Output dir for candidate markdown")
    args = parser.parse_args()

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
    )


if __name__ == "__main__":
    main()
