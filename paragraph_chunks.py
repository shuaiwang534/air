# -*- coding: utf-8 -*-

"""
Compatibility entrypoint.
Real implementation is in text_flow/paragraph_chunks.py
"""

from text_flow import paragraph_chunks as _impl

INPUT_DOCX = _impl.INPUT_DOCX
OUTPUT_JSONL = _impl.OUTPUT_JSONL
OUTPUT_TABLES_JSONL = _impl.OUTPUT_TABLES_JSONL

resolve_input_doc_path = _impl.resolve_input_doc_path
load_document_any = _impl.load_document_any
preprocess_document = _impl.preprocess_document
iter_block_items = _impl.iter_block_items
get_heading_level = _impl.get_heading_level
parse_heading = _impl.parse_heading
build_section_chunks = _impl.build_section_chunks


def main():
    _impl.INPUT_DOCX = INPUT_DOCX
    _impl.OUTPUT_JSONL = OUTPUT_JSONL
    _impl.OUTPUT_TABLES_JSONL = OUTPUT_TABLES_JSONL
    return _impl.main()


if __name__ == "__main__":
    main()

