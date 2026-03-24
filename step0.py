# semantic_pipeline/step0.py
import json
import os
from typing import Any, Dict, List

from io_utils import load_sections
from semantic_block.builder import build_candidates


def run_step0(
    input_jsonl: str = "section_chunks.jsonl",
    output_file: str = "output/paragraph_blocks.json"
) -> List[Dict[str, Any]]:
    sections = load_sections(input_jsonl)

    output_data = []
    for sec in sections:
        print("\n=== Section {0} | {1} ===".format(sec.section_id, sec.title))

        cands = build_candidates(sec)
        if not cands:
            print("(no candidates)")
            continue

        for c in cands:
            print("[{0:02d}] ({1}) {2}".format(c.order, c.source, c.text))
            output_data.append({
                "section_id": c.section_id,
                "title": c.title,
                "path": c.path,
                "order": c.order,
                "text": c.text,
                "source": c.source,
            })

    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    return output_data


def main():
    run_step0()


if __name__ == "__main__":
    main()
