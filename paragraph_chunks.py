from docx import Document
import re
import json
from typing import Optional

INPUT_DOCX = "input.docx"
OUTPUT_JSONL = "section_chunks.jsonl"


def get_heading_level(p) -> Optional[int]:
    if not p.style:
        return None
    name = p.style.name
    if name.startswith("Heading"):
        try:
            return int(name.split()[-1])
        except:
            return None
    return None


def parse_heading(text: str):
    """
    解析以下形式：
    - 8.7.1液压余度配置
    - 8.7.1 液压余度配置
    - 8.7.1：液压余度配置
    - 8.7.1-液压余度配置
    - 8.7.1 液压余度配置（主飞控）
    """
    text = text.strip()

    m = re.match(
        r"""
        ^(\d+(?:\.\d+)*)        # 章节编号
        [\s:：\-—]*             # 可选分隔符（空格/中文冒号/连字符）
        (.+?)                   # 标题正文
        $""",
        text,
        re.VERBOSE
    )

    if m:
        return m.group(1), m.group(2).strip()

    return None, text


def extract_table_text(table):
    """提取表格内容,转为文本表示"""
    lines = []
    for row in table.rows:
        cells = [cell.text.strip() for cell in row.cells]
        lines.append(" | ".join(cells))
    return "\n".join(lines)


def build_section_chunks(doc: Document):
    chunks = []
    heading_stack = []   # [{level, title}]

    current = None

    # 将段落和表格按文档顺序处理
    # 使用 element 来确定顺序
    elements = []
    
    # 收集所有段落
    for p in doc.paragraphs:
        elements.append(("paragraph", p))
    
    # 收集所有表格
    for table in doc.tables:
        elements.append(("table", table))
    
    # 按在文档中的位置排序
    elements.sort(key=lambda x: x[1]._element.getparent().index(x[1]._element) 
                  if hasattr(x[1]._element.getparent(), 'index') else 0)

    for elem_type, elem in elements:
        if elem_type == "paragraph":
            text = elem.text.strip()
            if not text:
                continue

            lvl = get_heading_level(elem)

            if lvl is not None:
                section_id, title = parse_heading(text)

                # 回退到父层级
                while heading_stack and heading_stack[-1]["level"] >= lvl:
                    heading_stack.pop()

                heading_stack.append({
                    "level": lvl,
                    "title": title
                })

                current = {
                    "section_id": section_id,
                    "title": title,
                    "path": [h["title"] for h in heading_stack],
                    "content_lines": []
                }
                chunks.append(current)
                continue

            # 普通正文（含图题、列表项）
            if current:
                current["content_lines"].append(text)
        
        elif elem_type == "table":
            # 处理表格
            if current:
                table_text = extract_table_text(elem)
                if table_text:
                    current["content_lines"].append("[表格]")
                    current["content_lines"].append(table_text)

    # 收尾
    for c in chunks:
        c["content"] = "\n".join(c["content_lines"]).strip()
        del c["content_lines"]

    return chunks


def main():
    doc = Document(INPUT_DOCX)
    chunks = build_section_chunks(doc)

    with open(OUTPUT_JSONL, "w", encoding="utf-8") as f:
        for c in chunks:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")

    print(f"生成章节数：{len(chunks)}")


if __name__ == "__main__":
    main()
