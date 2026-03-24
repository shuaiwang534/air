# semantic_pipeline/step1.py
"""
Step 1: CandidateBlock -> SemanticBlock
对每个候选块调用 LLM 进行语义分析与拆分
"""
import json
import os
from typing import List, Dict, Any
from llm.client import create_client


# ================================
# 配置
# ================================
OLLAMA_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
MODEL_NAME = "Qwen/Qwen2.5-72B-Instruct"

INPUT_FILE = "output/paragraph_blocks.json"
OUTPUT_FILE = "output/semantic_blocks.json"
SYSTEM_PROMPT = """你是一个【工程语义结构裁判（Semantic Block Judge）】。

你的任务是对输入的文本段落进行工程语义分析，评估文本中的语义块。无论文本是否包含工程语义，都必须返回有效的 JSON 格式,不得包含任何解释。

- 如果段落内容是对图像或表格等非文本内容的引用，判定为 `Other` 类型，不得返回空数组。
- 只有当段落中同时包含多个不同类型的工程语义时，才进行拆分。

--------------------------------
一、block_type 定义（严格）
--------------------------------

1. Component（组成 / 实体）
仅当文本在描述系统的物理或逻辑组成单元本身，
而不描述其行为、处理方式或运行逻辑时，判为 Component。
典型信号：
- “由……组成”
- “包括……模块 / 单元 / 计算机”
- 明确名词性实体罗列

------------------------------------------------

2. Interface（接口 / 交互边界）
仅当文本明确描述：
- 人与系统之间
- 系统与系统 / 子系统之间
的输入、输出、信号或指令交互关系时，判为 Interface。
典型信号：
- “输入 / 输出”
- “发送 / 接收”
- “通过……向……”
- “与……交互”

------------------------------------------------

3. Logic（逻辑 / 机制 / 运行规则）
当文本描述系统的处理方式、控制律、计算规则、
冗余策略、模式切换、因果或条件关系时，判为 Logic。
典型信号：
- “进行……计算”
- “在……模式下”
- “当……时”
- “采用……策略”

------------------------------------------------

4. Feature（功能 / 能力）
当文本描述系统具备的能力、功能或监控项，
但不涉及其实现机制或处理流程时，判为 Feature。
典型信号：
- “具备……功能”
- “支持……”
- “对……进行监控”

------------------------------------------------

5. Other（非工程语义正文）
以下内容必须判为 Other，且不得拆分：
- 表格内容（如缩略语表、参数表、对照表）
- 图注 / 表注
- 规范性、引导性或背景性说明
- 与工程要素建模无关的描述
- 表格、图表、图片等非文本内容的描述归为others，不能返回空内容（重要）

--------------------------------
二、拆分规则（非常重要）
--------------------------------

- 默认：一个段落只生成一个 SemanticBlock
- 只有当一个段落中【同时存在 block_type 不同的独立语义】时，才拆分
- 不得因为句号、分号、换行而机械拆分
- 不得为了数量而拆分

--------------------------------
三、content 规则
--------------------------------

- 每个 block 的 content 必须是【原文中的连续子串】
- 不得改写、补写、总结或推断
- 拆分后内容顺序必须与原文一致

--------------------------------
四、输出格式（严格）
--------------------------------

- 只输出 JSON
- 如果不需要拆分，输出单个 JSON 对象
- 如果需要拆分，输出 JSON 数组
- 不输出任何解释性文字

字段结构（当前阶段最小结构）：

{
  "block_type": "Component | Interface | Logic | Feature | Other",
  "content": string,
  "confidence": number   // 0.0 ~ 1.0
}

""".strip()


def load_candidate_blocks(file_path: str) -> List[Dict[str, Any]]:
    """加载 Step 0 生成的候选块"""
    with open(file_path, "r", encoding="utf-8") as f:
        blocks = json.load(f)
    return blocks


def build_prompt(candidate: Dict[str, Any]) -> str:
    candidate_json = json.dumps(candidate, ensure_ascii=False, indent=2)
    return "请对下面的CandidateBlock进行语义分析，并严格只输出JSON:\n\n{0}".format(candidate_json)


def process_candidate(
    client, 
    candidate: Dict[str, Any], 
    index: int, 
    total: int
) -> List[Dict[str, Any]]:
    """
    处理单个候选块
    
    返回：SemanticBlock 列表（可能是 1 个或多个）
    """
    print(f"\n[{index}/{total}] Processing section_id={candidate['section_id']}, order={candidate['order']}")
    print(f"  source={candidate['source']}, text_len={len(candidate['text'])}")
    
    # 构造 prompt
    prompt = build_prompt(candidate)
    
    # 调用 LLM（非流式，获取完整响应）
    try:
        response = client.chat(prompt=prompt, stream=False)
        content = client.extract_content(response)
        
        # 解析 JSON
        parsed = client.parse_json_response(content)
        
        # 规范化：如果是单个对象，包装成列表
        if isinstance(parsed, dict):
            parsed = [parsed]
        
        # 为每个 SemanticBlock 补充元数据
        semantic_blocks = []
        for i, block in enumerate(parsed):
            semantic_block = {
                # 继承原始元数据
                "section_id": candidate["section_id"],
                "title": candidate["title"],
                "path": candidate["path"],
                "original_order": candidate["order"],
                "original_source": candidate["source"],
                
                # LLM 输出的语义信息
                "block_type": block.get("block_type", "Other"),
                "content": block.get("content", ""),
                "confidence": block.get("confidence", 0.0),
                
                # 拆分序号（如果一个 candidate 拆成多个）
                "split_index": i + 1,
                "split_count": len(parsed)
            }
            semantic_blocks.append(semantic_block)
        
        print(f"  -> {len(semantic_blocks)} block(s) generated")
        return semantic_blocks
        
    except Exception as e:
        print(f"  [ERROR] {e}")
        # 出错时返回一个兜底 block
        return [{
            "section_id": candidate["section_id"],
            "title": candidate["title"],
            "path": candidate["path"],
            "original_order": candidate["order"],
            "original_source": candidate["source"],
            "block_type": "Other",
            "content": candidate["text"],
            "confidence": 0.0,
            "split_index": 1,
            "split_count": 1,
            "error": str(e)
        }]


def main():
    print("=" * 60)
    print("Step 1: CandidateBlock -> SemanticBlock")
    print("=" * 60)

    print("\n[1/4] Loading input: {0}".format(INPUT_FILE))
    candidates = load_candidate_blocks(INPUT_FILE)
    print("  Loaded: {0} candidate block(s)".format(len(candidates)))

    print("\n[1.5/4] Filtering figure/table blocks")
    before_filter = len(candidates)
    candidates = [c for c in candidates if c.get("source") not in ("figure", "table")]
    after_filter = len(candidates)
    print("  Before: {0}".format(before_filter))
    print("  After : {0}".format(after_filter))
    print("  Removed: {0}".format(before_filter - after_filter))

    print("\n[2/4] Initializing LLM client")
    print("  URL: {0}".format(OLLAMA_URL))
    print("  Model: {0}".format(MODEL_NAME))
    client = create_client(base_url=OLLAMA_URL, model=MODEL_NAME)

    print("\n[3/4] Processing candidates")
    all_semantic_blocks = []
    for i, candidate in enumerate(candidates, start=1):
        blocks = process_candidate(client, candidate, i, len(candidates))
        all_semantic_blocks.extend(blocks)

    print("\n[4/4] Saving output: {0}".format(OUTPUT_FILE))
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_semantic_blocks, f, ensure_ascii=False, indent=2)

    print("  Generated: {0} semantic block(s)".format(len(all_semantic_blocks)))

    print("\n" + "=" * 60)
    print("Statistics")
    print("=" * 60)
    print("Input candidates : {0}".format(len(candidates)))
    print("Output semantics : {0}".format(len(all_semantic_blocks)))
    ratio = float(len(all_semantic_blocks)) / float(len(candidates)) if candidates else 0.0
    print("Split ratio      : {0:.2f}x".format(ratio))

    type_counts = {}
    for block in all_semantic_blocks:
        bt = block.get("block_type", "Unknown")
        type_counts[bt] = type_counts.get(bt, 0) + 1

    print("\nBlock type distribution:")
    for bt, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print("  {0}: {1}".format(bt, count))

    print("\nStep 1 done")


if __name__ == "__main__":
    main()
