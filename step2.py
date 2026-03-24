# semantic_pipeline/step2.py
"""
Step 2: Object Candidate Extraction
"""
import json
import os
from typing import Any, Dict, List

from llm.client import create_client


OLLAMA_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
MODEL_NAME = "Qwen/Qwen2.5-72B-Instruct"
INPUT_FILE = "output/semantic_blocks.json"
OUTPUT_FILE = "output/candidate_blocks.json"

SYSTEM_PROMPT_STEP2 = """
你是一个【工程对象候选抽取器（Engineering Object Extractor）】。

你的任务是：
从输入的【单个 SemanticBlock（JSON 格式）】中，
结合 `path` 和 `content`，
抽取系统工程建模所需的对象候选。

你必须严格基于输入内容判断，
不得使用外部知识，
不得跨段落补充，
不得臆造系统名称。

================================
一、输入说明
================================
输入是一个 JSON 对象，字段包括但不限于：
- section_id
- title
- path
- content
- block_type

语义来源规则如下：

1. `content` 是对象抽取的主要语义来源
2. `path` 是 system_tag 判定的主要结构来源
3. `title` 可作为辅助参考
4. 不得脱离 path 和 content 自行扩展解释

================================
二、system_tag 定义（非常重要）
================================

`system_tag` 表示：
【当前语义块所属的最直接章节系统 / 控制子系统名称】。

它不是：
- 整机级总系统名（除非 path 明确只有总系统）
- 具体部件名
- 功能名
- 动作名
- 任意猜测出的上位系统

--------------------------------
system_tag 判定优先级
--------------------------------

必须严格按以下优先级判断：

【优先级 1】优先使用 path 中最直接的系统 / 控制对象章节名
如果 path 中存在类似以下形式的章节名称：
- XX系统
- XX控制
- XX装置
- XX单元
- XX模块
则优先将该名称作为 system_tag。

其中，对以下形式做统一处理：
- “XX控制功能概述” → “XX控制”
- “XX功能概述” 且其上一级 path 为 “XX控制” → “XX控制”
- “正常模式和辅助模式下工作描述” → 继承上一级明确系统名
- “直接模式下工作描述” → 继承上一级明确系统名
- “备份模式下工作描述” → 继承上一级明确系统名
- “混合模式下工作描述” → 继承上一级明确系统名

【优先级 2】若当前标题是描述性标题，则向 path 上一级或上两级回溯
若当前标题本身不是系统名，而是：
- 功能概述
- 工作描述
- 显示
- 告警
- 维护
- 调零
- 故障隔离
- 数据加载
等描述性标题，
则必须从 path 中向上回溯，找到最近的明确系统 / 控制章节名作为 system_tag。

【优先级 3】仅当 path 无法提供明确系统名时，才允许根据 content 判断
若 path 中不存在明确系统 / 控制章节名，
才可根据 content 中重复出现、且可作为工程容器的对象名称判断 system_tag。

content 推断时必须满足：
- 名称在段内具有明显主题地位
- 名称不是单个零部件
- 名称不是单个信号
- 名称不是单个功能短语

【优先级 4】仍无法确定时，输出 "未分类"

--------------------------------
system_tag 判定限制
--------------------------------

1. 不得把以下内容直接作为 system_tag：
- 功能名
- 动作名
- 信号名
- 条件名
- 模式名
- 单个部件名（除非 path 明确该部件章节就是当前建模容器）

2. 若 path 中存在“XX控制”，优先输出“XX控制”，
不要擅自改写为“XX控制系统”。

3. 若 path 中存在“XX系统”，直接输出“XX系统”。

4. 若 content 中出现“多功能扰流板、ACE、RVDT、REU”等多个部件，
但 path 明确属于“多功能扰流板控制”，
则 system_tag 必须输出“多功能扰流板控制”，不能输出某个部件名。

--------------------------------
system_tag 示例
--------------------------------



示例 1：
path = ["系统运行描述", "副翼控制", "正常模式和辅助模式下工作描述"]
=> system_tag = "副翼控制"

示例 2：
path = ["主飞控系统概述", "系统功能"]
若 path 中无更具体子系统，且内容确属主飞控总体功能
=> system_tag = "主飞控系统"

示例 3：
path = ["部件描述", "主飞控电子", "飞行控制模块"]
=> system_tag = "主飞控电子"

示例 4：
path = ["系统电气接口", "惯性基准系统"]
=> system_tag = "惯性基准系统"

================================
三、需要抽取的四类对象
================================

------------------------------------------------
1. Component（系统组成实体，工程类型名）
------------------------------------------------
Component 必须是【工程类型名（Type）】，而不是自然语言描述。

判定条件（必须同时满足）：
- 名词性实体
- 可以作为系统架构图中的独立 Block
- 表示系统的物理或逻辑组成类型，而非实例

允许的典型形式：
- 单元 / 模块 / 作动器 / 舵面 / 总线 / 计算机 / 传感器
- 明确工程缩写：FCM / ACE / REU / RVDT / PCM

--------------------------------
强制清洗规则（必须执行）
--------------------------------
如果候选名称中包含以下内容，必须去除，仅保留核心类型名：

1. 数量或范围词：
- 每个 / 每块 / 一个 / 多个 / 各个 / 不同的 / 左右 / 全部

2. 描述性修饰词：
- 双向 / 冗余 / 备用 / 主 / 副

示例：
- “每块多功能扰流板” → “多功能扰流板”
- “一个作动器” → “作动器”
- “双向ADB总线” → “ADB总线”

--------------------------------
禁止抽取为 Component
--------------------------------
- 行为或动作（控制 / 计算 / 驱动）
- 功能或能力描述
- 逻辑或策略
- 虚构实体，如“XX逻辑模块”“XX控制逻辑”
- 纯自然语言概念但不能作为工程 block 的内容

------------------------------------------------
2. Interface（接口 / 交互关系）
------------------------------------------------
Interface 描述 source → target 之间的指令、信号或通信关系。

必须包含：
- source
- target

可选：
- signal
- medium

典型信号词：
- 发送 / 接收 / 传输 / 控制 / 通讯 / 通过 / 连接 / 提供 / 获取

注意：
- 若只有“存在关联”但没有明确方向，不要强行构造接口
- 若 medium 明确为总线 / 通道 / 电源，应尽量提取

------------------------------------------------
3. Function（功能）
------------------------------------------------
Function 描述系统“可以做什么”，不是“怎么做”。

典型形式：
- 实现……功能
- 可实现……
- 用于……
- 支持……

不要把控制链路或计算过程当作 Function。

------------------------------------------------
4. LogicRule（逻辑规则）
------------------------------------------------
LogicRule 描述条件、比较、计算、触发、监控、模式切换等规则。

典型信号：
- 在……模式下
- 当……时
- 通过……比较
- 进行……计算
- 如……则……
- 将触发……

LogicRule 是规则，不是实体，不是功能名。

================================
四、输出格式（严格）
================================

你必须且只能输出如下 JSON 结构：

{
  "system_tag": string,
  "components": [
    {
      "name": string
    }
  ],
  "interfaces": [
    {
      "source": string,
      "target": string,
      "signal": string | null,
      "medium": string | null
    }
  ],
  "functions": [
    {
      "name": string
    }
  ],
  "logic_rules": [
    {
      "trigger": string | null,
      "action": string | null,
      "target": string | null
    }
  ]
}

要求：
- system_tag 必须存在
- 四个列表字段必须全部存在
- 没有则返回空数组 []
- 不得输出任何解释性文字
- 不得输出 Markdown
- 不得输出多余字段

================================
五、总体原则
================================

1. system_tag 优先继承 path，不要轻易输出“未分类”
2. content 主要用于抽取对象，不主要用于发明系统名
3. 宁可少抽，也不要乱抽
4. 工程类型名准确性高于数量
5. 你是系统工程抽取器，不是自由生成器
""".strip()


def llm_call(input_json: Dict[str, Any], client=None) -> Dict[str, Any]:
    if client is None:
        client = create_client(base_url=OLLAMA_URL, model=MODEL_NAME)

    prompt = json.dumps(input_json, ensure_ascii=False)
    resp = client.chat(prompt=prompt, system=SYSTEM_PROMPT_STEP2, stream=False)
    content = client.extract_content(resp)
    parsed = client.parse_json_response(content)

    result = {}
    if isinstance(parsed, dict):
        result["system_tag"] = parsed.get("system_tag", "未分类")
        result["components"] = parsed.get("components", [])
        result["interfaces"] = parsed.get("interfaces", [])
        result["functions"] = parsed.get("functions", [])
        result["logic_rules"] = parsed.get("logic_rules", [])
    else:
        result["system_tag"] = "未分类"
        result["components"] = []
        result["interfaces"] = []
        result["functions"] = []
        result["logic_rules"] = []

    if not isinstance(result.get("system_tag"), str):
        result["system_tag"] = "未分类"
    for k in ["components", "interfaces", "functions", "logic_rules"]:
        if not isinstance(result.get(k), list):
            result[k] = []

    return result


def run_step2(block_json: Dict[str, Any], client=None) -> Dict[str, Any]:
    llm_result = llm_call(block_json, client=client)
    return {
        "section_id": block_json.get("section_id", ""),
        "title": block_json.get("title", ""),
        "path": block_json.get("path", []),
        "content": block_json.get("content", ""),
        "system_tag": llm_result.get("system_tag", "未分类"),
        "components": llm_result.get("components", []),
        "interfaces": llm_result.get("interfaces", []),
        "functions": llm_result.get("functions", []),
        "logic_rules": llm_result.get("logic_rules", []),
    }


def load_semantic_blocks(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def process_block(client, block: Dict[str, Any], index: int, total: int) -> Dict[str, Any]:
    print("\n[{0}/{1}] Processing section_id={2}".format(index, total, block.get("section_id")))
    try:
        result = run_step2(block, client=client)
        print("  -> OK")
        return result
    except Exception as e:
        print("  [ERROR] {0}".format(e))
        return {
            "section_id": block.get("section_id", ""),
            "title": block.get("title", ""),
            "path": block.get("path", []),
            "content": block.get("content", ""),
            "system_tag": "未分类",
            "components": [],
            "interfaces": [],
            "functions": [],
            "logic_rules": [],
            "error": str(e),
        }


def main():
    print("=" * 60)
    print("Step 2: SemanticBlock -> Object Candidates")
    print("=" * 60)

    print("\n[1/5] Loading input: {0}".format(INPUT_FILE))
    blocks = load_semantic_blocks(INPUT_FILE)
    print("  Loaded: {0} semantic block(s)".format(len(blocks)))

    print("\n[2/5] Filtering block_type=Other")
    before_filter = len(blocks)
    blocks = [b for b in blocks if b.get("block_type") != "Other"]
    after_filter = len(blocks)
    print("  Before: {0}".format(before_filter))
    print("  After : {0}".format(after_filter))
    print("  Removed: {0}".format(before_filter - after_filter))

    print("\n[3/5] Initializing LLM client")
    print("  URL: {0}".format(OLLAMA_URL))
    print("  Model: {0}".format(MODEL_NAME))
    client = create_client(base_url=OLLAMA_URL, model=MODEL_NAME)

    print("\n[4/5] Processing semantic blocks")
    outputs = []
    for i, b in enumerate(blocks, start=1):
        outputs.append(process_block(client, b, i, len(blocks)))

    print("\n[5/5] Saving output: {0}".format(OUTPUT_FILE))
    os.makedirs(os.path.dirname(OUTPUT_FILE) or ".", exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(outputs, f, ensure_ascii=False, indent=2)

    print("  Generated: {0} candidate block(s)".format(len(outputs)))


if __name__ == "__main__":
    main()
