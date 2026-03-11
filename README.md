# Semantic Pipeline - 工程文档语义分析流水线

基于大语言模型（LLM）的工程文档语义分析与对象抽取流水线。

## 项目概述

本项目将工程文档（如航空系统规范）的章节文本转换为结构化的语义块（Semantic Blocks）和对象候选（Object Candidates），包括组件、接口、功能和逻辑规则的自动抽取。

**核心流程：**

```
原始文档 → Step 0 (段落切分) → Step 1 (语义分析) → Step 2 (对象抽取) → 结构化输出
```

## 环境要求

- **Python**: 3.9+
- **依赖库**:
  - `requests` (用于调用 Ollama API)
  - 标准库: `json`, `os`, `pathlib`, `argparse`, `re`, `typing`
- **Ollama**: 本地 LLM 推理服务（用于 Step 1 和 Step 2）

## 安装步骤

### 1. 克隆项目

```bash
git clone <your-repo-url>
cd semantic_pipeline
```

### 2. 安装 Python 依赖

```bash
pip install requests
```

### 3. 部署本地大模型（Ollama）

#### 3.1 安装 Ollama

访问 [Ollama 官网](https://ollama.ai/) 下载并安装适合你操作系统的版本。

Windows 用户可以下载 `.exe` 安装包，或使用包管理器：

```powershell
# 使用 Scoop (可选)
scoop install ollama
```

#### 3.2 下载模型权重文件

将预训练的 GGUF 模型文件放置在项目的 `llm_model/` 目录下：

```
llm_model/
  └── qwen2.5-7b/
      ├── Qwen2.5-7B-Instruct-Q4_0.gguf
      ├── Modelfile            # Step 1 模型配置
      └── Modelfile_step2       # Step 2 模型配置
```

**注意**: `.gguf` 文件通常较大（数GB），建议从官方渠道或 HuggingFace 下载。

#### 3.3 创建并注册模型到 Ollama

在 `llm_model/qwen2.5-7b/` 目录下，分别创建两个模型：

**Step 1 模型（语义分析）：**

```bash
cd llm_model/qwen2.5-7b
ollama create qwen2.5-7b-step1 -f Modelfile
```

**Step 2 模型（对象抽取）：**

```bash
ollama create qwen2.5-7b-step2 -f Modelfile_step2
```

验证模型是否创建成功：

```bash
ollama list
```

应该看到类似输出：

```
NAME                    ID              SIZE    MODIFIED
qwen2.5-7b-step1        abc123def456    4.1GB   2 hours ago
qwen2.5-7b-step2        def789ghi012    4.1GB   2 hours ago
```

#### 3.4 启动 Ollama 服务

```bash
ollama serve
```

服务默认运行在 `http://localhost:11434`。

**测试模型调用：**

```bash
curl http://localhost:11434/api/chat -d '{
  "model": "qwen2.5-7b-step1",
  "messages": [{"role": "user", "content": "测试"}],
  "stream": false
}'
```

## 使用指南

### Step 0: 段落切分（手动或预处理）

将原始文档按章节结构切分为段落，输出为 JSONL 或 JSON 格式：

**输入示例 (`data/section_chunks_test.jsonl`)**:

```json
{"section_id": "9.4.1", "title": "副翼控制功能概述", "path": ["系统运行描述", "副翼控制", "副翼控制功能概述"], "content": "副翼控制主要用于..."}
```

**输出**: `output/paragraph_blocks.json`

> **注意**: 本项目中 Step 0 通常由外部工具或脚本完成，如 `demo.py` 或其他文档解析工具。

---

### Step 1: 语义分析

对段落级文本进行语义分类（Component / Interface / Logic / Feature / Other）。

**运行命令：**

```bash
python step1.py
```

**配置参数** (在 `step1.py` 中修改):

```python
OLLAMA_URL = "http://localhost:11434"
MODEL_NAME = "qwen2.5-7b-step1"
INPUT_FILE = "output/paragraph_blocks.json"
OUTPUT_FILE = "output/semantic_blocks.json"
```

**输入**: `output/paragraph_blocks.json`

```json
[
  {
    "section_id": "9.4.1",
    "title": "副翼控制功能概述",
    "path": ["系统运行描述", "副翼控制", "副翼控制功能概述"],
    "order": 1,
    "text": "副翼控制主要用于侧杆滚转控制...",
    "source": "para"
  }
]
```

**输出**: `output/semantic_blocks.json`

```json
[
  {
    "section_id": "9.4.1",
    "title": "副翼控制功能概述",
    "path": ["系统运行描述", "副翼控制", "副翼控制功能概述"],
    "block_type": "Feature",
    "content": "副翼控制主要用于侧杆滚转控制...",
    "confidence": 0.95,
    "split_index": 1,
    "split_count": 1
  }
]
```

**关键字段说明：**

- `block_type`: 语义类型（Component / Interface / Logic / Feature / Other）
- `confidence`: 模型置信度（0.0 ~ 1.0）
- `split_index` / `split_count`: 如果一个段落被拆分为多个语义块，标记序号

---

### Step 2: 对象抽取

从语义块中抽取工程对象（组件、接口、功能、逻辑规则）。

**运行命令：**

```bash
python step2.py
```

**配置参数** (在 `step2.py` 中修改):

```python
OLLAMA_URL = "http://localhost:11434"
MODEL_NAME = "qwen2.5-7b-step2"
INPUT_FILE = "output/semantic_blocks.json"
OUTPUT_FILE = "output/candidate_blocks.json"
```

**输入**: `output/semantic_blocks.json` (Step 1 的输出)

**输出**: `output/candidate_blocks.json`

```json
[
  {
    "section_id": "9.4.1",
    "title": "副翼控制功能概述",
    "path": ["系统运行描述", "副翼控制", "副翼控制功能概述"],
    "content": "副翼控制主要用于侧杆滚转控制...",
    "components": [
      {"name": "副翼作动器"}
    ],
    "interfaces": [
      {"source": "侧杆", "target": "副翼控制系统", "signal": null, "medium": null}
    ],
    "functions": [
      {"name": "侧杆滚转控制"},
      {"name": "滚转自动配平"}
    ],
    "logic_rules": [
      {"trigger": null, "action": "接收侧杆信号", "target": "ACE"}
    ]
  }
]
```

**抽取对象说明：**

- **components**: 系统组成单元（如"副翼作动器"、"ACE"）
- **interfaces**: 输入输出关系（source → target）
- **functions**: 系统能力（如"滚转控制"）
- **logic_rules**: 控制逻辑（触发条件 → 动作 → 目标）

---

### 辅助工具：生成 RAGFlow 证据文档

将结构化 JSON 转换为 Markdown 格式，便于导入 RAGFlow 或其他知识库系统。

#### 转换段落级文档

```bash
python pipeline_integration/section_build.py \
  --in data/section_chunks_test.jsonl \
  --out ragflow_evidence \
  --mode both
```

#### 转换对象候选文档

```bash
python pipeline_integration/candidate_to_md.py \
  --in output/candidate_blocks.json \
  --out ragflow_evidence_candidates \
  --mode both
```

**输出：**

- `ragflow_evidence/evidence_all.md` (单文件)
- `ragflow_evidence/chunks/*.md` (多文件)
- `ragflow_evidence_candidates/evidence_all.md`
- `ragflow_evidence_candidates/chunks/*.md`

---

## 项目结构

```
semantic_pipeline/
├── README.md                   # 本文档
├── .gitignore                  # Git 忽略规则
├── step0.py                    # (可选) 段落切分入口
├── step1.py                    # Step 1: 语义分析主程序
├── step2.py                    # Step 2: 对象抽取主程序
├── demo.py                     # 演示脚本（段落切分 + 候选块生成）
├── io_utils.py                 # 文件 I/O 工具函数
├── test.py                     # 单元测试或调试脚本
├── llm/
│   └── client.py               # Ollama API 客户端封装
├── llm_model/
│   └── qwen2.5-7b/
│       ├── Qwen2.5-7B-Instruct-Q4_0.gguf  # 模型权重
│       ├── Modelfile           # Step 1 模型配置
│       └── Modelfile_step2     # Step 2 模型配置
├── semantic_block/
│   ├── models.py               # 数据模型定义 (Section, CandidateBlock)
│   └── builder.py              # 候选块构建逻辑
├── splitter/
│   ├── paragraph_split.py      # 段落分割器
│   └── heuristic_split.py      # 启发式规则分割
├── pipeline_integration/
│   ├── section_build.py        # JSONL → Markdown 转换器（段落级）
│   └── candidate_to_md.py      # JSON → Markdown 转换器（对象候选级）
├── data/                       # 输入数据目录 (被 .gitignore 忽略)
│   └── section_chunks_test.jsonl
├── output/                     # 输出目录 (被 .gitignore 忽略)
│   ├── paragraph_blocks.json
│   ├── semantic_blocks.json
│   └── candidate_blocks.json
└── ragflow_evidence*/          # Markdown 证据文档 (被 .gitignore 忽略)
```

---

## 常见问题

### Q1: Ollama 模型加载失败

**错误信息**: `model 'qwen2.5-7b-step1' not found`

**解决方法**:

1. 确认模型已创建: `ollama list`
2. 重新创建模型: `ollama create qwen2.5-7b-step1 -f Modelfile`
3. 检查 `Modelfile` 中的 `FROM` 路径是否正确指向 `.gguf` 文件

### Q2: LLM 返回内容解析失败

**错误信息**: `LLM 返回内容不是有效 JSON`

**原因**: 模型输出了非 JSON 格式的内容或附带了解释性文字。

**解决方法**:

- 调整 `Modelfile` 中的 `SYSTEM` 提示，强调"只输出 JSON"
- 降低 `temperature` 参数（在 `Modelfile` 中设置）
- 检查 `llm/client.py` 中的 `parse_json_response` 方法是否正确提取 JSON

### Q3: 如何调整模型参数？

在 `Modelfile` 或 `Modelfile_step2` 中修改：

```
PARAMETER temperature 0.3      # 降低随机性
PARAMETER top_p 0.9
PARAMETER num_ctx 4096         # 上下文长度
```

修改后需要重新创建模型：

```bash
ollama create qwen2.5-7b-step1 -f Modelfile
```

### Q4: 如何处理大文件？

如果输入文件过大（上千个段落），建议：

1. 分批处理: 将 `INPUT_FILE` 拆分为多个小文件
2. 增加超时时间: 修改 `llm/client.py` 中的 `timeout=120` 参数
3. 使用后台运行: `nohup python step1.py > step1.log 2>&1 &` (Linux/Mac)

---
