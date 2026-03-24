# llm/client.py
import json
import os
import subprocess
from typing import Any, Dict, List, Optional, Union


class OllamaClient:
    """OpenAI-compatible HTTP client wrapper (implemented via curl)."""

    def __init__(
        self,
        base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1",
        model: str = "Qwen/Qwen2.5-72B-Instruct",
        api_key: Optional[str] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key or os.getenv("DASHSCOPE_API_KEY", "你的DashScopeKey")
        self.chat_endpoint = "{0}/chat/completions".format(self.base_url)

    def _decode_bytes(self, b: bytes) -> str:
        if not isinstance(b, bytes):
            return str(b)
        try:
            return b.decode("utf-8")
        except Exception:
            return b.decode("utf-8", errors="replace")

    def _post_by_curl(self, payload_json: str) -> Dict[str, Any]:
        cmd = [
            "curl.exe",
            "-sS",
            "--proxy",
            "",
            "--http1.1",
            "--tlsv1.2",
            "--ssl-no-revoke",
            "-X",
            "POST",
            self.chat_endpoint,
            "-H",
            "Content-Type: application/json",
            "-d",
            payload_json,
            "-w",
            "\n__STATUS__:%{http_code}",
        ]

        if self.api_key:
            cmd.extend(["-H", "Authorization: Bearer {0}".format(self.api_key)])

        try:
            clean_env = os.environ.copy()
            for k in [
                "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                "http_proxy", "https_proxy", "all_proxy",
                "GIT_HTTP_PROXY", "GIT_HTTPS_PROXY",
            ]:
                clean_env.pop(k, None)

            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=180,
                env=clean_env,
            )
        except FileNotFoundError:
            raise RuntimeError("curl not found. Please install curl or add it to PATH.")
        except subprocess.TimeoutExpired:
            raise RuntimeError("curl request timeout")

        stdout = self._decode_bytes(proc.stdout)
        stderr = self._decode_bytes(proc.stderr).strip()

        marker = "\n__STATUS__:"
        idx = stdout.rfind(marker)
        if idx < 0:
            raise RuntimeError("Unexpected curl output: {0}".format(stderr or stdout[:300]))

        body = stdout[:idx]
        status_text = stdout[idx + len(marker):].strip().splitlines()[0]

        try:
            status_code = int(status_text)
        except Exception:
            raise RuntimeError("Failed to parse HTTP status from curl output: {0}".format(status_text))

        if proc.returncode != 0 and status_code == 0:
            raise RuntimeError("curl failed: {0}".format(stderr or "unknown error"))

        return {
            "status_code": status_code,
            "body": body,
            "stderr": stderr,
        }

    def chat(
        self,
        prompt: str,
        system: Optional[str] = None,
        temperature: Optional[float] = None,
        stream: bool = False,
    ) -> Union[Dict[str, Any], str]:
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": system if system else "你是一个严谨的 AI 助手",
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            "temperature": temperature if temperature is not None else 0.7,
            "max_tokens": 30000,
        }
        if stream:
            payload["stream"] = True

        result = self._post_by_curl(json.dumps(payload, ensure_ascii=False))
        status_code = result["status_code"]
        body = result["body"]

        if status_code != 200:
            raise RuntimeError("API调用失败:status={0},body={1}".format(status_code, body))

        if stream:
            full_text = ""
            for line in body.splitlines():
                line = line.strip()
                if not line.startswith("data: "):
                    continue
                data_str = line[6:].strip()
                if data_str == "[DONE]":
                    break
                try:
                    chunk = json.loads(data_str)
                    delta = chunk["choices"][0]["delta"]
                    piece = delta.get("content", "")
                    if piece:
                        full_text += piece
                except Exception:
                    continue

            if full_text:
                return full_text

            # Fallback: some services may return normal JSON even when stream=True
            try:
                parsed = json.loads(body)
                return parsed["choices"][0]["message"]["content"]
            except Exception:
                return ""

        try:
            return json.loads(body)
        except Exception:
            raise RuntimeError("API返回非JSON: {0}".format(body[:500]))

    def extract_content(self, response) -> str:
        """兼容非流式(dict)和流式(str)"""
        if isinstance(response, str):
            return response

        try:
            return response["choices"][0]["message"]["content"]
        except Exception:
            return ""

    def parse_json_response(self, content: str) -> Union[Dict, List]:
        """
        解析 LLM 返回的 JSON 内容

        支持：
        - 单个对象：{...}
        - 对象数组：[{...}, {...}]
        - 去除 markdown 代码块标记
        """
        content = content.strip()

        if content.startswith("```json"):
            content = content[7:]
        if content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]

        content = content.strip()

        try:
            parsed = json.loads(content)
            return parsed
        except json.JSONDecodeError as e:
            raise ValueError(
                "LLM 返回内容不是有效 JSON: {0}\n原始内容:\n{1}".format(e, content)
            )


def create_client(
    base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1",
    model: str = "Qwen/Qwen2.5-72B-Instruct",
    api_key: Optional[str] = None,
) -> OllamaClient:
    """工厂函数：创建客户端"""
    return OllamaClient(base_url=base_url, model=model, api_key=api_key)
