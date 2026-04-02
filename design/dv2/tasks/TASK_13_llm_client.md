# TASK_13: llm_client.py — Configurable LLM Client

## File
`generators/dv_generator/llm_client.py`

## Purpose
Provides a unified LLM interface that abstracts over two providers: **Claude** (Anthropic SDK) and any **OpenAI-compatible** endpoint (OpenAI SDK, local Ollama, proxy models like `qwen-coder`). Provider is selected via `LLM_PROVIDER` env var. All callers use a single `LLMClient.complete_with_tools()` method regardless of backend. This keeps the AI classifier decoupled from any specific LLM vendor.

## Depends on
- Standard library + `anthropic` or `openai` (provider-dependent, imported lazily)
- `python-dotenv` (already in requirements.txt)

## Inputs (env vars)

| Var | Required | Description |
|-----|----------|-------------|
| `LLM_PROVIDER` | Yes | `claude` or `openai_compatible` |
| `LLM_API_KEY` | Yes | Anthropic or OpenAI API key |
| `LLM_BASE_URL` | No | Override base URL (for OpenAI-compatible local/proxy endpoints) |
| `LLM_MODEL` | No | Model name override. Defaults: `claude-sonnet-4-6` (claude) or `gpt-4o` (openai_compatible) |
| `LLM_MAX_TOKENS` | No | Max tokens in response. Default: `4096` |

## Outputs
- `ToolCallResult` dataclass with `tool_name: str`, `tool_input: dict`, `raw_response: dict`

## Key classes / functions

```python
import os
from dataclasses import dataclass
from typing import Any

@dataclass
class ToolCallResult:
    """The AI's tool use response — structured JSON output."""
    tool_name: str
    tool_input: dict
    raw_response: dict   # full API response for debugging/logging

@dataclass
class Message:
    role: str   # "user" | "assistant" | "system"
    content: str

class LLMClient:
    """Configurable LLM client that supports Claude and OpenAI-compatible APIs.

    Usage:
        client = LLMClient()
        result = client.complete_with_tools(
            messages=[Message(role="user", content="...")],
            tools=[{"name": "classify_dv_model", "description": "...", "input_schema": {...}}],
            system="You are a Data Vault 2.0 expert..."
        )
        print(result.tool_input)  # structured DVModel JSON
    """

    DEFAULT_MODELS = {
        "claude": "claude-sonnet-4-6",
        "openai_compatible": "gpt-4o",
    }

    def __init__(self):
        self.provider = os.getenv("LLM_PROVIDER", "claude").lower()
        self.api_key = os.getenv("LLM_API_KEY") or os.getenv("ANTHROPIC_API_KEY") or os.getenv("OPENAI_API_KEY")
        self.base_url = os.getenv("LLM_BASE_URL")
        self.model = os.getenv("LLM_MODEL") or self.DEFAULT_MODELS.get(self.provider, "claude-sonnet-4-6")
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "4096"))

        if not self.api_key:
            raise ValueError("LLM_API_KEY (or ANTHROPIC_API_KEY / OPENAI_API_KEY) must be set")
        if self.provider not in ("claude", "openai_compatible"):
            raise ValueError(f"LLM_PROVIDER must be 'claude' or 'openai_compatible', got: {self.provider}")

    def complete_with_tools(
        self,
        messages: list[Message],
        tools: list[dict],
        system: str = None,
    ) -> ToolCallResult:
        """Call the LLM with tool definitions and return the first tool call result.

        Args:
            messages: Conversation history (typically a single user message).
            tools: Tool definitions in Anthropic tool schema format (canonical).
            system: Optional system prompt.

        Returns:
            ToolCallResult with the AI's structured tool call output.

        Raises:
            LLMError: If the API call fails or the model doesn't call a tool.
        """
        if self.provider == "claude":
            return self._complete_claude(messages, tools, system)
        else:
            return self._complete_openai(messages, tools, system)

    def _complete_claude(self, messages, tools, system) -> ToolCallResult:
        """Call Anthropic Claude API with tool_choice={"type": "tool"} to force tool use."""
        import anthropic
        client = anthropic.Anthropic(api_key=self.api_key)

        kwargs = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "tools": tools,
            "tool_choice": {"type": "tool", "name": tools[0]["name"]},  # force the specific tool
        }
        if system:
            kwargs["system"] = system

        response = client.messages.create(**kwargs)
        tool_use_block = next(b for b in response.content if b.type == "tool_use")
        return ToolCallResult(
            tool_name=tool_use_block.name,
            tool_input=tool_use_block.input,
            raw_response=response.model_dump(),
        )

    def _complete_openai(self, messages, tools, system) -> ToolCallResult:
        """Call OpenAI-compatible API, converting Anthropic tool schema to OpenAI format."""
        import openai
        kwargs = {"api_key": self.api_key}
        if self.base_url:
            kwargs["base_url"] = self.base_url
        client = openai.OpenAI(**kwargs)

        oai_messages = []
        if system:
            oai_messages.append({"role": "system", "content": system})
        oai_messages += [{"role": m.role, "content": m.content} for m in messages]

        oai_tools = [self._to_openai_tool(t) for t in tools]

        response = client.chat.completions.create(
            model=self.model,
            messages=oai_messages,
            tools=oai_tools,
            tool_choice={"type": "function", "function": {"name": tools[0]["name"]}},
        )
        tool_call = response.choices[0].message.tool_calls[0]
        import json
        return ToolCallResult(
            tool_name=tool_call.function.name,
            tool_input=json.loads(tool_call.function.arguments),
            raw_response=response.model_dump(),
        )

    @staticmethod
    def _to_openai_tool(anthropic_tool: dict) -> dict:
        """Convert Anthropic tool schema format to OpenAI function format."""
        return {
            "type": "function",
            "function": {
                "name": anthropic_tool["name"],
                "description": anthropic_tool.get("description", ""),
                "parameters": anthropic_tool.get("input_schema", {}),
            },
        }

class LLMError(Exception):
    """Raised when the LLM call fails or returns unexpected output."""
```

## Schema conversion note (Anthropic → OpenAI)
Anthropic tools use `input_schema` (JSON Schema); OpenAI uses `parameters` (same JSON Schema, different key). The `_to_openai_tool()` method handles this transparently.

## Acceptance criteria
- `LLMClient()` initialises without error when `LLM_PROVIDER=claude` and `LLM_API_KEY=<key>` are set
- `LLMClient()` initialises without error when `LLM_PROVIDER=openai_compatible` and `LLM_BASE_URL=http://localhost:11434/v1`
- `complete_with_tools()` with Claude returns `ToolCallResult` with `tool_input` matching the tool's schema
- `complete_with_tools()` with OpenAI-compatible returns equivalent `ToolCallResult`
- Missing `LLM_API_KEY` raises `ValueError` on init (not at call time)
- Invalid `LLM_PROVIDER` raises `ValueError` on init
- `_to_openai_tool()` correctly maps `input_schema` → `parameters`
