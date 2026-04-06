"""
DV 2.0 Generator — Configurable LLM Client (TASK_13)

Abstracts over Claude (Anthropic SDK) and OpenAI-compatible endpoints.
Provider selected via LLM_PROVIDER env var.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass


@dataclass
class ToolCallResult:
    """The AI's tool use response — structured JSON output."""

    tool_name: str
    tool_input: dict
    raw_response: dict  # full API response for debugging


@dataclass
class Message:
    role: str     # "user" | "assistant" | "system"
    content: str


class LLMError(Exception):
    """Raised when the LLM call fails or returns unexpected output."""


class LLMClient:
    """Configurable LLM client supporting Claude, OpenAI-compatible, and Databricks APIs.

    Configuration via env vars:

    - ``LLM_PROVIDER``: ``claude`` | ``openai_compatible`` | ``databricks``  (default: ``claude``)
    - ``LLM_API_KEY``: API key (or ``ANTHROPIC_API_KEY`` / ``OPENAI_API_KEY``).
      Not required for ``databricks`` provider — uses ``DATABRICKS_TOKEN`` instead.
    - ``LLM_BASE_URL``: Override base URL for OpenAI-compatible endpoints.
      Not required for ``databricks`` — derived from ``DATABRICKS_HOST`` automatically.
    - ``LLM_MODEL``: Model name override
    - ``LLM_MAX_TOKENS``: Max response tokens (default: 4096)

    **databricks provider**: calls Databricks Foundation Model APIs (OpenAI-compatible serving
    endpoints) using the workspace credentials already in ``DATABRICKS_HOST`` +
    ``DATABRICKS_TOKEN``. No separate API key needed.

    Usage::

        client = LLMClient()
        result = client.complete_with_tools(
            messages=[Message(role="user", content="Classify these tables...")],
            tools=[{"name": "classify_dv_model", "description": "...", "input_schema": {...}}],
            system="You are a DV 2.0 architect...",
        )
        print(result.tool_input)
    """

    DEFAULT_MODELS = {
        "claude": "claude-sonnet-4-6",
        "openai_compatible": "gpt-4o",
        "databricks": "databricks-meta-llama-3-3-70b-instruct",
    }

    def __init__(self) -> None:
        self.provider = os.getenv("LLM_PROVIDER", "claude").lower()

        if self.provider == "databricks":
            # Use Databricks workspace credentials — no separate LLM_API_KEY needed
            databricks_host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
            self.api_key = os.getenv("DATABRICKS_TOKEN")
            self.base_url = (
                os.getenv("LLM_BASE_URL")
                or (f"{databricks_host}/serving-endpoints" if databricks_host else None)
            )
            if not self.api_key or not databricks_host:
                raise ValueError(
                    "DATABRICKS_HOST and DATABRICKS_TOKEN must be set for LLM_PROVIDER=databricks"
                )
        else:
            self.api_key = (
                os.getenv("LLM_API_KEY")
                or os.getenv("ANTHROPIC_API_KEY")
                or os.getenv("OPENAI_API_KEY")
            )
            self.base_url = os.getenv("LLM_BASE_URL")
            if not self.api_key:
                raise ValueError(
                    "Set LLM_API_KEY (or ANTHROPIC_API_KEY / OPENAI_API_KEY) before using LLMClient"
                )

        self.model = os.getenv("LLM_MODEL") or self.DEFAULT_MODELS.get(
            self.provider, "claude-sonnet-4-6"
        )
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "4096"))

        if self.provider not in ("claude", "openai_compatible", "databricks"):
            raise ValueError(
                f"LLM_PROVIDER must be 'claude', 'openai_compatible', or 'databricks', "
                f"got: {self.provider!r}"
            )

    def complete_with_tools(
        self,
        messages: list[Message],
        tools: list[dict],
        system: str | None = None,
    ) -> ToolCallResult:
        """Call the LLM with tool definitions and return the first tool call result.

        Args:
            messages: Conversation history (typically a single user message).
            tools: Tool definitions in **Anthropic** tool schema format (canonical).
                   OpenAI format is derived automatically.
            system: Optional system prompt.

        Returns:
            :class:`ToolCallResult` with the AI's structured tool call output.

        Raises:
            :class:`LLMError`: If the API call fails or the model doesn't call a tool.
        """
        if self.provider == "claude":
            return self._complete_claude(messages, tools, system)
        return self._complete_openai(messages, tools, system)  # openai_compatible + databricks

    # ------------------------------------------------------------------
    # Provider implementations
    # ------------------------------------------------------------------

    def _complete_claude(
        self,
        messages: list[Message],
        tools: list[dict],
        system: str | None,
    ) -> ToolCallResult:
        try:
            import anthropic
        except ImportError as exc:
            raise LLMError(
                "anthropic package not installed. Run: pip install anthropic"
            ) from exc

        client = anthropic.Anthropic(api_key=self.api_key)
        kwargs: dict = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "tools": tools,
            # Force the model to use the specified tool
            "tool_choice": {"type": "tool", "name": tools[0]["name"]},
        }
        if system:
            kwargs["system"] = system

        try:
            response = client.messages.create(**kwargs)
        except Exception as exc:
            raise LLMError(f"Claude API call failed: {exc}") from exc

        tool_use_blocks = [b for b in response.content if b.type == "tool_use"]
        if not tool_use_blocks:
            raise LLMError(
                f"Claude did not return a tool_use block. "
                f"Stop reason: {response.stop_reason}. "
                f"Content: {response.content}"
            )
        block = tool_use_blocks[0]
        return ToolCallResult(
            tool_name=block.name,
            tool_input=block.input,
            raw_response=response.model_dump(),
        )

    def _complete_openai(
        self,
        messages: list[Message],
        tools: list[dict],
        system: str | None,
    ) -> ToolCallResult:
        try:
            import openai as _openai
        except ImportError as exc:
            raise LLMError(
                "openai package not installed. Run: pip install openai"
            ) from exc

        kwargs: dict = {"api_key": self.api_key}
        if self.base_url:
            kwargs["base_url"] = self.base_url
        client = _openai.OpenAI(**kwargs)

        oai_messages: list[dict] = []
        if system:
            oai_messages.append({"role": "system", "content": system})
        oai_messages += [{"role": m.role, "content": m.content} for m in messages]

        oai_tools = [self._to_openai_tool(t) for t in tools]

        try:
            response = client.chat.completions.create(
                model=self.model,
                messages=oai_messages,
                tools=oai_tools,
                tool_choice={
                    "type": "function",
                    "function": {"name": tools[0]["name"]},
                },
            )
        except Exception as exc:
            raise LLMError(f"OpenAI-compatible API call failed: {exc}") from exc

        choice = response.choices[0]
        tool_calls = choice.message.tool_calls
        if not tool_calls:
            raise LLMError(
                f"Model did not return a tool call. "
                f"Finish reason: {choice.finish_reason}. "
                f"Content: {choice.message.content}"
            )
        tc = tool_calls[0]
        return ToolCallResult(
            tool_name=tc.function.name,
            tool_input=json.loads(tc.function.arguments),
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
