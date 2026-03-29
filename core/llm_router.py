# enterprise_cost_intelligence/core/llm_router.py
"""
LLM Router — smart model selection + exponential-backoff retry.

FIX #7:  Singleton is now thread-safe using a module-level Lock.
FIX #16: load_dotenv() is called at module import so .env files work out of box.

Strategy:
  HEAVY tasks (root cause, recommendations, verification) → llama-3.3-70b-versatile
  LIGHT tasks (classification, summaries, structured extraction) → llama-3.1-8b-instant
  On rate-limit (429): exponential backoff up to MAX_RETRIES, then fallback model.
"""

import os
import time
import json
import logging
import threading
from typing import Any, Optional

# FIX #16: load .env before reading os.environ
from dotenv import load_dotenv
load_dotenv()

from groq import Groq, RateLimitError, APIStatusError

logger = logging.getLogger(__name__)

HEAVY_MODEL = "llama-3.3-70b-versatile"
LIGHT_MODEL  = "llama-3.1-8b-instant"

MAX_RETRIES  = 5
BASE_BACKOFF = 4   # seconds; doubles each retry

# FIX #7: module-level lock + instance variable (not attribute on function)
_router_lock: threading.Lock = threading.Lock()
_router_instance: Optional["LLMRouter"] = None

# Token budget tracking (soft guard for 500K/day free limit)
_daily_token_estimate: int = 0
_token_lock: threading.Lock = threading.Lock()
DAILY_TOKEN_WARN = 400_000


def get_router() -> "LLMRouter":
    """Thread-safe singleton factory."""
    global _router_instance
    with _router_lock:
        if _router_instance is None:
            _router_instance = LLMRouter()
        return _router_instance


def _track_tokens(prompt: str, response: str) -> None:
    """Rough token estimate: words * 1.3. Warns if approaching daily limit."""
    global _daily_token_estimate
    estimate = int((len(prompt.split()) + len(response.split())) * 1.3)
    with _token_lock:
        _daily_token_estimate += estimate
        if _daily_token_estimate > DAILY_TOKEN_WARN:
            logger.warning(
                f"[TOKEN BUDGET] Estimated daily usage ~{_daily_token_estimate:,} tokens. "
                f"Groq free limit is 500K/day. Consider reducing evidence payload sizes."
            )


class LLMRouter:
    def __init__(self, api_key: Optional[str] = None):
        key = api_key or os.environ.get("GROQ_API_KEY")
        if not key:
            raise ValueError(
                "GROQ_API_KEY not set. Either export GROQ_API_KEY=... "
                "or create a .env file with GROQ_API_KEY=your_key"
            )
        self.client = Groq(api_key=key)

    def call(
        self,
        messages: list[dict],
        task_weight: str = "heavy",
        tools: Optional[list[dict]] = None,
        tool_choice: str = "auto",
        temperature: float = 0.2,
        max_tokens: int = 2048,
        response_format: Optional[dict] = None,
    ) -> str:
        """
        Make an LLM call with automatic retry and model fallback.
        Returns the assistant text content string.
        """
        primary  = HEAVY_MODEL if task_weight == "heavy" else LIGHT_MODEL
        fallback = LIGHT_MODEL if task_weight == "heavy" else HEAVY_MODEL

        for model in [primary, fallback]:
            result = self._call_with_retry(
                model=model,
                messages=messages,
                tools=tools,
                tool_choice=tool_choice,
                temperature=temperature,
                max_tokens=max_tokens,
                response_format=response_format,
            )
            if result is not None:
                # Track approximate token usage
                prompt_text = " ".join(m.get("content", "") for m in messages)
                _track_tokens(prompt_text, result)
                return result
            logger.warning(f"Model {model} exhausted all retries — trying fallback model...")

        raise RuntimeError(
            "Both LLM models exhausted all retries. "
            "Check your GROQ_API_KEY and daily token quota."
        )

    def call_with_tools(
        self,
        messages: list[dict],
        tools: list[dict],
        task_weight: str = "heavy",
        max_tokens: int = 2048,
    ) -> tuple[str, list[dict]]:
        """
        Execute a single tool-use turn.
        Returns (assistant_text, list_of_tool_call_dicts).
        Caller is responsible for handling tool results and looping.
        """
        primary  = HEAVY_MODEL if task_weight == "heavy" else LIGHT_MODEL
        fallback = LIGHT_MODEL if task_weight == "heavy" else HEAVY_MODEL

        for model in [primary, fallback]:
            for attempt in range(MAX_RETRIES):
                try:
                    response = self.client.chat.completions.create(
                        model=model,
                        messages=messages,
                        tools=tools,
                        tool_choice="auto",
                        max_tokens=max_tokens,
                        temperature=0.1,
                    )
                    msg = response.choices[0].message
                    tool_calls_made = []

                    if msg.tool_calls:
                        for tc in msg.tool_calls:
                            tool_calls_made.append({
                                "name": tc.function.name,
                                "arguments": json.loads(tc.function.arguments),
                                "id": tc.id,
                            })
                        return msg.content or "", tool_calls_made

                    return msg.content or "", []

                except RateLimitError:
                    wait = BASE_BACKOFF * (2 ** attempt)
                    logger.warning(
                        f"[{model}] Rate limited (tool call). "
                        f"Retrying in {wait}s (attempt {attempt+1}/{MAX_RETRIES})"
                    )
                    time.sleep(wait)
                except APIStatusError as e:
                    logger.error(f"[{model}] API error: {e}")
                    break
                except Exception as e:
                    logger.error(f"[{model}] Unexpected error: {e}")
                    break

        raise RuntimeError("Tool-use call failed on all models and retries.")

    def _call_with_retry(
        self,
        model: str,
        messages: list[dict],
        tools,
        tool_choice,
        temperature,
        max_tokens,
        response_format,
    ) -> Optional[str]:
        kwargs: dict[str, Any] = dict(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = tool_choice
        if response_format:
            kwargs["response_format"] = response_format

        for attempt in range(MAX_RETRIES):
            try:
                response = self.client.chat.completions.create(**kwargs)
                return response.choices[0].message.content or ""
            except RateLimitError:
                wait = BASE_BACKOFF * (2 ** attempt)
                logger.warning(
                    f"[{model}] Rate limited. Waiting {wait}s "
                    f"(attempt {attempt+1}/{MAX_RETRIES})"
                )
                time.sleep(wait)
            except APIStatusError as e:
                logger.error(f"[{model}] API status error {e.status_code}: {e.message}")
                return None   # signal to caller to try fallback
            except Exception as e:
                logger.error(f"[{model}] Unexpected: {e}")
                return None

        return None   # exhausted retries — signal to try fallback