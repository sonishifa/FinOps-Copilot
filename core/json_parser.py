# enterprise_cost_intelligence/core/json_parser.py
"""
Shared LLM response JSON parser.

FIX #15: The same fragile JSON extraction logic was copy-pasted into
vendor_agent.py, infrastructure_agent.py, operations_agent.py, and
verification.py. Any bug fix had to be made 4 times.

This module centralizes it with:
  - Markdown fence stripping
  - Truncated JSON detection (LLM hit max_tokens mid-response)
  - Graceful fallback with structured warning — never raises JSONDecodeError
  - Both object ({}) and array ([]) extraction
"""

import json
import re
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def parse_json_object(raw: str, caller: str = "unknown") -> Optional[dict]:
    """
    Extract and parse a JSON object from an LLM response string.

    Handles:
      - ```json ... ``` fences
      - Leading/trailing whitespace and backticks
      - Truncated JSON (LLM hit max_tokens mid-response)
      - Responses that have prose before/after the JSON block

    Returns:
      dict on success, None on failure (logs a warning).
    """
    return _extract(raw, pattern=r"\{.*\}", kind="object", caller=caller)


def parse_json_array(raw: str, caller: str = "unknown") -> Optional[list]:
    """
    Extract and parse a JSON array from an LLM response string.
    Same robustness as parse_json_object.

    Returns:
      list on success, None on failure (logs a warning).
    """
    return _extract(raw, pattern=r"\[.*\]", kind="array", caller=caller)


def _extract(raw: str, pattern: str, kind: str, caller: str) -> Any:
    if not raw or not raw.strip():
        logger.warning(f"[{caller}] Empty LLM response — cannot parse {kind}")
        return None

    # Step 1: strip markdown code fences
    clean = re.sub(r"```(?:json)?", "", raw, flags=re.IGNORECASE)
    clean = clean.strip().strip("`").strip()

    # Step 2: find the outermost JSON block
    match = re.search(pattern, clean, re.DOTALL)
    if not match:
        logger.warning(
            f"[{caller}] No JSON {kind} found in LLM response. "
            f"First 200 chars: {raw[:200]!r}"
        )
        return None

    candidate = match.group()

    # Step 3: detect truncation — unbalanced braces/brackets
    if _is_truncated(candidate, kind):
        logger.warning(
            f"[{caller}] LLM response appears truncated (unbalanced braces). "
            f"This usually means max_tokens was hit. Attempting partial parse..."
        )
        candidate = _repair_truncated(candidate, kind)

    # Step 4: parse
    try:
        return json.loads(candidate)
    except json.JSONDecodeError as e:
        logger.warning(
            f"[{caller}] JSONDecodeError after extraction: {e}. "
            f"Candidate (200 chars): {candidate[:200]!r}"
        )
        return None


def _is_truncated(text: str, kind: str) -> bool:
    """Check if JSON is structurally incomplete."""
    opens = text.count("{") + text.count("[")
    closes = text.count("}") + text.count("]")
    return opens != closes


def _repair_truncated(text: str, kind: str) -> str:
    """
    Attempt to close truncated JSON by counting unmatched open braces/brackets.
    This is a best-effort repair — not guaranteed to produce valid JSON,
    but often works for responses cut off mid-string-value.
    """
    # Count unmatched openers
    depth_curly = 0
    depth_square = 0
    in_string = False
    escape_next = False

    for char in text:
        if escape_next:
            escape_next = False
            continue
        if char == "\\" and in_string:
            escape_next = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if char == "{":
            depth_curly += 1
        elif char == "}":
            depth_curly -= 1
        elif char == "[":
            depth_square += 1
        elif char == "]":
            depth_square -= 1

    # If we're mid-string, close the string first
    if in_string:
        text += '"'

    # Close any open structures
    text += "}" * max(0, depth_curly)
    text += "]" * max(0, depth_square)
    return text