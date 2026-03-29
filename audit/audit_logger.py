# enterprise_cost_intelligence/audit/audit_logger.py
"""
Audit Agent — continuous, append-only logging of every agent action.

FIX #17: Log files are now named per run_id (not just per day), so multiple
pipeline runs in one day don't pollute a single file. A date-bucketed directory
is still used so logs stay organized.

Thread-safe: all writes go through a single lock.
"""

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

_lock = threading.Lock()
_event_buffer: list[dict] = []  # In-memory buffer for batch Supabase flush

AUDIT_DIR = Path(__file__).parent / "logs"
AUDIT_DIR.mkdir(parents=True, exist_ok=True)


def _audit_file(run_id: str) -> Path:
    """
    FIX #17: One file per run_id inside a date-bucketed folder.
    e.g. audit/logs/2025-03-28/RUN-ABC123.jsonl
    """
    date_dir = AUDIT_DIR / datetime.now(timezone.utc).strftime("%Y-%m-%d")
    date_dir.mkdir(parents=True, exist_ok=True)
    return date_dir / f"{run_id}.jsonl"


def log_event(
    run_id: str,
    agent: str,
    event_type: str,
    payload: dict[str, Any],
    severity: str = "info",
    anomaly_id: Optional[str] = None,
    action_id: Optional[str] = None,
) -> dict[str, Any]:
    """
    Append an event to the audit log (thread-safe).
    Also buffers for batch Supabase flush at pipeline end.
    Returns the event dict so callers can also store it in PipelineState.audit_log.
    """
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "agent": agent,
        "event_type": event_type,
        "severity": severity,
        "anomaly_id": anomaly_id,
        "action_id": action_id,
        "payload": payload,
    }
    line = json.dumps(event, default=str)
    with _lock:
        with open(_audit_file(run_id), "a") as f:
            f.write(line + "\n")
        _event_buffer.append(event)
    return event


def get_audit_trail(run_id: str) -> list[dict]:
    """Return all events for a given run_id from its dedicated log file."""
    path = _audit_file(run_id)
    if not path.exists():
        return []
    results = []
    with open(path) as f:
        for line in f:
            try:
                results.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return results


def flush_to_supabase(run_id: str) -> int:
    """
    Batch-insert all buffered audit events to Supabase in one call.
    Called once at pipeline end to avoid sync HTTP blocking during the run.
    Returns number of events flushed (0 if Supabase is not connected).
    """
    global _event_buffer
    with _lock:
        events = _event_buffer.copy()
        _event_buffer.clear()

    if not events:
        return 0

    try:
        from core.database import get_db
        db = get_db()

        # Flatten payload dicts to JSON strings for Supabase compatibility
        for event in events:
            if isinstance(event.get("payload"), dict):
                event["payload"] = json.dumps(event["payload"], default=str)

        count = db.insert_rows("audit_events", events)
        logging.getLogger(__name__).info(
            f"Flushed {count} audit events to Supabase for run {run_id}"
        )
        return count
    except Exception as e:
        logging.getLogger(__name__).error(f"Supabase audit flush failed: {e}")
        return 0


def export_audit_report(run_id: str, out_path: Optional[Path] = None) -> Path:
    """Write a human-readable JSON audit report for a completed run."""
    events = get_audit_trail(run_id)
    out = out_path or AUDIT_DIR / f"report_{run_id}.json"
    with open(out, "w") as f:
        json.dump(events, f, indent=2, default=str)

    # Also flush to Supabase if connected
    flush_to_supabase(run_id)

    return out