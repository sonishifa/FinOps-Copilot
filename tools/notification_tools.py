# enterprise_cost_intelligence/tools/notification_tools.py
"""
Notification tools for the Execution Agent.

Provides send_escalation_brief() and notify_stakeholder() which
produce auditable, visible output. In production these would integrate
with email/Slack APIs — for the prototype they write to disk and console.

NOTE: These are pure utility functions. Audit logging is handled by the
caller (execution.py) to maintain clean layer separation — tools/ does
NOT import from audit/.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Escalation briefs are also saved to disk for demo visibility
BRIEF_DIR = Path(__file__).parent.parent / "audit" / "escalations"
BRIEF_DIR.mkdir(parents=True, exist_ok=True)


def send_escalation_brief(
    run_id: str,
    action_id: str,
    recipient: str,
    subject: str,
    body: str,
    channel: str = "email",
) -> dict:
    """
    Send an escalation brief to a stakeholder for human approval.

    In production: dispatches via email / Slack / Teams API.
    Prototype: saves brief to disk + prints to console.

    Returns a receipt dict for audit purposes.
    Caller (execution.py) is responsible for logging to audit trail.
    """
    receipt = {
        "action_id":   action_id,
        "recipient":   recipient,
        "subject":     subject,
        "channel":     channel,
        "sent_at":     datetime.now(timezone.utc).isoformat(),
        "status":      "sent_simulated",
    }

    # Save brief to disk for demo inspection
    brief_path = BRIEF_DIR / f"{action_id}_escalation.txt"
    with open(brief_path, "w") as f:
        f.write(f"TO: {recipient}\n")
        f.write(f"SUBJECT: {subject}\n")
        f.write(f"CHANNEL: {channel}\n")
        f.write(f"SENT AT: {receipt['sent_at']}\n")
        f.write(f"\n{'─'*60}\n\n")
        f.write(body)

    # Console output for live demo visibility
    logger.info(
        f"📧 ESCALATION BRIEF SENT\n"
        f"   To:      {recipient}\n"
        f"   Subject: {subject}\n"
        f"   Channel: {channel}\n"
        f"   Brief saved → {brief_path}"
    )

    return receipt


def notify_stakeholder(
    run_id: str,
    action_id: str,
    message: str,
    severity: str = "info",
    channel: str = "audit_log",
) -> dict:
    """
    Notify a stakeholder about an action that was auto-executed.

    In production: routes to the appropriate channel (email, Slack, etc.)
    based on severity and business_rules.json notification_channels config.
    Prototype: prints to console.

    Returns a receipt dict. Caller logs to audit trail.
    """
    receipt = {
        "action_id":   action_id,
        "message":     message,
        "severity":    severity,
        "channel":     channel,
        "sent_at":     datetime.now(timezone.utc).isoformat(),
        "status":      "delivered_simulated",
    }

    # Console output
    icon = {"info": "ℹ️", "warning": "⚠️", "critical": "🚨"}.get(severity, "📢")
    logger.info(f"{icon} STAKEHOLDER NOTIFICATION: {message}")

    return receipt
