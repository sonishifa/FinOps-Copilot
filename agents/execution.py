# enterprise_cost_intelligence/agents/execution.py
"""
Execution Agent

FIX #1:  The financial summary had a syntax bug:
             ActionStatus.STAGED_FOR_APPROVAL if False else ActionStatus.PENDING_HUMAN
         ActionStatus.STAGED_FOR_APPROVAL does not exist in schema.py.
         The `if False` short-circuit made it produce PENDING_HUMAN at runtime
         without crashing, but the calculation was wrong — it only counted
         EXECUTED actions when the comment implied it should count both
         EXECUTED and PENDING_HUMAN.

         Fix: explicit, readable set membership check with the correct enum values.

FIX #4 (related): execution.py now defensively handles None anomaly/rc lookups
         (which could happen if a regenerated action's anomaly_id slipped through).
         Escalation briefs degrade gracefully instead of silently dropping context.
"""

import logging
from datetime import datetime, timezone
from state.schema import (
    PipelineState, ActionRecommendation, ActionStatus, ExecutionResult
)
from tools.notification_tools import send_escalation_brief, notify_stakeholder
from audit.audit_logger import log_event

logger = logging.getLogger(__name__)

# Statuses that represent "recoverable" value — either already executed
# or staged for approval (will be executed once human approves)
_RECOVERABLE_STATUSES = {
    ActionStatus.AUTO_APPROVED,
    ActionStatus.EXECUTED,
    ActionStatus.PENDING_HUMAN,
    ActionStatus.HUMAN_APPROVED,
}


def run(state: PipelineState) -> PipelineState:
    logger.info("=== Execution Agent: Processing approved actions ===")

    auto_approved  = [a for a in state.action_recommendations if a.status == ActionStatus.AUTO_APPROVED]
    pending_human  = [a for a in state.action_recommendations if a.status == ActionStatus.PENDING_HUMAN]

    logger.info(
        f"Auto-executing {len(auto_approved)} actions, "
        f"staging {len(pending_human)} for human review"
    )

    results: list[ExecutionResult] = []

    # ── Auto-execute ─────────────────────────────────────────────────────────
    for action in auto_approved:
        result        = _execute_action(action, state)
        action.status = ActionStatus.EXECUTED if result.outcome == "success" else ActionStatus.FAILED
        results.append(result)

        notify_stakeholder(
            run_id   = state.run_id,
            action_id= action.action_id,
            message  = (
                f"Auto-executed: '{action.title}' "
                f"(${action.expected_savings_usd:,.0f} expected savings)"
            ),
            severity = "info",
        )
        result.stakeholder_notified = True

        log_event(
            state.run_id, "execution_agent", "action_executed",
            {
                "action_id":   action.action_id,
                "action_type": action.action_type,
                "outcome":     result.outcome,
                "savings_usd": action.expected_savings_usd,
            },
            action_id=action.action_id,
        )

    # ── Stage for human approval ──────────────────────────────────────────────
    for action in pending_human:
        brief = _build_escalation_brief(action, state)
        send_escalation_brief(
            run_id    = state.run_id,
            action_id = action.action_id,
            recipient = "enterprise-manager@company.com",
            subject   = f"[ACTION REQUIRED] {action.title}",
            body      = brief,
            channel   = "email",
        )
        result = ExecutionResult(
            action_id              = action.action_id,
            executed_at            = datetime.now(timezone.utc),
            outcome                = "staged_for_approval",
            details                = "Escalation brief sent. Awaiting human approval.",
            rollback_available     = False,
            stakeholder_notified   = True,
            escalation_brief_sent  = True,
        )
        results.append(result)

        log_event(
            state.run_id, "execution_agent", "escalation_sent",
            {
                "action_id":  action.action_id,
                "recipient":  "enterprise-manager@company.com",
                "savings_usd":action.expected_savings_usd,
            },
            action_id=action.action_id,
        )

    state.execution_results = results

    # ── Financial summary (FIX #1) ────────────────────────────────────────────
    all_actions = state.action_recommendations

    # FIX #1: was `ActionStatus.STAGED_FOR_APPROVAL if False else ...`
    # (non-existent enum + dead conditional). Now explicit and correct.
    total_exposure     = sum(a.financial_impact_usd   for a in state.anomalies)
    total_recoverable  = sum(
        a.expected_savings_usd for a in all_actions
        if a.status in _RECOVERABLE_STATUSES
    )
    auto_savings       = sum(
        a.expected_savings_usd for a in all_actions
        if a.status == ActionStatus.EXECUTED
    )
    pending_savings    = sum(
        a.expected_savings_usd for a in all_actions
        if a.status == ActionStatus.PENDING_HUMAN
    )
    rejected_savings   = sum(
        a.expected_savings_usd for a in all_actions
        if a.status == ActionStatus.REJECTED
    )

    state.financial_summary = {
        "total_financial_exposure_usd":        round(total_exposure,    2),
        "total_recoverable_savings_usd":        round(total_recoverable, 2),
        "auto_executed_savings_usd":            round(auto_savings,     2),
        "pending_human_approval_savings_usd":   round(pending_savings,  2),
        "rejected_savings_usd":                 round(rejected_savings, 2),
        "anomalies_detected":                   len(state.anomalies),
        "actions_generated":                    len(all_actions),
        "actions_auto_executed":                len(auto_approved),
        "actions_pending_human":                len(pending_human),
        "actions_failed":                       sum(1 for a in all_actions if a.status == ActionStatus.FAILED),
        "roi_multiple": round(
            total_recoverable / max(total_exposure, 1) * 100, 1
        ),  # % of exposure that is recoverable
    }

    # ── Persist execution results to Supabase ──────────────────────────────
    try:
        from core.database import get_db
        db = get_db()
        exec_rows = [
            {
                "run_id":       state.run_id,
                "action_id":    r.action_id,
                "executed_at":  r.executed_at.isoformat() if r.executed_at else None,
                "outcome":      r.outcome,
                "details":      r.details,
                "rollback_available":     r.rollback_available,
                "stakeholder_notified":   r.stakeholder_notified,
                "escalation_brief_sent":  r.escalation_brief_sent,
            }
            for r in results
        ]
        if exec_rows:
            db.insert_rows("execution_results", exec_rows)
            logger.info(f"Persisted {len(exec_rows)} execution results to Supabase")
    except Exception as e:
        logger.error(f"Supabase execution persistence failed: {e}")

    logger.info(
        f"=== Execution complete. "
        f"Auto-executed: {len(auto_approved)} actions (${auto_savings:,.0f} savings). "
        f"Pending human approval: {len(pending_human)} (${pending_savings:,.0f}). ==="
    )
    return state


# ── Execution simulation ──────────────────────────────────────────────────────

def _execute_action(action: ActionRecommendation, state: PipelineState) -> ExecutionResult:
    """
    Simulate executing an action.
    In production: API calls to cloud providers, procurement systems, ITSM.
    """
    simulations = {
        "consolidate_vendor":    "Vendor consolidation initiated. Duplicate vendor records flagged for merge. Contract review scheduled with procurement team.",
        "resize_resource":       "Resource resize request submitted. Instance type change queued in cloud provider. Expected completion: 15 minutes.",
        "decommission_resource": "Decommission request logged. Resource scheduled for termination after 48-hour hold period for rollback window.",
        "reassign_ticket":       "Tickets reassigned in ITSM system. Affected agents notified. SLA clock reset for reassigned tickets.",
        "renegotiate_contract":  "Contract renegotiation flag raised in procurement system. Vendor account manager notified via procurement portal.",
        "enforce_compliance":    "Compliance enforcement rule applied. Non-compliant POs flagged for mandatory review before next payment cycle.",
        "patch_config":          "Configuration patch request created in change management system. Change advisory board review scheduled.",
        "escalate_to_manager":   "Escalation ticket created in ITSM and assigned to department manager with P1 priority.",
    }
    detail = simulations.get(
        action.action_type,
        f"Action '{action.action_type}' executed (simulated)."
    )
    return ExecutionResult(
        action_id          = action.action_id,
        executed_at        = datetime.now(timezone.utc),
        outcome            = "success",
        details            = detail,
        rollback_available = action.action_type in ["resize_resource", "patch_config"],
    )


def _build_escalation_brief(action: ActionRecommendation, state: PipelineState) -> str:
    """
    Build a structured escalation brief for the human approver.
    FIX #4 (related): anomaly and rc lookups are now None-safe —
    if a regenerated action's anomaly_id isn't in state.anomalies,
    the brief still renders with graceful "N/A" fallbacks.
    """
    anomaly = next(
        (a for a in state.anomalies if a.anomaly_id == action.anomaly_id), None
    )
    rc = next(
        (r for r in state.root_cause_reports if r.anomaly_id == action.anomaly_id), None
    )

    steps_text = "\n".join(
        f"  {i+1}. {s}" for i, s in enumerate(action.implementation_steps)
    )

    return f"""FINOPS COPILOT — ACTION REQUIRED
Run ID:    {state.run_id}
Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RECOMMENDED ACTION  (Priority #{action.priority_rank})
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Action ID:        {action.action_id}
Title:            {action.title}
Type:             {action.action_type}
Expected Savings: ${action.expected_savings_usd:,.2f}

DESCRIPTION:
{action.description}

IMPLEMENTATION STEPS:
{steps_text if steps_text else "  No steps specified."}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ANOMALY CONTEXT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Anomaly:           {anomaly.title if anomaly else "N/A"}
Financial Exposure: ${f'{anomaly.financial_impact_usd:,.2f}' if anomaly else "N/A"}
Root Cause:        {rc.root_cause if rc else "N/A"}
Confidence:        {f'{rc.confidence_score:.0%}' if rc else "N/A"}
Severity:          {anomaly.severity.value.upper() if anomaly else "N/A"}

Verification Notes: {action.verification_notes or "None"}
Business Rule:      {action.business_rule_triggered or "None triggered"}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ACTION REQUIRED: Please APPROVE or REJECT this recommendation.
All decisions are logged to the immutable audit trail.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""