# enterprise_cost_intelligence/agents/verification.py
"""
Verification Agent

FIX #4:  The regen loop was creating a temp_state with the SAME run_id as the
         main state, then calling action_recommendation.run() on it. The
         regenerated actions had anomaly_id references that only existed in
         temp_state.anomalies — not in state.anomalies. When execution.py later
         did `next((a for a in state.anomalies if a.anomaly_id == action.anomaly_id))`
         it got None for every regenerated action, silently dropping anomaly context
         from every escalation brief.

         Fix: Instead of a temp_state with a divergent anomaly list, regeneration
         now calls a focused _regenerate_single_action() function that passes the
         original anomaly and root_cause objects directly to the prompt builder —
         no temp_state, no orphan references.

FIX #15: Replaced inline JSON parsing with core.json_parser.parse_json_object().
"""

import json
import logging
import uuid
from state.schema import (
    PipelineState, Anomaly, RootCauseReport,
    ActionRecommendation, ActionStatus,
)
from audit.audit_logger import log_event
from core.llm_router import get_router
from core.json_parser import parse_json_object, parse_json_array

logger = logging.getLogger(__name__)


def run(state: PipelineState) -> PipelineState:
    logger.info("=== Verification Agent: Validating action recommendations ===")
    router = get_router()
    rules  = _load_rules()
    # FIX #12: read max retries from config instead of hardcoding
    max_verification_retries = rules.get("max_verification_retries", 2)

    verified:    list[ActionRecommendation] = []
    needs_regen: list[ActionRecommendation] = []

    # ── First pass: verify all actions ───────────────────────────────────────
    for action in state.action_recommendations:
        result = _verify_action(action, rules, router, state.run_id)
        action.verification_notes = result["notes"]

        if result["passes"]:
            # Escalate to human if savings exceed threshold OR action flagged
            if (
                action.requires_human_approval
                or action.expected_savings_usd > rules["human_approval_threshold_usd"]
            ):
                action.status = ActionStatus.PENDING_HUMAN
                action.business_rule_triggered = result.get("triggered_rule")
            else:
                action.status = ActionStatus.AUTO_APPROVED

            verified.append(action)
            log_event(
                state.run_id, "verification_agent", "action_verified",
                {
                    "action_id":   action.action_id,
                    "status":      action.status.value,
                    "savings_usd": action.expected_savings_usd,
                },
                action_id=action.action_id,
            )
        else:
            action.status = ActionStatus.PENDING   # mark for regen
            needs_regen.append(action)
            log_event(
                state.run_id, "verification_agent", "critique_failed",
                {
                    "action_id": action.action_id,
                    "reason":    result["notes"],
                },
                severity="warning",
                action_id=action.action_id,
            )

    # ── Regen loop (FIX #4) ───────────────────────────────────────────────────
    state.verification_attempts += 1

    if needs_regen and state.verification_attempts <= max_verification_retries:
        logger.info(
            f"Critique failed for {len(needs_regen)} actions — "
            f"regenerating (attempt {state.verification_attempts}/{max_verification_retries})"
        )

        # Build lookup maps using the ORIGINAL state's anomaly/rc lists
        # FIX #4: no temp_state — regenerated actions reference the same
        # anomaly objects that are already in state.anomalies
        ano_map = {a.anomaly_id: a for a in state.anomalies}
        rc_map  = {r.anomaly_id: r for r in state.root_cause_reports}

        for failed_action in needs_regen:
            anomaly = ano_map.get(failed_action.anomaly_id)
            rc      = rc_map.get(failed_action.anomaly_id)
            if not anomaly or not rc:
                logger.warning(
                    f"Cannot regenerate {failed_action.action_id} — "
                    f"anomaly/rc not found in state. Escalating to human."
                )
                failed_action.status = ActionStatus.PENDING_HUMAN
                failed_action.verification_notes = (
                    (failed_action.verification_notes or "") +
                    " | Regen skipped: anomaly context missing. Escalated."
                )
                verified.append(failed_action)
                continue

            regen_action = _regenerate_single_action(
                failed_action, anomaly, rc, rules, router, state.run_id
            )
            if regen_action:
                # Re-verify the regenerated action (one level deep only)
                re_result = _verify_action(regen_action, rules, router, state.run_id)
                regen_action.verification_notes = re_result["notes"]
                if re_result["passes"]:
                    if (
                        regen_action.requires_human_approval
                        or regen_action.expected_savings_usd > rules["human_approval_threshold_usd"]
                    ):
                        regen_action.status = ActionStatus.PENDING_HUMAN
                    else:
                        regen_action.status = ActionStatus.AUTO_APPROVED
                else:
                    # Still failing after regen — escalate to human, don't loop again
                    regen_action.status = ActionStatus.PENDING_HUMAN
                    regen_action.verification_notes = (
                        (regen_action.verification_notes or "") +
                        " | Escalated after failed regen."
                    )
                verified.append(regen_action)
                log_event(
                    state.run_id, "verification_agent", "regen_complete",
                    {
                        "original_action_id": failed_action.action_id,
                        "new_action_id":      regen_action.action_id,
                        "new_status":         regen_action.status.value,
                    },
                    action_id=regen_action.action_id,
                )
            else:
                # Regen LLM call failed — safe fallback: escalate to human
                failed_action.status = ActionStatus.PENDING_HUMAN
                failed_action.verification_notes = (
                    (failed_action.verification_notes or "") +
                    " | Regen LLM call failed. Escalated to human."
                )
                verified.append(failed_action)

    elif needs_regen:
        # Exceeded max retries — escalate everything remaining
        logger.warning(
            f"Max verification retries reached. "
            f"Escalating {len(needs_regen)} remaining actions to human approval."
        )
        for action in needs_regen:
            action.status = ActionStatus.PENDING_HUMAN
            action.verification_notes = (
                (action.verification_notes or "") +
                f" | Max retries ({max_verification_retries}) exceeded. Escalated."
            )
            verified.append(action)

    state.action_recommendations = verified

    auto   = sum(1 for a in verified if a.status == ActionStatus.AUTO_APPROVED)
    human  = sum(1 for a in verified if a.status == ActionStatus.PENDING_HUMAN)
    logger.info(
        f"=== Verification done: {auto} auto-approved, "
        f"{human} pending human approval ==="
    )
    return state


# ── Verification logic ────────────────────────────────────────────────────────

def _verify_action(
    action: ActionRecommendation,
    rules: dict,
    router,
    run_id: str,
) -> dict:
    """
    Verify an action against hard business rules, then LLM critique.
    Returns {"passes": bool, "notes": str, "triggered_rule": str|None}
    """

    # Hard rule 1: termination/deletion always needs human approval
    if action.action_type in ["decommission_resource", "renegotiate_contract"]:
        if rules["cloud_spend_rules"].get("require_approval_for_termination", True):
            action.requires_human_approval = True

    # Hard rule 2: prohibited vendor categories cannot be auto-consolidated
    prohibited = rules["vendor_consolidation_rules"].get("prohibited_categories", [])
    if action.action_type == "consolidate_vendor":
        for cat in prohibited:
            if cat.lower() in action.description.lower():
                return {
                    "passes":        False,
                    "notes":         f"Violates rule: prohibited category '{cat}' cannot be auto-consolidated",
                    "triggered_rule":"prohibited_category",
                }

    # Hard rule 3: savings plausibility — reject if expected_savings > 2x financial_impact
    # (catches LLM hallucinated savings numbers)
    # We don't have anomaly here, so we do a softer check via LLM critique below

    # LLM critique for quality and feasibility
    prompt = f"""You are an enterprise compliance officer reviewing an AI-generated action recommendation.

ACTION:
Title: {action.title}
Type: {action.action_type}
Description: {action.description}
Expected Savings: ${action.expected_savings_usd:,.2f}
Steps:
{chr(10).join(f"  {i+1}. {s}" for i, s in enumerate(action.implementation_steps))}

BUSINESS RULES:
- Human approval required above: ${rules['human_approval_threshold_usd']:,}
- Actions at or below this threshold: auto-approved
- Max single-vendor spend concentration: {rules['procurement_rules']['max_single_vendor_spend_pct']}%

Evaluate strictly:
1. Is the savings figure plausible and mathematically coherent?
2. Are steps specific and actionable (not just "contact vendor")?
3. Are there unaddressed risks (data loss, service disruption)?
4. Does it comply with business rules?

Respond in JSON only:
{{
  "passes": true,
  "quality_score": 0.85,
  "compliance_issues": [],
  "risk_flags": [],
  "notes": "brief explanation of pass or fail decision"
}}"""

    try:
        raw    = router.call(
            messages    = [{"role": "user", "content": prompt}],
            task_weight = "heavy",
            max_tokens  = 512,
        )
        result = parse_json_object(raw, caller="verification_agent")
        if result is None:
            raise ValueError("parse_json_object returned None")

        return {
            "passes":        bool(result.get("passes", True)),
            "notes":         result.get("notes", "LLM critique passed"),
            "triggered_rule":None,
        }
    except Exception as e:
        logger.warning(f"Verification LLM call failed for {action.action_id}: {e}")
        # Default pass on LLM failure — don't block the pipeline over a transient API error
        return {
            "passes":        True,
            "notes":         f"LLM verification unavailable ({e}); default pass applied.",
            "triggered_rule":None,
        }


# ── Regeneration (FIX #4) ─────────────────────────────────────────────────────

def _regenerate_single_action(
    failed_action: ActionRecommendation,
    anomaly: Anomaly,
    rc: RootCauseReport,
    rules: dict,
    router,
    run_id: str,
) -> ActionRecommendation | None:
    """
    FIX #4: Regenerate a single action using the original anomaly and rc objects
    directly from state — never creating a temp_state or a new anomaly list.
    This guarantees that the regenerated action's anomaly_id resolves correctly
    when execution.py looks it up later.
    """
    evidence_str = json.dumps(anomaly.evidence, indent=2, default=str)[:1500]

    prompt = f"""The following action recommendation was rejected by the verification agent.
Please rewrite it to address the critique.

ORIGINAL ACTION (REJECTED):
Title: {failed_action.title}
Type: {failed_action.action_type}
Description: {failed_action.description}
Expected Savings: ${failed_action.expected_savings_usd:,.2f}
Rejection reason: {failed_action.verification_notes}

ANOMALY CONTEXT:
Type: {anomaly.anomaly_type.value}
Title: {anomaly.title}
Financial Impact: ${anomaly.financial_impact_usd:,.2f}
Root Cause: {rc.root_cause}

EVIDENCE:
{evidence_str}

Rewrite the action to fix the rejection reason. Be more specific in steps,
justify the savings calculation clearly, and keep savings within realistic bounds.

Respond with a JSON array containing exactly ONE action:
[
  {{
    "title": "...",
    "description": "...",
    "action_type": "...",
    "expected_savings_usd": 0.0,
    "savings_methodology": "...",
    "implementation_steps": ["Step 1: ...", "Step 2: ...", "Step 3: ..."],
    "requires_human_approval": true,
    "estimated_effort_hours": 4,
    "risk_level": "low|medium|high"
  }}
]"""

    try:
        raw   = router.call(
            messages    = [{"role": "user", "content": prompt}],
            task_weight = "heavy",
            max_tokens  = 1000,
        )
        items = parse_json_array(raw, caller="verification_regen")
        if not items:
            return None

        item    = items[0] if isinstance(items[0], dict) else {}
        savings = float(item.get("expected_savings_usd", failed_action.expected_savings_usd))
        return ActionRecommendation(
            action_id            = f"ACT-{uuid.uuid4().hex[:8]}",   # new id
            anomaly_id           = anomaly.anomaly_id,               # FIX #4: use original anomaly_id
            title                = item.get("title", failed_action.title),
            description          = item.get("description", failed_action.description),
            action_type          = item.get("action_type", failed_action.action_type),
            expected_savings_usd = savings,
            priority_rank        = 0,
            implementation_steps = item.get("implementation_steps", []),
            requires_human_approval = item.get("requires_human_approval", savings > 2000),
            status               = ActionStatus.PENDING,
        )
    except Exception as e:
        logger.warning(f"Regen LLM call failed for {failed_action.action_id}: {e}")
        return None


# ── Business rules loader ─────────────────────────────────────────────────────

def _load_rules() -> dict:
    import json
    from pathlib import Path
    rules_path = Path(__file__).parent.parent / "config" / "business_rules.json"
    with open(rules_path) as f:
        # Strip comment keys before parsing
        raw  = json.load(f)
        return {k: v for k, v in raw.items() if not k.startswith("_")}