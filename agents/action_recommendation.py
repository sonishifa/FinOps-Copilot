# enterprise_cost_intelligence/agents/action_recommendation.py
"""
Action Recommendation Agent

FIX #15: Replaced inline array JSON parsing with core.json_parser.parse_json_array().
         Truncated LLM responses no longer raise unhandled JSONDecodeError.
Token budget: evidence truncated, max_tokens set to keep responses within budget.
"""

import uuid
import json
import logging
from state.schema import (
    PipelineState, Anomaly, RootCauseReport,
    ActionRecommendation, ActionStatus,
)
from audit.audit_logger import log_event
from core.llm_router import get_router
from core.json_parser import parse_json_array

logger = logging.getLogger(__name__)

MAX_EVIDENCE_CHARS = 1500


def run(state: PipelineState) -> PipelineState:
    logger.info("=== Action Recommendation Agent: Generating action plans ===")
    router = get_router()

    rc_map  = {r.anomaly_id: r for r in state.root_cause_reports}
    ano_map = {a.anomaly_id: a for a in state.anomalies}

    recommendations: list[ActionRecommendation] = []

    for anomaly_id, rc in rc_map.items():
        anomaly = ano_map.get(anomaly_id)
        if not anomaly:
            continue

        prompt = _build_prompt(anomaly, rc)

        try:
            raw = router.call(
                messages    = [{"role": "user", "content": prompt}],
                task_weight = "heavy",
                max_tokens  = 1500,
            )
            # FIX #15: shared array parser — handles fences and truncation
            items = parse_json_array(raw, caller="action_recommendation_agent")

            if items is None:
                raise ValueError("parse_json_array returned None")

            actions = _build_actions(items, anomaly_id)
            recommendations.extend(actions)
            log_event(
                state.run_id, "action_recommendation_agent", "actions_generated",
                {
                    "anomaly_id":   anomaly_id,
                    "action_count": len(actions),
                    "total_savings":round(sum(a.expected_savings_usd for a in actions), 2),
                },
                anomaly_id=anomaly_id,
            )
        except Exception as e:
            logger.warning(f"Action recommendation failed for {anomaly_id}: {e}")
            state.warnings.append(f"Action rec fallback for {anomaly_id}: {e}")
            recommendations.append(_fallback_action(anomaly, rc))

    # Global priority ranking by expected savings (highest first)
    recommendations.sort(key=lambda a: a.expected_savings_usd, reverse=True)
    for rank, action in enumerate(recommendations, 1):
        action.priority_rank = rank

    state.action_recommendations = recommendations

    total_savings = sum(a.expected_savings_usd for a in recommendations)
    log_event(
        state.run_id, "action_recommendation_agent", "ranking_complete",
        {
            "total_actions":             len(recommendations),
            "total_expected_savings_usd":round(total_savings, 2),
            "requiring_human_approval":  sum(1 for a in recommendations if a.requires_human_approval),
        }
    )
    logger.info(
        f"=== Action Recommendation: {len(recommendations)} actions, "
        f"${total_savings:,.0f} expected savings ==="
    )
    return state


def _build_prompt(anomaly: Anomaly, rc: RootCauseReport) -> str:
    evidence_str = json.dumps(anomaly.evidence, indent=2, default=str)
    if len(evidence_str) > MAX_EVIDENCE_CHARS:
        evidence_str = evidence_str[:MAX_EVIDENCE_CHARS] + "\n... [truncated]"

    return f"""You are a senior enterprise cost intelligence advisor.

ANOMALY:
ID: {anomaly.anomaly_id}
Type: {anomaly.anomaly_type.value}
Title: {anomaly.title}
Financial Impact: ${anomaly.financial_impact_usd:,.2f}

ROOT CAUSE:
{rc.root_cause}
Confidence: {rc.confidence_score:.0%}
Contributing factors: {', '.join(rc.contributing_factors[:3])}

EVIDENCE (summarised):
{evidence_str}

Generate 1-3 concrete, actionable recommendations. For each:
- Specific implementation steps (not vague advice)
- Realistic savings calculation with methodology shown
- Whether human approval is needed (required if savings > $2,000 or involves termination/deletion)
- action_type must be one of: consolidate_vendor | resize_resource | decommission_resource |
  reassign_ticket | renegotiate_contract | enforce_compliance | escalate_to_manager | patch_config

Respond with a JSON array only — no prose before or after:
[
  {{
    "title": "...",
    "description": "...",
    "action_type": "...",
    "expected_savings_usd": 0.0,
    "savings_methodology": "explain the calculation here",
    "implementation_steps": ["Step 1: ...", "Step 2: ...", "Step 3: ..."],
    "requires_human_approval": true,
    "estimated_effort_hours": 4,
    "risk_level": "low|medium|high"
  }}
]"""


def _build_actions(items: list, anomaly_id: str) -> list[ActionRecommendation]:
    actions = []
    for item in items:
        if not isinstance(item, dict):
            continue
        savings = float(item.get("expected_savings_usd", 0))
        actions.append(ActionRecommendation(
            action_id            = f"ACT-{uuid.uuid4().hex[:8]}",
            anomaly_id           = anomaly_id,
            title                = item.get("title", "Untitled action"),
            description          = item.get("description", ""),
            action_type          = item.get("action_type", "escalate_to_manager"),
            expected_savings_usd = savings,
            priority_rank        = 0,     # placeholder; set after global sort
            implementation_steps = item.get("implementation_steps", []),
            requires_human_approval = item.get("requires_human_approval", savings > 2000),
            status               = ActionStatus.PENDING,
        ))
    return actions


def _fallback_action(anomaly: Anomaly, rc: RootCauseReport) -> ActionRecommendation:
    return ActionRecommendation(
        action_id            = f"ACT-{uuid.uuid4().hex[:8]}",
        anomaly_id           = anomaly.anomaly_id,
        title                = f"Manual review required: {anomaly.title[:60]}",
        description          = f"LLM action generation failed. Root cause: {rc.root_cause[:200]}",
        action_type          = "escalate_to_manager",
        expected_savings_usd = anomaly.financial_impact_usd * 0.5,
        priority_rank        = 0,
        implementation_steps = [
            "Review the anomaly and root cause report manually",
            "Consult the relevant team lead",
            "Implement corrective action and verify outcome",
        ],
        requires_human_approval = True,
        status               = ActionStatus.PENDING,
    )