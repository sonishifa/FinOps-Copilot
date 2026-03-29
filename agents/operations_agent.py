# enterprise_cost_intelligence/agents/operations_agent.py
"""
Operations Agent

FIX #5:  Now uses analyse_sla_breach_history() evidence (passed through
         anomaly.evidence from anomaly_detection.py) rather than re-running
         live queue analysis that returns empty on historical data.

FIX #15: Replaced inline _parse_json() with core.json_parser.parse_json_object().
Token budget: evidence truncated to 2000 chars per call.
"""

import json
import logging
from state.schema import PipelineState, Anomaly, RootCauseReport, AnomalyType
from audit.audit_logger import log_event
from core.llm_router import get_router
from core.json_parser import parse_json_object

logger = logging.getLogger(__name__)

MAX_EVIDENCE_CHARS = 2000


def run(state: PipelineState, anomalies: list[Anomaly]) -> PipelineState:
    logger.info(f"=== Operations Agent: Processing {len(anomalies)} anomalies ===")
    router = get_router()

    for anomaly in anomalies:
        log_event(
            state.run_id, "operations_agent", "processing_anomaly",
            {"anomaly_id": anomaly.anomaly_id, "type": anomaly.anomaly_type.value},
            anomaly_id=anomaly.anomaly_id,
        )

        evidence_str = json.dumps(anomaly.evidence, indent=2, default=str)
        if len(evidence_str) > MAX_EVIDENCE_CHARS:
            evidence_str = evidence_str[:MAX_EVIDENCE_CHARS] + "\n... [truncated]"

        prompt = f"""You are a service operations and ITSM specialist.

ANOMALY:
Title: {anomaly.title}
Type: {anomaly.anomaly_type.value}
Financial Impact (penalty exposure): ${anomaly.financial_impact_usd:,.2f}
Description: {anomaly.description}

EVIDENCE (historical breach analysis):
{evidence_str}

Based on this breach history data, diagnose:
1. Primary root cause — is this capacity, skill gap, process failure, tooling, or external factors?
2. Which team/priority tier is most impacted?
3. A specific 3-step recovery plan with clear owners and timelines
4. Whether immediate escalation is warranted

Respond in JSON only — no prose before or after:
{{
  "root_cause": "...",
  "issue_category": "capacity|skill|process|tooling|external",
  "confidence_score": 0.85,
  "contributing_factors": ["...", "...", "..."],
  "evidence_summary": "...",
  "recovery_plan": [
    "Step 1: ...",
    "Step 2: ...",
    "Step 3: ..."
  ],
  "escalate_to": "...",
  "llm_reasoning": "..."
}}"""

        try:
            # ── Step 1: PROCESS — initial root cause diagnosis ────────────
            raw = router.call(
                messages    = [{"role": "user", "content": prompt}],
                task_weight = "heavy",
                max_tokens  = 1200,
            )
            # FIX #15: shared parser
            result = parse_json_object(raw, caller="operations_agent")

            if result is None:
                raise ValueError("parse_json_object returned None")

            logger.info(
                f"[Process] {anomaly.anomaly_id}: "
                f"category = {result.get('issue_category', '?')}, "
                f"cause = {result.get('root_cause', '?')[:50]}"
            )

            # ── Step 2: REFLECTION — self-critique the diagnosis ──────────
            recovery_plan_str = json.dumps(result.get("recovery_plan", []))
            reflection_prompt = f"""You are a service operations director reviewing a diagnosis and recovery plan.

ORIGINAL ANOMALY:
Title: {anomaly.title}
Penalty Exposure: ${anomaly.financial_impact_usd:,.2f}

INITIAL DIAGNOSIS:
Root Cause: {result.get('root_cause', 'Unknown')}
Category: {result.get('issue_category', 'unknown')}
Confidence: {result.get('confidence_score', 0.5)}
Contributing Factors: {json.dumps(result.get('contributing_factors', []))}
Recovery Plan: {recovery_plan_str}
Escalate To: {result.get('escalate_to', 'none')}
Reasoning: {result.get('llm_reasoning', '')[:500]}

Critically evaluate:
1. Is the recovery plan SPECIFIC and ACTIONABLE within the time constraint?
   - Each step should have a clear owner and timeline
   - Generic advice like "improve processes" is not acceptable
2. Is the issue_category correct? capacity vs process vs tooling are distinct
3. Was the escalation target appropriate for the severity?
4. Were any contributing factors missed (team skill gaps, tool outages, etc.)?

Respond with refined JSON only:
{{
  "root_cause": "refined diagnosis",
  "issue_category": "capacity|skill|process|tooling|external",
  "confidence_score": 0.85,
  "contributing_factors": ["refined list"],
  "evidence_summary": "refined summary",
  "recovery_plan": ["Step 1: specific action (owner: X, by: Y)", "Step 2: ...", "Step 3: ..."],
  "escalate_to": "specific role or team",
  "missed_considerations": ["anything overlooked"],
  "reflection_notes": "what changed and why",
  "llm_reasoning": "refined reasoning"
}}"""

            try:
                reflection_raw = router.call(
                    messages    = [{"role": "user", "content": reflection_prompt}],
                    task_weight = "light",
                    max_tokens  = 800,
                )
                refined = parse_json_object(reflection_raw, caller="operations_agent_reflection")

                if refined:
                    result["root_cause"] = refined.get("root_cause", result.get("root_cause"))
                    result["issue_category"] = refined.get("issue_category", result.get("issue_category"))
                    result["confidence_score"] = refined.get("confidence_score", result.get("confidence_score"))
                    result["contributing_factors"] = refined.get("contributing_factors", result.get("contributing_factors"))
                    result["evidence_summary"] = refined.get("evidence_summary", result.get("evidence_summary"))
                    result["recovery_plan"] = refined.get("recovery_plan", result.get("recovery_plan", []))
                    result["escalate_to"] = refined.get("escalate_to", result.get("escalate_to"))
                    result["llm_reasoning"] = refined.get("llm_reasoning", result.get("llm_reasoning"))

                    log_event(
                        state.run_id, "operations_agent", "reflection_complete",
                        {
                            "anomaly_id":   anomaly.anomaly_id,
                            "refined":      True,
                            "category":     refined.get("issue_category", "?"),
                            "notes":        refined.get("reflection_notes", "")[:100],
                        },
                        anomaly_id=anomaly.anomaly_id,
                    )
                    logger.info(
                        f"[Reflection] {anomaly.anomaly_id}: "
                        f"refined → {refined.get('issue_category', '?')} "
                        f"(conf={refined.get('confidence_score', '?')})"
                    )
            except Exception as ref_err:
                logger.debug(f"Reflection step failed (non-fatal): {ref_err}")

            # Enrich anomaly evidence with the recovery plan for the action agent
            anomaly.evidence["recovery_plan"] = result.get("recovery_plan", [])
            anomaly.evidence["escalate_to"]   = result.get("escalate_to", "service_manager")

            # ── Build final report from (potentially refined) result ───────
            report = RootCauseReport(
                anomaly_id           = anomaly.anomaly_id,
                root_cause           = result.get("root_cause", "Unknown"),
                contributing_factors = result.get("contributing_factors", []),
                evidence_summary     = result.get("evidence_summary", ""),
                confidence_score     = float(result.get("confidence_score", 0.5)),
                llm_reasoning        = result.get("llm_reasoning", raw),
                diagnosed_by         = "operations_agent",
            )
            state.root_cause_reports.append(report)
            log_event(
                state.run_id, "operations_agent", "root_cause_diagnosed",
                {
                    "anomaly_id":     anomaly.anomaly_id,
                    "root_cause":     report.root_cause[:80],
                    "issue_category": result.get("issue_category", "unknown"),
                    "confidence":     report.confidence_score,
                    "reflected":      True,
                },
                anomaly_id=anomaly.anomaly_id,
            )
            logger.info(f"Diagnosed {anomaly.anomaly_id}: {report.root_cause[:70]}")

        except Exception as e:
            logger.warning(f"Operations agent failed for {anomaly.anomaly_id}: {e}")
            state.warnings.append(f"Operations agent fallback for {anomaly.anomaly_id}: {e}")
            state.root_cause_reports.append(_fallback(anomaly))

    return state


def _fallback(anomaly: Anomaly) -> RootCauseReport:
    return RootCauseReport(
        anomaly_id           = anomaly.anomaly_id,
        root_cause           = "Capacity shortfall — ticket volume exceeds team capacity at current staffing",
        contributing_factors = [
            "High breach rate across multiple priority tiers",
            "Uneven workload distribution across agent groups",
            "Possible tooling friction slowing resolution times",
        ],
        evidence_summary     = (
            f"Historical breach analysis indicates systemic SLA compliance issues. "
            f"Projected penalty exposure: ${anomaly.financial_impact_usd:,.0f}."
        ),
        confidence_score     = 0.55,
        llm_reasoning        = "LLM unavailable; rule-based fallback used.",
        diagnosed_by         = "operations_agent_fallback",
    )