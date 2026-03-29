# enterprise_cost_intelligence/agents/infrastructure_agent.py
"""
Infrastructure Agent

FIX #15: Replaced inline _parse_json() with core.json_parser.parse_json_object().
Token budget: evidence truncated to 2000 chars per call.
"""

import json
import logging
from state.schema import PipelineState, Anomaly, RootCauseReport, AnomalyType
from tools.infrastructure_tools import classify_spend_spike_cause
from audit.audit_logger import log_event
from core.llm_router import get_router
from core.json_parser import parse_json_object

logger = logging.getLogger(__name__)

MAX_EVIDENCE_CHARS = 2000


def run(state: PipelineState, anomalies: list[Anomaly]) -> PipelineState:
    logger.info(f"=== Infrastructure Agent: Processing {len(anomalies)} anomalies ===")
    router = get_router()
    ds     = state.raw_datasets

    for anomaly in anomalies:
        log_event(
            state.run_id, "infrastructure_agent", "processing_anomaly",
            {"anomaly_id": anomaly.anomaly_id, "type": anomaly.anomaly_type.value},
            anomaly_id=anomaly.anomaly_id,
        )

        # Pre-classify with heuristics before calling LLM (reduces hallucination)
        heuristic_signals: dict = {}
        if anomaly.anomaly_type == AnomalyType.SPEND_SPIKE and "cloud_spend" in ds:
            try:
                heuristic_signals = classify_spend_spike_cause(
                    anomaly.evidence, ds["cloud_spend"]
                )
            except Exception as e:
                logger.debug(f"Heuristic pre-classification failed: {e}")

        prompt = _build_prompt(anomaly, heuristic_signals)

        try:
            # ── Step 1: PROCESS — initial root cause diagnosis ────────────
            raw = router.call(
                messages    = [{"role": "user", "content": prompt}],
                task_weight = "heavy",
                max_tokens  = 1200,
            )
            # FIX #15: shared parser — handles truncation and malformed JSON
            result = parse_json_object(raw, caller="infrastructure_agent")

            if result is None:
                raise ValueError("parse_json_object returned None")

            logger.info(
                f"[Process] {anomaly.anomaly_id}: "
                f"initial classification = {result.get('spike_category', '?')}, "
                f"cause = {result.get('root_cause', '?')[:50]}"
            )

            # ── Step 2: REFLECTION — self-critique the diagnosis ──────────
            reflection_prompt = f"""You are a cloud infrastructure cost analyst reviewing an initial diagnosis.

ORIGINAL ANOMALY:
Title: {anomaly.title}
Type: {anomaly.anomaly_type.value}
Financial Impact: ${anomaly.financial_impact_usd:,.2f}

INITIAL DIAGNOSIS:
Root Cause: {result.get('root_cause', 'Unknown')}
Category: {result.get('spike_category', 'unknown')}
Confidence: {result.get('confidence_score', 0.5)}
Contributing Factors: {json.dumps(result.get('contributing_factors', []))}
Corrective Action: {result.get('recommended_corrective_action', 'None')}
Reasoning: {result.get('llm_reasoning', '')[:500]}

Critically evaluate:
1. Is the spike_category correct? Could it actually be a different cause?
   - provisioning_error vs autoscaling_misconfiguration are often confused
   - traffic_driven spikes should correlate with business metrics
2. Is the corrective action SPECIFIC enough to be actionable?
3. Were any infrastructure-specific factors missed (region, service, environment)?
4. Is the confidence justified?

Respond with refined JSON only:
{{
  "root_cause": "refined diagnosis",
  "spike_category": "provisioning_error|traffic_driven|autoscaling_misconfiguration|shadow_it|other",
  "confidence_score": 0.85,
  "contributing_factors": ["refined list"],
  "evidence_summary": "refined summary",
  "recommended_corrective_action": "refined, specific action",
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
                refined = parse_json_object(reflection_raw, caller="infrastructure_agent_reflection")

                if refined:
                    result["root_cause"] = refined.get("root_cause", result.get("root_cause"))
                    result["spike_category"] = refined.get("spike_category", result.get("spike_category"))
                    result["confidence_score"] = refined.get("confidence_score", result.get("confidence_score"))
                    result["contributing_factors"] = refined.get("contributing_factors", result.get("contributing_factors"))
                    result["evidence_summary"] = refined.get("evidence_summary", result.get("evidence_summary"))
                    result["llm_reasoning"] = refined.get("llm_reasoning", result.get("llm_reasoning"))

                    log_event(
                        state.run_id, "infrastructure_agent", "reflection_complete",
                        {
                            "anomaly_id":   anomaly.anomaly_id,
                            "refined":      True,
                            "old_category": result.get("spike_category"),
                            "notes":        refined.get("reflection_notes", "")[:100],
                        },
                        anomaly_id=anomaly.anomaly_id,
                    )
                    logger.info(
                        f"[Reflection] {anomaly.anomaly_id}: "
                        f"refined → {refined.get('spike_category', '?')} "
                        f"(conf={refined.get('confidence_score', '?')})"
                    )
            except Exception as ref_err:
                logger.debug(f"Reflection step failed (non-fatal): {ref_err}")

            # ── Build final report from (potentially refined) result ───────
            report = RootCauseReport(
                anomaly_id           = anomaly.anomaly_id,
                root_cause           = result.get("root_cause", "Unknown"),
                contributing_factors = result.get("contributing_factors", []),
                evidence_summary     = result.get("evidence_summary", ""),
                confidence_score     = float(result.get("confidence_score", 0.5)),
                llm_reasoning        = result.get("llm_reasoning", raw),
                diagnosed_by         = "infrastructure_agent",
            )
            state.root_cause_reports.append(report)
            log_event(
                state.run_id, "infrastructure_agent", "root_cause_diagnosed",
                {
                    "anomaly_id":       anomaly.anomaly_id,
                    "root_cause":       report.root_cause[:80],
                    "spike_category":   result.get("spike_category", "unknown"),
                    "confidence":       report.confidence_score,
                    "reflected":        True,
                },
                anomaly_id=anomaly.anomaly_id,
            )
            logger.info(f"Diagnosed {anomaly.anomaly_id}: {report.root_cause[:70]}")

        except Exception as e:
            logger.warning(f"Infrastructure agent failed for {anomaly.anomaly_id}: {e}")
            state.warnings.append(f"Infrastructure agent fallback for {anomaly.anomaly_id}: {e}")
            state.root_cause_reports.append(_fallback(anomaly, heuristic_signals))

    return state


def _build_prompt(anomaly: Anomaly, signals: dict) -> str:
    evidence_str = json.dumps(anomaly.evidence, indent=2, default=str)
    if len(evidence_str) > MAX_EVIDENCE_CHARS:
        evidence_str = evidence_str[:MAX_EVIDENCE_CHARS] + "\n... [truncated]"
    signals_str = json.dumps(signals, indent=2, default=str)

    return f"""You are a cloud infrastructure cost analyst (AWS/Azure/GCP expert).

ANOMALY:
Title: {anomaly.title}
Type: {anomaly.anomaly_type.value}
Financial Impact: ${anomaly.financial_impact_usd:,.2f}
Description: {anomaly.description}

EVIDENCE:
{evidence_str}

PRE-CLASSIFICATION SIGNALS (rule-based):
{signals_str}

Diagnose whether the root cause is:
  (a) provisioning_error — over-provisioned or duplicated resources
  (b) traffic_driven — legitimate workload growth
  (c) autoscaling_misconfiguration — scaling rule triggered incorrectly
  (d) shadow_it — untagged or unmanaged resources
  (e) other — explain

What specific corrective action should be taken?

Respond in JSON only — no prose before or after:
{{
  "root_cause": "...",
  "spike_category": "provisioning_error|traffic_driven|autoscaling_misconfiguration|shadow_it|other",
  "confidence_score": 0.85,
  "contributing_factors": ["...", "..."],
  "evidence_summary": "...",
  "recommended_corrective_action": "...",
  "llm_reasoning": "..."
}}"""


def _fallback(anomaly: Anomaly, signals: dict) -> RootCauseReport:
    classification = signals.get("heuristic_classification", "unknown")
    return RootCauseReport(
        anomaly_id           = anomaly.anomaly_id,
        root_cause           = f"Rule-based heuristic: {classification}",
        contributing_factors = [
            "Heuristic pre-classification only (LLM unavailable)",
            f"Signal: {json.dumps(signals, default=str)[:150]}",
        ],
        evidence_summary     = f"Heuristic classification: {classification}",
        confidence_score     = 0.5,
        llm_reasoning        = "LLM unavailable; heuristic fallback used.",
        diagnosed_by         = "infrastructure_agent_fallback",
    )