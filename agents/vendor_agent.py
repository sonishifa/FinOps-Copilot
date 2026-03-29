# enterprise_cost_intelligence/agents/vendor_agent.py
"""
Vendor Intelligence Agent

FIX #15: Replaced inline JSON parsing with core.json_parser.parse_json_object().
         Truncated LLM responses (hit max_tokens mid-JSON) now produce a logged
         warning and fall through to the fallback diagnosis instead of raising
         an unhandled JSONDecodeError that silently swallowed the diagnosis.

Token budget: evidence dicts are truncated to 2000 chars before being sent
to the LLM to stay within Groq's 500K/day free limit.
"""

import json
import logging
from state.schema import (
    PipelineState, Anomaly, RootCauseReport, AnomalyType
)
from tools.vendor_tools import vendor_spend_concentration
from audit.audit_logger import log_event
from core.llm_router import get_router
from core.json_parser import parse_json_object

logger = logging.getLogger(__name__)

# Token budget: max chars of evidence/context per LLM call
MAX_EVIDENCE_CHARS = 2000
MAX_CONTEXT_CHARS  = 1500


def run(state: PipelineState, anomalies: list[Anomaly]) -> PipelineState:
    logger.info(f"=== Vendor Agent: Processing {len(anomalies)} anomalies ===")
    router = get_router()
    ds     = state.raw_datasets

    for anomaly in anomalies:
        log_event(
            state.run_id, "vendor_agent", "processing_anomaly",
            {"anomaly_id": anomaly.anomaly_id, "type": anomaly.anomaly_type.value},
            anomaly_id=anomaly.anomaly_id,
        )

        context  = _build_context(anomaly, ds)
        # Truncate evidence to stay within token budget
        evidence_str = json.dumps(anomaly.evidence, indent=2, default=str)
        if len(evidence_str) > MAX_EVIDENCE_CHARS:
            evidence_str = evidence_str[:MAX_EVIDENCE_CHARS] + "\n... [truncated for token budget]"
        if len(context) > MAX_CONTEXT_CHARS:
            context = context[:MAX_CONTEXT_CHARS] + "\n... [truncated]"

        prompt = f"""You are a senior procurement analyst and vendor intelligence expert.

ANOMALY DETECTED:
Type: {anomaly.anomaly_type.value}
Title: {anomaly.title}
Description: {anomaly.description}
Financial Impact: ${anomaly.financial_impact_usd:,.2f}

EVIDENCE:
{evidence_str}

ADDITIONAL CONTEXT:
{context}

Your task:
1. Identify the ROOT CAUSE of this anomaly with a confidence score (0.0-1.0)
2. List 3-5 CONTRIBUTING FACTORS
3. Write a concise EVIDENCE SUMMARY (2-3 sentences)
4. Provide CHAIN OF THOUGHT reasoning

Respond in this exact JSON format only — no prose before or after:
{{
  "root_cause": "...",
  "confidence_score": 0.85,
  "contributing_factors": ["...", "...", "..."],
  "evidence_summary": "...",
  "llm_reasoning": "..."
}}"""

        try:
            # ── Step 1: PROCESS — initial root cause diagnosis ────────────
            raw = router.call(
                messages    = [{"role": "user", "content": prompt}],
                task_weight = "heavy",
                max_tokens  = 1024,
            )
            # FIX #15: shared parser handles fences, truncation, JSONDecodeError
            result = parse_json_object(raw, caller="vendor_agent")

            if result is None:
                raise ValueError("parse_json_object returned None — using fallback")

            logger.info(
                f"[Process] {anomaly.anomaly_id}: "
                f"initial diagnosis = {result.get('root_cause', '?')[:60]}"
            )

            # ── Step 2: REFLECTION — self-critique the diagnosis ──────────
            reflection_prompt = f"""You are a senior procurement analyst reviewing a diagnosis.

ORIGINAL ANOMALY:
Title: {anomaly.title}
Type: {anomaly.anomaly_type.value}
Financial Impact: ${anomaly.financial_impact_usd:,.2f}

INITIAL DIAGNOSIS:
Root Cause: {result.get('root_cause', 'Unknown')}
Confidence: {result.get('confidence_score', 0.5)}
Contributing Factors: {json.dumps(result.get('contributing_factors', []))}
Reasoning: {result.get('llm_reasoning', '')[:500]}

Critically evaluate this diagnosis:
1. Is the root cause accurate and specific enough, or is it too vague?
2. Are there contributing factors that were MISSED?
3. Is the confidence score justified — too high or too low given the evidence?
4. Could there be an alternative explanation that was not considered?

Respond with a refined diagnosis in JSON only:
{{
  "root_cause": "refined root cause (keep original if correct, improve if vague)",
  "confidence_score": 0.85,
  "contributing_factors": ["original + any missed factors"],
  "evidence_summary": "refined summary",
  "missed_considerations": ["anything the initial diagnosis overlooked"],
  "reflection_notes": "what was changed and why",
  "llm_reasoning": "refined chain of thought"
}}"""

            try:
                reflection_raw = router.call(
                    messages    = [{"role": "user", "content": reflection_prompt}],
                    task_weight = "light",
                    max_tokens  = 800,
                )
                refined = parse_json_object(reflection_raw, caller="vendor_agent_reflection")

                if refined:
                    # Merge reflection into result — reflection overrides initial
                    result["root_cause"] = refined.get("root_cause", result.get("root_cause"))
                    result["confidence_score"] = refined.get("confidence_score", result.get("confidence_score"))
                    result["contributing_factors"] = refined.get("contributing_factors", result.get("contributing_factors"))
                    result["evidence_summary"] = refined.get("evidence_summary", result.get("evidence_summary"))
                    result["llm_reasoning"] = refined.get("llm_reasoning", result.get("llm_reasoning"))
                    result["reflection_notes"] = refined.get("reflection_notes", "")

                    log_event(
                        state.run_id, "vendor_agent", "reflection_complete",
                        {
                            "anomaly_id":   anomaly.anomaly_id,
                            "refined":      True,
                            "missed_items": refined.get("missed_considerations", []),
                            "notes":        refined.get("reflection_notes", "")[:100],
                        },
                        anomaly_id=anomaly.anomaly_id,
                    )
                    logger.info(
                        f"[Reflection] {anomaly.anomaly_id}: "
                        f"refined confidence {refined.get('confidence_score', '?')}"
                    )
            except Exception as ref_err:
                logger.debug(f"Reflection step failed (non-fatal): {ref_err}")
                # Continue with initial diagnosis — reflection is optional

            # ── Build final report from (potentially refined) result ───────
            report = RootCauseReport(
                anomaly_id          = anomaly.anomaly_id,
                root_cause          = result.get("root_cause", "Unable to determine"),
                contributing_factors= result.get("contributing_factors", []),
                evidence_summary    = result.get("evidence_summary", ""),
                confidence_score    = float(result.get("confidence_score", 0.5)),
                llm_reasoning       = result.get("llm_reasoning", raw),
                diagnosed_by        = "vendor_agent",
            )
            state.root_cause_reports.append(report)
            log_event(
                state.run_id, "vendor_agent", "root_cause_diagnosed",
                {
                    "anomaly_id":  anomaly.anomaly_id,
                    "root_cause":  report.root_cause[:80],
                    "confidence":  report.confidence_score,
                    "reflected":   True,
                },
                anomaly_id=anomaly.anomaly_id,
            )
            logger.info(
                f"Diagnosed {anomaly.anomaly_id} "
                f"(conf={report.confidence_score:.0%}): {report.root_cause[:70]}..."
            )

        except Exception as e:
            logger.warning(f"Vendor agent LLM call failed for {anomaly.anomaly_id}: {e}")
            state.warnings.append(f"Vendor agent fallback for {anomaly.anomaly_id}: {e}")
            state.root_cause_reports.append(_fallback_diagnosis(anomaly))

    return state


def _build_context(anomaly: Anomaly, ds: dict) -> str:
    lines = []
    if anomaly.anomaly_type == AnomalyType.DUPLICATE_VENDOR and "corporate_procurement" in ds:
        df = ds["corporate_procurement"]
        lines.append(f"Total contracts in dataset: {len(df):,}")
        if "contract_award_amount" in df.columns:
            top = (
                df.nlargest(5, "contract_award_amount")
                [["supplier", "contract_award_amount", "commodity_category"]]
                .to_string(index=False)
            )
            lines.append(f"Top 5 suppliers by value:\n{top}")
    elif "procurement_kpi" in ds:
        df = ds["procurement_kpi"]
        lines.append(f"Procurement KPI records: {len(df):,}")
        if "compliance" in df.columns:
            comp_rate = df["compliance"].str.strip().str.lower().eq("yes").mean() * 100
            lines.append(f"Overall compliance rate: {comp_rate:.1f}%")
    return "\n".join(lines) if lines else "No additional context available."


def _fallback_diagnosis(anomaly: Anomaly) -> RootCauseReport:
    return RootCauseReport(
        anomaly_id           = anomaly.anomaly_id,
        root_cause           = "Rule-based fallback: vendor overlap detected via fuzzy name matching",
        contributing_factors = [
            "Multiple vendors providing similar services",
            "Lack of centralised vendor master data",
            "Decentralised procurement process without approval gates",
        ],
        evidence_summary     = (
            f"Automated fuzzy matching detected overlap. "
            f"Estimated impact: ${anomaly.financial_impact_usd:,.0f}."
        ),
        confidence_score     = 0.6,
        llm_reasoning        = "LLM call failed or returned unparseable JSON; rule-based fallback used.",
        diagnosed_by         = "vendor_agent_fallback",
    )