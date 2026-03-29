# enterprise_cost_intelligence/main.py
"""
Enterprise Cost Intelligence — Pipeline Entry Point

FIX #2: Original pipeline_stages was a list of 3-tuples where the second
element (the raw agent.run reference) was captured by `_` and thrown away.
The lambda in position 3 duplicated it. Confusing and fragile.

Fix: Plain list of (name, callable) pairs. Clean, readable, no dead elements.
Each callable is just the agent's run function — no lambda wrapper needed
because all stage-level agents take only `state` as their argument.
(Specialist agents take `state + anomalies` but they are called from
orchestrator.run, not directly from main.)
"""

import uuid
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from state.schema import PipelineState
from audit.audit_logger import log_event, flush_to_supabase

import agents.ingestion as ingestion_agent
import agents.anomaly_detection as anomaly_detection_agent
import agents.orchestrator as orchestrator_agent
import agents.action_recommendation as action_recommendation_agent
import agents.verification as verification_agent
import agents.execution as execution_agent

# ── Logging ───────────────────────────────────────────────────────────────────
# FIX #2: ensure the log directory exists before creating the FileHandler.
# audit_logger.py creates it too, but that module may not be imported yet when
# logging.basicConfig() runs at module-level.
_log_dir = Path(__file__).parent / "audit" / "logs"
_log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    datefmt = "%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_log_dir / "pipeline.log", mode="a"),
    ],
)
logger = logging.getLogger("main")


def run_pipeline() -> PipelineState:
    run_id = f"RUN-{uuid.uuid4().hex[:12].upper()}"

    logger.info(f"\n{'='*70}")
    logger.info(f"  FINOPS COPILOT — ENTERPRISE COST INTELLIGENCE PIPELINE")
    logger.info(f"  Run ID:  {run_id}")
    logger.info(f"  Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info(f"{'='*70}\n")

    state = PipelineState(run_id=run_id)

    # ── Pipeline stages ───────────────────────────────────────────────────────
    pipeline_stages = [
        ("1. Data Ingestion",            ingestion_agent.run),
        ("2. Anomaly Detection",         anomaly_detection_agent.run),
        ("3. Root Cause Analysis",       orchestrator_agent.run),
        ("4. Action Recommendation",     action_recommendation_agent.run),
        ("5. Verification",              verification_agent.run),
        ("6. Execution",                 execution_agent.run),
    ]

    for stage_name, stage_fn in pipeline_stages:
        logger.info(f"\n{'─'*50}")
        logger.info(f"  STAGE: {stage_name}")
        logger.info(f"{'─'*50}")

        log_event(run_id, "main", "stage_start", {"stage": stage_name})

        try:
            state = stage_fn(state)
            log_event(run_id, "main", "stage_complete", {
                "stage": stage_name,
                "anomalies": len(state.anomalies),
                "actions":   len(state.action_recommendations),
                "errors":    len(state.errors),
            })
        except Exception as e:
            error_msg = f"Stage '{stage_name}' failed: {e}"
            logger.error(error_msg, exc_info=True)
            state.errors.append(error_msg)
            log_event(run_id, "main", "stage_error", {
                "stage": stage_name,
                "error": str(e),
            }, severity="error")
            # Continue to next stage — don't crash the whole pipeline

    # ── Finalize ──────────────────────────────────────────────────────────────
    log_event(run_id, "main", "pipeline_complete", state.financial_summary)

    # Flush audit events to Supabase
    try:
        flushed = flush_to_supabase(run_id)
        logger.info(f"Flushed {flushed} audit events to Supabase")
    except Exception as e:
        logger.warning(f"Audit flush to Supabase failed: {e}")

    # Save pipeline run summary to Supabase
    try:
        from core.database import get_db
        db = get_db()
        db.save_pipeline_run(run_id, state.financial_summary)
        logger.info("Pipeline run saved to Supabase")
    except Exception as e:
        logger.warning(f"Pipeline run save failed: {e}")

    # ── Final report ──────────────────────────────────────────────────────────
    fs = state.financial_summary

    print(f"\n{'═'*70}")
    print(f"  FINOPS COPILOT — FINAL REPORT")
    print(f"  Run ID:    {state.run_id}")
    print(f"  Completed: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'═'*70}")

    print(f"\n  ANOMALIES DETECTED:  {fs.get('anomalies_detected', 0)}")
    for a in state.anomalies:
        severity_tag = a.severity.value.upper()
        print(f"    [{severity_tag:8s}] {a.title}")
        print(f"             Impact: ${a.financial_impact_usd:,.0f}")
        print(f"             Agent:  {a.assigned_agent}")

    print(f"\n  ACTIONS GENERATED:   {fs.get('actions_generated', 0)}")
    for ar in state.action_recommendations:
        status_tag = ar.status.value.upper()
        print(f"    [{status_tag:14s}] {ar.title}")
        print(f"             Savings: ${ar.expected_savings_usd:,.0f}")

    print(f"\n  {'─'*50}")
    print(f"  FINANCIAL IMPACT SUMMARY")
    print(f"  {'─'*50}")
    print(f"    Total Exposure:         ${fs.get('total_financial_exposure_usd', 0):>15,.0f}")
    print(f"    Recoverable Savings:    ${fs.get('total_recoverable_savings_usd', 0):>15,.0f}")
    print(f"    Auto-Executed:          ${fs.get('auto_executed_savings_usd', 0):>15,.0f}")
    print(f"    Pending Human Approval: ${fs.get('pending_human_approval_savings_usd', 0):>15,.0f}")
    print(f"    Rejected:               ${fs.get('rejected_savings_usd', 0):>15,.0f}")
    print(f"    ROI:                     {fs.get('roi_multiple', 0):>14.1f}%")
    print(f"  {'─'*50}")

    if state.errors:
        print(f"\n  ⚠ ERRORS ({len(state.errors)}):")
        for err in state.errors:
            print(f"    • {err}")

    if state.warnings:
        print(f"\n  ⚠ WARNINGS ({len(state.warnings)}):")
        for w in state.warnings[:10]:
            print(f"    • {w}")
        if len(state.warnings) > 10:
            print(f"    ... and {len(state.warnings) - 10} more")

    print(f"\n{'═'*70}\n")

    return state


if __name__ == "__main__":
    run_pipeline()
