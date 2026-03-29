# enterprise_cost_intelligence/agents/ingestion.py
"""
Data Ingestion Agent

FIX #11: LLM quality narration was being appended to normalization_notes
(a list[str]) as a multi-line string blob, making it impossible to
serialize cleanly and mixing structured notes with freeform LLM output.
Now stored in DataQualityReport.llm_quality_assessment (dedicated str field).
"""

import logging
from state.schema import PipelineState, DataQualityReport
from tools.ingestion_tools import load_all_datasets, profile_dataframe
from audit.audit_logger import log_event
from core.llm_router import get_router

logger = logging.getLogger(__name__)


def run(state: PipelineState) -> PipelineState:
    logger.info("=== Data Ingestion Agent: Starting ===")
    log_event(state.run_id, "ingestion_agent", "agent_start", {})

    # 1. Load all datasets
    try:
        datasets = load_all_datasets()
        state.raw_datasets = datasets
        logger.info(f"Loaded {len(datasets)} datasets: {list(datasets.keys())}")
    except Exception as e:
        state.errors.append(f"Ingestion failed: {e}")
        log_event(state.run_id, "ingestion_agent", "load_error",
                  {"error": str(e)}, severity="error")
        return state

    # 2. Profile each dataset
    profiles      = [profile_dataframe(name, df) for name, df in datasets.items()]
    total_records = sum(p["rows"]           for p in profiles)
    total_dups    = sum(p["duplicate_rows"] for p in profiles)
    all_nulls     = {
        f"{p['source']}.{col}": count
        for p in profiles
        for col, count in p["null_counts"].items()
    }

    # 3. Build quality report with structured fields
    quality = DataQualityReport(
        total_records        = total_records,
        null_counts          = all_nulls,
        duplicate_count      = total_dups,
        data_sources_loaded  = list(datasets.keys()),
        schema_issues        = [],
        normalization_notes  = [
            "Currency columns coerced to float via pd.to_numeric(errors='coerce')",
            "Column names normalized to snake_case",
            "Non-numeric currency values converted to NaN (not dropped)",
        ],
        llm_quality_assessment = "",   # filled below
    )

    # 4. LLM narrates the quality report — stored in its own field
    try:
        router = get_router()
        profile_summary = "\n".join(
            f"- {p['source']}: {p['rows']:,} rows, "
            f"{p['duplicate_rows']} dups, "
            f"{len(p['null_counts'])} cols with nulls"
            for p in profiles
        )
        prompt = (
            f"You are a data quality analyst reviewing dataset profiles for an "
            f"enterprise cost intelligence pipeline.\n\n"
            f"Profiles:\n{profile_summary}\n\n"
            f"Total records: {total_records:,}, Total duplicate rows: {total_dups}\n\n"
            f"Summarize data quality concerns that could affect downstream anomaly detection. "
            f"Be concise — 3 to 5 bullet points max. Focus on: missing values in key financial "
            f"columns, duplicate records that could inflate savings estimates, and dataset size "
            f"adequacy for statistical anomaly detection."
        )
        narration = router.call(
            messages    = [{"role": "user", "content": prompt}],
            task_weight = "light",
            max_tokens  = 512,
        )
        # FIX #11: store in dedicated field, not mixed into normalization_notes
        quality.llm_quality_assessment = narration.strip()
        logger.info(f"Quality assessment: {narration[:150]}...")
    except Exception as e:
        state.warnings.append(f"Quality narration LLM call failed: {e}")
        quality.llm_quality_assessment = f"LLM assessment unavailable: {e}"

    state.data_quality_report = quality

    log_event(
        state.run_id, "ingestion_agent", "ingestion_complete",
        {
            "datasets_loaded":  len(datasets),
            "total_records":    total_records,
            "total_duplicates": total_dups,
            "llm_assessment_preview": quality.llm_quality_assessment[:100],
        }
    )
    logger.info(
        f"=== Data Ingestion Agent: Done. "
        f"{total_records:,} records across {len(datasets)} sources ==="
    )
    return state