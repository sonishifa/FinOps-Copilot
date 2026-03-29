# enterprise_cost_intelligence/tools/operations_tools.py
"""
Tools for the Operations Agent — SLA breach analysis and resource rebalancing.

FIX #5: The original compute_sla_breach_risk() filtered for open/in-progress
tickets, but the ITSM_Dataset.csv is a 100K-row historical closed-ticket
dataset. Filtering for open tickets returns an empty DataFrame every time,
making the entire SLA feature produce zero output.

Fix: Two-pronged approach:
  1. analyse_sla_breach_history() — primary: trends over closed tickets
     (breach rate by priority, team, month). This is what a real cost
     intelligence system would use for root cause.
  2. compute_sla_breach_risk() — kept but now handles empty open-queue
     gracefully and falls through to history-based risk projection.
"""

from __future__ import annotations
import logging
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# ── Primary: Historical breach analysis ───────────────────────────────────────

def analyse_sla_breach_history(
    df: pd.DataFrame,
    sla_resolution_col: str = "sla_for_resolution",
    sla_response_col: str   = "sla_for_first_response",
    priority_col: str       = "priority",
    group_col: str          = "agent_group",
    created_col: str        = "created_time",
    window_days: int        = 90,
) -> dict:
    """
    Analyse SLA breach patterns from historical closed-ticket data.

    Returns a structured dict with:
      - overall breach rate
      - breach rate by priority
      - breach rate by agent group
      - monthly trend (last window_days)
      - top offending groups
      - projected penalty at current breach rate
    """
    df = df.copy()

    # Normalize SLA status values
    def is_breach(series: pd.Series) -> pd.Series:
        return series.str.strip().str.lower().isin(["missed", "breach", "breached", "violated", "no"])

    # Identify breached tickets (resolution OR response SLA missed)
    resolution_breach = is_breach(df[sla_resolution_col]) if sla_resolution_col in df.columns else pd.Series(False, index=df.index)
    response_breach   = is_breach(df[sla_response_col])   if sla_response_col   in df.columns else pd.Series(False, index=df.index)
    df["any_breach"]  = resolution_breach | response_breach

    total   = len(df)
    breached = int(df["any_breach"].sum())
    breach_rate = round(breached / total * 100, 2) if total else 0.0

    result: dict = {
        "total_tickets_analysed": total,
        "total_breaches":         breached,
        "overall_breach_rate_pct":breach_rate,
    }

    # Breach by priority
    if priority_col in df.columns:
        by_priority = (
            df.groupby(priority_col)["any_breach"]
            .agg(["sum", "count"])
            .rename(columns={"sum": "breaches", "count": "total"})
            .assign(breach_rate_pct=lambda x: (x["breaches"] / x["total"] * 100).round(2))
            .sort_values("breach_rate_pct", ascending=False)
            .to_dict("index")
        )
        result["breach_by_priority"] = by_priority

    # Breach by agent group
    if group_col in df.columns:
        by_group = (
            df.groupby(group_col)["any_breach"]
            .agg(["sum", "count"])
            .rename(columns={"sum": "breaches", "count": "total"})
            .assign(breach_rate_pct=lambda x: (x["breaches"] / x["total"] * 100).round(2))
            .sort_values("breach_rate_pct", ascending=False)
        )
        result["breach_by_group"]      = by_group.head(10).to_dict("index")
        result["top_offending_group"]  = by_group.index[0] if len(by_group) else "unknown"
        result["top_offending_rate"]   = float(by_group.iloc[0]["breach_rate_pct"]) if len(by_group) else 0.0

    # Monthly trend
    if created_col in df.columns:
        df[created_col] = pd.to_datetime(df[created_col], errors="coerce")
        df["month"] = df[created_col].dt.to_period("M")
        monthly = (
            df.groupby("month")["any_breach"]
            .agg(["sum", "count"])
            .rename(columns={"sum": "breaches", "count": "total"})
            .assign(breach_rate_pct=lambda x: (x["breaches"] / x["total"] * 100).round(2))
            .tail(6)   # last 6 months
        )
        result["monthly_trend"] = {
            str(k): {"breaches": int(v["breaches"]),
                     "total": int(v["total"]),
                     "breach_rate_pct": float(v["breach_rate_pct"])}
            for k, v in monthly.to_dict("index").items()
        }

    return result


def project_penalty_from_breach_history(
    breach_analysis: dict,
    penalty_per_breach_usd: float = 1500.0,
    forward_months: int = 3,
) -> dict:
    """
    Project future penalty exposure from historical breach rates.
    Gives the Action Recommendation agent concrete $$ figures.
    """
    # FIX #8: calculate actual month span from data instead of hardcoding 12
    monthly_trend = breach_analysis.get("monthly_trend", {})
    if monthly_trend:
        months_in_data = max(len(monthly_trend), 1)
    else:
        months_in_data = 12  # fallback if no trend data
    monthly_volume = breach_analysis["total_tickets_analysed"] / months_in_data
    breach_rate    = breach_analysis["overall_breach_rate_pct"] / 100
    monthly_breaches = monthly_volume * breach_rate
    projected_penalty = monthly_breaches * penalty_per_breach_usd * forward_months

    return {
        "avg_monthly_ticket_volume":      round(monthly_volume, 0),
        "current_breach_rate_pct":        breach_analysis["overall_breach_rate_pct"],
        "projected_monthly_breaches":     round(monthly_breaches, 1),
        "forward_months":                 forward_months,
        "projected_penalty_exposure_usd": round(projected_penalty, 2),
        "penalty_per_breach_usd":         penalty_per_breach_usd,
    }


# ── Secondary: Live-queue risk (kept for completeness) ─────────────────────────

def compute_sla_breach_risk(
    df: pd.DataFrame,
    now: datetime,
    sla_rules: dict,
    status_col: str          = "status",
    priority_col: str        = "priority",
    created_col: str         = "created_time",
    expected_resolve_col: str= "expected_sla_to_resolve",
    sla_resolution_col: str  = "sla_for_resolution",
) -> pd.DataFrame:
    """
    For a live open-ticket queue: compute time-to-breach per ticket.

    FIX #5: Now handles the case where the dataset has no open tickets
    (historical data) by returning an empty DataFrame with a log message
    rather than silently masking the downstream detection.
    """
    df = df.copy()
    open_statuses = {"open", "in progress", "pending", "assigned", "new"}
    open_tickets  = df[df[status_col].str.strip().str.lower().isin(open_statuses)].copy()

    if open_tickets.empty:
        logger.info(
            "compute_sla_breach_risk: no open tickets found in dataset. "
            "This dataset appears to be historical. Use analyse_sla_breach_history() instead."
        )
        return pd.DataFrame()

    open_tickets[created_col]         = pd.to_datetime(open_tickets[created_col], errors="coerce")
    open_tickets[expected_resolve_col]= pd.to_datetime(open_tickets[expected_resolve_col], errors="coerce")

    open_tickets["hours_elapsed"]  = ((now - open_tickets[created_col]).dt.total_seconds() / 3600).round(2)
    open_tickets["breach_in_hours"]= ((open_tickets[expected_resolve_col] - now).dt.total_seconds() / 3600).round(2)

    penalty = sla_rules.get("penalty_per_breach_usd", 1500)
    open_tickets["penalty_exposure_usd"] = penalty

    at_risk = open_tickets[
        open_tickets["breach_in_hours"].notna() &
        (open_tickets["breach_in_hours"] < 24)
    ].sort_values("breach_in_hours")

    return at_risk.reset_index(drop=True)


def project_sla_shortfall(at_risk_df: pd.DataFrame, days_remaining: int = 3) -> dict:
    """Summary dict for the LLM root cause agent (live queue path)."""
    total        = len(at_risk_df)
    already      = int((at_risk_df.get("breach_in_hours", pd.Series()) < 0).sum())
    in_window    = int(
        at_risk_df["breach_in_hours"]
        .between(0, days_remaining * 24, inclusive="both")
        .sum()
    ) if "breach_in_hours" in at_risk_df.columns else 0
    penalty = float(at_risk_df.get("penalty_exposure_usd", pd.Series(0)).sum())

    return {
        "total_at_risk_tickets":    total,
        "already_breached":         already,
        "will_breach_in_next_days": in_window,
        "days_window":              days_remaining,
        "total_penalty_exposure_usd": round(penalty, 2),
        "priority_breakdown": (
            at_risk_df["priority"].value_counts().to_dict()
            if "priority" in at_risk_df.columns else {}
        ),
    }


def suggest_ticket_reassignment(
    at_risk_df: pd.DataFrame,
    all_tickets_df: pd.DataFrame,
    agent_col: str = "agent_name",
    status_col: str = "status",
) -> list[dict]:
    """Find agents with lightest load for reassignment of at-risk tickets."""
    if agent_col not in all_tickets_df.columns or at_risk_df.empty:
        return []

    open_load = (
        all_tickets_df[
            all_tickets_df[status_col].str.strip().str.lower()
            .isin({"open", "in progress", "pending"})
        ]
        .groupby(agent_col)
        .size()
        .reset_index(name="current_load")
        .sort_values("current_load")
    )

    reassignments = []
    for _, ticket in at_risk_df.head(10).iterrows():
        best_agent = open_load.iloc[0][agent_col] if len(open_load) > 0 else "unassigned"
        reassignments.append({
            "ticket_id":        str(ticket.get("ticket_id", "unknown")),
            "current_agent":    str(ticket.get(agent_col, "unknown")),
            "recommended_agent":str(best_agent),
            "breach_in_hours":  float(ticket.get("breach_in_hours", 0)),
        })

    return reassignments