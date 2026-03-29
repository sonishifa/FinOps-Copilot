# enterprise_cost_intelligence/tools/infrastructure_tools.py
"""
Analytical tools for the Infrastructure Agent.

FIX #9: detect_spend_spikes() used .shift(1) inside a groupby without
guaranteeing sort order first. pandas groupby does not preserve sort order
from a preceding .sort_values() call. Fixed by sorting within the groupby
using sort=True (default) and explicitly resetting index before shifting.

FIX #6: find_shadow_it() now loads threshold from business_rules.json.
FIX #7: detect_spend_spikes() now loads threshold from business_rules.json.
"""

from __future__ import annotations
import json
import logging
from pathlib import Path

import pandas as pd
import numpy as np
from typing import Optional

logger = logging.getLogger(__name__)

_RULES_PATH = Path(__file__).parent.parent / "config" / "business_rules.json"

def _load_config() -> dict:
    """Load business rules config."""
    with open(_RULES_PATH) as f:
        return json.load(f)


# ── Cloud Spend Anomaly ───────────────────────────────────────────────────────

def detect_spend_spikes(
    df: pd.DataFrame,
    date_col: str = "date",
    cost_col: str = "cost_usd",
    service_col: str = "service_name",
    spike_pct_threshold: float | None = None,
) -> pd.DataFrame:
    """
    Detect month-over-month cost spikes per cloud service.
    Threshold loaded from business_rules.json if not passed explicitly.

    FIX #9: Previous version sorted before groupby but shift(1) inside
    groupby doesn't respect outer sort. Correct pattern:
      1. Compute monthly aggregates
      2. Sort by [service, month] and reset_index
      3. Use groupby(sort=False) with shift — safe because we pre-sorted
    """
    # FIX #7: load threshold from config if not explicitly passed
    if spike_pct_threshold is None:
        config = _load_config()
        spike_pct_threshold = config.get("cloud_spend_rules", {}).get("spike_threshold_pct", 30.0)
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df["month"] = df[date_col].dt.to_period("M")

    monthly = (
        df.groupby([service_col, "month"], sort=True)[cost_col]
        .sum()
        .reset_index()                    # FIX: reset so index is clean
        .sort_values([service_col, "month"])
        .reset_index(drop=True)           # FIX: reset again after sort
    )

    # FIX #9: sort=False here is safe because we already sorted above.
    # shift(1) now always refers to the chronologically previous row.
    monthly["prev_month_cost"] = (
        monthly.groupby(service_col, sort=False)[cost_col].shift(1)
    )

    monthly["mom_change_pct"] = (
        (monthly[cost_col] - monthly["prev_month_cost"])
        / monthly["prev_month_cost"] * 100
    ).round(2)

    spikes = monthly[
        monthly["mom_change_pct"].notna() &
        (monthly["mom_change_pct"] >= spike_pct_threshold)
    ].copy()

    spikes["spike_usd"] = (spikes[cost_col] - spikes["prev_month_cost"]).round(2)
    return spikes.reset_index(drop=True)


def classify_spend_spike_cause(
    spike_row: dict,
    df: pd.DataFrame,
    resource_col: str = "resource_id",
    tag_col: str = "tag_environment",
    label_col: str = "anomaly_label",
) -> dict:
    """
    Rule-based pre-classification of a spend spike before sending to LLM.
    Checks anomaly labels, tag distribution, and resource count changes.
    """
    service = spike_row.get("service_name") or spike_row.get("service")
    month   = spike_row.get("month")

    if not service:
        return {"heuristic_classification": "unknown", "error": "no service in spike_row"}

    service_df = df[df["service_name"] == service].copy()
    service_df["month"] = pd.to_datetime(service_df["date"]).dt.to_period("M")
    month_df   = service_df[service_df["month"] == month] if month else service_df

    signals: dict = {}

    if label_col in month_df.columns:
        signals["anomaly_labels"] = month_df[label_col].value_counts().to_dict()

    if tag_col in month_df.columns:
        signals["env_distribution"] = month_df[tag_col].value_counts().to_dict()

    if resource_col in month_df.columns and month:
        prev = service_df[service_df["month"] < month]
        signals["current_resource_count"] = int(month_df[resource_col].nunique())
        signals["prev_resource_count"]    = int(prev[resource_col].nunique()) if len(prev) else 0

    # Heuristic classification
    classification = "unknown"
    labels = signals.get("anomaly_labels", {})
    curr_rc = signals.get("current_resource_count", 0)
    prev_rc = signals.get("prev_resource_count", 1)  # avoid div-by-zero

    if labels.get("spike", 0) > 0:
        classification = "anomaly_label_spike"
    elif prev_rc > 0 and curr_rc > prev_rc * 1.5:
        classification = "likely_provisioning_error"
    elif signals.get("env_distribution", {}).get("production", 0) > len(month_df) * 0.8:
        classification = "likely_traffic_driven"

    signals["heuristic_classification"] = classification
    return signals


def find_shadow_it(
    df: pd.DataFrame,
    unused_days_threshold: int | None = None,
    cost_col: str = "monthly_cost_usd",
    days_col: str = "days_since_used",
    label_col: str = "shadow_label",
) -> pd.DataFrame:
    """Identify shadow IT: unused or untagged resources burning money."""
    # FIX #6: load threshold from config if not explicitly passed
    if unused_days_threshold is None:
        config = _load_config()
        unused_days_threshold = config.get("shadow_it_rules", {}).get("unused_days_threshold", 30)
    conditions = df[days_col] >= unused_days_threshold
    if label_col in df.columns:
        conditions = conditions | (df[label_col] == "shadow_it")
    shadow = df[conditions].copy()
    return shadow.sort_values(cost_col, ascending=False).reset_index(drop=True)


def calculate_waste_savings(shadow_df: pd.DataFrame, cost_col: str = "monthly_cost_usd") -> float:
    """Monthly savings if all identified shadow IT resources are decommissioned."""
    if shadow_df.empty or cost_col not in shadow_df.columns:
        return 0.0
    return round(float(shadow_df[cost_col].sum()), 2)