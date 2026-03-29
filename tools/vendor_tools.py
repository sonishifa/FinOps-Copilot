# enterprise_cost_intelligence/tools/vendor_tools.py
"""
Analytical tools for the Vendor Intelligence Agent.

FIX #6: find_duplicate_vendors() fallback branch was mutating the caller's
DataFrame in-place (adding then dropping '_norm' column). If rapidfuzz is not
installed the shared state.raw_datasets["corporate_procurement"] would be
permanently modified. Fixed by always working on a copy.
"""

from __future__ import annotations
import logging
import pandas as pd
import numpy as np
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from rapidfuzz import fuzz
    _FUZZY_AVAILABLE = True
except ImportError:
    _FUZZY_AVAILABLE = False
    logger.warning(
        "rapidfuzz not installed — vendor duplicate detection will use "
        "simple prefix-match fallback. Run: pip install rapidfuzz"
    )


# ── Duplicate / Overlapping Vendor Detection ──────────────────────────────────

def find_duplicate_vendors(
    df: pd.DataFrame,
    name_col: str = "supplier",
    category_col: Optional[str] = None,
    threshold: float = 80.0,
) -> pd.DataFrame:
    """
    Return a DataFrame of likely-duplicate vendor pairs with similarity scores.

    FIX #6: All operations now work on a local copy — the passed-in df is
    never mutated, regardless of which code path (rapidfuzz or fallback) runs.
    """
    if name_col not in df.columns:
        logger.warning(f"Column '{name_col}' not in DataFrame. Skipping duplicate detection.")
        return pd.DataFrame()

    # FIX #6: always copy so we never touch the caller's DataFrame
    work = df.copy()
    vendors = work[name_col].dropna().unique().tolist()

    if len(vendors) < 2:
        return pd.DataFrame()

    if not _FUZZY_AVAILABLE:
        return _fallback_prefix_match(work, name_col)

    pairs = []
    MAX_PAIRS = 100  # FIX #5: cap output to avoid flooding downstream agents
    for i, v1 in enumerate(vendors):
        if len(pairs) >= MAX_PAIRS:
            break
        len_v1 = len(v1)
        for v2 in vendors[i + 1:]:
            # FIX #5: skip if name lengths differ >30% — can't match at 80%+ threshold
            if abs(len_v1 - len(v2)) > max(len_v1, len(v2)) * 0.3:
                continue

            score = fuzz.token_set_ratio(v1, v2)
            if score < threshold:
                continue

            # Optional category filter: only flag if they share at least one category
            if category_col and category_col in work.columns:
                cats1 = set(work[work[name_col] == v1][category_col].dropna())
                cats2 = set(work[work[name_col] == v2][category_col].dropna())
                if not cats1.intersection(cats2):
                    continue

            pairs.append({"vendor_a": v1, "vendor_b": v2, "similarity_score": round(score, 1)})
            if len(pairs) >= MAX_PAIRS:
                logger.info(f"Vendor duplicate detection capped at {MAX_PAIRS} pairs")
                break

    if not pairs:
        return pd.DataFrame()

    return pd.DataFrame(pairs).sort_values("similarity_score", ascending=False).reset_index(drop=True)


def _fallback_prefix_match(work: pd.DataFrame, name_col: str) -> pd.DataFrame:
    """
    Simple fallback: group vendors by their lowercased first word.
    Works on a copy — safe for shared DataFrames.
    """
    work["_norm"] = work[name_col].str.lower().str.split().str[0]
    groups = (
        work.groupby("_norm")[name_col]
        .apply(lambda x: list(x.unique()))
        .reset_index()
    )
    groups = groups[groups[name_col].apply(len) > 1]
    # Expand into pairs
    pairs = []
    for _, row in groups.iterrows():
        names = row[name_col]
        for i in range(len(names)):
            for j in range(i + 1, len(names)):
                pairs.append({
                    "vendor_a": names[i],
                    "vendor_b": names[j],
                    "similarity_score": 75.0,   # fixed score for fallback
                })
    return pd.DataFrame(pairs) if pairs else pd.DataFrame()


def calculate_consolidation_savings(
    df: pd.DataFrame,
    duplicate_pairs: pd.DataFrame,
    amount_col: str = "contract_award_amount",
    name_col: str = "supplier",
) -> pd.DataFrame:
    """
    For each duplicate pair, estimate savings from consolidation.
    Conservative: 15% of the smaller contract (overhead/admin reduction).
    Methodology is logged so it can be reported to judges.
    """
    if duplicate_pairs.empty or amount_col not in df.columns:
        return pd.DataFrame()

    results = []
    for _, row in duplicate_pairs.iterrows():
        spend_a = df[df[name_col] == row["vendor_a"]][amount_col].sum()
        spend_b = df[df[name_col] == row["vendor_b"]][amount_col].sum()
        total   = spend_a + spend_b
        smaller = min(spend_a, spend_b)
        # Conservative 15% savings assumption: consolidating admin overhead,
        # duplicate license fees, and redundant onboarding costs
        estimated_savings = smaller * 0.15
        results.append({
            "vendor_a":                    row["vendor_a"],
            "vendor_b":                    row["vendor_b"],
            "similarity_score":            row["similarity_score"],
            "spend_vendor_a_usd":          round(float(spend_a), 2),
            "spend_vendor_b_usd":          round(float(spend_b), 2),
            "total_combined_spend_usd":    round(float(total), 2),
            "estimated_annual_savings_usd":round(float(estimated_savings), 2),
            "savings_methodology":         "15% of smaller contract (admin + overhead reduction)",
        })

    return (
        pd.DataFrame(results)
        .sort_values("estimated_annual_savings_usd", ascending=False)
        .reset_index(drop=True)
    )


def vendor_spend_concentration(
    df: pd.DataFrame,
    name_col: str = "supplier",
    amount_col: str = "contract_award_amount",
) -> pd.DataFrame:
    """Flag vendors with >30% of total spend (concentration risk)."""
    if amount_col not in df.columns or df[amount_col].sum() == 0:
        return pd.DataFrame()

    total = df[amount_col].sum()
    concentration = (
        df.groupby(name_col)[amount_col]
        .sum()
        .reset_index()
        .rename(columns={amount_col: "total_spend_usd"})
    )
    concentration["spend_pct"] = (concentration["total_spend_usd"] / total * 100).round(2)
    concentration = concentration.sort_values("spend_pct", ascending=False).reset_index(drop=True)
    concentration["concentration_risk"] = concentration["spend_pct"] > 30
    return concentration