# enterprise_cost_intelligence/tools/pricing_tools.py
"""
AWS Pricing analysis tools.

Cross-references actual cloud spend against AWS list prices to detect
overpaying instances. All thresholds loaded from business_rules.json.
"""

from __future__ import annotations
import json
import logging
from pathlib import Path

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

_RULES_PATH = Path(__file__).parent.parent / "config" / "business_rules.json"


def _load_pricing_rules() -> dict:
    with open(_RULES_PATH) as f:
        return json.load(f).get("pricing_rules", {})


def load_aws_pricing(data_dir: Path) -> pd.DataFrame:
    """
    Load all AWS pricing CSVs from data/aws_pricing/ into a single DataFrame.
    Normalises column names and adds a clean 'region' column.
    """
    pricing_dir = data_dir / "aws_pricing"
    if not pricing_dir.exists():
        logger.warning(f"AWS pricing directory not found: {pricing_dir}")
        return pd.DataFrame()

    frames = []
    for csv_file in sorted(pricing_dir.glob("*.csv")):
        df = pd.read_csv(csv_file, low_memory=False)
        # Normalise column names (e.g., "Instan Type" → "instance_type")
        df.columns = [
            c.strip().lower().replace(" ", "_") for c in df.columns
        ]
        # Fix known typo: "instan_type" → "instance_type"
        if "instan_type" in df.columns:
            df = df.rename(columns={"instan_type": "instance_type"})
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined["price"] = pd.to_numeric(combined["price"], errors="coerce")
    logger.info(f"Loaded AWS pricing: {len(combined):,} rows from {len(frames)} files")
    return combined


def detect_overpaying(
    cloud_spend_df: pd.DataFrame,
    pricing_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cross-reference actual cloud spend vs. AWS list prices.

    For each service/instance used in cloud_spend, finds the corresponding
    list price and flags rows where actual cost exceeds list price by more
    than the configured overpay_threshold_pct.

    Returns a DataFrame of overpriced instances with savings estimates.
    """
    rules = _load_pricing_rules()
    threshold_pct = rules.get("overpay_threshold_pct", 15)

    if cloud_spend_df.empty or pricing_df.empty:
        return pd.DataFrame()

    # The cloud_spend has columns: date, service_name, resource_id, cost_usd, ...
    # The pricing has: date, instance_type, os, region, price
    # We need to match on instance_type ↔ service_name (closest available)

    # Get latest list price per instance type / region
    latest_prices = (
        pricing_df
        .sort_values("date", ascending=False)
        .drop_duplicates(subset=["instance_type", "region"], keep="first")
        [["instance_type", "region", "price"]]
    )

    # Estimate: monthly list price ≈ hourly_price × 730 hours
    latest_prices["monthly_list_price_usd"] = (latest_prices["price"] * 730).round(2)

    # Aggregate actual monthly spend per service
    cloud_spend_df = cloud_spend_df.copy()
    cloud_spend_df["month"] = cloud_spend_df["date"].dt.to_period("M")
    monthly_actual = (
        cloud_spend_df
        .groupby(["service_name", "month"])["cost_usd"]
        .sum()
        .reset_index()
        .rename(columns={"cost_usd": "actual_monthly_usd"})
    )

    # Precompute lookup: instance_type → cheapest monthly list price (O(m))
    price_lookup: dict[str, float] = {}
    for _, prow in latest_prices.iterrows():
        itype = str(prow["instance_type"]).lower()
        price = float(prow["monthly_list_price_usd"])
        if itype not in price_lookup or price < price_lookup[itype]:
            price_lookup[itype] = price

    # Match service_name to instance_type — O(n) with O(k) substring scan
    # k = number of unique instance types, done once per service
    instance_types = list(price_lookup.keys())

    overpaying = []
    for _, row in monthly_actual.iterrows():
        service = str(row["service_name"]).lower()

        # Try exact match first (O(1))
        list_price = price_lookup.get(service)

        # Fallback: substring match (O(k) but k is small — number of instance types)
        if list_price is None:
            for itype in instance_types:
                if itype in service or service in itype:
                    candidate = price_lookup[itype]
                    if list_price is None or candidate < list_price:
                        list_price = candidate

        if list_price is None or list_price <= 0:
            continue

        actual = float(row["actual_monthly_usd"])
        if actual > list_price:
            overpay_pct = ((actual - list_price) / list_price * 100)
            if overpay_pct >= threshold_pct:
                overpaying.append({
                    "service_name":          row["service_name"],
                    "month":                 str(row["month"]),
                    "actual_monthly_usd":    round(actual, 2),
                    "list_price_monthly_usd":round(list_price, 2),
                    "overpay_pct":           round(overpay_pct, 1),
                    "overpay_usd":           round(actual - list_price, 2),
                    "threshold_pct":         threshold_pct,
                })

    if not overpaying:
        return pd.DataFrame()
    return pd.DataFrame(overpaying).sort_values("overpay_usd", ascending=False).reset_index(drop=True)


def find_cheaper_regions(
    pricing_df: pd.DataFrame,
    instance_type: str,
) -> pd.DataFrame:
    """
    For a given instance type, find all regions and their prices,
    sorted cheapest first. Used for recommendations.
    """
    if pricing_df.empty:
        return pd.DataFrame()

    filtered = pricing_df[
        pricing_df["instance_type"].str.lower() == instance_type.lower()
    ].copy()

    if filtered.empty:
        return pd.DataFrame()

    # Get latest price per region
    latest = (
        filtered.sort_values("date", ascending=False)
        .drop_duplicates(subset=["region"], keep="first")
        [["instance_type", "region", "price"]]
        .sort_values("price")
        .reset_index(drop=True)
    )
    latest["monthly_cost_usd"] = (latest["price"] * 730).round(2)
    return latest
