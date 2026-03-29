#!/usr/bin/env python3
"""
seed.py — One-Time Data Seeder

Loads all CSV datasets from data/ and pushes them into Supabase tables.
After seeding, the pipeline reads from Supabase — no CSV dependency.

Usage:
    python3 seed.py              # seed all datasets
    python3 seed.py --only cloud_spend shadow_it   # seed specific ones
    python3 seed.py --verify     # check what's already in Supabase

Tables are auto-created by Supabase if using the Dashboard import,
or must be pre-created via SQL (see README for schema).
"""

import sys
import argparse
import logging
import json
import re
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("seed")

DATA_DIR = Path(__file__).parent / "data"

# ── Dataset registry ────────────────────────────────────────────────────────
# (logical_name, csv_filename, date_columns, max_rows_to_seed)
DATASET_REGISTRY = [
    ("procurement_kpi",         "procurement_-kpi.csv",
     ["order_date", "delivery_date"], 100_000),

    ("corporate_procurement",   "corporate-procurement-contract-awards.csv",
     ["award_date"], 100_000),

    ("government_procurement",  "government-procurement.csv",
     ["award_date"], 100_000),

    ("cloud_spend",             "synthetic_cloud_spend.csv",
     ["date"], 100_000),

    ("shadow_it",               "synthetic_shadow_it.csv",
     ["created_at", "last_used_at"], 100_000),

    ("itsm",                    "ITSM_Dataset.csv",
     ["created_time", "expected_sla_to_resolve",
      "expected_sla_to_first_response", "first_response_time",
      "resolution_time", "close_time"], 100_000),

    ("saas_sales",              "SaaS-Sales.csv",
     ["order_date"], 100_000),

    ("ravenstack_accounts",     "ravenstack_accounts.csv",
     ["signup_date"], 100_000),

    ("ravenstack_subscriptions","ravenstack_subscriptions.csv",
     ["start_date", "end_date"], 100_000),

    ("ravenstack_churn",        "ravenstack_churn_events.csv",
     ["churn_date"], 100_000),

    ("ravenstack_features",     "ravenstack_feature_usage.csv",
     ["usage_date"], 100_000),

    ("ravenstack_tickets",      "ravenstack_support_tickets.csv",
     ["submitted_at", "closed_at"], 100_000),

    # Fraud: seed 50K rows (the full 6M is too large for Supabase free tier)
    ("fraud_transactions",      "fraud-detection-paysim.csv",
     [], 50_000),
]

# logical_name → Supabase table name
TABLE_MAP = {
    "procurement_kpi":          "ds_procurement_kpi",
    "corporate_procurement":    "ds_corporate_procurement",
    "government_procurement":   "ds_government_procurement",
    "cloud_spend":              "ds_cloud_spend",
    "shadow_it":                "ds_shadow_it",
    "itsm":                     "ds_itsm",
    "saas_sales":               "ds_saas_sales",
    "ravenstack_accounts":      "ds_ravenstack_accounts",
    "ravenstack_subscriptions": "ds_ravenstack_subscriptions",
    "ravenstack_churn":         "ds_ravenstack_churn",
    "ravenstack_features":      "ds_ravenstack_features",
    "ravenstack_tickets":       "ds_ravenstack_tickets",
    "fraud_transactions":       "ds_fraud_transactions",
}


def _snake(col: str) -> str:
    col = col.strip().lower()
    col = re.sub(r"[\s\-/]+", "_", col)
    col = re.sub(r"[^\w]", "", col)
    return col


def load_csv(filename: str, date_cols: list[str], max_rows: int) -> pd.DataFrame:
    """Load a CSV, normalize columns, truncate."""
    path = DATA_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Not found: {path}")

    df = pd.read_csv(path, low_memory=False, nrows=max_rows)
    df.columns = [_snake(c) for c in df.columns]

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            # Convert to ISO string for Supabase (JSON serializable)
            df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S").where(df[col].notna())

    return df


def seed_all(only: list[str] | None = None):
    """Load CSVs and push to Supabase."""
    from core.database import get_db
    db = get_db()

    results = {}
    for name, filename, date_cols, max_rows in DATASET_REGISTRY:
        if only and name not in only:
            continue

        table = TABLE_MAP.get(name)
        if not table:
            logger.warning(f"No table mapping for '{name}', skipping")
            continue

        try:
            logger.info(f"Loading '{name}' from {filename}...")
            df = load_csv(filename, date_cols, max_rows)
            logger.info(f"  → {len(df):,} rows loaded, pushing to '{table}'...")

            # Convert to JSON-safe records
            records = json.loads(df.to_json(orient="records", date_format="iso"))
            count = db.insert_rows(table, records, batch_size=500)

            results[name] = count
            logger.info(f"  ✅ Seeded {count:,} rows → '{table}'")

        except FileNotFoundError:
            logger.warning(f"  ⚠️  CSV not found: {filename}, skipping")
            results[name] = 0
        except Exception as e:
            logger.error(f"  ❌ Seed failed for '{name}': {e}")
            results[name] = 0

    # Summary
    print(f"\n{'═'*50}")
    print(f"  SEED COMPLETE")
    print(f"{'═'*50}")
    total = 0
    for name, count in results.items():
        status = "✅" if count > 0 else "❌"
        print(f"  {status} {name:<30} {count:>8,} rows")
        total += count
    print(f"{'─'*50}")
    print(f"     Total rows seeded: {total:,}")
    print(f"{'═'*50}\n")


def verify():
    """Check what data already exists in Supabase."""
    from core.database import get_db
    db = get_db()

    print(f"\n{'═'*50}")
    print(f"  SUPABASE DATA VERIFICATION")
    print(f"{'═'*50}")
    total = 0
    for name, table in TABLE_MAP.items():
        try:
            df = db.read_table(table, limit=1)
            # Get count via a full read (limited)
            full = db.read_table(table)
            count = len(full)
            status = "✅" if count > 0 else "⚪"
            print(f"  {status} {table:<35} {count:>8,} rows")
            total += count
        except Exception:
            print(f"  ❌ {table:<35} (table not found)")

    print(f"{'─'*50}")
    print(f"     Total rows in Supabase: {total:,}")
    print(f"{'═'*50}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed CSV data into Supabase")
    parser.add_argument("--only", nargs="+", help="Seed only specific datasets")
    parser.add_argument("--verify", action="store_true", help="Verify Supabase data")
    args = parser.parse_args()

    if args.verify:
        verify()
    else:
        seed_all(only=args.only)
