# enterprise_cost_intelligence/tools/ingestion_tools.py
"""
Tools used by the Data Ingestion Agent.

Data flows:   Supabase (PostgreSQL) → pandas DataFrames → agents
Seeding:      CSV files → seed.py → Supabase tables (run once)

The pipeline NEVER reads CSV files directly. All data comes from
Supabase. The only time CSVs are touched is during initial seeding
via seed.py.

Exception: Invoice OCR (reads image files from data/invoices/).
"""

import re
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"

# ── Supabase table name mapping ─────────────────────────────────────────────
# Maps logical dataset names → Supabase table names.
_TABLE_MAP = {
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

# Columns that need currency normalization after loading
_CURRENCY_COLS = [
    "contract_award_amount", "awarded_amt", "unit_price",
    "negotiated_price", "cost_usd", "monthly_cost_usd",
    "mrr_amount", "arr_amount", "refund_amount_usd",
    "sales", "profit",
]


def _snake(col: str) -> str:
    col = col.strip().lower()
    col = re.sub(r"[\s\-/]+", "_", col)
    col = re.sub(r"[^\w]", "", col)
    return col


def profile_dataframe(name: str, df: pd.DataFrame) -> dict:
    """Profile a DataFrame for data quality reporting."""
    null_counts = df.isnull().sum().to_dict()
    dup_count   = int(df.duplicated().sum())
    return {
        "source":          name,
        "rows":            len(df),
        "columns":         list(df.columns),
        "null_counts":     {k: int(v) for k, v in null_counts.items() if v > 0},
        "duplicate_rows":  dup_count,
        "dtypes":          {c: str(t) for c, t in df.dtypes.items()},
    }


def normalize_currency(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Strip currency symbols and coerce to float.
    Uses pd.to_numeric(errors='coerce') so bad values become NaN.
    """
    if col not in df.columns:
        return df
    df = df.copy()
    cleaned = (
        df[col]
        .astype(str)
        .str.replace(r"[$,£€\s]", "", regex=True)
        .str.strip()
        .replace({"": None, "nan": None, "None": None, "N/A": None,
                  "TBD": None, "—": None, "-": None})
    )
    coerced = pd.to_numeric(cleaned, errors="coerce")
    lost = coerced.isna().sum() - df[col].isna().sum()
    if lost > 0:
        logger.debug(f"normalize_currency: {lost} non-numeric values coerced to NaN in '{col}'")
    df[col] = coerced
    return df


def load_all_datasets() -> dict[str, pd.DataFrame]:
    """
    Load all enterprise datasets from Supabase (PostgreSQL).

    This is the production data path:
        Supabase tables → pandas DataFrames → returned to agents

    CSV files are never read here. Initial data is loaded into
    Supabase via seed.py (run once during setup).
    """
    from core.database import get_db
    db = get_db()

    datasets: dict[str, pd.DataFrame] = {}

    # ── Load from Supabase ────────────────────────────────────────────────
    for logical_name, table_name in _TABLE_MAP.items():
        try:
            df = db.read_table(table_name)
            if not df.empty:
                # Normalize currency columns
                for col in _CURRENCY_COLS:
                    if col in df.columns:
                        df = normalize_currency(df, col)
                datasets[logical_name] = df
                logger.info(f"Loaded '{logical_name}' from Supabase ({table_name}): {len(df):,} rows")
            else:
                logger.warning(
                    f"Table '{table_name}' is empty — "
                    f"run 'python3 seed.py' to populate from CSV files"
                )
        except Exception as e:
            logger.warning(f"Failed to read '{table_name}': {e}")

    if not datasets:
        logger.error(
            "No datasets loaded from Supabase. "
            "Run 'python3 seed.py' first to populate the database from CSV files."
        )

    # ── AWS Pricing (3 CSVs in a subdirectory — static reference data) ────
    # AWS pricing is reference/lookup data, not enterprise operational data.
    # It stays as local CSV since it comes from AWS, not the enterprise DB.
    try:
        from tools.pricing_tools import load_aws_pricing
        aws_df = load_aws_pricing(DATA_DIR)
        if not aws_df.empty:
            datasets["aws_pricing"] = aws_df
            logger.info(f"Loaded 'aws_pricing' (reference data): {len(aws_df):,} rows")
    except Exception as e:
        logger.debug(f"AWS pricing load skipped: {e}")

    # ── Invoice OCR (reads image files — not database content) ────────────
    # Invoice images are binary assets, not tabular data.
    # OCR extracts text → DataFrame, which is then used by anomaly detection.
    invoice_dir = DATA_DIR / "invoices"
    if invoice_dir.exists():
        try:
            from tools.invoice_tools import batch_ocr_invoices
            invoices_df = batch_ocr_invoices(invoice_dir)
            if not invoices_df.empty:
                datasets["invoices"] = invoices_df
                logger.info(f"Loaded 'invoices' via OCR: {len(invoices_df):,} parsed")
        except Exception as e:
            logger.debug(f"Invoice OCR skipped: {e}")

    logger.info(
        f"=== Data loading complete: {len(datasets)} datasets, "
        f"{sum(len(df) for df in datasets.values()):,} total rows ==="
    )
    return datasets