# enterprise_cost_intelligence/core/database.py
"""
Supabase integration layer — REQUIRED.

Provides a unified interface for:
  - Connecting to Supabase (URL + key from .env)
  - Seeding CSV data into Supabase tables
  - Reading data back as DataFrames
  - Persisting audit events and pipeline run summaries

Requires SUPABASE_URL and SUPABASE_KEY in .env.
The pipeline will NOT start without a valid Supabase connection.
"""

from __future__ import annotations
import os
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

logger = logging.getLogger(__name__)

_db_instance: "SupabaseDB | None" = None


def get_db() -> "SupabaseDB":
    """Singleton factory for the Supabase connection."""
    global _db_instance
    if _db_instance is None:
        _db_instance = SupabaseDB()
    return _db_instance


class SupabaseDB:
    """
    Thin wrapper around the Supabase Python client.

    SUPABASE_URL and SUPABASE_KEY must be set in .env.
    Raises RuntimeError on init if credentials are missing or connection fails.
    """

    def __init__(self):
        self.url = os.environ.get("SUPABASE_URL", "")
        self.key = os.environ.get("SUPABASE_KEY", "")

        if not self.url or not self.key:
            raise RuntimeError(
                "Supabase credentials not found. "
                "Set SUPABASE_URL and SUPABASE_KEY in your .env file. "
                "Create a free project at https://supabase.com"
            )

        try:
            self.client: Client = create_client(self.url, self.key)
            logger.info("✅ Supabase connected successfully")
        except Exception as e:
            raise RuntimeError(
                f"Supabase connection failed: {e}. "
                f"Check your SUPABASE_URL ({self.url[:30]}...) and SUPABASE_KEY."
            ) from e

    # ── Read ──────────────────────────────────────────────────────────────────

    def read_table(
        self,
        table_name: str,
        columns: str = "*",
        limit: int = 100_000,
    ) -> pd.DataFrame:
        """Read a Supabase table into a DataFrame."""
        try:
            response = (
                self.client.table(table_name)
                .select(columns)
                .limit(limit)
                .execute()
            )
            if response.data:
                return pd.DataFrame(response.data)
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"Failed to read table '{table_name}': {e}")
            return pd.DataFrame()

    # ── Write (for seeding and audit logging) ─────────────────────────────────

    def insert_rows(
        self,
        table_name: str,
        rows: list[dict],
        batch_size: int = 500,
    ) -> int:
        """
        Insert rows into a Supabase table in batches.
        Returns the number of successfully inserted rows.
        """
        inserted = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            try:
                self.client.table(table_name).insert(batch).execute()
                inserted += len(batch)
            except Exception as e:
                logger.warning(
                    f"Insert batch {i // batch_size} into '{table_name}' failed: {e}"
                )
        return inserted

    def upsert_rows(
        self,
        table_name: str,
        rows: list[dict],
        batch_size: int = 500,
    ) -> int:
        """Upsert (insert or update on conflict) rows into a table."""
        upserted = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            try:
                self.client.table(table_name).upsert(batch).execute()
                upserted += len(batch)
            except Exception as e:
                logger.warning(
                    f"Upsert batch {i // batch_size} into '{table_name}' failed: {e}"
                )
        return upserted

    # ── Audit log persistence ─────────────────────────────────────────────────

    def log_audit_event(self, event: dict) -> bool:
        """Write a single audit event to the audit_events table."""
        try:
            self.client.table("audit_events").insert(event).execute()
            return True
        except Exception as e:
            logger.debug(f"Audit event write failed: {e}")
            return False

    # ── Pipeline state persistence ────────────────────────────────────────────

    def save_pipeline_run(self, run_id: str, state_summary: dict) -> bool:
        """Persist a pipeline run summary for dashboard consumption."""
        # Only send keys that exist in the pipeline_runs table schema
        allowed_keys = {
            "total_financial_exposure_usd",
            "total_recoverable_savings_usd",
            "auto_executed_savings_usd",
            "pending_human_approval_savings_usd",
            "roi_multiple",
        }
        row = {"run_id": run_id}
        for key in allowed_keys:
            if key in state_summary:
                row[key] = state_summary[key]

        try:
            self.client.table("pipeline_runs").upsert(row).execute()
            return True
        except Exception as e:
            logger.debug(f"Pipeline run save failed: {e}")
            return False

    # ── Seed helper ───────────────────────────────────────────────────────────

    def seed_from_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        max_rows: int = 50_000,
    ) -> int:
        """
        Seed a Supabase table from a DataFrame.
        Converts DataFrame to records (dicts), truncates to max_rows,
        and inserts in batches.
        """
        # Truncate for large datasets
        if len(df) > max_rows:
            logger.info(
                f"Seeding '{table_name}': truncating {len(df):,} → {max_rows:,} rows"
            )
            df = df.head(max_rows)

        # Convert to records — handle NaN, datetime, etc.
        records = json.loads(df.to_json(orient="records", date_format="iso"))
        return self.insert_rows(table_name, records)
