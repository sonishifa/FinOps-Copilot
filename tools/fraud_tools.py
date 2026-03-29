# enterprise_cost_intelligence/tools/fraud_tools.py
"""
Heuristic-based fraud detection tools.

Instead of relying on a pre-labelled isFraud column, the system detects
fraud through its own rule-based heuristics:

  1. Balance drain:  amount > X% of sender's balance
  2. Ghost receiver:  large transfer received but destination balance unchanged
  3. Layering pattern: TRANSFER immediately followed by CASH_OUT to same dest
  4. High-value anomaly: single transaction above configured threshold

All thresholds loaded from business_rules.json → fraud_rules.
Processes data in chunks to handle 6M+ row datasets without OOM.
"""

from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

_RULES_PATH = Path(__file__).parent.parent / "config" / "business_rules.json"


def _load_fraud_rules() -> dict:
    with open(_RULES_PATH) as f:
        return json.load(f).get("fraud_rules", {})


def detect_fraud_heuristic(
    filepath: Path,
    chunk_size: Optional[int] = None,
) -> dict:
    """
    Scan a fraud transaction CSV using heuristic rules.
    Processes in chunks to handle 6M+ rows without loading entire file.

    Returns a structured summary dict with:
      - total_transactions_scanned
      - flagged_transactions (list of dicts)
      - fraud_summary (by type)
      - total_exposure_usd
      - high_risk_accounts
    """
    rules = _load_fraud_rules()
    if chunk_size is None:
        chunk_size = rules.get("chunk_size", 50_000)

    drain_threshold = rules.get("balance_drain_threshold_pct", 90) / 100
    zero_dest_flag  = rules.get("zero_dest_balance_flag", True)
    min_amount      = rules.get("min_flaggable_amount_usd", 10_000)
    risky_types     = set(rules.get("high_risk_transaction_types", ["TRANSFER", "CASH_OUT"]))

    flagged = []
    total_scanned = 0
    type_counts: dict[str, int] = {}
    risky_accounts: dict[str, float] = {}

    for chunk in pd.read_csv(filepath, chunksize=chunk_size, low_memory=False):
        total_scanned += len(chunk)

        # Normalise column names
        chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]

        # Ensure numeric
        for col in ["amount", "oldbalanceorg", "newbalanceorig", "oldbalancedest", "newbalancedest"]:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

        # ── Heuristic 1: Balance drain ────────────────────────────────────
        # Sender loses >90% of balance in one transaction
        if "oldbalanceorg" in chunk.columns and "amount" in chunk.columns:
            balance_mask = (
                (chunk["oldbalanceorg"] > 0) &
                (chunk["amount"] / chunk["oldbalanceorg"] > drain_threshold)
            )
            drains = chunk[balance_mask].copy()
            drains["fraud_signal"] = "balance_drain"
            if not drains.empty:
                flagged.append(drains.head(50))  # cap per chunk

        # ── Heuristic 2: Ghost receiver ───────────────────────────────────
        # Large transfer but destination balance doesn't change
        if zero_dest_flag and all(c in chunk.columns for c in ["newbalancedest", "oldbalancedest", "amount"]):
            ghost_mask = (
                (chunk["amount"] > min_amount) &
                (chunk["type"].isin(risky_types) if "type" in chunk.columns else True) &
                (chunk["newbalancedest"] == chunk["oldbalancedest"])
            )
            ghosts = chunk[ghost_mask].copy()
            ghosts["fraud_signal"] = "ghost_receiver"
            if not ghosts.empty:
                flagged.append(ghosts.head(50))

        # ── Heuristic 3: High-value risky transaction ─────────────────────
        if "type" in chunk.columns:
            risky_mask = (
                (chunk["type"].isin(risky_types)) &
                (chunk["amount"] > min_amount)
            )
            risky = chunk[risky_mask].copy()
            risky["fraud_signal"] = "high_value_risky_type"
            if not risky.empty:
                flagged.append(risky.head(50))

        # Track type distribution
        if "type" in chunk.columns:
            for t, count in chunk["type"].value_counts().items():
                type_counts[t] = type_counts.get(t, 0) + int(count)

        # Track high-risk accounts (any account with large outflows)
        if "nameorig" in chunk.columns and "amount" in chunk.columns:
            large = chunk[chunk["amount"] > min_amount]
            for _, row in large.head(20).iterrows():
                acct = str(row.get("nameorig", "unknown"))
                risky_accounts[acct] = risky_accounts.get(acct, 0) + float(row["amount"])

        logger.debug(f"Fraud scan: processed {total_scanned:,} rows...")

    # Deduplicate and build final flagged DataFrame
    if flagged:
        all_flagged = pd.concat(flagged, ignore_index=True).drop_duplicates()
        # Cap at 500 total flagged for performance
        all_flagged = all_flagged.head(500)
    else:
        all_flagged = pd.DataFrame()

    # Build summary
    total_exposure = float(all_flagged["amount"].sum()) if not all_flagged.empty else 0.0
    fraud_by_signal = (
        all_flagged["fraud_signal"].value_counts().to_dict()
        if "fraud_signal" in all_flagged.columns else {}
    )

    # Top 10 risky accounts
    top_accounts = sorted(risky_accounts.items(), key=lambda x: x[1], reverse=True)[:10]

    result = {
        "total_transactions_scanned": total_scanned,
        "total_flagged": len(all_flagged),
        "total_exposure_usd": round(total_exposure, 2),
        "fraud_by_signal": fraud_by_signal,
        "transaction_type_distribution": type_counts,
        "high_risk_accounts": [
            {"account": acct, "total_outflow_usd": round(amt, 2)}
            for acct, amt in top_accounts
        ],
        "flagged_sample": (
            all_flagged.head(20).to_dict("records")
            if not all_flagged.empty else []
        ),
    }

    logger.info(
        f"Fraud scan complete: {total_scanned:,} transactions → "
        f"{len(all_flagged)} flagged, ${total_exposure:,.0f} exposure"
    )
    return result


def detect_layering_pattern(filepath: Path, chunk_size: int = 100_000) -> list[dict]:
    """
    Detect TRANSFER→CASH_OUT layering pattern:
    A destination that receives a TRANSFER and then issues a CASH_OUT
    of similar amount shortly after — classic money laundering signal.

    Returns list of suspicious sequences.
    """
    rules = _load_fraud_rules()
    min_amount = rules.get("min_flaggable_amount_usd", 10_000)

    suspicious = []
    prev_transfers: dict[str, dict] = {}  # namedest → last transfer info

    for chunk in pd.read_csv(filepath, chunksize=chunk_size, low_memory=False):
        chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]

        for col in ["amount"]:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

        # Skip chunk if required columns are missing
        if "type" not in chunk.columns or "amount" not in chunk.columns:
            continue

        transfers = chunk[
            (chunk["type"] == "TRANSFER") &
            (chunk["amount"] > min_amount)
        ]
        for _, row in transfers.iterrows():
            dest = str(row.get("namedest", ""))
            if dest:
                prev_transfers[dest] = {
                    "step": int(row.get("step", 0)),
                    "amount": float(row["amount"]),
                    "sender": str(row.get("nameorig", "")),
                }

        cashouts = chunk[
            (chunk["type"] == "CASH_OUT") &
            (chunk["amount"] > min_amount)
        ]
        for _, row in cashouts.iterrows():
            sender = str(row.get("nameorig", ""))
            if sender in prev_transfers:
                prev = prev_transfers[sender]
                amount_ratio = float(row["amount"]) / prev["amount"] if prev["amount"] > 0 else 0
                # Flag if cashout is 80-120% of the transfer amount
                if 0.8 <= amount_ratio <= 1.2:
                    suspicious.append({
                        "pattern": "TRANSFER_then_CASH_OUT",
                        "transfer_sender": prev["sender"],
                        "intermediary": sender,
                        "transfer_amount": prev["amount"],
                        "cashout_amount": float(row["amount"]),
                        "amount_ratio": round(amount_ratio, 2),
                    })
                    if len(suspicious) >= 100:
                        break

        if len(suspicious) >= 100:
            break

    logger.info(f"Layering detection: found {len(suspicious)} suspicious sequences")
    return suspicious
