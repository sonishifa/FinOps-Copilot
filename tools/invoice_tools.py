# enterprise_cost_intelligence/tools/invoice_tools.py
"""
Invoice OCR and anomaly detection tools.

Uses pytesseract to extract structured data from invoice images, then
applies rule-based checks:
  - Math verification (qty × unit ≠ line total, sum ≠ subtotal)
  - Tax rate anomalies (rate != expected for country)
  - Duplicate invoice detection (same supplier + amount)
  - Extended payment terms risk (NET90+)

All thresholds loaded from business_rules.json → invoice_rules.
"""

from __future__ import annotations
import re
import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np
import pytesseract
from PIL import Image

logger = logging.getLogger(__name__)

_RULES_PATH = Path(__file__).parent.parent / "config" / "business_rules.json"


def _load_invoice_rules() -> dict:
    with open(_RULES_PATH) as f:
        return json.load(f).get("invoice_rules", {})


# ── OCR Extraction ────────────────────────────────────────────────────────────

def ocr_invoice(image_path: Path) -> dict | None:
    """
    OCR a single invoice image and parse into structured fields.

    Expected invoice format (from sample):
        Supplier ID: <uuid>
        Country: <code>
        Invoice ID: <id>
        Invoice Date: <date>
        Payment Terms: <terms>
        Item X  Qty Y  Unit Z  Line W
        Subtotal: <amount>
        Tax (X%): <amount>
        Total: <amount> <currency>

    Returns dict with parsed fields, or None on failure.
    """
    try:
        img = Image.open(image_path)
        text = pytesseract.image_to_string(img)
        return _parse_invoice_text(text, str(image_path))
    except Exception as e:
        logger.debug(f"OCR failed for {image_path}: {e}")
        return None


def _parse_invoice_text(text: str, source: str = "") -> Optional[dict]:
    """Parse OCR text output into structured invoice dict."""
    lines = text.strip().split("\n")
    result: dict = {"source_file": source, "raw_text_lines": len(lines)}

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Header fields
        if line.lower().startswith("supplier id:"):
            result["supplier_id"] = line.split(":", 1)[1].strip()
        elif line.lower().startswith("country:"):
            result["country"] = line.split(":", 1)[1].strip()
        elif line.lower().startswith("invoice id:"):
            result["invoice_id"] = line.split(":", 1)[1].strip()
        elif line.lower().startswith("invoice date:"):
            result["invoice_date"] = line.split(":", 1)[1].strip()
        elif line.lower().startswith("payment terms:"):
            result["payment_terms"] = line.split(":", 1)[1].strip()

        # Line items: "Item X Qty Y Unit Z Line W"
        item_match = re.match(
            r"Item\s+(\d+)\s+Qty\s+([\d,.]+)\s+Unit\s+([\d,.]+)\s+Line\s+([\d,.]+)",
            line, re.IGNORECASE,
        )
        if item_match:
            items = result.setdefault("line_items", [])
            items.append({
                "item_number": int(item_match.group(1)),
                "quantity":    _parse_number(item_match.group(2)),
                "unit_price":  _parse_number(item_match.group(3)),
                "line_total":  _parse_number(item_match.group(4)),
            })

        # Subtotal
        sub_match = re.match(r"Subtotal:\s*([\d,.]+)", line, re.IGNORECASE)
        if sub_match:
            result["subtotal"] = _parse_number(sub_match.group(1))

        # Tax
        tax_match = re.match(r"Tax\s*\(([\d.]+)%\):\s*([\d,.]+)", line, re.IGNORECASE)
        if tax_match:
            result["tax_rate_pct"] = float(tax_match.group(1))
            result["tax_amount"] = _parse_number(tax_match.group(2))

        # Total
        total_match = re.match(r"Total:\s*([\d,.]+)\s*(\w+)?", line, re.IGNORECASE)
        if total_match:
            result["total"] = _parse_number(total_match.group(1))
            if total_match.group(2):
                result["currency"] = total_match.group(2).upper()

    return result if "invoice_id" in result else None


def _parse_number(s: str) -> float:
    """Parse a number string, removing commas."""
    return float(s.replace(",", ""))


# ── Batch OCR ─────────────────────────────────────────────────────────────────

def batch_ocr_invoices(
    invoice_dir: Path,
    sample_size: Optional[int] = None,
) -> pd.DataFrame:
    """
    OCR a sample of invoices from a directory.
    Returns a DataFrame with one row per successfully parsed invoice.
    """
    rules = _load_invoice_rules()
    if sample_size is None:
        sample_size = rules.get("ocr_sample_size", 200)

    if not invoice_dir.exists():
        raise FileNotFoundError(f"Invoice directory not found: {invoice_dir}")

    image_files = sorted(invoice_dir.glob("*.png"))[:sample_size]
    if not image_files:
        image_files = sorted(invoice_dir.glob("*.jpg"))[:sample_size]

    logger.info(f"OCR processing {len(image_files)} invoices from {invoice_dir}")

    parsed = []
    for i, img_path in enumerate(image_files):
        result = ocr_invoice(img_path)
        if result:
            parsed.append(result)
        if (i + 1) % 50 == 0:
            logger.info(f"  OCR progress: {i + 1}/{len(image_files)}")

    if not parsed:
        return pd.DataFrame()

    df = pd.DataFrame(parsed)
    logger.info(f"OCR complete: {len(parsed)}/{len(image_files)} invoices parsed successfully")
    return df


# ── Invoice Anomaly Detection ─────────────────────────────────────────────────

def verify_invoice_math(invoices_df: pd.DataFrame) -> list[dict]:
    """
    Check each invoice for math errors:
      - qty × unit_price ≠ line_total
      - sum(line_totals) ≠ subtotal
      - subtotal + tax ≠ total
      - tax_amount / subtotal ≠ stated tax_rate

    Returns list of error dicts with invoice_id, error_type, details.
    """
    rules = _load_invoice_rules()
    tolerance = rules.get("math_tolerance_usd", 0.05)
    expected_rates = rules.get("expected_tax_rates", {})
    tax_tolerance = rules.get("tax_tolerance_pct", 2)

    errors = []

    for _, row in invoices_df.iterrows():
        inv_id = row.get("invoice_id", "unknown")
        items = row.get("line_items", [])

        if not isinstance(items, list):
            continue

        # Check each line item: qty × unit = line_total
        for item in items:
            if not isinstance(item, dict):
                continue
            expected = item.get("quantity", 0) * item.get("unit_price", 0)
            actual = item.get("line_total", 0)
            if abs(expected - actual) > tolerance:
                errors.append({
                    "invoice_id": inv_id,
                    "error_type": "line_item_math",
                    "item_number": item.get("item_number"),
                    "expected": round(expected, 2),
                    "actual": round(actual, 2),
                    "discrepancy_usd": round(abs(expected - actual), 2),
                })

        # Check sum of line totals vs. subtotal
        if items and "subtotal" in row:
            computed_subtotal = sum(
                i.get("line_total", 0) for i in items if isinstance(i, dict)
            )
            if abs(computed_subtotal - row["subtotal"]) > tolerance:
                errors.append({
                    "invoice_id": inv_id,
                    "error_type": "subtotal_mismatch",
                    "computed": round(computed_subtotal, 2),
                    "stated": round(row["subtotal"], 2),
                    "discrepancy_usd": round(abs(computed_subtotal - row["subtotal"]), 2),
                })

        # Check subtotal + tax = total
        if all(k in row for k in ["subtotal", "tax_amount", "total"]):
            expected_total = row["subtotal"] + row["tax_amount"]
            if abs(expected_total - row["total"]) > tolerance:
                errors.append({
                    "invoice_id": inv_id,
                    "error_type": "total_mismatch",
                    "expected": round(expected_total, 2),
                    "stated": round(row["total"], 2),
                    "discrepancy_usd": round(abs(expected_total - row["total"]), 2),
                })

        # Check tax rate against expected for country
        country = str(row.get("country", "")).upper()
        if country in expected_rates and "tax_rate_pct" in row:
            expected_rate = expected_rates[country]
            actual_rate = row["tax_rate_pct"]
            if abs(expected_rate - actual_rate) > tax_tolerance:
                errors.append({
                    "invoice_id": inv_id,
                    "error_type": "tax_rate_anomaly",
                    "country": country,
                    "expected_rate_pct": expected_rate,
                    "actual_rate_pct": actual_rate,
                })

    return errors


def detect_duplicate_invoices(invoices_df: pd.DataFrame) -> pd.DataFrame:
    """
    Find invoices with same supplier_id + same total amount but different
    invoice IDs — potential duplicate billing.
    """
    if invoices_df.empty or "supplier_id" not in invoices_df.columns:
        return pd.DataFrame()

    required = ["supplier_id", "total", "invoice_id"]
    if not all(c in invoices_df.columns for c in required):
        return pd.DataFrame()

    # Group by supplier + total, find groups with multiple invoice IDs
    grouped = (
        invoices_df.groupby(["supplier_id", "total"])["invoice_id"]
        .apply(list)
        .reset_index()
    )
    duplicates = grouped[grouped["invoice_id"].apply(len) > 1].copy()

    if duplicates.empty:
        return pd.DataFrame()

    results = []
    for _, row in duplicates.iterrows():
        results.append({
            "supplier_id": row["supplier_id"],
            "total_amount": row["total"],
            "duplicate_invoice_ids": row["invoice_id"],
            "count": len(row["invoice_id"]),
        })

    return pd.DataFrame(results).sort_values("total_amount", ascending=False).reset_index(drop=True)


def detect_risky_payment_terms(invoices_df: pd.DataFrame) -> pd.DataFrame:
    """
    Flag invoices with extended payment terms (NET90+) as financial risk.
    """
    rules = _load_invoice_rules()
    max_days = rules.get("max_payment_terms_days", 60)

    if "payment_terms" not in invoices_df.columns:
        return pd.DataFrame()

    def extract_days(terms: str) -> int:
        match = re.search(r"NET(\d+)", str(terms).upper())
        return int(match.group(1)) if match else 0

    df = invoices_df.copy()
    df["payment_days"] = df["payment_terms"].apply(extract_days)
    risky = df[df["payment_days"] > max_days].copy()

    if risky.empty:
        return pd.DataFrame()

    return risky[["invoice_id", "supplier_id", "payment_terms", "payment_days", "total"]].reset_index(drop=True)
