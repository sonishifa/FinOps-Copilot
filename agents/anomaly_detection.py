# enterprise_cost_intelligence/agents/anomaly_detection.py
"""
Anomaly Detection Agent

FIX #5:  SLA detection now uses analyse_sla_breach_history() on the historical
         ITSM dataset instead of compute_sla_breach_risk() which requires an
         open-ticket queue. The ITSM dataset has 100K closed tickets — perfect
         for breach trend analysis.

FIX #8:  Duplicate vendor detection was running independently on both
         corporate_procurement and procurement_kpi datasets, then generating
         separate Anomaly objects whose savings were summed in the financial
         summary. Many vendors overlap across both datasets, causing
         double-counting. Now: run dedup on each dataset but tag the anomaly
         with the source and deduplicate the final pair list by vendor names.

FIX #18: defect_rate calculation on procurement_kpi was not filling NaN values
         in defective_units before division, producing NaN rates that were
         silently excluded, understating the defect anomaly count.
"""

import uuid
import logging
from pathlib import Path

from state.schema import PipelineState, Anomaly, AnomalyType, Severity
from tools.infrastructure_tools import detect_spend_spikes, find_shadow_it
from tools.vendor_tools import find_duplicate_vendors, calculate_consolidation_savings
from tools.operations_tools import analyse_sla_breach_history, project_penalty_from_breach_history
from audit.audit_logger import log_event

logger = logging.getLogger(__name__)


def _severity_from_impact(usd: float) -> Severity:
    if usd >= 100_000: return Severity.CRITICAL
    if usd >= 20_000:  return Severity.HIGH
    if usd >= 5_000:   return Severity.MEDIUM
    return Severity.LOW


def run(state: PipelineState) -> PipelineState:
    logger.info("=== Anomaly Detection Agent: Starting ===")
    log_event(state.run_id, "anomaly_detection_agent", "agent_start", {})

    anomalies: list[Anomaly] = []
    ds = state.raw_datasets

    # ── 1. Cloud spend spikes ─────────────────────────────────────────────────
    if "cloud_spend" in ds:
        try:
            spikes = detect_spend_spikes(ds["cloud_spend"])
            for _, row in spikes.iterrows():
                impact = abs(float(row.get("spike_usd", 0)))
                anomalies.append(Anomaly(
                    anomaly_id     = f"ANO-{uuid.uuid4().hex[:8]}",
                    anomaly_type   = AnomalyType.SPEND_SPIKE,
                    severity       = _severity_from_impact(impact),
                    title          = f"Cloud spend spike: {row['service_name']} +{row['mom_change_pct']:.1f}% MoM",
                    description    = (
                        f"{row['service_name']} costs rose {row['mom_change_pct']:.1f}% "
                        f"in {row['month']} (${impact:,.0f} increase)."
                    ),
                    affected_entity = row["service_name"],
                    financial_impact_usd = impact,
                    evidence = {
                        "service":         row["service_name"],
                        "month":           str(row["month"]),
                        "cost_usd":        round(float(row["cost_usd"]), 2),
                        "prev_cost_usd":   round(float(row["prev_month_cost"]), 2),
                        "mom_change_pct":  float(row["mom_change_pct"]),
                        "spike_usd":       round(impact, 2),
                    },
                    assigned_agent = "infrastructure_agent",
                ))
            logger.info(f"Cloud spend: {len(spikes)} spikes detected")
        except Exception as e:
            state.warnings.append(f"Cloud spend detection error: {e}")
            logger.warning(f"Cloud spend detection: {e}")

    # ── 2. Shadow IT ──────────────────────────────────────────────────────────
    if "shadow_it" in ds:
        try:
            shadow = find_shadow_it(ds["shadow_it"])
            if not shadow.empty:
                monthly_waste  = float(shadow["monthly_cost_usd"].sum())
                annual_waste   = monthly_waste * 12
                anomalies.append(Anomaly(
                    anomaly_id     = f"ANO-{uuid.uuid4().hex[:8]}",
                    anomaly_type   = AnomalyType.SHADOW_IT,
                    severity       = _severity_from_impact(annual_waste),
                    title          = f"Shadow IT: {len(shadow)} unmanaged resources (${monthly_waste:,.0f}/month)",
                    description    = (
                        f"{len(shadow)} cloud resources unused for 30+ days, "
                        f"costing ${monthly_waste:,.0f}/month (${annual_waste:,.0f}/year)."
                    ),
                    affected_entity = "cloud_infrastructure",
                    financial_impact_usd = annual_waste,
                    evidence = {
                        "resource_count":   len(shadow),
                        "monthly_waste_usd":round(monthly_waste, 2),
                        "annual_waste_usd": round(annual_waste, 2),
                        "top_resources":    shadow.head(5)[["resource_id", "monthly_cost_usd"]].to_dict("records"),
                    },
                    assigned_agent = "infrastructure_agent",
                ))
            logger.info(f"Shadow IT: {len(shadow)} resources flagged")
        except Exception as e:
            state.warnings.append(f"Shadow IT detection error: {e}")

    # ── 3. Duplicate vendors (FIX #8) ─────────────────────────────────────────
    # Run dedup on each source dataset, then merge the pair lists and deduplicate
    # by {min(vendor_a, vendor_b), max(vendor_a, vendor_b)} to avoid double-count.
    _seen_pairs: set[tuple[str, str]] = set()

    for ds_name, name_col, cat_col, amount_col in [
        ("corporate_procurement", "supplier", "commodity_category", "contract_award_amount"),
        ("procurement_kpi",       "supplier", "item_category",      "unit_price"),
    ]:
        if ds_name not in ds:
            continue
        try:
            dupes = find_duplicate_vendors(ds[ds_name], name_col=name_col, category_col=cat_col)
            if dupes.empty:
                continue

            # FIX #8: deduplicate pairs across datasets
            unique_dupes = []
            for _, row in dupes.iterrows():
                pair_key = (min(row["vendor_a"], row["vendor_b"]),
                            max(row["vendor_a"], row["vendor_b"]))
                if pair_key not in _seen_pairs:
                    _seen_pairs.add(pair_key)
                    unique_dupes.append(row)

            if not unique_dupes:
                logger.info(f"Vendor duplicates ({ds_name}): all pairs already counted from prior source")
                continue

            import pandas as pd
            unique_df = pd.DataFrame(unique_dupes)
            savings_df = calculate_consolidation_savings(
                ds[ds_name], unique_df, amount_col=amount_col, name_col=name_col
            )
            total_savings = float(savings_df["estimated_annual_savings_usd"].sum()) if not savings_df.empty else 0

            anomalies.append(Anomaly(
                anomaly_id     = f"ANO-{uuid.uuid4().hex[:8]}",
                anomaly_type   = AnomalyType.DUPLICATE_VENDOR,
                severity       = _severity_from_impact(total_savings),
                title          = f"Duplicate vendors in {ds_name}: {len(unique_df)} unique overlapping pairs",
                description    = (
                    f"Found {len(unique_df)} likely-duplicate vendor pairs in {ds_name}. "
                    f"Consolidation could save ~${total_savings:,.0f}/year."
                ),
                affected_entity = ds_name,
                financial_impact_usd = total_savings,
                evidence = {
                    "unique_duplicate_pairs": len(unique_df),
                    "top_pairs":              unique_df.head(10).to_dict("records"),
                    "savings_breakdown":      savings_df.head(10).to_dict("records") if not savings_df.empty else [],
                    "total_annual_savings":   round(total_savings, 2),
                    "dataset_source":         ds_name,
                },
                assigned_agent = "vendor_agent",
            ))
            logger.info(f"Vendor duplicates ({ds_name}): {len(unique_df)} unique pairs, ${total_savings:,.0f} savings")
        except Exception as e:
            state.warnings.append(f"Vendor dup detection error ({ds_name}): {e}")
            logger.warning(f"Vendor dup ({ds_name}): {e}")

    # ── 4. SLA breach risk — FIX #5: use historical breach analysis ───────────
    if "itsm" in ds:
        try:
            breach_analysis = analyse_sla_breach_history(ds["itsm"])
            projection      = project_penalty_from_breach_history(
                breach_analysis, penalty_per_breach_usd=1500.0, forward_months=3
            )
            penalty = projection["projected_penalty_exposure_usd"]

            if breach_analysis["overall_breach_rate_pct"] > 0:
                anomalies.append(Anomaly(
                    anomaly_id     = f"ANO-{uuid.uuid4().hex[:8]}",
                    anomaly_type   = AnomalyType.SLA_BREACH_RISK,
                    severity       = _severity_from_impact(penalty),
                    title          = (
                        f"SLA breach trend: {breach_analysis['overall_breach_rate_pct']:.1f}% breach rate "
                        f"(${penalty:,.0f} projected 3-month penalty)"
                    ),
                    description    = (
                        f"Historical analysis of {breach_analysis['total_tickets_analysed']:,} tickets shows "
                        f"{breach_analysis['overall_breach_rate_pct']:.1f}% SLA breach rate. "
                        f"At current volume, projected 3-month penalty exposure: ${penalty:,.0f}."
                    ),
                    affected_entity = "itsm_service_desk",
                    financial_impact_usd = penalty,
                    evidence = {
                        **breach_analysis,
                        **projection,
                    },
                    assigned_agent = "operations_agent",
                ))
            logger.info(
                f"SLA breach history: {breach_analysis['overall_breach_rate_pct']:.1f}% breach rate, "
                f"${penalty:,.0f} projected penalty"
            )
        except Exception as e:
            state.warnings.append(f"SLA detection error: {e}")
            logger.warning(f"SLA detection: {e}")

    # ── 5. Contract anomalies — zero-amount awards ────────────────────────────
    if "corporate_procurement" in ds:
        try:
            df = ds["corporate_procurement"]
            if "contract_award_amount" in df.columns:
                zero_awards = df[df["contract_award_amount"].fillna(0) == 0]
                if not zero_awards.empty:
                    anomalies.append(Anomaly(
                        anomaly_id     = f"ANO-{uuid.uuid4().hex[:8]}",
                        anomaly_type   = AnomalyType.CONTRACT_ANOMALY,
                        severity       = Severity.MEDIUM,
                        title          = f"Contract anomaly: {len(zero_awards)} zero-amount awards",
                        description    = (
                            f"{len(zero_awards)} contracts recorded with $0 award amount — "
                            f"potential unreported spend or data entry errors."
                        ),
                        affected_entity = "corporate_procurement",
                        financial_impact_usd = 0,
                        evidence = {
                            "zero_award_count": len(zero_awards),
                            "sample": (
                                zero_awards.head(5)[["supplier", "contract_description"]]
                                .to_dict("records")
                            ),
                        },
                        assigned_agent = "vendor_agent",
                    ))
        except Exception as e:
            state.warnings.append(f"Contract anomaly detection error: {e}")

    # ── 6. Procurement KPI — defect rate (FIX #18) ────────────────────────────
    if "procurement_kpi" in ds:
        try:
            df = ds["procurement_kpi"].copy()
            if "defective_units" in df.columns and "quantity" in df.columns:
                # FIX #18: fillna(0) before division so NaN rows are included
                df["defective_units"] = df["defective_units"].fillna(0)
                df["defect_rate"]     = df["defective_units"] / df["quantity"].replace(0, float("nan"))

                high_defect = df[df["defect_rate"] > 0.10]
                if not high_defect.empty:
                    loss_est = float(
                        (high_defect["defective_units"] * high_defect["unit_price"])
                        .sum()
                    )
                    anomalies.append(Anomaly(
                        anomaly_id     = f"ANO-{uuid.uuid4().hex[:8]}",
                        anomaly_type   = AnomalyType.CONTRACT_ANOMALY,
                        severity       = _severity_from_impact(loss_est),
                        title          = f"High defect rate: {len(high_defect)} POs with >10% defects",
                        description    = (
                            f"{len(high_defect)} purchase orders have defect rates above 10%. "
                            f"Estimated loss from defective goods: ${loss_est:,.0f}."
                        ),
                        affected_entity = "procurement_kpi",
                        financial_impact_usd = loss_est,
                        evidence = {
                            "high_defect_po_count": len(high_defect),
                            "total_pos_analysed":   len(df),
                            "top_offenders": (
                                high_defect.nlargest(5, "defect_rate")
                                [["po_id", "supplier", "defect_rate", "defective_units"]]
                                .round({"defect_rate": 3})
                                .to_dict("records")
                            ),
                        },
                        assigned_agent = "vendor_agent",
                    ))
            logger.info(f"Procurement KPI: defect analysis complete")
        except Exception as e:
            state.warnings.append(f"Procurement KPI detection error: {e}")

    # ── 9. AWS Instance overpaying ────────────────────────────────────────────
    if "cloud_spend" in ds and "aws_pricing" in ds:
        try:
            from tools.pricing_tools import detect_overpaying
            overpay_df = detect_overpaying(ds["cloud_spend"], ds["aws_pricing"])
            if not overpay_df.empty:
                total_overpay = float(overpay_df["overpay_usd"].sum())
                anomalies.append(Anomaly(
                    anomaly_id     = f"ANO-{uuid.uuid4().hex[:8].upper()}",
                    anomaly_type   = AnomalyType.INSTANCE_OVERPAY,
                    severity       = _severity_from_impact(total_overpay),
                    description    = (
                        f"{len(overpay_df)} cloud instances paying above AWS list price. "
                        f"Total overpayment: ${total_overpay:,.0f}."
                    ),
                    affected_entity = "cloud_spend × aws_pricing",
                    financial_impact_usd = total_overpay,
                    evidence = {
                        "overpaying_instances": len(overpay_df),
                        "total_overpay_usd": round(total_overpay, 2),
                        "top_offenders": overpay_df.head(5).to_dict("records"),
                    },
                    assigned_agent = "infrastructure_agent",
                ))
            logger.info(f"AWS pricing: overpay detection complete")
        except Exception as e:
            state.warnings.append(f"AWS pricing overpay detection error: {e}")

    # ── 10. Fraud detection (heuristic-based) ─────────────────────────────────
    fraud_path = Path(__file__).parent.parent / "data" / "fraud-detection-paysim.csv"
    if fraud_path.exists():
        try:
            from tools.fraud_tools import detect_fraud_heuristic
            fraud_result = detect_fraud_heuristic(fraud_path)
            if fraud_result["total_flagged"] > 0:
                exposure = fraud_result["total_exposure_usd"]
                anomalies.append(Anomaly(
                    anomaly_id     = f"ANO-{uuid.uuid4().hex[:8].upper()}",
                    anomaly_type   = AnomalyType.FRAUD_SIGNAL,
                    severity       = _severity_from_impact(exposure),
                    title          = f"Fraud: {fraud_result['total_flagged']} suspicious transactions (${exposure:,.0f} exposure)",
                    description    = (
                        f"{fraud_result['total_flagged']} suspicious transactions detected "
                        f"via heuristic analysis across {fraud_result['total_transactions_scanned']:,} "
                        f"records. Estimated exposure: ${exposure:,.0f}."
                    ),
                    affected_entity = "fraud_transactions",
                    financial_impact_usd = exposure,
                    evidence = {
                        "total_scanned":      fraud_result["total_transactions_scanned"],
                        "total_flagged":      fraud_result["total_flagged"],
                        "fraud_by_signal":    fraud_result["fraud_by_signal"],
                        "high_risk_accounts": fraud_result["high_risk_accounts"][:5],
                        "sample_flagged":     fraud_result["flagged_sample"][:5],
                    },
                    assigned_agent = "operations_agent",
                ))
            logger.info(f"Fraud detection: heuristic scan complete")
        except Exception as e:
            state.warnings.append(f"Fraud detection error: {e}")

    # ── 11. Invoice anomalies (OCR-based) ─────────────────────────────────────
    if "invoices" in ds and not ds["invoices"].empty:
        try:
            from tools.invoice_tools import (
                verify_invoice_math, detect_duplicate_invoices, detect_risky_payment_terms
            )
            inv_df = ds["invoices"]

            math_errors = verify_invoice_math(inv_df)
            duplicates = detect_duplicate_invoices(inv_df)
            risky_terms = detect_risky_payment_terms(inv_df)

            total_issues = len(math_errors) + len(duplicates) + len(risky_terms)
            if total_issues > 0:
                # Estimate financial impact from math discrepancies
                math_impact = sum(e.get("discrepancy_usd", 0) for e in math_errors)
                dup_impact = float(duplicates["total_amount"].sum()) if not duplicates.empty else 0
                total_impact_inv = math_impact + dup_impact

                anomalies.append(Anomaly(
                    anomaly_id     = f"ANO-{uuid.uuid4().hex[:8].upper()}",
                    anomaly_type   = AnomalyType.INVOICE_ANOMALY,
                    severity       = _severity_from_impact(total_impact_inv),
                    description    = (
                        f"{total_issues} invoice anomalies detected from OCR analysis of "
                        f"{len(inv_df)} invoices: {len(math_errors)} math errors, "
                        f"{len(duplicates)} duplicate invoices, {len(risky_terms)} risky "
                        f"payment terms. Estimated impact: ${total_impact_inv:,.0f}."
                    ),
                    affected_entity = "invoices",
                    financial_impact_usd = total_impact_inv,
                    evidence = {
                        "invoices_scanned":     len(inv_df),
                        "math_errors":          math_errors[:5],
                        "duplicate_invoices":   (
                            duplicates.head(5).to_dict("records")
                            if not duplicates.empty else []
                        ),
                        "risky_payment_terms":  (
                            risky_terms.head(5).to_dict("records")
                            if not risky_terms.empty else []
                        ),
                    },
                    assigned_agent = "operations_agent",
                ))
            logger.info(f"Invoice analysis: OCR anomaly check complete")
        except Exception as e:
            state.warnings.append(f"Invoice anomaly detection error: {e}")

    # Sort by financial impact descending
    anomalies.sort(key=lambda a: a.financial_impact_usd, reverse=True)
    state.anomalies = anomalies

    total_impact = sum(a.financial_impact_usd for a in anomalies)

    # Log each anomaly individually (for dashboard display)
    for a in anomalies:
        log_event(
            state.run_id, "anomaly_detection_agent", "anomaly_detected",
            {
                "anomaly_type":         a.anomaly_type.value,
                "severity":             a.severity.value,
                "title":                a.title,
                "description":          a.description,
                "affected_entity":      a.affected_entity,
                "financial_impact_usd": round(a.financial_impact_usd, 2),
                "assigned_agent":       a.assigned_agent,
            },
            anomaly_id=a.anomaly_id,
        )

    log_event(
        state.run_id, "anomaly_detection_agent", "detection_complete",
        {
            "total_anomalies":          len(anomalies),
            "total_financial_impact_usd": round(total_impact, 2),
            "by_type": {
                t.value: sum(1 for a in anomalies if a.anomaly_type == t)
                for t in AnomalyType
            },
        }
    )
    logger.info(
        f"=== Anomaly Detection: {len(anomalies)} anomalies, "
        f"${total_impact:,.0f} total impact ==="
    )
    return state