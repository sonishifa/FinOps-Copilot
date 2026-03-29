# enterprise_cost_intelligence/state/schema.py
# STATUS: Clean — no fixes required from review.

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional
from enum import Enum


class Severity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ActionStatus(str, Enum):
    PENDING = "pending"
    AUTO_APPROVED = "auto_approved"
    PENDING_HUMAN = "pending_human"
    HUMAN_APPROVED = "human_approved"
    REJECTED = "rejected"
    EXECUTED = "executed"
    FAILED = "failed"
    # FIX #1 (pre-emptive): removed the non-existent STAGED_FOR_APPROVAL that
    # execution.py was referencing. PENDING_HUMAN is the correct term throughout.


class AnomalyType(str, Enum):
    SPEND_SPIKE = "spend_spike"
    DUPLICATE_VENDOR = "duplicate_vendor"
    SLA_BREACH_RISK = "sla_breach_risk"
    SHADOW_IT = "shadow_it"
    CONTRACT_ANOMALY = "contract_anomaly"
    CHURN_RISK = "churn_risk"
    RESOURCE_WASTE = "resource_waste"
    INSTANCE_OVERPAY = "instance_overpay"
    FRAUD_SIGNAL = "fraud_signal"
    INVOICE_ANOMALY = "invoice_anomaly"


@dataclass
class DataQualityReport:
    total_records: int = 0
    null_counts: dict[str, int] = field(default_factory=dict)
    duplicate_count: int = 0
    schema_issues: list[str] = field(default_factory=list)
    data_sources_loaded: list[str] = field(default_factory=list)
    normalization_notes: list[str] = field(default_factory=list)
    # FIX #11: added dedicated field for LLM narration so it doesn't
    # get string-stuffed into normalization_notes
    llm_quality_assessment: str = ""


@dataclass
class Anomaly:
    anomaly_id: str
    anomaly_type: AnomalyType
    severity: Severity
    title: str
    description: str
    affected_entity: str
    financial_impact_usd: float
    evidence: dict[str, Any]
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    assigned_agent: Optional[str] = None


@dataclass
class RootCauseReport:
    anomaly_id: str
    root_cause: str
    contributing_factors: list[str]
    evidence_summary: str
    confidence_score: float
    llm_reasoning: str
    diagnosed_by: str


@dataclass
class ActionRecommendation:
    action_id: str
    anomaly_id: str
    title: str
    description: str
    action_type: str
    expected_savings_usd: float
    priority_rank: int          # 0 = placeholder; set after global sort
    implementation_steps: list[str]
    requires_human_approval: bool
    business_rule_triggered: Optional[str] = None
    status: ActionStatus = ActionStatus.PENDING
    verification_notes: Optional[str] = None
    rejection_reason: Optional[str] = None


@dataclass
class ExecutionResult:
    action_id: str
    executed_at: datetime
    outcome: str                # "success" | "partial" | "failed" | "staged_for_approval"
    details: str
    rollback_available: bool = False
    stakeholder_notified: bool = False
    escalation_brief_sent: bool = False


@dataclass
class PipelineState:
    """Master state object passed between all agents. Single source of truth."""
    run_id: str
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Stage 1 – Ingestion
    raw_datasets: dict[str, Any] = field(default_factory=dict)
    data_quality_report: Optional[DataQualityReport] = None

    # Stage 2 – Anomaly Detection
    anomalies: list[Anomaly] = field(default_factory=list)

    # Stage 3 – Specialist agents
    root_cause_reports: list[RootCauseReport] = field(default_factory=list)

    # Stage 4 – Action recommendation + verification loop
    action_recommendations: list[ActionRecommendation] = field(default_factory=list)
    verification_attempts: int = 0

    # Stage 5 – Execution
    execution_results: list[ExecutionResult] = field(default_factory=list)

    # Audit trail (append-only)
    audit_log: list[dict[str, Any]] = field(default_factory=list)

    # Pipeline metadata
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    financial_summary: dict[str, float] = field(default_factory=dict)