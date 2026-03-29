# enterprise_cost_intelligence/agents/orchestrator.py
"""
Orchestration Agent

FIX #3: Top-level agent imports (import agents.vendor_agent as vendor_agent)
create a circular import risk — if any agent ever imports a shared utility
that imports orchestrator, Python's import lock deadlocks silently.

Fix: Move agent module imports inside run() (lazy import pattern). This is
the same pattern verification.py was already using for action_recommendation,
but inconsistently. Now all agent cross-imports are lazy everywhere.
"""

import logging
from state.schema import PipelineState, AnomalyType, Anomaly
from audit.audit_logger import log_event

logger = logging.getLogger(__name__)

_ROUTING: dict[AnomalyType, str] = {
    AnomalyType.DUPLICATE_VENDOR:  "vendor_agent",
    AnomalyType.CONTRACT_ANOMALY:  "vendor_agent",
    AnomalyType.SPEND_SPIKE:       "infrastructure_agent",
    AnomalyType.SHADOW_IT:         "infrastructure_agent",
    AnomalyType.RESOURCE_WASTE:    "infrastructure_agent",
    AnomalyType.INSTANCE_OVERPAY:  "infrastructure_agent",
    AnomalyType.SLA_BREACH_RISK:   "operations_agent",
    AnomalyType.CHURN_RISK:        "operations_agent",
    AnomalyType.FRAUD_SIGNAL:      "operations_agent",
    AnomalyType.INVOICE_ANOMALY:   "vendor_agent",
}


def run(state: PipelineState) -> PipelineState:
    # FIX #3: lazy imports — only resolved when run() is called, not at module load
    import agents.vendor_agent         as vendor_agent
    import agents.infrastructure_agent as infrastructure_agent
    import agents.operations_agent     as operations_agent

    _agent_map = {
        "vendor_agent":         vendor_agent,
        "infrastructure_agent": infrastructure_agent,
        "operations_agent":     operations_agent,
    }

    logger.info("=== Orchestration Agent: Routing anomalies to specialist agents ===")
    log_event(state.run_id, "orchestrator", "routing_start",
              {"anomaly_count": len(state.anomalies)})

    # Group anomalies by assigned specialist agent
    agent_queues: dict[str, list[Anomaly]] = {k: [] for k in _agent_map}
    unrouted: list[Anomaly] = []

    for anomaly in state.anomalies:
        # Prefer anomaly.assigned_agent if set by detection; fall back to routing table
        agent_name = anomaly.assigned_agent or _ROUTING.get(anomaly.anomaly_type)
        if agent_name and agent_name in _agent_map:
            agent_queues[agent_name].append(anomaly)
        else:
            unrouted.append(anomaly)
            logger.warning(f"No route for anomaly {anomaly.anomaly_id} type={anomaly.anomaly_type}")

    if unrouted:
        state.warnings.append(
            f"Unrouted anomalies ({len(unrouted)}): "
            f"{[a.anomaly_id for a in unrouted]}"
        )

    # Dispatch to each specialist agent
    for agent_name, anomalies in agent_queues.items():
        if not anomalies:
            continue

        logger.info(f"Dispatching {len(anomalies)} anomaly/anomalies → {agent_name}")
        log_event(
            state.run_id, "orchestrator", "dispatch",
            {
                "agent":       agent_name,
                "anomaly_ids": [a.anomaly_id for a in anomalies],
                "types":       [a.anomaly_type.value for a in anomalies],
            }
        )

        try:
            module = _agent_map[agent_name]
            state  = module.run(state, anomalies)
        except Exception as e:
            err = f"{agent_name} raised an exception: {e}"
            state.errors.append(err)
            logger.error(err, exc_info=True)
            log_event(
                state.run_id, "orchestrator", "agent_error",
                {"agent": agent_name, "error": err},
                severity="error",
            )

    log_event(
        state.run_id, "orchestrator", "routing_complete",
        {
            "root_cause_reports":    len(state.root_cause_reports),
            "errors":                len(state.errors),
        }
    )
    logger.info(
        f"=== Orchestrator done. {len(state.root_cause_reports)} root cause reports ==="
    )
    return state