-- Enterprise Cost Intelligence — Supabase Schema
-- Run this in Supabase SQL Editor before running seed.py
--
-- This creates:
--   1. Pipeline output tables (4)  — audit_events, pipeline_runs, execution_results, action_approvals
--   2. Dataset tables (13)         — ds_* tables for enterprise operational data

-- ═══════════════════════════════════════════════════════════════
-- PIPELINE OUTPUT TABLES (written by agents during pipeline run)
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS audit_events (
    id BIGSERIAL PRIMARY KEY,
    timestamp TEXT,
    run_id TEXT,
    agent TEXT,
    event_type TEXT,
    severity TEXT,
    anomaly_id TEXT,
    action_id TEXT,
    payload JSONB
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    total_financial_exposure_usd NUMERIC,
    total_recoverable_savings_usd NUMERIC,
    auto_executed_savings_usd NUMERIC,
    pending_human_approval_savings_usd NUMERIC,
    roi_multiple NUMERIC
);

CREATE TABLE IF NOT EXISTS execution_results (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT,
    action_id TEXT,
    executed_at TEXT,
    outcome TEXT,
    details TEXT,
    rollback_available BOOLEAN,
    stakeholder_notified BOOLEAN,
    escalation_brief_sent BOOLEAN
);

CREATE TABLE IF NOT EXISTS action_approvals (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT,
    action_id TEXT,
    status TEXT DEFAULT 'pending_human',
    approved_by TEXT,
    decided_at TIMESTAMP DEFAULT NOW(),
    notes TEXT
);


-- ═══════════════════════════════════════════════════════════════
-- DATASET TABLES (populated by seed.py, read by the pipeline)
-- ═══════════════════════════════════════════════════════════════

-- Procurement KPI
CREATE TABLE IF NOT EXISTS ds_procurement_kpi (
    id BIGSERIAL PRIMARY KEY,
    po_id TEXT,
    supplier TEXT,
    order_date TEXT,
    delivery_date TEXT,
    item_category TEXT,
    order_status TEXT,
    quantity NUMERIC,
    unit_price NUMERIC,
    negotiated_price NUMERIC,
    defective_units NUMERIC,
    compliance TEXT
);

-- Corporate Procurement
CREATE TABLE IF NOT EXISTS ds_corporate_procurement (
    id BIGSERIAL PRIMARY KEY,
    award_date TEXT,
    quarter_and_fiscal_year TEXT,
    commodity_category TEXT,
    contract_description TEXT,
    wbg_organization TEXT,
    selection_number TEXT,
    supplier TEXT,
    supplier_country TEXT,
    supplier_country_code TEXT,
    contract_award_amount NUMERIC,
    fund_source TEXT,
    vpu_description TEXT
);

-- Government Procurement
CREATE TABLE IF NOT EXISTS ds_government_procurement (
    id BIGSERIAL PRIMARY KEY,
    tender_no TEXT,
    tender_description TEXT,
    agency TEXT,
    award_date TEXT,
    tender_detail_status TEXT,
    supplier_name TEXT,
    awarded_amt NUMERIC
);

-- Cloud Spend
CREATE TABLE IF NOT EXISTS ds_cloud_spend (
    id BIGSERIAL PRIMARY KEY,
    date TEXT,
    service_name TEXT,
    resource_id TEXT,
    region TEXT,
    cost_usd NUMERIC,
    usage_hours NUMERIC,
    tag_owner TEXT,
    tag_project TEXT,
    tag_environment TEXT,
    anomaly_label TEXT
);

-- Shadow IT
CREATE TABLE IF NOT EXISTS ds_shadow_it (
    id BIGSERIAL PRIMARY KEY,
    resource_id TEXT,
    resource_type TEXT,
    cloud_provider TEXT,
    region TEXT,
    created_at TEXT,
    last_used_at TEXT,
    days_since_used NUMERIC,
    monthly_cost_usd NUMERIC,
    tag_owner TEXT,
    tag_project TEXT,
    tag_environment TEXT,
    shadow_label TEXT
);

-- ITSM (IT Service Management)
CREATE TABLE IF NOT EXISTS ds_itsm (
    id BIGSERIAL PRIMARY KEY,
    status TEXT,
    ticket_id TEXT,
    priority TEXT,
    source TEXT,
    topic TEXT,
    agent_group TEXT,
    agent_name TEXT,
    created_time TEXT,
    expected_sla_to_resolve TEXT,
    expected_sla_to_first_response TEXT,
    first_response_time TEXT,
    sla_for_first_response TEXT,
    resolution_time TEXT,
    sla_for_resolution TEXT,
    close_time TEXT,
    agent_interactions NUMERIC,
    survey_results TEXT,
    product_group TEXT,
    support_level TEXT,
    country TEXT,
    latitude NUMERIC,
    longitude NUMERIC
);

-- SaaS Sales
CREATE TABLE IF NOT EXISTS ds_saas_sales (
    id BIGSERIAL PRIMARY KEY,
    row_id NUMERIC,
    order_id TEXT,
    order_date TEXT,
    date_key NUMERIC,
    contact_name TEXT,
    country TEXT,
    city TEXT,
    region TEXT,
    subregion TEXT,
    customer TEXT,
    customer_id TEXT,
    industry TEXT,
    segment TEXT,
    product TEXT,
    license TEXT,
    sales NUMERIC,
    quantity NUMERIC,
    discount NUMERIC,
    profit NUMERIC
);

-- Ravenstack Accounts
CREATE TABLE IF NOT EXISTS ds_ravenstack_accounts (
    id BIGSERIAL PRIMARY KEY,
    account_id TEXT,
    account_name TEXT,
    industry TEXT,
    country TEXT,
    signup_date TEXT,
    referral_source TEXT,
    plan_tier TEXT,
    seats NUMERIC,
    is_trial BOOLEAN,
    churn_flag BOOLEAN
);

-- Ravenstack Subscriptions
CREATE TABLE IF NOT EXISTS ds_ravenstack_subscriptions (
    id BIGSERIAL PRIMARY KEY,
    subscription_id TEXT,
    account_id TEXT,
    start_date TEXT,
    end_date TEXT,
    plan_tier TEXT,
    seats NUMERIC,
    mrr_amount NUMERIC,
    arr_amount NUMERIC,
    is_trial BOOLEAN,
    upgrade_flag BOOLEAN,
    downgrade_flag BOOLEAN,
    churn_flag BOOLEAN,
    billing_frequency TEXT,
    auto_renew_flag BOOLEAN
);

-- Ravenstack Churn Events
CREATE TABLE IF NOT EXISTS ds_ravenstack_churn (
    id BIGSERIAL PRIMARY KEY,
    churn_event_id TEXT,
    account_id TEXT,
    churn_date TEXT,
    reason_code TEXT,
    refund_amount_usd NUMERIC,
    preceding_upgrade_flag BOOLEAN,
    preceding_downgrade_flag BOOLEAN,
    is_reactivation BOOLEAN,
    feedback_text TEXT
);

-- Ravenstack Feature Usage
CREATE TABLE IF NOT EXISTS ds_ravenstack_features (
    id BIGSERIAL PRIMARY KEY,
    usage_id TEXT,
    subscription_id TEXT,
    usage_date TEXT,
    feature_name TEXT,
    usage_count NUMERIC,
    usage_duration_secs NUMERIC,
    error_count NUMERIC,
    is_beta_feature BOOLEAN
);

-- Ravenstack Support Tickets
CREATE TABLE IF NOT EXISTS ds_ravenstack_tickets (
    id BIGSERIAL PRIMARY KEY,
    ticket_id TEXT,
    account_id TEXT,
    submitted_at TEXT,
    closed_at TEXT,
    resolution_time_hours NUMERIC,
    priority TEXT,
    first_response_time_minutes NUMERIC,
    satisfaction_score NUMERIC,
    escalation_flag BOOLEAN
);

-- Fraud Transactions (seeded sample — 50K of 6M total)
CREATE TABLE IF NOT EXISTS ds_fraud_transactions (
    id BIGSERIAL PRIMARY KEY,
    step NUMERIC,
    type TEXT,
    amount NUMERIC,
    nameorig TEXT,
    oldbalanceorg NUMERIC,
    newbalanceorig NUMERIC,
    namedest TEXT,
    oldbalancedest NUMERIC,
    newbalancedest NUMERIC,
    isfraud NUMERIC,
    isflaggedfraud NUMERIC
);
