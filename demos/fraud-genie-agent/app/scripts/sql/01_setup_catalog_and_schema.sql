-- ============================================================================
-- MuleGraph Investigator: Catalog, Schema, and Gold Tables Setup
-- ============================================================================
--
-- BEFORE RUNNING: Edit the catalog and schema names below to match your
-- workspace conventions. The defaults are:
--   - Catalog: mulegraph
--   - Schema:  investigations
--
-- This script is designed to be run in a Databricks SQL editor or a notebook
-- %sql cell. It creates all 8 Gold tables as Delta tables if they don't exist.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Create catalog and schema (edit these names as needed)
-- ---------------------------------------------------------------------------

CREATE CATALOG IF NOT EXISTS mulegraph;
CREATE SCHEMA IF NOT EXISTS mulegraph.investigations;

USE CATALOG mulegraph;
USE SCHEMA investigations;

-- ---------------------------------------------------------------------------
-- 2. Gold tables (8 tables, matching src/pipeline/gold.py output exactly)
-- ---------------------------------------------------------------------------

-- accounts: All accounts with detection results and case-level headline numbers
CREATE TABLE IF NOT EXISTS accounts (
    account_id STRING,
    cohort STRING,
    account_role STRING,
    open_date DATE,
    display_name STRING,
    scenario_type STRING,
    scenario_label STRING,
    tenure_days BIGINT,
    distinct_source_count BIGINT,
    total_inbound_amount DOUBLE,
    distinct_destination_count BIGINT,
    distinct_outbound_months BIGINT,
    total_outbound_amount DOUBLE,
    raw_fan_pattern_flag BOOLEAN,
    override_applied BOOLEAN,
    is_flagged_mule_network BOOLEAN,
    detection_reason STRING,
    risk_band STRING,
    case_total_exposure_permissive DOUBLE,
    case_total_exposure_strict DOUBLE,
    case_other_connected_accounts_permissive BIGINT,
    case_other_connected_accounts_strict BIGINT
) USING DELTA;

-- evidence: Device linkage and account-takeover evidence with confidence scores
CREATE TABLE IF NOT EXISTS evidence (
    evidence_id STRING,
    account_id STRING,
    related_account_id STRING,
    device_id STRING,
    evidence_type STRING,
    confidence STRING,
    rail STRING,
    cohort STRING,
    fund_flow_amount DOUBLE,
    description STRING
) USING DELTA;

-- network_edges: All edges in the network graph (device-sharing and fund-flow)
CREATE TABLE IF NOT EXISTS network_edges (
    edge_id STRING,
    account_a STRING,
    account_b STRING,
    edge_type STRING,
    device_id STRING,
    amount DOUBLE,
    strict_included BOOLEAN,
    permissive_included BOOLEAN
) USING DELTA;

-- transfers: Individual transactions with cohort annotations
CREATE TABLE IF NOT EXISTS transfers (
    txn_id STRING,
    source_account STRING,
    dest_account STRING,
    amount DOUBLE,
    txn_date DATE,
    channel STRING,
    month STRING,
    source_cohort STRING,
    dest_cohort STRING
) USING DELTA;

-- case_summary: Per-case headline metrics for both evidence policies
CREATE TABLE IF NOT EXISTS case_summary (
    case_id STRING,
    seed_account STRING,
    scenario_type STRING,
    scenario_label STRING,
    total_exposure_permissive DOUBLE,
    total_exposure_strict DOUBLE,
    other_connected_accounts_permissive BIGINT,
    other_connected_accounts_strict BIGINT,
    shared_devices_permissive BIGINT,
    shared_devices_strict BIGINT,
    potential_victims_count BIGINT,
    destinations_count BIGINT
) USING DELTA;

-- control_cohort: Legitimate high-volume accounts protected by tenure/recurrence override
CREATE TABLE IF NOT EXISTS control_cohort (
    account_id STRING,
    cohort STRING,
    account_role STRING,
    tenure_days BIGINT,
    distinct_outbound_months BIGINT,
    distinct_source_count BIGINT,
    distinct_destination_count BIGINT,
    total_outbound_amount DOUBLE,
    raw_fan_pattern_flag BOOLEAN,
    override_applied BOOLEAN,
    is_flagged_mule_network BOOLEAN,
    detection_reason STRING
) USING DELTA;

-- freshness: Data freshness tracking for all Gold tables
CREATE TABLE IF NOT EXISTS freshness (
    table_name STRING,
    last_refreshed_ts STRING,
    freshness_contract_hours BIGINT,
    is_stale BOOLEAN
) USING DELTA;

-- export_citations: Pre-computed citation text for case file exports
CREATE TABLE IF NOT EXISTS export_citations (
    citation_id STRING,
    source_table STRING,
    source_row_id STRING,
    account_id STRING,
    citation_text STRING
) USING DELTA;

-- ---------------------------------------------------------------------------
-- 3. Policy-scoped views for Genie
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW evidence_strict_v AS
SELECT *
FROM evidence
WHERE evidence_type IN ('device_and_fund_flow', 'account_takeover_provenance');

CREATE OR REPLACE VIEW evidence_permissive_v AS
SELECT *
FROM evidence;

CREATE OR REPLACE VIEW network_edges_strict_v AS
SELECT *
FROM network_edges
WHERE strict_included = TRUE;

CREATE OR REPLACE VIEW network_edges_permissive_v AS
SELECT *
FROM network_edges
WHERE permissive_included = TRUE;
