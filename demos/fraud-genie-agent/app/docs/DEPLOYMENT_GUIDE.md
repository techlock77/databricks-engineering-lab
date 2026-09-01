# MuleGraph Investigator: manual deployment in the Databricks web UI

This beginner guide goes from an empty Databricks Free Edition workspace to a live app. Everything is point-and-click in the Databricks website, apart from copying supplied file contents. Do not use a terminal, Git, a CLI, or a personal access token.

Complete the stages in order. Keep the supplied `demos/fraud-genie-agent/app` folder available in your computer's file browser. A value written as `<REPLACE_ME_LIKE_THIS>` must be replaced, without angle brackets, using the instructions beside it.

## Stage 1 — Log in and find the workspace UI

### DO

1. Open the Free Edition sign-in link received at registration and sign in. If an account page lists workspaces, click the workspace to use.
2. Record `<REPLACE_ME_WORKSPACE_HOST>` from the browser address bar: copy the URL through the Databricks domain, such as `https://dbc-12345678-abcd.cloud.databricks.com`, without the page path. This merely identifies the workspace; the app receives its host automatically.
3. Find the left navigation. Expand it or hover over icons to see labels. This guide uses **SQL Editor**, **SQL Warehouses**, **Workspace**, **Catalog**, **Genie**, and **Databricks Apps**. If hidden, use the app switcher—the grid/dots icon near the Databricks logo that switches between Data, Machine Learning, Apps, and other areas; data tools are under **Analytics and AI** and apps under **Databricks Apps**.

### RUN

Click **Workspace** and wait for its file browser to open.

### VALIDATE

Confirm the URL starts with `<REPLACE_ME_WORKSPACE_HOST>`, your profile appears at top-right, and both **Workspace** and **SQL Editor** are reachable.

**PASS:** You are in the intended workspace and can open those two pages. Go to Stage 2.

#### Troubleshooting

- Account console shown: click the Free Edition workspace tile.
- Item missing: expand the sidebar or use the app switcher.
- Multiple workspaces: compare the address bar with `<REPLACE_ME_WORKSPACE_HOST>`; all later resources must be in one workspace.

## Stage 2 — Create the catalog, schema, eight tables, and four views

### DO

1. Click **SQL Editor** > **New query**.
2. Use the top **Compute** selector to choose a SQL warehouse (the compute that runs your SQL queries). If none exists, click **SQL Warehouses** > **Create SQL warehouse**, accept the Free Edition defaults, and click **Create**.
3. Paste the complete, verbatim contents of `scripts/sql/01_setup_catalog_and_schema.sql` below into the query editor:

```sql
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
```

### RUN

Click **Run all** and wait for every statement to succeed.

### VALIDATE

Click **Catalog**, expand `mulegraph` > `investigations`, and verify these tables: `accounts`, `evidence`, `network_edges`, `transfers`, `case_summary`, `control_cohort`, `freshness`, `export_citations`. Verify these views: `evidence_strict_v`, `evidence_permissive_v`, `network_edges_strict_v`, `network_edges_permissive_v`.

In a new SQL query, run this guide-only validation query:

```sql
SELECT 'accounts' AS object_name, COUNT(*) AS row_count FROM mulegraph.investigations.accounts
UNION ALL SELECT 'evidence', COUNT(*) FROM mulegraph.investigations.evidence
UNION ALL SELECT 'network_edges', COUNT(*) FROM mulegraph.investigations.network_edges
UNION ALL SELECT 'transfers', COUNT(*) FROM mulegraph.investigations.transfers
UNION ALL SELECT 'case_summary', COUNT(*) FROM mulegraph.investigations.case_summary
UNION ALL SELECT 'control_cohort', COUNT(*) FROM mulegraph.investigations.control_cohort
UNION ALL SELECT 'freshness', COUNT(*) FROM mulegraph.investigations.freshness
UNION ALL SELECT 'export_citations', COUNT(*) FROM mulegraph.investigations.export_citations;
```

**PASS:** Catalog shows 8 tables and 4 views; the query returns 8 rows, each with count `0`. Go to Stage 3.

#### Troubleshooting

- `PERMISSION_DENIED` on catalog creation: use the Free Edition workspace you own or obtain `CREATE CATALOG`; changing names also requires changing `app.yaml`.
- No compute selected: choose a warehouse in the editor and rerun.
- Views missing: rerun the last four `CREATE OR REPLACE VIEW` statements after the two `USE` statements.

## Stage 3 — Upload source and load synthetic data as a notebook

The notebook imports `src`, so upload the app tree now; Stage 5 deploys this same folder.

### DO

1. Click **Workspace** > **Users** > your email folder. Click **Create** > **Folder**, name it `mulegraph-investigator`, and open it.
2. Record `<REPLACE_ME_APP_SOURCE_PATH>` from the breadcrumb. It means this workspace folder and normally resembles `/Workspace/Users/<REPLACE_ME_YOUR_EMAIL>/mulegraph-investigator`; find `<REPLACE_ME_YOUR_EMAIL>` in the top-right profile menu.
3. In this folder choose the three-dot menu > **Import**, or drag files into it. Upload the contents of the supplied `app` directory while preserving folders. If necessary, create subfolders with **Create** > **Folder** and import their files separately.
4. Confirm the folder directly contains `app.yaml`, `requirements.txt`, `.streamlit/config.toml`, complete `src` (including every `__init__.py`, `data_generator`, `genie`, and `pipeline`), and `scripts`.
5. Open `<REPLACE_ME_APP_SOURCE_PATH>/scripts/generate_synthetic_data.py`. Because its first line is `# Databricks notebook source`, Databricks imports it as a multi-cell notebook. If it is absent/plain, use the `scripts` folder menu > **Import**, select that `.py`, and click **Import**.
6. Choose available serverless compute if prompted. Run the first Python cell once so its widgets appear. That exact cell is:

```python
dbutils.widgets.text("catalog", "mulegraph", "Catalog name")
dbutils.widgets.text("schema", "investigations", "Schema name")
dbutils.widgets.text("scale_factor", "3", "Number of independent cases to generate")
dbutils.widgets.text("seed", "42", "Random seed for reproducibility")
dbutils.widgets.text(
    "repo_path",
    "/Workspace/Repos/<your-username>/mulegraph-implementation",
    "Path to repo root (e.g. /Workspace/Repos/user@example.com/mulegraph-implementation)"
)
```

7. Do not edit the cell. In the top widget bar enter: Catalog `mulegraph`; Schema `investigations`; scale `3`; seed `42`; repo path `<REPLACE_ME_APP_SOURCE_PATH>`. “Repo path” is a historical label: it means the uploaded folder directly containing `src`, not Git.

> **Choosing `scale_factor` -- this affects what the Streamlit app itself shows, not just Genie.**
> The deployed app's own single-case view (case header, Blast Radius, Evidence, export) always
> reads whichever `case_summary` row loads first -- with `scale_factor 3` there are 3 cases and
> no guaranteed row order, so the app can show a *different* account and different numbers on
> every reload/redeploy. If you want the app's own UI to be predictable (e.g. to match
> `docs/CONTEST_DEMO_SCRIPT.md`, which is written against the `ACC_M_COLLECTOR` case), rerun this
> notebook with **`scale_factor 1`** instead -- that leaves exactly one case, so the ambiguity
> disappears. Use `scale_factor 3` only when your goal is a richer, multi-case **Genie Space**
> for open-ended questions and you don't need the app's own tabs to be deterministic.

### RUN

Click **Run all** and wait for `Data generation complete. Genie can now query these tables.` The notebook overwrites all eight tables on every run.

### VALIDATE

The final output must show: `accounts` 138, `evidence` 52, `network_edges` 129, `transfers` 332, `case_summary` 9, `control_cohort` 9, `freshness` 8, `export_citations` 61 (nine independently selectable scenario cases, not one). Rerun Stage 2's count query for an independent check.

**PASS:** The notebook succeeds and all eight counts match. Go to Stage 4.

#### Troubleshooting

- Cannot find `src`: copy the Workspace path of the folder directly containing `src` into the widget; remove an accidental extra `app` level.
- Placeholder `<your-username>` error: replace the widget value with `<REPLACE_ME_APP_SOURCE_PATH>`; do not edit source.
- Write denied/missing table: verify catalog/schema widgets and that Stage 2 passed on Unity Catalog-capable compute.

## Stage 4 — Create and test a Genie Space

### DO

1. Click **Genie** > **New**. In **Connect your data**, browse to `mulegraph.investigations`.
2. Select all 8 tables and all 4 views listed in Stage 2, then click **Create**. Name it `MuleGraph Investigator` if prompted.
3. In **Configure** / the data panel, confirm all 12 sources. Policy questions must use the scoped views instead of raw `evidence` or `network_edges`; the app also adds this instruction to each prompt.
4. Record `<REPLACE_ME_GENIE_SPACE_ID>` from the open space URL: copy the value after `/genie/rooms/` (or the final long URL identifier), not its display name.

### RUN

Ask: “How many cases are in case_summary, and what is the total permissive exposure across them?” Send it and inspect **Show SQL** or its query attachment.

### VALIDATE

It must query `mulegraph.investigations.case_summary`, report 9 cases, and calculate a total. Then ask: “Under the strict evidence policy, count rows using evidence_strict_v. Do not use the raw evidence table.” Confirm its SQL uses `evidence_strict_v`.

**PASS:** The space lists 12 sources and live Genie answers both questions using the expected objects. Go to Stage 5.

#### Troubleshooting

- Missing source: use **Configure data** to attach it and save.
- Empty answer: confirm `case_summary` has 9 rows in SQL Editor.
- Raw policy table used: confirm all views are attached, repeat the explicit view instruction, and save a successful question/SQL example before continuing.
- **Genie questions fail or return no answer after you change the Gold table schema (e.g. add new columns, rerun the notebook with a different scenario count):** Genie Spaces snapshot table/column metadata when a source is attached and do **not** automatically detect schema changes on an already-attached table. Reopen the Space, go to **Configure data**, and re-select/re-save `accounts` and `case_summary` (and re-verify all 4 views) so Genie's schema grounding picks up the change. Then retry the same question directly inside the Genie Space UI (not through the app) to confirm it's fixed before testing the app again. The app's own “Ask Genie” panel now shows a collapsed **Technical details** section under any failed answer with the real exception message -- expand it first to see whether the failure is Genie-side (schema/warehouse) or something else.

## Stage 5 — Create and deploy the Databricks App

### DO

1. Click **SQL Warehouses**, open the warehouse used above, then **Connection details**. Record `<REPLACE_ME_SQL_WAREHOUSE_ID>` from the details/page URL and `<REPLACE_ME_SQL_WAREHOUSE_HTTP_PATH>` from **HTTP path**. The code needs only the ID; recording the HTTP path helps verify that the same warehouse was selected.
2. Look at the left sidebar. If you see an item literally labeled **Apps**, click it directly. If you do not see it there, click the app-switcher icon (the grid/dots icon, usually top-left near the Databricks logo) and choose **Databricks Apps** from that menu. Click **Create app** > **Create a custom app**. Name it `mulegraph-investigator`, or use `<REPLACE_ME_UNIQUE_APP_NAME>` for a unique lowercase/hyphenated name.
3. In **App resources**, click **+ Add resource** > **SQL warehouse**. Select the warehouse matching the recorded ID/path, permission **Can use**, and set its custom resource key to exactly `sql_warehouse` (underscore).
4. Add **Genie Agent** (sometimes **Genie Space**). Select the space matching `<REPLACE_ME_GENIE_SPACE_ID>`, permission **Can run**, and key exactly `genie_space`.
   If neither **Genie Agent** nor **Genie Space** is offered as an App resource type, open the app's **Environment** / **Settings** UI, add a plain custom environment variable named exactly `DATABRICKS_GENIE_SPACE_ID` with `<REPLACE_ME_GENIE_SPACE_ID>` as its literal value, and skip the `valueFrom: genie_space` indirection for this variable only.
5. Add each of the 8 tables and 4 views from Stage 2 as **Unity Catalog table** resources with **Select**. Their generated keys are unused and may remain unchanged. This grants the app identity `USE CATALOG`, `USE SCHEMA`, and `SELECT`. If views are unavailable as resources, open each view under **Catalog** > **Permissions** > **Grant**, choose the service principal (the automatic technical identity Databricks creates for your app) shown on the app's **Authorization** tab, and grant **SELECT**.

The complete checked-in `app.yaml` is reproduced verbatim:

```yaml
# Databricks Apps manifest.
#
# This file lives at the app's own bundle root (the directory containing
# databricks.yml, not the monorepo root), alongside the full source tree
# (src/, requirements.txt). databricks.yml's source_code_path is
# also that bundle root, so the deployed app always has every sibling
# package the entrypoint imports available next to it -- see the note in
# databricks.yml for why that pairing matters.
command: ["streamlit", "run", "src/app/app.py", "--server.port", "8000", "--server.address", "0.0.0.0"]
env:
  - name: DATABRICKS_WAREHOUSE_ID
    valueFrom: sql_warehouse
  - name: DATABRICKS_GENIE_SPACE_ID
    valueFrom: genie_space
  - name: DATABRICKS_CATALOG
    value: mulegraph
  - name: DATABRICKS_SCHEMA
    value: investigations
```

`DATABRICKS_WAREHOUSE_ID` is required by `src/data_access.py`; `DATABRICKS_GENIE_SPACE_ID` by `src/genie/interface.py`; catalog/schema are read by `data_access.py`. There is no HTTP-path or token variable. Both modules create `WorkspaceClient()`, which uses the Databricks App's automatically injected host and OAuth credentials. Never paste a token.

6. In **Workspace**, open `<REPLACE_ME_APP_SOURCE_PATH>`. Confirm `app.yaml` matches above and sits beside `requirements.txt` and `src`. The complete `requirements.txt` is:

```text
pandas>=2.0,<3.0
streamlit>=1.32,<2.0
databricks-sdk>=0.66,<1.0
```

Import any missing files through the folder menu. `app.yaml`, `requirements.txt`, `.streamlit`, and complete `src` are essential.

### RUN

Open the app, click **Deploy**, choose **Workspace folder** / **From workspace**, select `<REPLACE_ME_APP_SOURCE_PATH>`, then **Select** > **Deploy**. Wait for **Running** / **Active**, then click **Open app**.

### VALIDATE

The app must be Running, open without a red exception, and show populated case cards/tabs. **Logs** must not contain missing-variable, `PERMISSION_DENIED`, `ModuleNotFoundError`, or empty `case_summary` errors.

**PASS:** The Running app renders persisted investigation data. Go to Stage 6.

#### Troubleshooting

- Missing variable: keys must be exactly `sql_warehouse` and `genie_space`; `app.yaml` must be at the deployed folder root. Fix and redeploy.
- Permission/auth error: confirm **Can use**, **Can run**, and **Select** on all 12 objects for the app identity; redeploy.
- Import/dependency error: deploy the folder directly containing `app.yaml`, `requirements.txt`, and complete `src`.

## Stage 6 — End-to-end live Genie validation

### DO

Open the Running app's **Ask Genie** panel, leave policy on **Strict**, and enter: “For the current seed account, summarize connected accounts and total exposure under the strict policy. Cite the Databricks sources used.”

### RUN

Click its ask/send button and wait for the result.

### VALIDATE

The answer must reference the displayed seed account; contain persisted findings; show at least one `Databricks Genie` citation/query attachment; show a freshness note derived from `freshness`; and use `evidence_strict_v` and/or `network_edges_strict_v` rather than raw policy-mixing tables. Switch to **Permissive**, repeat, and confirm the permissive views are used. Compare the strict and permissive answers and confirm that the connected-account count and/or total exposure differs: for the seed case, the stricter cohort definition should shrink the network relative to the permissive policy.

The deployed code has no silent mock/canned fallback. A real answer with Genie attachments proves this path:

```text
Delta tables/views → SQL warehouse + Genie Space → Databricks App → cited Ask Genie answer
```

**FINAL PASS:** The app shows persisted case data and Ask Genie returns a real, cited, Databricks-backed answer using the selected policy views. Deployment is complete.

#### Troubleshooting

- App data works but Genie errors: verify `genie_space`, **Can run**, and the selected space ID; redeploy.
- No query/citation: ask Stage 4's concrete aggregation question and first confirm it produces SQL directly in Genie.
- Raw tables used: stop; attach all four views and repeat Stage 4's policy smoke test until its SQL uses scoped views.

## Deliberately not required

No CLI, bundle deployment, Git, terminal, local Streamlit server, personal token, client secret, or manually created service principal is needed. The supplied `databricks.yml` is automation metadata and is not used by this UI-only path. Re-running the notebook overwrites the synthetic tables; with scale 3, Genie sees three cases while the fixed app layout opens the first persisted case.
