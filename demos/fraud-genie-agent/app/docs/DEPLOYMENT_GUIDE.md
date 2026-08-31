# Deployment Guide — MuleGraph Investigator on Databricks Free Edition

MuleGraph Investigator is delivered inside the `databricks-engineering-lab` monorepo. It has
two ordered pieces:

1. **Unity Catalog setup and scaled synthetic-data scripts** (`scripts/`). Run these first to
   populate persisted Delta tables for the app and Genie.
2. **A custom Streamlit investigation app** (`src/app/app.py`). It reads those tables through a
   SQL warehouse and sends investigator questions to the configured native Genie Space.

The deployed flow is Databricks Data → Genie → Streamlit App. The app does not generate its core
dataset at startup.

## 1. Get the files

For a new checkout:

```bash
git clone https://github.com/techlock77/databricks-engineering-lab.git
cd databricks-engineering-lab/demos/fraud-genie-agent/app
```

If the monorepo is already cloned, update it using your normal Git workflow, then run:

```bash
cd demos/fraud-genie-agent/app
```

**Every shell command below assumes the current directory is
`demos/fraud-genie-agent/app`.**

The delivered app directory is exactly:

```text
app/
├── .streamlit/
│   └── config.toml
├── docs/
│   ├── CONTEST_DEMO_SCRIPT.md
│   └── DEPLOYMENT_GUIDE.md
├── scripts/
│   ├── generate_synthetic_data.py
│   └── sql/
│       └── 01_setup_catalog_and_schema.sql
├── src/
│   ├── __init__.py
│   ├── app/
│   │   ├── __init__.py
│   │   └── app.py
│   ├── data_generator/
│   │   ├── __init__.py
│   │   └── generator.py
│   ├── genie/
│   │   ├── __init__.py
│   │   ├── export.py
│   │   ├── interface.py
│   │   └── responder.py
│   └── pipeline/
│       ├── __init__.py
│       ├── detection.py
│       ├── gold.py
│       ├── network.py
│       ├── orchestrator.py
│       ├── policy.py
│       ├── silver.py
│       └── views.py
├── app.yaml
├── databricks.yml
└── requirements.txt
```

## 2. Run locally against Databricks

Local execution requires Databricks authentication plus an existing SQL warehouse, populated
Gold tables, and Genie Space. Configure:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
export DATABRICKS_WAREHOUSE_ID=<warehouse-id>
export DATABRICKS_GENIE_SPACE_ID=<genie-space-id>
# Optional when using the defaults shown below:
export DATABRICKS_CATALOG=mulegraph
export DATABRICKS_SCHEMA=investigations
streamlit run src/app/app.py
```

Open `http://localhost:8501`. Stop the server with Ctrl+C.

## 3. Unity Catalog setup and scaled synthetic data for a Genie Space

These scripts are Databricks-native and require a live Databricks workspace. They populate the
persisted data that both the Streamlit app and Genie require.

### 3.1 Put the monorepo in your workspace

Connect the `databricks-engineering-lab` repository using Databricks Git folders (formerly
Repos), or upload the repository through the workspace file browser. Note the app directory's
workspace path. A typical Git-folder path is:

```text
/Workspace/Repos/<you>/databricks-engineering-lab/demos/fraud-genie-agent/app
```

The exact prefix may differ with your workspace's Git-folder layout. What matters is that the
chosen directory directly contains `src/`, `scripts/`, `app.yaml`, and `databricks.yml`.

### 3.2 Create the catalog, schema, and Gold tables

Open `scripts/sql/01_setup_catalog_and_schema.sql` in a Databricks SQL editor, or paste it into
a notebook `%sql` cell. Run it as-is, or change the catalog and schema names at the top first.
The defaults are `mulegraph` and `investigations`.

The SQL creates these eight Gold-layer Delta tables: `accounts`, `evidence`, `network_edges`,
`transfers`, `case_summary`, `control_cohort`, `freshness`, and `export_citations`.

### 3.3 Generate and load the scaled dataset

Open `scripts/generate_synthetic_data.py` as a Databricks notebook. Its Databricks notebook
header allows the workspace importer to render it as a notebook. Configure the widgets:

| Widget | Default in the notebook | Value to use |
|---|---|---|
| `catalog` | `mulegraph` | The catalog created in 3.2 |
| `schema` | `investigations` | The schema created in 3.2 |
| `scale_factor` | `3` | Number of independent mule-network and control-cohort cases |
| `seed` | `42` | Keep for reproducibility, or change for different synthetic data |
| `repo_path` | Old standalone-repo placeholder | The **app directory** from 3.1, for example `/Workspace/Repos/<you>/databricks-engineering-lab/demos/fraud-genie-agent/app` |

The checked-in notebook retains an old placeholder string in the `repo_path` widget. Replace it
before running if the first import attempt does not already succeed. The notebook specifically
expects `repo_path` to be a directory containing `src/pipeline/orchestrator.py`; therefore the
nested app path above—not the monorepo root—is the correct value. If the app directory is already
on Python's import path, the notebook imports directly and does not use the fallback widget.

Run all cells. The final cell prints a row count for every Gold table, confirming the load.

### 3.4 Create a native Genie Space

1. In the Databricks workspace, open **Genie** and choose **New Genie Space**.
2. Select the catalog and schema from 3.2 (`mulegraph.investigations` by default).
3. Add all eight Gold tables as data sources.
4. Optionally reuse the table and column comments in
   `scripts/sql/01_setup_catalog_and_schema.sql` as Genie instructions/context.
5. Copy the Genie Space ID from its workspace URL or settings. You will supply it at deployment.

With `scale_factor=3` or higher, the native Genie Space can query multiple distinct cases, a
control cohort, and a broad baseline pool.

## 4. Deploy the Streamlit app with Databricks Asset Bundles

Install the Databricks CLI if needed, then authenticate to the intended workspace. From
`demos/fraud-genie-agent/app`, supply the IDs of the existing warehouse and the Genie Space from
3.4, then run:

```bash
databricks auth login --host https://<your-workspace-host>.cloud.databricks.com
databricks bundle validate -t demo \
  --var warehouse_id=<warehouse-id> \
  --var genie_space_id=<genie-space-id>
databricks bundle deploy -t demo \
  --var warehouse_id=<warehouse-id> \
  --var genie_space_id=<genie-space-id>
databricks bundle run -t demo mulegraph_investigator
```

The names above match `databricks.yml`: the sole target is `demo`, and the app resource key is
`mulegraph_investigator`. The deployed app name is `mulegraph-investigator`.

The commented `workspace.host` entry is intentional. Authentication may supply the host, or you
may configure it using the CLI or `DATABRICKS_HOST` rather than committing a workspace URL.
The bundle attaches both existing resources to the app with least-privilege access. `app.yaml`
maps the `sql_warehouse` and `genie_space` resource keys to `DATABRICKS_WAREHOUSE_ID` and
`DATABRICKS_GENIE_SPACE_ID`; the App service principal authenticates `WorkspaceClient()`
automatically. Ensure that principal also has `USE CATALOG`, `USE SCHEMA`, and `SELECT` on the
eight Gold tables. For non-bundle deployments, attach both resources in the Apps UI or set the
two environment variables explicitly.

### Why `source_code_path: .` is correct in this monorepo

The Databricks CLI treats the directory containing `databricks.yml` as the bundle root. Here that
directory is `demos/fraud-genie-agent/app`, even though it is nested inside a larger monorepo.
Consequently, the resource's `source_code_path: .` resolves to the app directory, not to the
monorepo root. Running the bundle commands from inside the app directory makes this relationship
explicit and ensures `app.yaml`, `requirements.txt`, and the complete `src/` package are deployed
together.

The `demo` target deploys under
`/Workspace/Users/<you>/.bundle/mulegraph-investigator/demo`. `app.yaml` starts Streamlit on the
Databricks Apps port and address. On each cold start, the app fetches the already-populated Gold
tables through the attached SQL warehouse; it never runs the synthetic generator or pipeline.

## 5. Known limitations

- **The app requires live workspace resources.** Local and deployed runs need access to the SQL
  warehouse, the eight persisted tables, and the configured Genie Space; there is no silent local
  data or rule-responder fallback.
- **The current UI opens the first case in `case_summary`.** `scale_factor` controls how many cases
  the offline notebook persists and Genie can query, while the fixed investigation layout selects
  the first persisted seed account for its cards and tabs.
- **The generated Unity Catalog data is synthetic and overwritten on each notebook run.** The
  notebook writes every Gold table in `overwrite` mode; it is a demo loader, not an incremental
  production pipeline.
- **Workspace steps still require user configuration.** Catalog/schema permissions, a usable SQL
  compute context, Databricks authentication, and native Genie Space creation depend on the target
  workspace and are not performed by the local app.
