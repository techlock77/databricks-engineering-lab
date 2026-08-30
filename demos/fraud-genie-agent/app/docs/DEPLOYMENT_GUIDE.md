# Deployment Guide — MuleGraph Investigator on Databricks Free Edition

MuleGraph Investigator is delivered inside the `databricks-engineering-lab` monorepo. It has
two independent pieces:

1. **A custom Streamlit investigation app** (`src/app/app.py`) with a deterministic, in-memory
   demo case (seed 42). Deploy this as a Databricks App for the investigator UI.
2. **Unity Catalog setup and scaled synthetic-data scripts** (`scripts/`). Run these in a
   Databricks workspace to populate Delta tables for a native Genie Space with multiple cases.

You can use either piece independently, or both. The Streamlit app does not read the Unity
Catalog tables; see [Known limitations](#5-known-limitations).

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

## 2. Run locally

The app needs no environment variables, workspace connection, or network access at runtime.
It generates its demo data locally in memory.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
streamlit run src/app/app.py
```

Open `http://localhost:8501`. Stop the server with Ctrl+C.

## 3. Unity Catalog setup and scaled synthetic data for a Genie Space

These scripts are Databricks-native and require a live Databricks workspace. They are separate
from the self-contained Streamlit app.

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

With `scale_factor=3` or higher, the native Genie Space can query multiple distinct cases, a
control cohort, and a broad baseline pool.

## 4. Deploy the Streamlit app with Databricks Asset Bundles

Install the Databricks CLI if needed, then authenticate to the intended workspace. From
`demos/fraud-genie-agent/app`, run:

```bash
databricks auth login --host https://<your-workspace-host>.cloud.databricks.com
databricks bundle validate -t demo
databricks bundle deploy -t demo
databricks bundle run -t demo mulegraph_investigator
```

The names above match `databricks.yml`: the sole target is `demo`, and the app resource key is
`mulegraph_investigator`. The deployed app name is `mulegraph-investigator`.

The commented `workspace.host` entry is intentional. Authentication may supply the host, or you
may configure it using the CLI or `DATABRICKS_HOST` rather than committing a workspace URL.

### Why `source_code_path: .` is correct in this monorepo

The Databricks CLI treats the directory containing `databricks.yml` as the bundle root. Here that
directory is `demos/fraud-genie-agent/app`, even though it is nested inside a larger monorepo.
Consequently, the resource's `source_code_path: .` resolves to the app directory, not to the
monorepo root. Running the bundle commands from inside the app directory makes this relationship
explicit and ensures `app.yaml`, `requirements.txt`, and the complete `src/` package are deployed
together.

The `demo` target deploys under
`/Workspace/Users/<you>/.bundle/mulegraph-investigator/demo`. `app.yaml` starts Streamlit on the
Databricks Apps port and address. On each cold start, the app regenerates its seed-42 dataset in
memory.

## 5. Known limitations

- **The Streamlit app and Unity Catalog use separate data paths.** The app recomputes one demo
  case in memory and does not read the scaled tables populated by the notebook.
- **The in-app “Ask Genie” panel is not connected to a native Genie Space.** It is a deterministic,
  rule-based responder over in-memory Gold tables and recognizes approximately nine investigation
  question patterns. `src/genie/interface.py` provides a seam for a future live integration.
- **`scale_factor` affects only the data-loading notebook.** The deployed app always runs its
  single polished case, equivalent to `scale_factor=1`.
- **The generated Unity Catalog data is synthetic and overwritten on each notebook run.** The
  notebook writes every Gold table in `overwrite` mode; it is a demo loader, not an incremental
  production pipeline.
- **Workspace steps still require user configuration.** Catalog/schema permissions, a usable SQL
  compute context, Databricks authentication, and native Genie Space creation depend on the target
  workspace and are not performed by the local app.
