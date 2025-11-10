## Travel Hacking LATAM – Databricks Free Edition

Hackathon project showcasing how to use Databricks Free Edition to build a medallion architecture with Lakeflow, MLflow, agents, and dashboards around a travel-hacking scenario in Latin America. All datasets are synthetic and generated inside the workspace to avoid external dependencies while mimicking real fluctuations in flight, hotel, and event pricing.

### Key components

- **Lakeflow Pipelines**
  - Bronze: generate and land flight/hotel/event quotes with enforced schema and de-duplication.
  - Silver: entity-relationship modeling (route, time, carrier, property dimensions + normalized facts).
  - Gold: volatility KPIs, opportunity indices, and ML outputs ready for BI and agents.
- **ML & AI**
  - Training notebook with MLflow to build the `OpportunityScorer`.
  - Registered model, scoring table, and batch-scoring notebook.
  - Lakehouse AI agent wired to Gold tables and documented in Unity Catalog.
- **Dashboards & Genie**
  - AI/BI dashboard with KPIs, geo visuals, and agent-generated narratives.
  - Comprehensive metadata (descriptions, owners, tags) so Genie can answer questions accurately.
- **Developer Experience**
  - Databricks Bundle (`databricks.yml`) orchestrating pipelines, notebooks, and configuration.
  - Repository ready for Databricks Repos or Git + `databricks bundle deploy`.
  - Optional orchestration Job (`travel-hacking-medallion-run`) that triggers Bronze → Silver → Gold sequentially using pipeline tasks.
  - GitHub Actions workflow (`.github/workflows/bundle-ci.yml`) validates the bundle, deploys on pushes to `main`, and runs the medallion job.
  - Dockerfile with `uv` + Databricks CLI for repeatable local tooling (`docker build -t travel-hacking .`).

### Prerequisites

1. Databricks Free Edition workspace (serverless).
2. Create a local `.env` file (keep it out of version control) with:
   ```bash
   DATABRICKS_HOST="https://<your-workspace>.databricks.com"
   BUNDLE_VAR_workspace_root_path="/Users/<user>/travel-hacking-latam"
   ```
   Load it before running bundle commands: `source .env`.
3. Install [Databricks CLI v0.215+](https://docs.databricks.com/en/dev-tools/cli/index.html) and authenticate:
   ```bash
   databricks configure --host "$DATABRICKS_HOST" --token "$DATABRICKS_TOKEN"
   ```
4. Enable bundles experimental mode if required:
   ```bash
   databricks bundle enable
   ```
5. (Optional) Install [`just`](https://github.com/casey/just) for task automation.
6. (Optional) Install Terraform ≥ 1.6 to demonstrate IaC alongside Bundles.
7. Capture the ID of the reusable serverless cluster (SQL warehouse or all-purpose) and pass it as `var.serverless_cluster_id` when deploying the bundle.

### Recommended flow

1. `databricks bundle validate` to check syntax.
2. `databricks bundle deploy --target dev` to provision catalog, schemas, pipelines, and notebooks.
   - Provide your cluster or warehouse ID via `--var serverless_cluster_id=<cluster-id>` or set it in `databricks.yml` before deployment.
3. Run Bronze, Silver, and Gold pipelines from the Lakeflow UI or CLI.
4. Execute the ML notebooks manually (training and batch scoring) from the workspace until Free Edition supports Jobs.
5. Create the agent and the dashboard (CLI, scripts, or UI).
6. Record the demo highlighting problem statement, architecture, and outcomes.
7. (Optional) Build the Docker image (`docker build -t travel-hacking .`) and run scripts with consistent tooling (CLI + uv-managed deps).

### MLflow usage

- Training runs log to the experiment at `/Shared/travel_hacking/experiments/opportunity_scorer`; open MLflow Experiments in the workspace UI to review metrics (`roc_auc`, `avg_precision`) and artifacts.
- The registered model `travel_hacking_opportunity_scorer` captures each version; promote the best run to `Staging` before batch scoring.
- Batch scoring writes predictions to `workspace.gold_travel.gold_predictions`, including `model_version`, `scored_at`, and uplift metrics for auditing.
- Capture run insights (metrics screenshots, confusion matrices, model summary) for the final presentation deck or demo.

### Metadata & Genie

- Execute `notebooks/docs/01_apply_metadata.py` after running the pipelines to register table/column comments and properties such as `quality_tier` and `dataset_purpose`.
- Rich metadata powers Unity Catalog and improves Genie / Lakehouse AI answers.
- Detailed reference lives in `docs/data_dictionary.md`.

### Terraform (optional)

- The `terraform/` folder provisions schemas, Lakeflow pipelines, and (when supported) ML jobs using the official provider.
- Copy `terraform/terraform.tfvars.example` to `terraform/terraform.tfvars` and fill in:
  ```hcl
  databricks_host  = "https://<your-workspace>.databricks.com"
  databricks_token = "dapi..."
  repo_path        = "/Repos/<user>/databricks-free-edition-hackaton"
  ```
- Sample workflow with Just:
  ```bash
  just terraform-init
  just terraform-plan
  just terraform-apply
  ```
- Terraform cannot create the Free Edition workspace or dedicated clusters, but it can manage objects within the serverless workspace.

### Repository structure

```
databricks-free-edition-hackaton/
├── databricks.yml                # Main bundle definition
├── README.md                     # This guide
├── notebooks/
│   ├── bronze/
│   │   └── 01_generate_bronze_data.py
│   ├── silver/
│   │   └── 01_build_silver_model.py
│   ├── gold/
│   │   └── 01_publish_gold_kpis.py
│   └── ml/
│       ├── 01_train_opportunity_model.py
│       └── 02_batch_scoring.py
└── docs/
    └── data_dictionary.md
```

> **Note:** Some resources (agents, dashboards) still require manual setup in Free Edition. This repo includes configuration snippets and guidance for manual creation if needed.
