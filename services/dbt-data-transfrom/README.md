# TFN DBT Project

Data transformation and modeling for The Fuel Network (TFN) operations. This dbt project creates views and tables in BigQuery that power supplier management dashboards and operational analytics.

## Tech Stack

- **dbt**: 1.8.0 with dbt-bigquery adapter
- **BigQuery**: Data warehouse (EU region)
- **Cloud Run Jobs**: Container-based scheduled execution
- **Cloud Scheduler**: Cron-based job triggering
- **GitHub Actions**: CI/CD pipeline with Workload Identity Federation
- **Docker**: Containerized dbt runs
- **GCP Secret Manager**: Service account key storage

## Project Structure

```
tfn_dbt/
├── models/
│   ├── tfn_data/              # Views (dimension tables, live data)
│   ├── tfn_data_historical/   # Incremental tables (historical aggregates)
│   └── marts/
│       ├── tfn_reports/       # Business dashboards & reports
│       └── tfn_custom_ops/    # Custom operational models
├── Dockerfile                  # Container image definition
├── profiles.yml                # dbt profile with BigQuery config
├── dbt_project.yml             # Project settings & model configurations
├── .dockerignore               # Docker build exclusions
└── .github/
    └── workflows/
        └── deploy-dbt.yml      # GitHub Actions CI/CD pipeline
```

## Current Deployed Models

All models tagged with `+tags: ['daily_run']` execute on schedule:

| Model | Dataset | Type | Schedule |
|-------|---------|------|----------|
| `fuel_transactions_all_time` | tfn_reports | Incremental | 8x daily (SAST) |
| `data_tool` | tfn_data_historical | Incremental | 8x daily (SAST) |
| `tfn_live_vehicles` | tfn_data | View | 8x daily (SAST) |
| `tfn_orders_with_drivers` | tfn_custom_ops | Incremental | 8x daily (SAST) |

**Execution Times** (Africa/Johannesburg timezone):
- 12:00 AM, 7:00 AM, 9:00 AM, 11:00 AM
- 1:00 PM, 3:00 PM, 5:00 PM, 7:00 PM

## Data Sources

Raw data is stored in BigQuery:
- `tfn_data_historical.fuel_transactions` — Raw transaction data (partitioned by CapturedDate)
- `tfn_data_historical.customer` — Customer master data
- `tfn_data.supply_site` — Supply site master data
- Additional operational tables as needed

## Local Development Setup

### Prerequisites
- Python 3.11+
- dbt-bigquery 1.8.0
- Google Cloud credentials (ADC)

### Installation

```bash
# Clone the repository
git clone https://github.com/OminorAI/tfn-data-models.git
cd tfn-data-models

# Install dbt
pip install dbt-bigquery==1.8.0

# Install dbt dependencies
dbt deps
```

### Authentication (Local Development)

Use Application Default Credentials (ADC):

```bash
# Authenticate with Google Cloud
gcloud auth application-default login

# Verify connection
dbt debug
```

Your local `profiles.yml` will read from the `default` profile in your `~/.dbt/profiles.yml` or use environment variables.

### Common Commands

```bash
# Parse all models
dbt parse

# List models tagged for daily_run
dbt ls --select tag:daily_run

# Run a specific model
dbt run --select fuel_transactions_all_time

# Test data quality
dbt test

# Generate documentation
dbt docs generate

# Dry-run to see what would execute
dbt build --select tag:daily_run --dry-run
```

## Production Deployment

### Architecture Overview

```
GitHub push to main
    ↓
GitHub Actions (Workload Identity Federation)
    ↓
Build & push Docker image → Artifact Registry
    ↓
Cloud Scheduler (cron: 8x daily)
    ↓
Cloud Run Job (tfn-dbt-runner)
    ├── Service Account: dbt-runner@tfn-data-warehouse.iam.gserviceaccount.com
    ├── Environment: DBT_BQ_PROJECT, DBT_BQ_DATASET
    └── Secret: dbt-sa-key (mounted at /app/service-account.json)
    ↓
dbt build --select tag:daily_run
    ↓
BigQuery (models write to their configured +schema datasets)
```

### Docker Build & Push

The Docker image is automatically built and pushed to Artifact Registry when code is pushed to `main` branch.

**Manual build** (testing):
```bash
docker build -t tfn-dbt:latest .
```

**Manual push** (if needed):
```bash
docker tag tfn-dbt:latest europe-west1-docker.pkg.dev/tfn-data-warehouse/dbt-images/tfn-dbt:latest
docker push europe-west1-docker.pkg.dev/tfn-data-warehouse/dbt-images/tfn-dbt:latest
```

### Cloud Run Job Execution

**Manual execution** (test):
```bash
gcloud run jobs execute tfn-dbt-runner \
  --region europe-west1 \
  --project tfn-data-warehouse
```

**View execution logs**:
```bash
gcloud run jobs describe tfn-dbt-runner \
  --region europe-west1 \
  --project tfn-data-warehouse
```

## Adding New Models

To add a new model to the scheduled daily run:

1. **Create model SQL file**:
   ```bash
   touch models/marts/tfn_reports/my_new_model.sql
   ```

2. **Add tag in `dbt_project.yml`**:
   ```yaml
   models:
     marts:
       tfn_reports:
         my_new_model:
           +tags: ['daily_run']
   ```

   Or add to the model's `config()` block:
   ```sql
   {{ config(
       tags=['daily_run'],
       schema='tfn_reports',
       materialized='table'
   ) }}
   ```

3. **Push to main branch**:
   ```bash
   git add .
   git commit -m "feat: Add my_new_model"
   git push origin main
   ```

4. **Verify**:
   - GitHub Actions pipeline runs automatically
   - Docker image is rebuilt and pushed
   - Model will execute on next scheduled run (no GCP changes needed)

## Configuration Reference

### Environment Variables (Production)

Set in Cloud Run Job via `--set-env-vars`:

| Variable | Purpose | Example |
|----------|---------|---------|
| `DBT_BQ_PROJECT` | GCP project for execution | `tfn-data-warehouse` |
| `DBT_BQ_DATASET` | Default dataset (overridden by `+schema`) | `analytics` |

### Profiles.yml Structure

- **Type**: BigQuery
- **Method**: Service account (JSON keyfile)
- **Keyfile**: Mounted from GCP Secret Manager at runtime
- **Region**: EU (london)
- **Priority**: Batch (cost-optimized)
- **Threads**: 4
- **Timeout**: 300 seconds

### Model Configuration

Each model uses `+schema` to write to its specific dataset:

```yaml
models:
  tfn_data:
    +schema: tfn_data      # All models in this folder write to tfn_data dataset
  tfn_data_historical:
    +schema: tfn_data_historical
    +materialized: incremental
  marts:
    tfn_reports:
      +schema: tfn_reports
      +materialized: incremental
```

**Key Config Options**:
- `+schema`: BigQuery dataset name
- `+materialized`: `view`, `table`, or `incremental`
- `+tags`: For model selection (e.g., `['daily_run']`)
- `+partition_by`: For BigQuery partitioning (incremental/table only)
- `+cluster_by`: For BigQuery clustering

## Monitoring & Troubleshooting

### Check Execution Status

```bash
# List recent executions
gcloud run jobs describe tfn-dbt-runner \
  --region europe-west1 \
  --project tfn-data-warehouse

# View detailed logs
gcloud run jobs executions describe [EXECUTION_ID] \
  --job tfn-dbt-runner \
  --region europe-west1 \
  --project tfn-data-warehouse
```

### Common Issues

**Model not running**:
- Verify `+tags: ['daily_run']` is set in `dbt_project.yml`
- Check GitHub Actions workflow succeeded (push to main)
- Confirm Docker image was pushed to Artifact Registry

**BigQuery permission errors**:
- Service account `dbt-runner@tfn-data-warehouse.iam.gserviceaccount.com` needs:
  - `roles/bigquery.dataEditor`
  - `roles/bigquery.jobUser`

**dbt parse errors**:
- Run locally: `dbt parse --select tag:daily_run`
- Check for syntax errors in model SQL or YAML
- Verify source table references exist in BigQuery

**Docker build failures**:
- Check GitHub Actions logs
- Manually test: `docker build .`
- Verify `dbt deps` completes successfully

### Viewing dbt Docs

Generate and view documentation locally:

```bash
dbt docs generate
dbt docs serve
# Opens at http://localhost:8000
```

## Development Workflow

### Before Committing

```bash
# Parse and validate all models
dbt parse

# Test specific tag
dbt ls --select tag:daily_run

# Run tests
dbt test

# Dry-run to verify logic
dbt build --select tag:daily_run --dry-run
```

### Commit & Push

```bash
git checkout -b feat/my-feature
# ... make changes ...
git add .
git commit -m "feat: description of changes"
git push origin feat/my-feature
# Create Pull Request on GitHub
```

### After Merge to Main

- GitHub Actions automatically builds and pushes Docker image
- Models execute on next scheduled time
- Monitor execution in Cloud Console

## Deployment Plan Reference

For complete GCP setup instructions (service accounts, secrets, Cloud Run, etc.), see the deployment plan documentation or contact your infrastructure team.

## Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [BigQuery dbt Adapter](https://docs.getdbt.com/docs/core/adapters/bigquery)
- [Cloud Run Documentation](https://cloud.google.com/run/docs)
- [Cloud Scheduler Documentation](https://cloud.google.com/scheduler/docs)
