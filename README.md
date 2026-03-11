# TFN Data Analytics Platform

The centralised monorepo for TFN. Housing dbt data transformations, analytics services, and related infrastructure.

---

## Overview

This repository consolidates TFN's data infrastructure into a single, organized monorepo. It is actively being built out with new services and features being added regularly.

**What is TFN?** Truck Fuel Network is building infrastructure for the fuel industry. This data platform powers analytics, reporting, and decision-making across the organisation.

---

## Services

Each service has its own directory under `services/` with dedicated documentation. Start with the service README for setup and usage details.

| Service | Description | Status |
|---|---|---|
| [`services/dbt-data-transfrom/`](services/dbt-data-transfrom) | dbt data transformation pipeline — syncs source data into BigQuery, builds analytics models, and drives scheduled reporting | Coming soon |
| [`services/depot-info/`](services/depot-info) | Service to sync data from Monday.com to BigQuery | Coming soon |

---

## Architecture

```
Source Data
    ↓
dbt (transformation)
    ↓
BigQuery (data warehouse)
    ↓
Marts & Reports (dashboards, analytics, business intelligence)
```

### Tech Stack

- **dbt 1.8.0** — data transformation and testing
- **Google BigQuery** — data warehouse (EU region, GCP project: `tfn-data-warehouse`)
- **Cloud Run Jobs** — serverless execution environment (8x daily via Cloud Scheduler)
- **GitHub Actions** — CI/CD pipeline (builds Docker images, pushes to Artifact Registry)
- **Docker** — containerised dbt runs for consistent, reproducible deployments

---

## Getting Started

### Prerequisites

- **Python 3.11** (see [`.python-version`](.python-version))
- **Google Cloud SDK** — authenticated with service account credentials for `tfn-data-warehouse`
- **Docker** (optional, for running containerised builds locally)
- **dbt CLI** (installed as part of the dbt service setup)

### Setup

1. Clone this repository:
   ```bash
   git clone <repo-url>
   cd tfn-data-analytics-platform
   ```

2. Follow the service-specific setup guide:
   - For dbt: see [`services/dbt-data-transfrom/README.md`](services/dbt-data-transfrom/README.md)

3. Authenticate with GCP:
   ```bash
   gcloud auth application-default login
   ```

---

## Contributing

### Branch Conventions

- Feature branches: `feat/description`
- Bug fixes: `fix/description`
- Refactoring: `refactor/description`
- Documentation: `docs/description`

### Adding a New Service

1. Create a directory under `services/`:
   ```bash
   mkdir services/my-new-service
   ```

2. Add a dedicated `README.md` describing the service, how to run it, and any setup requirements.

3. Update this root README's [Services](#services) table with the new service.

4. Open a pull request against `main` with your changes.

### PR Process

- All PRs target `main`
- CI/CD pipeline (`.github/workflows/deploy-dbt.yml`) runs automatically on push (Currently broken. Work in progress)
- Ensure Docker builds pass and dbt models compile without errors before merging

---

## Deployment

The platform is deployed to Google Cloud Run Jobs, scheduled 8 times daily via Cloud Scheduler (SAST timezone). The deployment pipeline:

1. **GitHub Actions** detects push to `main`
2. Builds and tests the Docker image
3. Pushes to Google Artifact Registry (`europe-west1`)
4. Cloud Run Jobs pulls the image and executes `dbt build --select tag:daily_run`

See [`.github/workflows/deploy-dbt.yml`](.github/workflows/deploy-dbt.yml) for pipeline details.

---

## Questions?

- **About dbt models, data warehouse setup, or transformations?** → See [`services/dbt-data-transfrom/README.md`](services/dbt-data-transfrom/README.md)
- **About deployment or infrastructure?** → Check the Dockerfile and GitHub Actions workflow

