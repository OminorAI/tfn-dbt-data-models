#!/usr/bin/env bash
# Run dbt models against the dbt_tests dataset.
# All models land in a single BigQuery dataset (dbt_tests).
#
# Usage:
#   ./test_models.sh model_name
#   ./test_models.sh model_a model_b
#   ./test_models.sh tag:daily_run
#   ./test_models.sh                    # runs all models

set -euo pipefail

if [ $# -eq 0 ]; then
    echo "No models specified. Running all models against dbt_tests..."
    dbt run --target dev
else
    dbt run --target dev --select "$@"
fi
