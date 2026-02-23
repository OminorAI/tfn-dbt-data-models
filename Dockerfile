FROM python:3.11-slim

WORKDIR /app

# Install dbt-bigquery (pinned to match local dev version)
RUN pip install --no-cache-dir dbt-bigquery==1.8.0

# Copy the dbt project into the image
COPY . .

# Tell dbt to look for profiles.yml inside /app (not ~/.dbt/)
ENV DBT_PROFILES_DIR=/app

# Install dbt packages (dbt_utils etc.) at build time so they're baked in
RUN dbt deps

# Runs all models tagged daily_run. SA JSON is mounted at runtime via Secret Manager.
CMD ["dbt", "build", "--target", "prod", "--select", "tag:daily_run"]
