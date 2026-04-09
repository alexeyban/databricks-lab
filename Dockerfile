FROM python:3.11-slim

# REQUIREMENTS_FILE — which requirements file to install.
#   requirements.txt      : all pipeline tools (default)
#   requirements-dbt.txt  : dbt-databricks only (used by dbt-gold; avoids
#                           databricks-sdk version conflict with requirements.txt)
ARG REQUIREMENTS_FILE=requirements.txt

# INSTALL_GIT — set to "true" for dbt-gold (dbt debug checks for git).
ARG INSTALL_GIT=false

WORKDIR /app

RUN if [ "$INSTALL_GIT" = "true" ]; then \
      apt-get update && apt-get install -y --no-install-recommends git \
      && rm -rf /var/lib/apt/lists/*; \
    fi

COPY requirements.txt requirements-dbt.txt ./
RUN pip install --no-cache-dir -r ${REQUIREMENTS_FILE}

# Copy all project sources — each service selects what it needs via its command.
COPY scripts/ scripts/
COPY generators/ generators/
COPY cdc_gold/ cdc_gold/
COPY pipeline_configs/datavault/dv_model.json pipeline_configs/datavault/dv_model.json
COPY docker/ docker/

# dbt profiles — harmless for non-dbt services.
COPY docker/profiles-cdc-gold.yml /root/.dbt/profiles.yml
