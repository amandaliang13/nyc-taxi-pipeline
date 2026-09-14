# Lean Airflow image: slim base (no bundled providers)
#
#   celery, redis   -> CeleryExecutor + Redis broker
#   postgres        -> metadata DB driver + PostgresHook for the warehouse
#   amazon          -> S3 hooks for the raw/ landing zone
#   fab             -> required by AIRFLOW__CORE__AUTH_MANAGER (FabAuthManager)

FROM apache/airflow:slim-3.3.1-python3.14

ARG AIRFLOW_VERSION=3.3.1
ARG PYTHON_VERSION=3.14

# curl: used by the api-server / flower container healthchecks in compose.yaml
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
      "apache-airflow[celery,redis,postgres,amazon,fab]==${AIRFLOW_VERSION}" \
      pandas pyarrow \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
