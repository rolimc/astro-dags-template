# dags/openfda_monthly_to_bq.py
from __future__ import annotations

from datetime import timedelta
import pandas as pd
import requests

# Airflow 3.x: use o SDK público
from airflow.sdk import dag, task, get_current_context
import pendulum  # ok usar; pode ser removido se preferir calendar.monthrange

# ========== CONFIG ==========
GCP_PROJECT = "cecr-enap"
BQ_DATASET  = "open_fda"
BQ_TABLE    = "sildenafil_weekly"
BQ_LOCATION = "US"            # combine com a região do dataset
GCP_CONN_ID = "cloud_google"  # nome da conexão no Airflow
# ===========================

def _end_of_month_day(year: int, month: int) -> int:
    return pendulum.datetime(year, month, 1, tz="UTC").end_of('month').day

def _generate_query_url(year: int, month: int) -> str:
    start_date = f"{year}{month:02d}01"
    end_date   = f"{year}{month:02d}{_end_of_month_day(year, month):02d}"
    return (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.medicinalproduct:%22sildenafil+citrate%22"
        f"+AND+receivedate:[{start_date}+TO+{end_date}]&count=receivedate"
    )

@task
def fetch_openfda_weekly() -> dict:
    """Agrega por semana os counts do mês da execução e retorna dict serializável."""
    ctx = get_current_context()
    exec_start = ctx["data_interval_start"]
    year, month = exec_start.year, exec_start.month

    url = _generate_query_url(year, month)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    payload = r.json()

    results = payload.get("results", [])
    if not results:
        return {"week_end_date": [], "count": []}

    df = pd.DataFrame(results)  # cols: time (YYYYMMDD), count
    if df.empty:
        return {"week_end_date": [], "count": []}

    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True)
    weekly = (
        df.groupby(pd.Grouper(key="time", freq="W"))["count"]
          .sum()
          .reset_index()
          .rename(columns={"time": "week_end"})
    )
    weekly["week_end_date"] = weekly["week_end"].dt.strftime("%Y-%m-%d")
    weekly = weekly[["week_end_date", "count"]]

    return weekly.to_dict(orient="list")

@task
def load_to_bigquery(data_dict: dict) -> None:
    """Carrega no BigQuery via pandas-gbq usando credenciais da conexão do Airflow."""
    if not data_dict or not data_dict.get("week_end_date"):
        print("Nada para carregar no BigQuery.")
        return

    # Import dentro do task evita quebrar o parse se provider faltar
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    df = pd.DataFrame(data_dict)
    if df.empty:
        print("Nada para carregar no BigQuery (DataFrame vazio).")
        return

    schema = [
        {"name": "week_end_date", "type": "DATE"},
        {"name": "count",         "type": "INTEGER"},
    ]

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()

    destination_table = f"{BQ_DATASET}.{BQ_TABLE}"
    df.to_gbq(
        destination_table=destination_table,
        project_id=GCP_PROJECT,
        if_exists="append",   # para idempotência total, considere MERGE depois
        credentials=credentials,
        table_schema=schema,
        location=BQ_LOCATION,
        progress_bar=False,
    )
    print(f"Carregadas {len(df)} linhas para {GCP_PROJECT}.{destination_table} ({BQ_LOCATION}).")

@dag(
    # Airflow 3.x: use 'schedule' (não 'schedule_interval')
    schedule="@monthly",
    start_date=pendulum.datetime(2020, 11, 1, tz="UTC"),
    catchup=True,
    max_active_tasks=1,
    default_args={"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["openfda", "etl", "bigquery", "pandas-gbq"],
)
def openfda_sildenafil_to_bq_monthly():
    data = fetch_openfda_weekly()
    load_to_bigquery(data)

dag = openfda_sildenafil_to_bq_monthly()


