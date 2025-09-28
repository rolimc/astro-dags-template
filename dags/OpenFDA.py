# dags/openfda_monthly_to_bq.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from datetime import datetime, timedelta
import pandas as pd
import requests

# ========== CONFIG ==========
GCP_PROJECT = "meu-projeto-gcp"        # ex.: "hardy-messenger-229417"
BQ_DATASET  = "openfda"                # ex.: "crypto" / "openfda"
BQ_TABLE    = "sildenafil_weekly"      # ex.: "sildenafil_weekly"
BQ_LOCATION = "US"                     # "US" ou "EU" — deve bater com o dataset
GCP_CONN_ID = "google_cloud_default"   # conexão do Airflow com SA que escreve no BQ
# ===========================

def _generate_query_url(year: int, month: int) -> str:
    start_date = f"{year}{month:02d}01"
    # último dia do mês:
    last_day = (datetime(year, month, 1) + timedelta(days=31)).replace(day=1) - timedelta(days=1)
    end_date = f"{year}{month:02d}{last_day:%d}"
    return (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.medicinalproduct:%22sildenafil+citrate%22"
        f"+AND+receivedate:[{start_date}+TO+{end_date}]&count=receivedate"
    )

@task
def fetch_openfda_weekly():
    """
    Busca os dados do mês da execução e agrega por semana (domingo como week-end).
    Retorna um dict serializável (para XCom) com colunas: week_end_date, count.
    """
    ctx = get_current_context()
    exec_start = ctx["data_interval_start"]  # início do mês agendado (com catchup)
    year, month = exec_start.year, exec_start.month

    url = _generate_query_url(year, month)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    payload = r.json()

    results = payload.get("results", [])
    if not results:
        return {"week_end_date": [], "count": []}

    df = pd.DataFrame(results)  # columns: time (string YYYYMMDD), count (int)
    # Converter 'time' (YYYYMMDD) para datetime e agrupar por semana (pandas 'W' = domingo como fim da semana)
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True)

    weekly = (
        df.groupby(pd.Grouper(key="time", freq="W"))["count"]
          .sum()
          .reset_index()
          .rename(columns={"time": "week_end"})
    )
    # Guardar como DATE (sem timezone)
    weekly["week_end_date"] = weekly["week_end"].dt.date.astype(str)
    weekly = weekly[["week_end_date", "count"]]

    return weekly.to_dict(orient="list")

@task
def load_to_bigquery(data_dict: dict):
    """
    Carrega no BigQuery com pandas-gbq usando credenciais do BigQueryHook.
    """
    if not data_dict or not data_dict.get("week_end_date"):
        print("Nada para carregar no BigQuery.")
        return

    df = pd.DataFrame(data_dict)
    # Schema explícito ajuda na criação inicial
    schema = [
        {"name": "week_end_date", "type": "DATE"},
        {"name": "count",         "type": "INTEGER"},
    ]

    # Pega credenciais da conexão do Airflow
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()

    destination_table = f"{BQ_DATASET}.{BQ_TABLE}"
    # Escreve (append). Se não existir, cria com schema e depois faz append
    df.to_gbq(
        destination_table=destination_table,
        project_id=GCP_PROJECT,
        if_exists="append",        # veja a seção Idempotência abaixo
        credentials=credentials,
        table_schema=schema,
        location=BQ_LOCATION,
        progress_bar=False,
    )
    print(f"Carregadas {len(df)} linhas para {GCP_PROJECT}.{destination_table} ({BQ_LOCATION}).")

@dag(
    schedule="@monthly",
    start_date=datetime(2020, 11, 1),
    catchup=True,
    max_active_tasks=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["openfda", "etl", "bigquery", "pandas-gbq"],
)
def openfda_sildenafil_to_bq_monthly():
    data = fetch_openfda_weekly()
    load_to_bigquery(data)

dag = openfda_sildenafil_to_bq_monthly()
