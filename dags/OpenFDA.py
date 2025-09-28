# dags/openfda_monthly_to_bq.py
from __future__ import annotations

from datetime import timedelta
import pandas as pd
import requests

# Airflow 3.x (SDK público)
from airflow.sdk import dag, task, get_current_context
import pendulum  # pode trocar por calendar.monthrange se quiser zerar esta dependência

# ========== CONFIG ==========
GCP_PROJECT = "cecr-enap"
BQ_DATASET  = "open_fda"
BQ_TABLE    = "sildenafil_weekly"
BQ_LOCATION = "US"            # combine com a região do dataset
GCP_CONN_ID = "cloud_google"  # nome da conexão no Airflow
# ===========================


def _end_of_month_day(year: int, month: int) -> int:
    return pendulum.datetime(year, month, 1, tz="UTC").end_of("month").day


def _generate_query_url(year: int, month: int) -> str:
    start_date = f"{year}{month:02d}01"
    end_date = f"{year}{month:02d}{_end_of_month_day(year, month):02d}"
    return (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.medicinalproduct:%22sildenafil+citrate%22"
        f"+AND+receivedate:[{start_date}+TO+{end_date}]&count=receivedate"
    )


@task
def fetch_openfda_weekly() -> dict:
    """Agrega por semana os counts do mês da execução e retorna dict serializável."""
    ctx = get_current_context()
    exec_start = ctx["data_interval_start"]  # início do mês agendado
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
    """
    Carrega no BigQuery usando o cliente nativo (sem airflow.providers.google).
    Lê credenciais do BaseHook (keyfile_dict / key_path) ou usa ADC se não houver.
    """
    if not data_dict or not data_dict.get("week_end_date"):
        print("Nada para carregar no BigQuery.")
        return

    # Imports dentro do task para não quebrar o parse se a lib não estiver instalada
    import json
    from airflow.hooks.base import BaseHook
    from google.cloud import bigquery
    from google.oauth2 import service_account

    df = pd.DataFrame(data_dict)
    if df.empty:
        print("Nada para carregar no BigQuery (DataFrame vazio).")
        return

    # ==== credenciais ====
    credentials = None
    conn = BaseHook.get_connection(GCP_CONN_ID)
    extra = conn.extra_dejson or {}

    # Prioridade: keyfile_dict -> key_path -> ADC
    if "keyfile_dict" in extra and extra["keyfile_dict"]:
        keyfile_dict = extra["keyfile_dict"]
        # pode vir como string JSON ou dict
        if isinstance(keyfile_dict, str):
            keyfile_dict = json.loads(keyfile_dict)
        credentials = service_account.Credentials.from_service_account_info(keyfile_dict)
    elif "key_path" in extra and extra["key_path"]:
        with open(extra["key_path"], "r") as f:
            credentials = service_account.Credentials.from_service_account_info(json.load(f))
    else:
        # ADC (GOOGLE_APPLICATION_CREDENTIALS ou workload identity)
        credentials = None

    # ==== cliente e carga ====
    client = bigquery.Client(project=GCP_PROJECT, credentials=credentials, location=BQ_LOCATION)
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("week_end_date", "DATE"),
            bigquery.SchemaField("count", "INTEGER"),
        ],
    )

    print(f"[BQ LOAD] table={table_id} location={BQ_LOCATION} rows={len(df)}")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # espera terminar
    print(f"[BQ LOAD OK] {len(df)} linhas inseridas em {table_id}.")


@dag(
    schedule="@monthly",  # Airflow 3.x: use 'schedule'
    start_date=pendulum.datetime(2020, 11, 1, tz="UTC"),
    catchup=True,
    max_active_tasks=1,
    default_args={"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["openfda", "etl", "bigquery", "no-provider"],
)
def openfda_sildenafil_to_bq_monthly():
    data = fetch_openfda_weekly()
    load_to_bigquery(data)


dag = openfda_sildenafil_to_bq_monthly()
