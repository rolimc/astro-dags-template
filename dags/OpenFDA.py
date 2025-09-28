# dags/openfda_monthly.py
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from datetime import datetime, timedelta
import pandas as pd
import requests

def generate_query_url(year, month):
    start_date = f"{year}{month:02d}01"
    last_day = (datetime(year, month, 1) + timedelta(days=31)).replace(day=1) - timedelta(days=1)
    end_date = f"{year}{month:02d}{last_day:%d}"
    return (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.medicinalproduct:%22sildenafil+citrate%22"
        f"+AND+receivedate:[{start_date}+TO+{end_date}]&count=receivedate"
    )

def fetch_openfda_data():
    ctx = get_current_context()
    exec_date = ctx["data_interval_start"]
    year, month = exec_date.year, exec_date.month

    r = requests.get(generate_query_url(year, month), timeout=30)
    weekly_sum = pd.DataFrame([])
    if r.ok:
        data = r.json()
        df = pd.DataFrame(data.get("results", []))
        if not df.empty:
            df["time"] = pd.to_datetime(df["time"])
            weekly_sum = df.groupby(pd.Grouper(key="time", freq="W"))["count"].sum().reset_index()
            weekly_sum["time"] = weekly_sum["time"].astype(str)

    ctx["ti"].xcom_push(key="openfda_data", value=weekly_sum.to_dict())

def save_to_postgresql():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    ctx = get_current_context()
    data_dict = ctx["ti"].xcom_pull(task_ids="fetch_openfda_data", key="openfda_data")
    if not data_dict:
        return
    df = pd.DataFrame.from_dict(data_dict)
    if df.empty:
        return
    pg_hook = PostgresHook(postgres_conn_id="postgres")
    engine = pg_hook.get_sqlalchemy_engine()
    df.to_sql("openfda_data", con=engine, if_exists="append", index=False)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="fetch_openfda_data_monthly",
    description="Retrieve OpenFDA data monthly",
    schedule="@monthly",  # em vez de schedule_interval
    start_date=datetime(2020, 11, 1),
    catchup=True,
    max_active_tasks=1,
    default_args=default_args,
    tags=["openfda", "etl", "postgres"],
)

fetch_data_task = PythonOperator(task_id="fetch_openfda_data", python_callable=fetch_openfda_data, dag=dag)
save_data_task  = PythonOperator(task_id="save_to_postgresql",  python_callable=save_to_postgresql,  dag=dag)

fetch_data_task >> save_data_task





