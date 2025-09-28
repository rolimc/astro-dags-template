# dags/openfda_alfa_drotrecogina_bq_conn.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List

import httpx
import pandas as pd
import pendulum
from urllib.parse import quote

from airflow.sdk import dag, task, get_current_context

# --- Configuração BigQuery (padrões conforme informado) ---
import os
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "cecr-enap")
BQ_DATASET = os.getenv("BQ_DATASET", "open_fda")
BQ_TABLE = os.getenv("BQ_TABLE", "alfa_drotrecogina_daily")  # particionada por coluna DATE "date"
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")
GCP_CONN_ID = os.getenv("GCP_CONN_ID", "cloud_google")

# --- Constantes da coleta ---
OPENFDA_BASE = "https://api.fda.gov/drug/event.json"
PRODUCT = "Alfa Drotrecogina"

# --- XCom-friendly wrapper (para debug/inspeção) ---
@dataclass
class PandasDataFrameXComValue:
    df: pd.DataFrame
    __version__ = 1

    def serialize(self) -> Dict[str, Any]:
        return {
            "version": self.__version__,
            "payload": json.loads(self.df.to_json(orient="records", date_format="iso")),
        }

    @classmethod
    def deserialize(cls, data: Dict[str, Any], version: int) -> "PandasDataFrameXComValue":
        df = pd.DataFrame(data)
        return cls(df=df)


@dag(
    dag_id="openfda_alfa_drotrecogina_to_bigquery_conn",
    start_date=pendulum.datetime(2020, 2, 1, tz="UTC"),  # 1ª execução cobre jan/2020
    schedule="@monthly",  # roda todo dia 1, cobrindo mês anterior
    catchup=True,
    tags=["openfda", "bigquery", "monthly"],
    default_args={"retries": 2},
)
def openfda_to_bq():
    @task
    def build_query_url() -> Dict[str, str]:
        """Monta URL da API e retorna também datas início/fim do mês (YYYY-MM-DD)."""
        ctx = get_current_context()
        start = ctx["data_interval_start"]  # primeiro dia do mês anterior
        end_exclusive = ctx["data_interval_end"]  # primeiro dia do mês atual (exclusivo)
        end_inclusive = end_exclusive.subtract(days=1)

        ymd = "%Y%m%d"
        ymd_dash = "%Y-%m-%d"
        start_str = start.strftime(ymd)
        end_str = end_inclusive.strftime(ymd)
        start_dash = start.strftime(ymd_dash)
        end_dash = end_inclusive.strftime(ymd_dash)

        product_q = quote(PRODUCT)
        search = (
            f'patient.drug.medicinalproduct:"{product_q}"+AND+'
            f"receivedate:[{start_str}+TO+{end_str}]"
        )
        url = f"{OPENFDA_BASE}?search={search}&count=receivedate"
        return {"url": url, "start_date": start_dash, "end_date": end_dash}

    @task
    def fetch_openfda(url_and_dates: Dict[str, str]) -> Dict[str, Any]:
        url = url_and_dates["url"]
        timeout = httpx.Timeout(30.0)
        for attempt in range(3):
            try:
                r = httpx.get(url, timeout=timeout)
                r.raise_for_status()
                data = r.json()
                data["_window"] = {
                    "start_date": url_and_dates["start_date"],
                    "end_date": url_and_dates["end_date"],
                }
                return data
            except Exception:
                if attempt == 2:
                    raise
        return {"results": [], "_window": url_and_dates}

    @task
    def to_pandas(payload: Dict[str, Any]) -> PandasDataFrameXComValue:
        """Converte resposta em DataFrame com schema amigável ao BigQuery."""
        results: List[Dict[str, Any]] = payload.get("results", []) or []
        if not results:
            df = pd.DataFrame(columns=["date", "count", "product"])  # date=DATE
        else:
            df = pd.DataFrame(results).rename(columns={"time": "date"})
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce").dt.date
            df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
            df["product"] = PRODUCT
            df = df[["date", "count", "product"]]
        return PandasDataFrameXComValue(df=df)

    @task
    def load_to_bigquery(df_wrap: PandasDataFrameXComValue, url_and_dates: Dict[str, str]) -> str:
        """
        Carrega/atualiza a partição do mês no BigQuery e retorna o identificador da tabela.
        Usa o **Airflow Connection** `GCP_CONN_ID` para autenticar.
        - Tabela particionada por coluna DATE "date".
        - Deleta a janela do mês e regrava (idempotente).
        """
        from google.cloud import bigquery
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        # Autentica via conexão do Airflow
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
        client = bq_hook.get_client(project_id=GCP_PROJECT_ID)

        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

        # Garante dataset
        dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET)
        try:
            client.get_dataset(dataset_ref)
        except Exception:
            client.create_dataset(bigquery.Dataset(dataset_ref))

        # Cria a tabela se não existir, particionada por coluna DATE "date"
        schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("count", "INTEGER"),
            bigquery.SchemaField("product", "STRING"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="date")
        try:
            client.get_table(table_id)
        except Exception:
            client.create_table(table)

        # Limpa a janela do mês e regrava (idempotência)
        start = url_and_dates["start_date"]
        end = url_and_dates["end_date"]
        delete_sql = f"""
        DELETE FROM `{table_id}`
        WHERE date BETWEEN DATE(@start) AND DATE(@end)
        """
        job = client.query(delete_sql, job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "STRING", start),
                bigquery.ScalarQueryParameter("end", "STRING", end),
            ]
        ))
        job.result()

        # Carrega o DataFrame
        df = df_wrap.df
        if df.empty:
            return table_id

        load_cfg = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema,
        )
        load_job = client.load_table_from_dataframe(df, table_id, job_config=load_cfg)
        load_job.result()
        return table_id

    @task
    def create_bigquery_dataframe(table_id: str, url_and_dates: Dict[str, str]) -> str:
        """
        Cria um BigQuery DataFrame (bigframes) referenciando a partição carregada.
        Usa as mesmas credenciais da conexão do Airflow.
        """
        try:
            import bigframes as bf
            from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

            bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
            creds = bq_hook.get_credentials()

            session = bf.Session(project=GCP_PROJECT_ID, location=BQ_LOCATION, credentials=creds)
            start = url_and_dates["start_date"]
            end = url_and_dates["end_date"]
            bdf = session.read_gbq(
                f"SELECT * FROM `{table_id}` WHERE date BETWEEN DATE('{start}') AND DATE('{end}')"
            )
            head_pdf = bdf.head(5).to_pandas()
            return head_pdf.to_string(index=False)
        except Exception as e:
            return f"BigQuery DataFrame não criado (bigframes opcional ou não instalado): {e}"

    # Orquestração
    url_and_dates = build_query_url()
    payload = fetch_openfda(url_and_dates)
    df_wrap = to_pandas(payload)
    table_id = load_to_bigquery(df_wrap, url_and_dates)
    create_bigquery_dataframe(table_id, url_and_dates)


dag_instance = openfda_to_bq()





