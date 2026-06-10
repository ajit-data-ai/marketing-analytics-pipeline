"""
Airflow DAG: daily marketing analytics pipeline.
Pulls Google Ads + Facebook Ads for yesterday, loads to BigQuery, triggers dbt.
"""
from __future__ import annotations
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

from connectors.google_ads import GoogleAdsConnector
from connectors.facebook_ads import FacebookAdsConnector

DATASET = "marketing_raw"
PROJECT = "{{ var.value.gcp_project }}"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}


def _pull_google_ads(ds: str, **_):
    run_date = date.fromisoformat(ds)
    connector = GoogleAdsConnector()
    df = connector.get_campaign_performance(run_date, run_date)
    from google.cloud import bigquery
    import os
    client = bigquery.Client(project=os.environ["GCP_PROJECT"])
    table = f"{os.environ['GCP_PROJECT']}.{DATASET}.google_ads_campaigns"
    job = client.load_table_from_dataframe(
        df, table,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema_update_options=["ALLOW_FIELD_ADDITION"],
        ),
    )
    job.result()


def _pull_facebook_ads(ds: str, **_):
    run_date = date.fromisoformat(ds)
    connector = FacebookAdsConnector()
    df = connector.get_campaign_performance(run_date, run_date)
    from google.cloud import bigquery
    import os
    client = bigquery.Client(project=os.environ["GCP_PROJECT"])
    table = f"{os.environ['GCP_PROJECT']}.{DATASET}.facebook_ads_campaigns"
    job = client.load_table_from_dataframe(
        df, table,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()


with DAG(
    dag_id="marketing_analytics_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 6 * * *",  # 06:00 UTC daily — after ad platforms finalize yesterday's data
    catchup=False,
    tags=["marketing", "google-ads", "facebook-ads", "bigquery"],
) as dag:

    pull_google = PythonOperator(
        task_id="pull_google_ads",
        python_callable=_pull_google_ads,
    )

    pull_facebook = PythonOperator(
        task_id="pull_facebook_ads",
        python_callable=_pull_facebook_ads,
    )

    run_dbt = BigQueryInsertJobOperator(
        task_id="run_dbt_models",
        configuration={
            "query": {
                "query": "SELECT 'dbt triggered via bash operator in prod' AS note",
                "useLegacySql": False,
            }
        },
        # In production: replace with BashOperator("dbt run --select marts.marketing")
    )

    [pull_google, pull_facebook] >> run_dbt
