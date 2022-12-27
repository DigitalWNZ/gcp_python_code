import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor,
)

project = os.environ.get("project")
dataset = os.environ.get("dataset")
dest_dataset = "{{ dag_run.conf['dest_dataset'] }}"
dest_table = "{{ dag_run.conf['dest_table'] }}"

QUERY1_SQL_PATH = "resources/parse_events.sql"

DAG_ID = "bigquery_queries_sample"

with models.DAG(
        DAG_ID,
        schedule_interval="@once",
        start_date=datetime(2022, 7, 26),
        catchup=False,
        tags=["example", "bigquery"],
        render_template_as_native_obj=True,
        user_defined_macros={"QUERY1_SQL_PATH": QUERY1_SQL_PATH,"project":project,"dataset":dataset,"dest_dataset":dest_dataset,"dest_table":dest_table},
) as dag:
    select_query_job1 = BigQueryInsertJobOperator(
        task_id="select_query_job1",
        configuration={
            "query": {
                "query": "{% include QUERY1_SQL_PATH %}",
                "useLegacySql": False,
                "destinationTable":{
                    "projectId": project,
                    "datasetId": dest_dataset,
                    "tableId": dest_table,
                },
                "write_disposition": "WRITE_TRUNCATE"
            }
        },
        params={"dest_dataset":"tencent_demo","dest_table":"parse_result","table":"hello"}
    )

    check_table_exists = BigQueryTableExistenceSensor(
        task_id="check_table_exists", project_id=project, dataset_id=dest_dataset, table_id=dest_table
    )

    export_to_gcs=BigQueryToGCSOperator(
        task_id="export_to_gcs",
        source_project_dataset_table=f"{project}.{dest_dataset}.{dest_table}",
        destination_cloud_storage_uris="gs://agolis-allen-first-tencent-demo/composer_sample/*.csv",
        export_format="CSV"
    )

    select_query_job1>>check_table_exists>>export_to_gcs
