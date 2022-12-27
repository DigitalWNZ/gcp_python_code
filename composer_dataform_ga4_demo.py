from datetime import datetime
from airflow import models
from airflow.models.baseoperator import chain
from dependencies.loca_dep import local_dependencies

from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.dataform import (
   DataformCreateCompilationResultOperator,
   DataformGetCompilationResultOperator,
   DataformCreateWorkflowInvocationOperator,
   DataformGetWorkflowInvocationOperator,
)

from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor,
)

from airflow.providers.google.cloud.sensors.dataform import (
   DataformWorkflowInvocationStateSensor
)

DAG_ID = "composer-datafomr-ga4-demo"
PROJECT_ID = "agolis-allen-first"
DATASET_ID= "dataform_demo"
TABLE_ID_events="stat_by_events"
TABLE_ID_device="stat_by_device_cat"
REGION = "us-central1"
REPOSITORY_ID = "dataform-events-demo"
WORKSPACE_ID="dataform_events_transform"
x=local_dependencies.x

with models.DAG(
      DAG_ID,
      schedule_interval="10 2 * * *",  # every day at 2:10
      # schedule_interval="*/5 * * * *", # every 5 minutes
      start_date=datetime(2022, 9, 26),
      catchup=False,
      tags=["example", "dataform"],
) as dag:
   create_compilation_result = DataformCreateCompilationResultOperator(
       task_id="create_compilation_result",
       project_id=PROJECT_ID,
       region=REGION,
       repository_id=REPOSITORY_ID,
       compilation_result={
           "git_commitish": "main",
           "workspace": (
               f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/"
               f"workspaces/{WORKSPACE_ID}"
           ),
       },
   )

   get_compilation_result = DataformGetCompilationResultOperator(
       task_id="get_compilation_result",
       project_id=PROJECT_ID,
       region=REGION,
       repository_id=REPOSITORY_ID,
       compilation_result_id=(
           "{{ task_instance.xcom_pull('create_compilation_result')['name'].split('/')[-1] }}"
       ),
   )

   create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
       task_id='create_workflow_invocation',
       project_id=PROJECT_ID,
       region=REGION,
       repository_id=REPOSITORY_ID,
       workflow_invocation={
           "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
       },
   )


   get_workflow_invocation = DataformGetWorkflowInvocationOperator(
       task_id='get_workflow_invocation',
       project_id=PROJECT_ID,
       region=REGION,
       repository_id=REPOSITORY_ID,
       workflow_invocation_id=(
           "{{ task_instance.xcom_pull('create_workflow_invocation')['name'].split('/')[-1] }}"
       ),
   )

   check_table_exists = BigQueryTableExistenceSensor(
       task_id="check_table_exists", project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID_events
   )

   events_export_to_gcs = BigQueryToGCSOperator(
       task_id="events_export_to_gcs",
       source_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_events}",
       destination_cloud_storage_uris="gs://agolis-allen-first-tencent-demo/composer_sample/event_*.csv",
       export_format="CSV"
   )

   device_export_to_gcs = BigQueryToGCSOperator(
       task_id="device_export_to_gcs",
       source_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID_device}",
       destination_cloud_storage_uris="gs://agolis-allen-first-tencent-demo/composer_sample/devide_*.csv",
       export_format="CSV"
   )


   chain(
       create_compilation_result>> get_compilation_result>>create_workflow_invocation >> get_workflow_invocation>>
       check_table_exists >> [events_export_to_gcs,device_export_to_gcs]
       # Unlock the following line for asynchronized call
       # create_workflow_invocation_async >> is_workflow_invocation_done,
   )