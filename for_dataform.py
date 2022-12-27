from datetime import datetime
from airflow import models
from airflow.models.baseoperator import chain


from airflow.providers.google.cloud.operators.dataform import (
   DataformCreateCompilationResultOperator,
   DataformGetCompilationResultOperator,
   DataformCreateWorkflowInvocationOperator,
   DataformGetWorkflowInvocationOperator,
)

from airflow.providers.google.cloud.sensors.dataform import (
   DataformWorkflowInvocationStateSensor
)
# from google.cloud.dataform_v1beta1 import WorkflowInvocation

DAG_ID = "Airflow_plus_dataform"
PROJECT_ID = "agolis-allen-first"
REGION = "europe-west4"
REPOSITORY_ID = "quickstart-repository"
WORKSPACE_ID="quickstart-workspace"

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

   # Unlock the following two steps for asynchronized call
   #
   # create_workflow_invocation_async = DataformCreateWorkflowInvocationOperator(
   #     task_id='create_workflow_invocation_async',
   #     project_id=PROJECT_ID,
   #     region=REGION,
   #     repository_id=REPOSITORY_ID,
   #     asynchronous=True,
   #     workflow_invocation={
   #         "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
   #     },
   # )
   #
   # is_workflow_invocation_done = DataformWorkflowInvocationStateSensor(
   #     task_id="is_workflow_invocation_done",
   #     project_id=PROJECT_ID,
   #     region=REGION,
   #     repository_id=REPOSITORY_ID,
   #     workflow_invocation_id=(
   #         "{{ task_instance.xcom_pull('create_workflow_invocation')['name'].split('/')[-1] }}"
   #     ),
   #     expected_statuses=2,
   # )

   get_workflow_invocation = DataformGetWorkflowInvocationOperator(
       task_id='get_workflow_invocation',
       project_id=PROJECT_ID,
       region=REGION,
       repository_id=REPOSITORY_ID,
       workflow_invocation_id=(
           "{{ task_instance.xcom_pull('create_workflow_invocation')['name'].split('/')[-1] }}"
       ),
   )

   chain(
       create_compilation_result>> get_compilation_result>>create_workflow_invocation >> get_workflow_invocation,
       # Unlock the following line for asynchronized call
       # create_workflow_invocation_async >> is_workflow_invocation_done,
   )