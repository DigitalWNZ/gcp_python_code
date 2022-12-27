from airflow import DAG
# from trigger_dag import DownStreamDagOperator
from airflow.operators.dummy import DummyOperator as EmptyOperator
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
# from dependencies.time_function import *

############################################################################
#############              Configure.         ##############################
############################################################################

##########################################################
#############             DAG_CONFIG         #############
##########################################################
DAG_ID = "looklook.stg_adsgo_adsgo_business_units"
TAGS = ["stg_adsgo_adsgo_business_units", "adsgo", "looklook"]
SCHEDULE_INTERVAL = '0 0 * * *'
DESCRIPTION = 'DI'

catchup = False


with DAG(
        DAG_ID,
        schedule_interval=SCHEDULE_INTERVAL,
        description=DESCRIPTION,
        start_date=datetime(2022, 7, 26),
        tags=TAGS,
        catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    gcp_to_bigquery_op = GCSToBigQueryOperator(
        bucket="plex-test",
        task_id="gcp_to_bigquery",
        source_objects="looklook.stg_looklook_looklook_tb_site_2022-09-01_data.json",
        destination_project_dataset_table="agolis-allen-first.ELM.ap_table",
        schema_object="ap_table.json",
        autodetect=False,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND"
    )
    start >> gcp_to_bigquery_op

