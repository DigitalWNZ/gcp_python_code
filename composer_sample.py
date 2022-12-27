import os
from datetime import datetime

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryValueCheckOperator,
)

from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor,
)
from airflow.utils.trigger_rule import TriggerRule

filter = os.environ.get("filter")

SELECT_QUERY = (
    f"select * from `wdtest-001.aopeng_test_us.tablez` where dim='{filter}';"
)

QUERY1_SQL_PATH = "resources/t1.sql"
QUERY2_SQL_PATH = "resources/t2.sql"
QUERY3_SQL_PATH = "resources/t3.sql"

DAG_ID = "bigquery_queries_sample"

conf_test="{{ dag_run.conf['conf_test'] }}"

with models.DAG(
        DAG_ID,
        schedule_interval="@once",
        start_date=datetime(2022, 7, 26),
        catchup=False,
        tags=["example", "bigquery"],
        user_defined_macros={"QUERY1_SQL_PATH": QUERY1_SQL_PATH, "QUERY2_SQL_PATH": QUERY2_SQL_PATH,"QUERY3_SQL_PATH": QUERY3_SQL_PATH},
) as dag:
    select_query_job1 = BigQueryInsertJobOperator(
        task_id="select_query_job1",
        configuration={
            "query": {
                "query": "{% include QUERY1_SQL_PATH %}",
                "useLegacySql": False,
            }
        }

    )

    select_query_job2 = BigQueryInsertJobOperator(
        task_id="select_query_job2",
        configuration={
            "query": {
                "query": "{% include QUERY2_SQL_PATH %}",
                "useLegacySql": False,
            }
        }
    )

    select_query_job3 = BigQueryInsertJobOperator(
        task_id="select_query_job3",
        configuration={
            "query": {
                "query": "{% include QUERY3_SQL_PATH %}",
                "useLegacySql": False,
            }
        }
    )


    check_table_exists = BigQueryTableExistenceSensor(
        task_id="check_table_exists", project_id='wdtest-001', dataset_id='aopeng_test_us', table_id='tablez'
    )

    check_value = BigQueryValueCheckOperator(
        task_id="check_value",
        sql=f"SELECT COUNT(*) FROM `wdtest-001.aopeng_test_us.tablez` where dim like '%{filter}%'",
        pass_value=1,
        use_legacy_sql=False,
    )

    check_value_with_conf = BigQueryValueCheckOperator(
        task_id="check_value_with_conf",
        sql=f"SELECT COUNT(*) FROM `wdtest-001.aopeng_test_us.tablez` where dim like '%{conf_test}%'",
        # sql="SELECT COUNT(*) FROM `wdtest-001.aopeng_test_us.tablez` where dim like '%{}%'".format({{dag_run.conf['conf_test']}}),

        pass_value=1,
        use_legacy_sql=False,
    )

    [select_query_job1,select_query_job2]>>select_query_job3>>[check_table_exists,check_value,check_value_with_conf]
    # from tests.system.utils.watcher import watcher
    # list(dag.tasks) >> watcher()
