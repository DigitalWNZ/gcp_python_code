#coding: utf-8

import datetime
import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

def task_success_function(context):
    """
    callback on each success task!
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    print ('Task Success send_alter')


def task_retry_function(context):
    """
    callback on each retry task!
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    print ('Task Retry send_alter')



def task_failure_function(context):
    """
    callback on each failure task!
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    print ('Task Failure send_alter')


def task_sla_function(*args, **kwargs):
    """
    callback on each task exceeded the time should have succeeded!
    """
    print ("Task delay send_alter")

# args setting
default_args = {
    "owner": "user",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "execution_timeout": timedelta(seconds=30),
    "on_failure_callback": task_failure_function,
    "on_success_callback": task_success_function,
    "on_retry_callback": task_retry_function
}

# create a dag task
t_dag = DAG(
    "test_sla_task",
    default_args=default_args,
    description="a tutorial",
    tags=["test"],
    schedule_interval="@once",
    start_date=datetime.datetime(2024, 7, 16, 16, 0, 0),
    sla_miss_callback=task_sla_function
)

t0 = BashOperator(
    task_id="task0",
    bash_command="echo {{ logical_date }}",
    # bash_command= 'echo "hello1"',
    dag=t_dag,
    sla=timedelta(seconds=20),
)


t0
