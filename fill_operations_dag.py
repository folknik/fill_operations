import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 10, 29),
    "end_date": datetime(2020, 11, 15),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15)
}

refs = dict()
refs['execution_date'] = '{{ ds }}'
refs['update_status'] = 'false'
refs['max_height_for_operation_heading'] = 500  # meter
refs['max_height_for_operation_type'] = 3000  # ft
refs['max_distance_from_airport'] = 10000  # meter
refs['gap'] = 10  # calculated operation heading should be in gap


dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    concurrency=1
)

task = DockerOperator(
    dag=dag,
    task_id='fill_operations_gaps',
    auto_remove=True,
    docker_url='unix://var/run/docker.sock',
    api_version='auto',
    image='fill_operations:v1.0',
    environment=refs
)