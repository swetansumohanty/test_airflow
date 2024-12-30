""" dags with cron expression """


from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "swetansu", "retries": 5, "retry_delay": timedelta(minutes=3)}

with DAG(
    dag_id="dag_with_cron_expression_v1",
    default_args=default_args,
    start_date=datetime(2024, 12, 20),
    schedule_interval="0 3 * * Tue-Fri",
    catchup=True,
) as dag:
    task1 = BashOperator(task_id="task1", bash_command="echo test_catchup")
