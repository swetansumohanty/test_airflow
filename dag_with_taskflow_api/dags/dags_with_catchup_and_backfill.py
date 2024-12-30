""" dags with catchup and backfill """

# helps to run dag in past


from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "swetansu", "retries": 5, "retry_delay": timedelta(minutes=3)}

with DAG(
    dag_id="test_dag_catchup_and_backfill_v3",
    default_args=default_args,
    start_date=datetime(2024, 12, 25),
    schedule_interval="@daily",
    # catchup=True,
    catchup=False,
) as dag:
    task1 = BashOperator(task_id="task1", bash_command="echo test_catchup")
