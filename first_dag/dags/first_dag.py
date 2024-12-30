""" first dag """


from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {"owner": "swetansu", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="first_dag_V3",
    description="my first dag",
    start_date=datetime(2024, 12, 30, 2),
    schedule_interval="@daily",
    default_args=default_args,
) as dag:
    task1 = BashOperator(task_id="first_task", bash_command="echo i am task1")

    task2 = BashOperator(
        task_id="second_task",
        bash_command="echo i am task 2 and will be running after task 1",
    )

    task3 = BashOperator(
        task_id="third_task",
        bash_command="echo i am task3 and will be running after task1",
    )

    # task dependency

    # method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # method2
    # task1 >> task2
    # task1 >> task3

    # method3
    task1 >> [task2, task3]
