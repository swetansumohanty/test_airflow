""" first py dag """


from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {"owner": "swetansu", "retries": 5, "retry_delay": timedelta(minutes=2)}


def greet(ti):
    # name = ti.xcom_pull(task_ids="get_name")
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    age = ti.xcom_pull(task_ids="get_age", key="age")
    print(f"hi i am {first_name} {last_name} and i am {age} years old")


# def get_name():
#     return "jerry"


def get_name(ti):  # ti = task_instance
    ti.xcom_push(key="first_name", value="swetansu")
    ti.xcom_push(key="last_name", value="mohanty")


def get_age(ti):
    ti.xcom_push(key="age", value=25)


with DAG(
    dag_id="first_py_dag_V7",
    description="my first py dag",
    start_date=datetime(2024, 12, 30, 2),
    schedule_interval="@daily",
    default_args=default_args,
) as dag:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet,
        # op_kwargs={"age": 25}  # can be used to share large datasets
    )

    task2 = PythonOperator(task_id="get_name", python_callable=get_name)

    task3 = PythonOperator(task_id="get_age", python_callable=get_age)

    # task dependency
    [task2, task3] >> task1


# ** note: max xcom data is 48KB
# Never share large dataset like Pandas df etc.
