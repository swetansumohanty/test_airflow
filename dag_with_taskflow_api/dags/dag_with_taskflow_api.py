"""
dag with taskflow api
=====================
it automatically detects dependencies.
"""


from airflow.decorators import dag, task
from datetime import datetime, timedelta


default_args = {"owner": "swetansu", "retries": 5, "retry_delay": timedelta(minutes=5)}


@dag(
    default_args=default_args,
    dag_id="dag_with_taskflow_api_v6",
    start_date=datetime(2024, 12, 30),
    schedule_interval="@daily",
)
def test_etl():
    @task(multiple_outputs=True)
    def get_name():
        return {"first_name": "swetansu", "last_name": "mohanty"}

    @task()
    def get_age():
        return 20

    @task()
    def greet(first_name, last_name, age):
        print(f"my name is {first_name} {last_name}, i am {age} years old")

    name_dict = get_name()
    first_name = name_dict["first_name"]
    last_name = name_dict["last_name"]
    age = get_age()

    greet(first_name, last_name, age)


test_etl_obj = test_etl()
