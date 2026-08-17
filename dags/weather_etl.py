import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from etl.extract import fetch_city_weather, load_cities
from etl.load import load_rows

logger = logging.getLogger(__name__)

ARCHIVE_LAG_DAYS = 5
DBT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/opt/dbt_venv/bin/dbt"

default_args = {
    "owner": "serafim",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="weather_etl",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["weather", "etl", "dbt"],
)
def weather_etl():
    @task
    def extract_load_raw(data_interval_start=None):
        target_date = (data_interval_start - timedelta(days=ARCHIVE_LAG_DAYS)).to_date_string()
        cities = load_cities()

        all_rows = []
        for city in cities:
            all_rows.extend(fetch_city_weather(city, target_date))

        loaded = load_rows(all_rows)
        logger.info("%s: loaded %d rows for %d cities", target_date, loaded, len(cities))
        return loaded

    dbt_env = f"cd {DBT_DIR} && DBT_PROFILES_DIR={DBT_DIR}"

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{dbt_env} {DBT_BIN} deps",
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{dbt_env} {DBT_BIN} run",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{dbt_env} {DBT_BIN} test",
    )

    extract_load_raw() >> dbt_deps >> dbt_run >> dbt_test


weather_etl()
