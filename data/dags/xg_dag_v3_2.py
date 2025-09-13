import csv
from datetime import datetime, timedelta

import pandas as pd
import requests as req
from clickhouse_driver import Client
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

# Константы
NAME = "andy_xg_dag_v3_7"
CURRENCY = "USD"


# Настройка подключения к ClickHouse
def get_clickhouse_client():
    HOST = BaseHook.get_connection("clickhouse_default").host
    USER = BaseHook.get_connection("clickhouse_default").login
    PASSWORD = BaseHook.get_connection("clickhouse_default").password
    DATABASE = BaseHook.get_connection("clickhouse_default").schema

    return Client(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
    )


# Определяем DAG с декоратором @dag
@dag(
    dag_id=NAME,
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 1),
    end_date=datetime(2025, 9, 12),
    max_active_runs=1,
    catchup=True,
    tags=["andy", "xg"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=60),
    },
)

def currency_etl_dag():
    @task(retries=3, retry_delay=timedelta(seconds=60))
    def extract_data(**context):
        """Извлечение данных с API."""
        TOKEN = Variable.get("TOKEN")
        execution_date = context["ds"]

        URL = (
            f"https://api.exchangerate.host/timeframe?access_key={TOKEN}"
            f"&source={CURRENCY}&start_date={execution_date}&end_date={execution_date}"
        )

        response = req.get(URL)
        if response.status_code != 200:
            raise AirflowException(f"Ошибка API: {response.status_code}, {response.text}")

        data = response.json()

        # Проверка наличия необходимых данных в ответе
        if "quotes" not in data or "start_date" not in data:
            raise AirflowException("Неверный формат ответа API: отсутствуют необходимые поля")

        start_date = data["start_date"]
        if start_date not in data["quotes"]:
            raise AirflowException(f"Нет данных для даты {start_date} в ответе API")

        csv_file_path = f"/tmp/extracted_data_{execution_date}.csv"

        # Запись данных в CSV-файл
        try:
            with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["start_date", "source", "char_code", "value"])

                # Извлечение данных из JSON
                source = data["source"]
                quotes = data["quotes"][start_date]

                # Запись курсов валют в CSV
                for key, value in quotes.items():
                    writer.writerow([start_date, source, key[3:], value])
        except Exception as e:
            raise AirflowException(f"Ошибка записи в CSV файл: {str(e)}")

        return csv_file_path

    @task(retries=3, retry_delay=timedelta(seconds=60))
    def upload_to_clickhouse(csv_file_path):
        """Загрузка данных в ClickHouse."""
        try:
            client = get_clickhouse_client()

            # Чтение данных из CSV
            data_frame = pd.read_csv(csv_file_path)
            # Проверка наличия данных
            if data_frame.empty:
                raise AirflowException("CSV файл пуст или не содержит данных")

            # Преобразование в float
            data_frame["value"] = data_frame["value"].astype(float)

            # Создание таблицы, если не существует
            client.execute(
                f"CREATE TABLE IF NOT EXISTS {NAME} "
                "(start_date String, source String, char_code String, value Float64) "
                "ENGINE = Log"
            )

            # Запись data frame в ClickHouse
            client.execute(
                f"INSERT INTO {NAME} VALUES",
                [tuple(x) for x in data_frame.to_numpy()],
            )

            print(f"Данные успешно загружены в таблицу {NAME}")
            return True

        except Exception as e:
            raise AirflowException(f"Ошибка загрузки в ClickHouse: {str(e)}")

    # Определение пайплайна с Taskflow
    csv_file = extract_data()
    upload_result = upload_to_clickhouse(csv_file)


# Создание DAG экземпляра
dag = currency_etl_dag()