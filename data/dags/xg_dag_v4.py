import requests as req                               # для выполнения HTTP-запросов
import pandas as pd                                  # для обработки данных
import csv                                           # для работы с CSV
from clickhouse_driver import Client                 # для подключения к ClickHouse
from airflow import DAG                              # объект DAG, ключевой элемент Airflow
from airflow.operators.python import PythonOperator  # с помощью которого него будем запускать Python код
from airflow.utils.dates import days_ago             # модуль, связанный с обработкой дат
from datetime import datetime                        # для даты
from airflow.hooks.base_hook import BaseHook         # для хуков
from airflow.models import Variable                  # для глобальных переменных

NAME = "andy_xg_dag_v3"           # Имя для DAG и таблицы в ClickHouse
TOKEN = Variable.get('TOKEN')     # Токен для API тянем из Variables
CURRENCY = 'USD'                  # Валюта, которая нас интересует
URL = (f'https://api.exchangerate.host/timeframe?access_key={TOKEN}'
       f'&source={CURRENCY}&start_date={{{{ ds }}}}&end_date={{{{ ds }}}}')  # Составной URL нашего API

# Настройка подключения к базе данных ClickHouse
# хукаем параметры из Connections
HOST = BaseHook.get_connection("clickhouse_default").host
USER = BaseHook.get_connection("clickhouse_default").login
PASSWORD = BaseHook.get_connection("clickhouse_default").password
DATABASE = BaseHook.get_connection("clickhouse_default").schema

CH_CLIENT = Client(
    host=HOST,          # IP-адрес сервера ClickHouse
    user=USER,          # Имя пользователя для подключения
    password=PASSWORD,  # Пароль для подключения
    database=DATABASE   # База данных, к которой подключаемся
)

# Функция для извлечения данных с API
def extract_data(url, csv_file):
    response = req.get(url)
    if response.status_code != 200:
        print(f"Ошибка API: {response.status_code}, {response.text}")
        return

    data = response.json()
    # Запись данных в CSV-файл
    with open(csv_file, "w", newline='', encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['start_date', 'source', 'char_code', 'value'])

        # Извлечение данных из JSON
        start_date = data['start_date']
        source = data['source']
        quotes = data['quotes'][start_date]

        # Запись курсов валют в CSV
        for key, value in quotes.items():
            writer.writerow([start_date, source, key[3:], value])


# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(csv_file, table_name, client):
    # Чтение данных из CSV
    data_frame = pd.read_csv(csv_file)
    # Преобразование в float
    data_frame['value'] = data_frame['value'].astype(float)     # Преобразование в float

    # Создание таблицы, ЕСЛИ НЕ СУЩЕСТВУЕТ ТО СОЗДАТЬ ТАБЛИЦУ
    client.execute(
        f'CREATE TABLE IF NOT EXISTS {table_name} (start_date String, source String, char_code String, value Float64) ENGINE = Log')

    # Запись data frame в ClickHouse
    client.execute(f'INSERT INTO {table_name} VALUES', [tuple(x) for x in data_frame.to_numpy()])

# Определяем DAG, это контейнер для описания нашего пайплайна
dag = DAG(
    dag_id=NAME,
    schedule_interval='@daily',         # Как часто запускать, счит. CRON запись
    start_date=datetime(2025,9,21),      # Начало загрузки
    end_date=datetime(2025,9,25),       # Конец загрузки
    max_active_runs=1,                  # Будет запускать только 1 DAG за раз
    tags=["andy", "xg"]                 # Тэги на свое усмотрение
)

# Задача для извлечения данных
task_extract = PythonOperator(
    task_id='extract_data',            # Уникальное имя задачи
    python_callable=extract_data,      # Функция, которая будет запущена (определена выше)

    # Параметры в виде списка которые будут переданы в функцию "extract_data"
    op_args=[URL, './extracted_data.csv'],
    dag=dag,    # DAG к которому приклеплена задача
)


# Задачи для загрузки данных
task_upload = PythonOperator(
    task_id='upload_to_clickhouse',
    python_callable=upload_to_clickhouse,
    op_args=['./extracted_data.csv', NAME, CH_CLIENT],
    dag=dag,
)

# Связываем задачи
task_extract >> task_upload