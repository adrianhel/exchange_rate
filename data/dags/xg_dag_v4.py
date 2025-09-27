import requests as req  # для выполнения HTTP-запросов
import pandas as pd  # для обработки данных
import csv  # для работы с CSV
from clickhouse_driver import Client  # для подключения к ClickHouse
from airflow import DAG  # объект DAG, ключевой элемент Airflow
from airflow.operators.python import PythonOperator  # с помощью которого него будем запускать Python код
from airflow.utils.dates import days_ago  # модуль, связанный с обработкой дат
from datetime import datetime  # для даты
from airflow.hooks.base_hook import BaseHook  # для хуков
from airflow.models import Variable  # для глобальных переменных
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

NAME = "andy_xg_notify"  # Имя для DAG и таблицы в ClickHouse
TOKEN = Variable.get('TOKEN')  # Токен для API тянем из Variables
CURRENCY = 'USD'  # Валюта, которая нас интересует
URL = (f'https://api.exchangerate.host/timeframe?access_key={TOKEN}'
       f'&source={CURRENCY}&start_date={{{{ ds }}}}&end_date={{{{ ds }}}}')  # Составной URL нашего API

# Настройка подключения к базе данных ClickHouse
# хукаем параметры из Connections
HOST = BaseHook.get_connection("clickhouse_default").host
USER = BaseHook.get_connection("clickhouse_default").login
PASSWORD = BaseHook.get_connection("clickhouse_default").password
DATABASE = BaseHook.get_connection("clickhouse_default").schema

CH_CLIENT = Client(
    host=HOST,  # IP-адрес сервера ClickHouse
    user=USER,  # Имя пользователя для подключения
    password=PASSWORD,  # Пароль для подключения
    database=DATABASE  # База данных, к которой подключаемся
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
    data_frame['value'] = data_frame['value'].astype(float)  # Преобразование в float

    # Запись data frame в ClickHouse
    client.execute(f'INSERT INTO {table_name} VALUES', [tuple(x) for x in data_frame.to_numpy()])


# Функция для обработки ошибки и отправки сообщения
def task_failure_callback(context):
    send_message = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_andy',
        chat_id='-1003046951828',
        text='❌ Задача «' + context.get('task_instance').task_id + '» не выполнена.',
        dag=dag)
    return send_message.execute(context=context)


# Функция для обработки ошибки и отправки сообщения
def task_success_callback(context):
    send_message = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_andy',
        chat_id='-1003046951828',
        text='✅ Задача «' + context.get('task_instance').task_id + '» выполнена.',
        dag=dag)
    return send_message.execute(context=context)


# Определение DAG
default_args = {
    'on_failure_callback': task_failure_callback,  # Задача не выполнена
    'on_success_callback': task_success_callback,  # Задача выполнена
}

dag = DAG(
    dag_id=NAME,
    default_args=default_args,
    schedule_interval='@daily',  # Как часто запускать, счит. CRON запись
    start_date=datetime(2025, 9, 21),  # Начало загрузки
    end_date=datetime(2025, 9, 25),  # Конец загрузки
    max_active_runs=1,  # Будет запускать только 1 DAG за раз
    tags=["andy", "xg"]  # Тэги на свое усмотрение
)

# Задача для извлечения данных
task_extract = PythonOperator(
    task_id='extract_data',  # Уникальное имя задачи
    python_callable=extract_data,  # Функция, которая будет запущена
    # Параметры в виде списка, которые будут переданы в функцию "extract_data"
    op_args=[URL, './extracted_data.csv'],
    dag=dag,  # DAG к которому приклеплена задача
)

# Оператор для выполнения запроса
create_table = ClickHouseOperator(
    task_id='create_table',
    sql='CREATE TABLE IF NOT EXISTS andy_xg_notify (start_date String, source String, char_code String, value Float64) ENGINE Log',
    clickhouse_conn_id='clickhouse_default',
    dag=dag,
)

# Задачи для загрузки данных
task_upload = PythonOperator(
    task_id='upload_to_clickhouse',
    python_callable=upload_to_clickhouse,
    op_args=['./extracted_data.csv', NAME, CH_CLIENT],
    dag=dag,
)

# Связываем задачи
task_extract >> create_table >> task_upload