import requests as req                   # для выполнения HTTP-запросов
import pandas as pd                      # для обработки данных
import csv                               # для работы с CSV
from clickhouse_driver import Client     # для подключения к ClickHouse


# Наш линк с токеном и датой
URL = f'https://api.exchangerate.host/timeframe?access_key=043dc9dad696914726d3064e9d917294&source=USD&start_date=2025-01-01&end_date=2025-01-01'

# Функция для извлечения данных с API
def extract_data(url, csv_file):
    response = req.get(url)
    if response.status_code != 200:
        print(f"Ошибка API: {response.status_code}, {response.text}")
        return

    data = response.json()
    #print(data)                     # Выводим ответ для проверки полей API
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


# Настройка подключения к базе данных ClickHouse
CH_CLIENT = Client(
    host='158.160.116.58',   # IP-адрес сервера ClickHouse
    user='student',          # Имя пользователя для подключения
    password='dfqh89fhq8',   # Пароль для подключения
    database='sandbox'       # База данных, к которой подключаемся
)

# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(csv_file, table_name, client):
    # Чтение данных из CSV
    data_frame = pd.read_csv(csv_file)
    # Преобразование в float
    data_frame['value'] = data_frame['value'].astype(float)  # Преобразование в float

    # Создание таблицы, ЕСЛИ НЕ СУЩЕСТВУЕТ ТО СОЗДАТЬ ТАБЛИЦУ
    client.execute(
        f'CREATE TABLE IF NOT EXISTS {table_name} (start_date String, source String, char_code String, value Float64) ENGINE = Log')

    # Запись data frame в ClickHouse
    client.execute(f'INSERT INTO {table_name} VALUES', [tuple(x) for x in data_frame.to_numpy()])

# Пример выполнения всех шагов
extract_data(URL, 'currency.csv')
try:
    upload_to_clickhouse('currency.csv', 'andy_currency_data', CH_CLIENT)
except Exception as e:
    print(f"Ошибка при загрузке данных в ClickHouse: {e}")

