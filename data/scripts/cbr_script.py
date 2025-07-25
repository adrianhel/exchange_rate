# Импортируем необходимые библиотеки
import requests as req                    # для выполнения HTTP-запросов
import pandas as pd                       # для обработки данных
from datetime import datetime, timedelta  # для работы с датами
import xml.etree.ElementTree as ET        # для парсинга XML
from clickhouse_driver import Client      # для подключения к ClickHouse
from dotenv import load_dotenv            # для подключения .env
import os                                 # см.выше


load_dotenv()                             # подключение .env

# Устанавливаем URL для API Центрального банка и дату для извлечения данных
URL = 'http://www.cbr.ru/scripts/XML_daily.asp'  # URL для получения курсов валют с сайта ЦБ
DATE = '01/01/2023'
DATE_FORMAT = str(datetime.strptime(DATE, '%d/%m/%Y').strftime('%Y_%m_%d'))
NAME = 'andy_cbr'
TABLE_NAME = f'{NAME}_{DATE_FORMAT}'

# Настройка подключения к базе данных ClickHouse
CH_CLIENT = Client(
    host=os.getenv('HOST'),           # IP-адрес сервера ClickHouse
    user=os.getenv('USER'),           # Имя пользователя для подключения
    password=os.getenv('PASSWORD'),   # Пароль для подключения
    database=os.getenv('DATABASE')    # База данных, к которой подключаемся
)

# Функция для извлечения данных с API Центрального банка и сохранения их в локальный файл
def extract_data(url, date, s_file):
    """
    Эта функция выгружает данные по валютам, используя GET-запрос,
    и сохраняет результат в локальный файл `s_file`.
    """
    request = req.get(f"{url}?date_req={date}")  # Выполняем GET-запрос для получения данных за указанную дату

    # Сохраняем полученные данные (в формате XML) в локальный файл
    with open(s_file, "w", encoding="utf-8") as tmp_file:
        tmp_file.write(request.text)  # Записываем текст ответа в файл

# Функция для обработки данных в формате XML и преобразования их в CSV
def transform_data(s_file, csv_file, date):
    """
    Эта функция обрабатывает полученные данные в формате XML
    и преобразует их в CSV формат для дальнейшей работы.
    """
    rows = list()  # Список для хранения строк данных

    # Устанавливаем парсер для XML с кодировкой UTF-8
    parser = ET.XMLParser(encoding="utf-8")
    # Чтение и парсинг XML файла
    tree = ET.parse(s_file, parser=parser).getroot()

    # Перебор всех валют в XML и извлечение нужных данных
    for child in tree.findall("Valute"):
        num_code = child.find("NumCode").text  # Номер валюты
        char_code = child.find("CharCode").text  # Символьный код валюты
        nominal = child.find("Nominal").text  # Номинал валюты
        name = child.find("Name").text  # Название валюты
        value = child.find("Value").text  # Значение валюты

        # Добавляем полученные данные в список строк
        rows.append((num_code, char_code, nominal, name, value))

    # Создание DataFrame из полученных данных
    data_frame = pd.DataFrame(
        rows, columns=["num_code", "char_code", "nominal", "name", "value"]
    )

    # Добавляем колонку с датой, чтобы отслеживать, когда данные были получены
    data_frame['date'] = date

    # Сохраняем данные в CSV файл
    data_frame.to_csv(csv_file, sep=",", encoding="utf-8", index=False)

# Функция для загрузки данных в ClickHouse из CSV
def upload_to_clickhouse(csv_file, table_name, client):

    # Чтение данных из CSV
    data_frame = pd.read_csv(csv_file)

    # Создание таблицы, ЕСЛИ НЕ СУЩЕСТВУЕТ ТО СОЗДАТЬ ТАБЛИЦУ
    client.execute(f'CREATE TABLE IF NOT EXISTS {table_name} (num_code Int64, char_code String, nominal Int64, name '
                   f'String, value String, date String) ENGINE Log')

    # Запись data frame в ClickHouse
    client.execute(f'INSERT INTO {table_name} VALUES', data_frame.to_dict('records'))

# Пример выполнения всех шагов
extract_data(URL, DATE, 'currency')  # Извлечение данных и сохранение их в файл 'currency'

transform_data('currency', 'currency.csv', DATE)  # Преобразование данных в формат CSV

# Пример загрузки данных в ClickHouse
upload_to_clickhouse(
    csv_file='currency.csv',
    table_name=TABLE_NAME,
    client=CH_CLIENT
)