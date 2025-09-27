# Работа с API Exchange Rate
Извлечения данных с API Exchange Rate и загрузкой в Clickhouse. 

### [Назад в Содержание ⤶](https://github.com/adrianhel/exchange_rate)

### Скрипт
Пишем скрипт для извлечения курса валют с API Exchange Rate.
- [Script](scripts/xg_script.py)

### DAG ver.1
Используем ранеее написанный скрипт для написания DAG для Airflow.
- [DAG v1](dags/xg_dag.py)

### DAG ver.2
Рефакторим ранее написанный DAG.
- [DAG v2](dags/xg_dag_v2.py)

### DAG ver.3
Добавлено использование Variables и Connections.
- [DAG v3](dags/xg_dag_v3.py)

Вариант этого DAG в современном стиле с использованием Ruff (линтер и форматтер для Python), декораторов и Taskflow API.  
- [DAG v3.2](dags/xg_dag_v3_2.py)

### DAG ver.4
Создание схемы вынесено в отдельную задачу + добавлен алертинг в Telegram.
- [DAG v4](dags/xg_dag_v4.py)