# Работа с API Центрального банка РФ
Извлечения данных с **API** _ЦРБ РФ_ и загрузкой в **Clickhouse**. 

### [Назад в Содержание ⤶](https://github.com/adrianhel/exchange_rate)

### Скрипт
Пишем скрипт для извлечения курса валют с _API_ Центрального банка.
- [Script](scripts/cbr_script.py)

### DAG ver.1
Используем ранеее написанный скрипт для написания _DAG_ для _Airflow_.
- [DAG](dags/cbr_dag.py)

### DAG ver.2
Рефакторим ранее написанный _DAG_.
- [DAG](dags/cbr_dag_v2.py)

### DAG ver.3
Рефакторим ранее написанный _DAG_ снова.
- [DAG](dags/cbr_dag_v3.py)