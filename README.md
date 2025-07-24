# Exchange Rate

### [На главную ⤶](https://github.com/adrianhel/adrianhel.md)

___

<img src="/img/cover.png" width="100%">

## О проекте
Проект содержит различные материалы (script's, DAG's) для работы с валютными сервисами.

## Используемые API
- [cbr API](https://www.cbr.ru/development/SXML/)
- [exchangerate API](https://exchangerate.host/documentation)

## Содержание
### Работа с Центральным банком
1. [Script](data/scripts/from_api_to_ch_script_cbr.py)
для извлечения данных с **API** _ЦРБ РФ_ и загрузкой в **Clickhouse**.  
2. [DAG](data/dags/from_api_to_ch_dag_cbr.py)
для извлечения данных с **API** _ЦРБ РФ_ и загрузкой в **Clickhouse**.  

### Работа с сервисом Exchange Rate
1. [Script](data/scripts/from_api_to_ch_script_xg.py)
для извлечения данных с **API** _Exchange Rate_ и загрузкой в **Clickhouse**.  
2. [DAG](data/dags/from_api_to_ch_dag_xg.py)
для извлечения данных с **API** _Exchange Rate_ и загрузкой в **Clickhouse**.  .  