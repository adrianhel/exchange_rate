# Exchange Rate

### [Назад в Содержание ⤶](https://github.com/adrianhel/adrianhel.md)

___

<img src="/img/cover.png" width="100%">

## О проекте
Проект содержит различные материалы (script's, DAG's) для работы с валютными сервисами.

## Используемые API

## Содержание
### Работа с Центральным банком
1. [Script](data/from_api_to_ch_cbrf_script.py)
для извлечения данных с **API** _ЦРБ РФ_ и загрузкой в **Clickhouse**.  
2. [DAG](data/from_api_to_ch_cbrf_dag.py)
для извлечения данных с **API** _ЦРБ РФ_ и загрузкой в **Clickhouse**.  

### Работа с сервисом Exchange Rate
1. [Script](data/from_api_to_ch_exgr_script.py)
для извлечения данных с **API** _Exchange Rate_ и загрузкой в **Clickhouse**.  
2. [DAG](data/from_api_to_ch_exgr_dag.py)
для извлечения данных с **API** _Exchange Rate_ и загрузкой в **Clickhouse**.  