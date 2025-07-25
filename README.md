# Exchange Rate

### [На главную ⤶](https://github.com/adrianhel/adrianhel.md)

___

<img src="/img/cover.png" width="100%">

## О проекте
Проект содержит различные материалы (script's, DAG's) для работы с валютными сервисами.

## Используемые API
- [cbr API](https://www.cbr.ru/development/SXML/) – документация по работе с API Центрального банка РФ.
- [exchangerate API](https://exchangerate.host/documentation) – документация по работе с API Exchange Rate.

## Содержание
### Работа с Центральным банком
- [Script](data/scripts/cbr_script.py)
для извлечения данных с **API** _ЦРБ РФ_ и загрузкой в **Clickhouse**.  
- [DAG](data/dags/cbr_dag.py)
для извлечения данных с **API** _ЦРБ РФ_ и загрузкой в **Clickhouse**.  

### Работа с сервисом Exchange Rate
- [Script](data/scripts/xg_script.py)
для извлечения данных с **API** _Exchange Rate_ и загрузкой в **Clickhouse**.  
- [DAG](data/dags/xg_dag.py)
для извлечения данных с **API** _Exchange Rate_ и загрузкой в **Clickhouse**.  .  