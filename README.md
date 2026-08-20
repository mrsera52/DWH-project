# Weather DWH

Пет-проект: ETL-пайплайн, который каждый день тянет погоду по нескольким городам
из Open- Meteo, кладёт в Postgres и строит витрины через dbt. Все поднимается в Docker,
оркестрация на Airflow, дашборд в Metabase.

Данные идут по слоям: `raw` (как пришло из API) -> `staging` (почистил через dbt) ->
`marts` (агрегаты по городам и аномалии температуры).

## Стек

Airflow, dbt, PostgreSQL, Docker, Metabase, Python

## Как запустить

Нужен Docker

```bash
cp .env.example .env
docker compose build
docker compose up -d
```

Потом:

- Airflow — http://localhost:8080 (admin / admin)
- Metabase — http://localhost:3000
- Postgres — localhost:5433, база `weather`, юзер/пароль `weather`

Дальше модно заходить в Airflow, включать DAG `weather_etl` и жать Trigger.  
чтобы залить историю за период:

```bash
docker compose exec airflow-scheduler airflow dags backfill weather_etl -s 2024-06-01 -e 2024-06-10
```

## Metabase

При первой настройке подключить Postgres: host `postgres`, port `5432`,
база/юзер/пароль — `weather`. Дашборды млжно строить на `marts.daily_city_weather` и
`marts.weather_anomalies`.

