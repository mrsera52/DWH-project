# Weather DWH — ETL-пайплайн погодных данных

Учебный, но «взрослый» дата-инженерный пет-проект: ежедневный пайплайн, который
собирает почасовую погоду по нескольким городам из **Open-Meteo API**, складывает
её в слоистое хранилище на PostgreSQL, трансформирует через **dbt**, проверяет
качество данных и отдаёт витрины в дашборд **Metabase**. Оркестрация — **Apache
Airflow**. Всё поднимается одной командой через **Docker Compose**.

## Архитектура

```
        Open-Meteo Archive API  (почасовая погода по N городам)
                     │  Python: requests (ретраи), инкремент по дате
                     ▼
   ┌──────────────────── Apache Airflow (DAG weather_etl, @daily) ───────────────────┐
   │   extract_load_raw  ──►  dbt run (staging → marts)  ──►  dbt test (DQ)           │
   └──────────────────────────────────────────────────────────────────────────────────┘
                     ▼
              PostgreSQL (БД weather)
              ├─ raw       raw.weather_hourly     — сырьё «как из API», UPSERT
              ├─ staging   stg_weather            — типизация, чистка, дедуп (dbt view)
              └─ marts     daily_city_weather     — агрегаты по городу и дню (dbt table)
                           weather_anomalies      — аномалии vs скользящее среднее
                     ▼
              Metabase  (дашборд: тренды, сравнение городов, аномалии)
```

Отдельная БД `airflow` хранит метаданные оркестратора — они не смешиваются с данными DWH.

## Стек

| Слой            | Технология                          |
|-----------------|-------------------------------------|
| Оркестрация     | Apache Airflow 2.9 (LocalExecutor)  |
| Хранилище / DWH | PostgreSQL 16 (raw / staging / marts) |
| Трансформации   | dbt (dbt-postgres) + dbt_utils      |
| Качество данных | dbt tests + source freshness        |
| Визуализация    | Metabase                            |
| Инфраструктура  | Docker Compose                      |

## Ключевые дата-инженерные практики

- **Инкрементальная загрузка** — DAG грузит только целевую дату (`data_interval_start`), а не всю историю каждый раз.
- **Идемпотентность** — `INSERT ... ON CONFLICT (city_id, ts) DO UPDATE`: повторный запуск за ту же дату не плодит дублей.
- **Слоистое хранилище** — raw → staging → marts; вся бизнес-логика в dbt, а не в Python.
- **Тесты качества** — `not_null`, уникальность ключей, допустимые диапазоны (`accepted_range`), свежесть источника (`freshness`).
- **Оконные функции** — 7-дневное скользящее среднее и детекция аномалий в `weather_anomalies`.

## Быстрый старт

Нужен установленный Docker Desktop.

```bash
cp .env.example .env
docker compose build
docker compose up -d
```

Первый старт занимает несколько минут (сборка образа, миграции Airflow). Когда
контейнеры поднимутся:

- **Airflow** — http://localhost:8080 (логин/пароль `admin` / `admin`)
- **Metabase** — http://localhost:3000 (первичная настройка через UI)
- **PostgreSQL** — `localhost:5433` (наружу проброшен порт 5433), БД `weather`, юзер `weather` / `weather`

### Запуск пайплайна

1. Открой Airflow → включи DAG `weather_etl` (снять паузу).
2. Нажми ▶ **Trigger DAG**, чтобы прогнать за сегодня (с учётом лага Archive API в 5 дней).
3. Загрузить историю (бэкфилл за диапазон дат):

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill weather_etl -s 2024-01-01 -e 2024-01-31
```

### Подключение Metabase к хранилищу

В мастере Metabase выбери **PostgreSQL** и укажи:

- Host: `postgres`, Port: `5432`
- Database: `weather`, User: `weather`, Password: `weather`

Строй дашборд на таблицах `marts.daily_city_weather` и `marts.weather_anomalies`.

## Структура репозитория

```
DWH-project/
├─ docker-compose.yml          # postgres, airflow (init/web/scheduler), metabase
├─ Dockerfile.airflow          # образ Airflow + ETL-пакеты + dbt в изолированном venv
├─ requirements-airflow.txt    # requests, psycopg2, pyyaml
├─ .env.example
├─ init-db/
│  └─ 01_init.sql              # БД weather, схемы raw/staging/marts, raw-таблица
├─ dags/
│  └─ weather_etl.py           # DAG: extract_load_raw → dbt run → dbt test
├─ etl/
│  ├─ extract.py              # запрос к Open-Meteo (ретраи, инкремент по дате)
│  ├─ load.py                 # UPSERT в raw (идемпотентность)
│  └─ cities.yml              # список городов
└─ dbt/
   ├─ dbt_project.yml
   ├─ profiles.yml
   ├─ packages.yml            # dbt_utils
   ├─ macros/
   │  └─ generate_schema_name.sql
   └─ models/
      ├─ staging/  (_sources.yml, stg_weather.sql, _stg_weather.yml)
      └─ marts/    (daily_city_weather.sql, weather_anomalies.sql, _marts.yml)
```

## Локальная проверка компонентов

```bash
# Проверить экстракцию без Airflow (нужен Python + requests + pyyaml):
python etl/extract.py

# Прогнать dbt вручную внутри контейнера:
docker compose exec airflow-scheduler bash -lc \
  'cd /opt/airflow/dbt && DBT_PROFILES_DIR=/opt/airflow/dbt /opt/dbt_venv/bin/dbt deps && \
   DBT_PROFILES_DIR=/opt/airflow/dbt /opt/dbt_venv/bin/dbt build'
```

## Возможные доработки (роадмап)

- ClickHouse как аналитическое хранилище для витрин
- Great Expectations для более строгих проверок качества
- Kafka + потоковая загрузка вместо batch
- CI (dbt build на PR), алерты в Telegram при падении DAG
