import logging
import os

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

_COLUMNS = [
    "city_id",
    "city_name",
    "ts",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "source_date",
]

_UPSERT_SQL = f"""
INSERT INTO raw.weather_hourly ({", ".join(_COLUMNS)})
VALUES %s
ON CONFLICT (city_id, ts) DO UPDATE SET
    temperature_2m       = EXCLUDED.temperature_2m,
    relative_humidity_2m = EXCLUDED.relative_humidity_2m,
    precipitation        = EXCLUDED.precipitation,
    wind_speed_10m       = EXCLUDED.wind_speed_10m,
    source_date          = EXCLUDED.source_date,
    loaded_at            = now();
"""


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("WAREHOUSE_HOST", "localhost"),
        port=os.environ.get("WAREHOUSE_PORT", "5432"),
        dbname=os.environ.get("WAREHOUSE_DB", "weather"),
        user=os.environ.get("WAREHOUSE_USER", "weather"),
        password=os.environ.get("WAREHOUSE_PASSWORD", "weather"),
    )


def load_rows(rows):
    if not rows:
        logger.warning("no rows to load")
        return 0

    values = [tuple(row[col] for col in _COLUMNS) for row in rows]

    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, _UPSERT_SQL, values, page_size=1000)
        logger.info("loaded %d rows", len(values))
        return len(values)
    finally:
        conn.close()
