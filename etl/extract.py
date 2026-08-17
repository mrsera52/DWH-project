import logging
from pathlib import Path

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
]

CITIES_PATH = Path(__file__).resolve().parent / "cities.yml"


def load_cities(path=CITIES_PATH):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)["cities"]


def _build_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_city_weather(city, target_date, session=None):
    session = session or _build_session()
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": target_date,
        "end_date": target_date,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "UTC",
    }
    resp = session.get(ARCHIVE_URL, params=params, timeout=60)
    resp.raise_for_status()
    payload = resp.json()

    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []

    rows = []
    for i, ts in enumerate(times):
        rows.append(
            {
                "city_id": city["id"],
                "city_name": city["name"],
                "ts": ts,
                "temperature_2m": _value_at(hourly, "temperature_2m", i),
                "relative_humidity_2m": _value_at(hourly, "relative_humidity_2m", i),
                "precipitation": _value_at(hourly, "precipitation", i),
                "wind_speed_10m": _value_at(hourly, "wind_speed_10m", i),
                "source_date": target_date,
            }
        )

    logger.info("%s (%s): %d rows for %s", city["name"], city["id"], len(rows), target_date)
    return rows


def _value_at(hourly, key, index):
    values = hourly.get(key) or []
    if index < len(values):
        return values[index]
    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sample = fetch_city_weather(load_cities()[0], "2024-01-15")
    print(f"rows: {len(sample)}")
    if sample:
        print(sample[0])
