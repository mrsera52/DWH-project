CREATE ROLE weather WITH LOGIN PASSWORD 'weather';
CREATE DATABASE weather OWNER weather;

\connect weather

SET ROLE weather;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.weather_hourly (
    city_id              INTEGER      NOT NULL,
    city_name            TEXT         NOT NULL,
    ts                   TIMESTAMP    NOT NULL,
    temperature_2m       DOUBLE PRECISION,
    relative_humidity_2m DOUBLE PRECISION,
    precipitation        DOUBLE PRECISION,
    wind_speed_10m       DOUBLE PRECISION,
    source_date          DATE         NOT NULL,
    loaded_at            TIMESTAMP    NOT NULL DEFAULT now(),
    PRIMARY KEY (city_id, ts)
);

CREATE INDEX IF NOT EXISTS ix_weather_hourly_source_date
    ON raw.weather_hourly (source_date);
