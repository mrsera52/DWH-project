with source as (
    select * from {{ source('raw', 'weather_hourly') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by city_id, ts
            order by loaded_at desc
        ) as rn
    from source
)

select
    city_id,
    city_name,
    ts,
    ts::date                          as weather_date,
    temperature_2m                    as temperature,
    relative_humidity_2m              as humidity,
    precipitation,
    wind_speed_10m                    as wind_speed,
    loaded_at
from deduplicated
where rn = 1
