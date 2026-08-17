with stg as (
    select * from {{ ref('stg_weather') }}
)

select
    city_id,
    city_name,
    weather_date,
    count(*)                                   as hours_count,
    round(avg(temperature)::numeric, 2)        as avg_temp,
    min(temperature)                           as min_temp,
    max(temperature)                           as max_temp,
    round(avg(humidity)::numeric, 2)           as avg_humidity,
    round(sum(precipitation)::numeric, 2)      as total_precip,
    round(max(wind_speed)::numeric, 2)         as max_wind
from stg
group by city_id, city_name, weather_date
