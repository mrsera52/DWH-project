with daily as (
    select * from {{ ref('daily_city_weather') }}
),

with_moving_avg as (
    select
        city_id,
        city_name,
        weather_date,
        avg_temp,
        round(
            avg(avg_temp) over (
                partition by city_id
                order by weather_date
                rows between 6 preceding and current row
            )::numeric,
            2
        ) as ma7_temp,
        count(*) over (
            partition by city_id
            order by weather_date
            rows between 6 preceding and current row
        ) as window_days
    from daily
)

select
    city_id,
    city_name,
    weather_date,
    avg_temp,
    ma7_temp,
    round((avg_temp - ma7_temp)::numeric, 2) as temp_deviation,
    case
        when window_days >= 7 and abs(avg_temp - ma7_temp) >= 5 then true
        else false
    end as is_anomaly
from with_moving_avg
