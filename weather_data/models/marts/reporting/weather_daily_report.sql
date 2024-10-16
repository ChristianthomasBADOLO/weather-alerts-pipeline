with daily_weather as (
    select * from {{ ref('int_daily_weather') }}
)

select
    date,
    count(distinct station_id) as nb_stations,
    avg(avg_daily_temp) as avg_temperature,
    avg(total_precipitation) as avg_precipitation,
    avg(avg_humidity) as avg_humidity
from daily_weather
group by date
order by date