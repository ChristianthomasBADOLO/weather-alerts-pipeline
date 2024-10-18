with weather_data as (
    select * from {{ ref('stg_raw_weather') }}
)

select
    date,
    station_id,
    avg(temperature) as avg_daily_temp,
    max(temperature) as max_daily_temp,
    min(temperature) as min_daily_temp,
    avg(humidity) as avg_humidity,
    sum(precipitation) as total_precipitation
from weather_data
group by date, station_id