with weather_data as (
    select * from {{ ref('stg_raw_weather') }}
)

select distinct
    station_id,
    first_value(created_at) over (partition by station_id order by created_at) as first_record_date,
    last_value(created_at) over (partition by station_id order by created_at) as last_record_date
from weather_data