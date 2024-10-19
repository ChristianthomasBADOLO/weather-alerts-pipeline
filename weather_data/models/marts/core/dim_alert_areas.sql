with alerts as (
    select * from {{ ref('stg_weather_alerts') }}
)

select distinct
    areadesc,
    same_codes,
    ugc_codes,
    coordinates,
    min(sent) over (partition by areadesc) as first_alert_date,
    max(sent) over (partition by areadesc) as last_alert_date,
    count(*) over (partition by areadesc) as total_alerts
from alerts