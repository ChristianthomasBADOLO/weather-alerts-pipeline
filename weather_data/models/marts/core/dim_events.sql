{{
    config(
        materialized='table',
        tags=['core', 'dimension']
    )
}}

with alerts as (
    select * from {{ ref('stg_weather_alerts') }}
)

select distinct
    event,
    category,
    count(*) over(partition by event) as total_occurrences,
    min(sent) over(partition by event) as first_occurrence,
    max(sent) over(partition by event) as last_occurrence,
    count(distinct severity) over(partition by event) as severity_variations,
    mode(severity) over(partition by event) as typical_severity,
    mode(urgency) over(partition by event) as typical_urgency
from alerts
where status = 'Actual'