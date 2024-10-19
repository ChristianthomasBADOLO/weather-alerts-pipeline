{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'metrics']
    )
}}

with alerts as (
    select * from {{ ref('stg_weather_alerts') }}
),

daily_metrics as (
    select
        date_trunc('day', sent) as alert_date,
        event,
        severity,
        certainty,
        urgency,
        count(*) as total_alerts,
        count(case when alert_status = 'New' then 1 end) as new_alerts,
        count(case when alert_status = 'Updated' then 1 end) as updated_alerts,
        count(case when alert_status = 'Cancelled' then 1 end) as cancelled_alerts,
        avg(alert_duration_hours) as avg_duration_hours,
        count(distinct areadesc) as affected_areas_count
    from alerts
    where status = 'Actual'
    group by 1, 2, 3, 4, 5
)

select * from daily_metrics