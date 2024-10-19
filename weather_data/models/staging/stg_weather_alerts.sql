{{
    config(
        materialized='view',
        tags=['staging', 'weather', 'alerts']
    )
}}

with source as (
    select * from {{ source('raw', 'weather_alerts') }}
),

validated as (
    select
        id,
        areadesc,
        same_codes,
        ugc_codes,
        affectedzones,
        sent,
        effective,
        onset,
        expires,
        ends,
        status,
        messagetype,
        category,
        severity,
        certainty,
        urgency,
        event,
        headline,
        coordinates,
        -- Ajout de champs dérivés utiles
        datediff('hour', effective, coalesce(ends, expires)) as alert_duration_hours,
        case 
            when status = 'Actual' and messagetype = 'Alert' then 'New'
            when status = 'Actual' and messagetype = 'Update' then 'Updated'
            when status = 'Actual' and messagetype = 'Cancel' then 'Cancelled'
            else 'Other'
        end as alert_status
    from source
)

select * from validated