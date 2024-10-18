with source as (
    select * from {{ source('raw', 'WEATHER_ALERTS') }}
),

renamed as (
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
        coordinates
    from source
)

select * from renamed