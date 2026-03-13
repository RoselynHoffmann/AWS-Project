with source as (
    select * from {{ source('raw', 'raw_telemetry') }}
),
cleaned as (
    select
        machine_id, site, model,
        cast(timestamp as timestamp) as reading_timestamp,
        case when temperature_c = -9999.0 then null else temperature_c end as temperature_c,
        vibration_mm_s, power_draw_kw, throughput_samples_hr,
        status, error_code, _ingested_at
    from source
)
select * from cleaned
