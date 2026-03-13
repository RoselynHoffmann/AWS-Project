with telemetry as (
    select * from {{ ref('stg_telemetry') }}
),
with_rolling as (
    select *,
        avg(temperature_c) over (partition by machine_id order by reading_timestamp rows between 60 preceding and current row) as rolling_avg_temp,
        avg(vibration_mm_s) over (partition by machine_id order by reading_timestamp rows between 60 preceding and current row) as rolling_avg_vibration
    from telemetry
    where temperature_c is not null
),
flagged as (
    select *,
        case when temperature_c - rolling_avg_temp > 10 then true else false end as is_temp_anomaly,
        case when vibration_mm_s > rolling_avg_vibration * 2 then true else false end as is_vibration_anomaly,
        case when status = 'error' then true else false end as is_error_state,
        (case when temperature_c - rolling_avg_temp > 10 then 1 else 0 end)
        + (case when vibration_mm_s > rolling_avg_vibration * 2 then 1 else 0 end)
        + (case when status = 'error' then 1 else 0 end) as anomaly_severity
    from with_rolling
)
select * from flagged where anomaly_severity > 0
