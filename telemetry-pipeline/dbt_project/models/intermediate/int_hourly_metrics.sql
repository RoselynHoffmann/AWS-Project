with telemetry as (
    select * from {{ ref('stg_telemetry') }}
)
select
    machine_id, site, model,
    date_trunc('hour', reading_timestamp) as hour_timestamp,
    avg(temperature_c) as avg_temp_c,
    min(temperature_c) as min_temp_c,
    max(temperature_c) as max_temp_c,
    avg(vibration_mm_s) as avg_vibration,
    max(vibration_mm_s) as max_vibration,
    avg(power_draw_kw) as avg_power_kw,
    max(power_draw_kw) as max_power_kw,
    avg(throughput_samples_hr) as avg_throughput,
    count(case when status = 'running' then 1 end) * 100.0 / count(*) as running_pct,
    count(case when status = 'error' then 1 end) as error_count,
    count(case when temperature_c is null then 1 end) as sensor_glitch_count,
    count(*) as reading_count
from telemetry
group by machine_id, site, model, date_trunc('hour', reading_timestamp)
