with hourly as (select * from {{ ref('int_hourly_metrics') }}),
machines as (select * from {{ ref('stg_machines') }}),
latest_hour as (
    select machine_id, max(hour_timestamp) as latest_timestamp
    from hourly group by machine_id
),
current_metrics as (
    select h.* from hourly h
    inner join latest_hour l on h.machine_id = l.machine_id and h.hour_timestamp = l.latest_timestamp
),
daily_summary as (
    select machine_id,
        avg(avg_temp_c) as avg_temp_24h, max(max_temp_c) as max_temp_24h,
        avg(avg_vibration) as avg_vibration_24h, avg(avg_power_kw) as avg_power_24h,
        avg(running_pct) as avg_availability_24h,
        sum(error_count) as total_errors_24h, sum(sensor_glitch_count) as total_glitches_24h
    from hourly
    where hour_timestamp >= (select max(hour_timestamp) - interval '24 hours' from hourly)
    group by machine_id
)
select
    c.machine_id, c.site, c.model, m.install_date, m.last_maintenance,
    c.avg_temp_c as current_temp, c.avg_vibration as current_vibration,
    c.avg_power_kw as current_power, c.avg_throughput as current_throughput,
    c.running_pct as current_availability, c.error_count as current_errors,
    c.hour_timestamp as last_reading_time,
    d.avg_temp_24h, d.max_temp_24h, d.avg_vibration_24h, d.avg_power_24h,
    d.avg_availability_24h, d.total_errors_24h, d.total_glitches_24h,
    case
        when c.error_count > 0 then 'critical'
        when c.avg_temp_c > 85 then 'critical'
        when c.avg_temp_c > 75 then 'warning'
        when c.avg_vibration > 5 then 'warning'
        when c.running_pct < 80 then 'warning'
        else 'healthy'
    end as health_status
from current_metrics c
left join daily_summary d on c.machine_id = d.machine_id
left join machines m on c.machine_id = m.machine_id
