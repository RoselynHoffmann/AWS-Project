with hourly as (select * from {{ ref('int_hourly_metrics') }}),
machines as (select * from {{ ref('stg_machines') }})
select h.machine_id, h.site, h.model, h.hour_timestamp,
    h.avg_temp_c, h.min_temp_c, h.max_temp_c,
    h.avg_vibration, h.max_vibration, h.avg_power_kw, h.avg_throughput,
    h.running_pct, h.error_count, h.sensor_glitch_count, h.reading_count,
    m.install_date, m.last_maintenance
from hourly h left join machines m on h.machine_id = m.machine_id
order by h.machine_id, h.hour_timestamp
