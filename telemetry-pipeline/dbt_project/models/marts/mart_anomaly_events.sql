with anomalies as (select * from {{ ref('int_anomaly_detection') }}),
error_codes as (select * from {{ source('raw', 'raw_error_codes') }})
select a.machine_id, a.site, a.reading_timestamp,
    a.temperature_c, a.vibration_mm_s, a.status, a.error_code,
    e.description as error_description,
    a.is_temp_anomaly, a.is_vibration_anomaly, a.is_error_state, a.anomaly_severity,
    case when a.anomaly_severity >= 2 then 'critical'
         when a.anomaly_severity = 1 then 'warning' else 'info' end as alert_level
from anomalies a left join error_codes e on a.error_code = e.code
order by a.reading_timestamp desc
