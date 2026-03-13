select
    machine_id, site, model,
    cast(install_date as date) as install_date,
    cast(last_maintenance as date) as last_maintenance
from {{ source('raw', 'raw_machines') }}
