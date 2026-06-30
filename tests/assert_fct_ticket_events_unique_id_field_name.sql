select
    id,
    field_name,
    count(*) as total_registros
from {{ ref('fct_ticket_events') }}
group by 1, 2
having count(*) > 1
