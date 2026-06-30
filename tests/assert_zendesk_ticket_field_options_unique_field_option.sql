select
    field_id,
    option_value,
    count(*) as total_registros
from {{ ref('zendesk_ticket_field_options') }}
group by 1, 2
having count(*) > 1
