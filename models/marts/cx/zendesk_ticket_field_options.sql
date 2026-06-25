select distinct
    field_id,
    title as field_title,
    opt.value:value::string as option_value,
    opt.value:name::string as option_label
from {{ ref('stg_zendesk__ticket_fields') }},
    lateral flatten(input => try_parse_json(custom_field_options)) opt
where custom_field_options is not null
