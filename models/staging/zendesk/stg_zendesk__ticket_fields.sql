with source as {{ zendesk_import('zendesk_ticket_fields') }},
renamed as (
    select
        raw_data:id::number as field_id,
        raw_data:type::string as type,
        raw_data:title::string as title,
        raw_data:raw_title::string as raw_title,
        raw_data:description::string as description,
        raw_data:active::boolean as active,
        raw_data:required::boolean as required,
        raw_data:collapsed_for_agents::boolean as collapsed_for_agents,
        raw_data:regexp_for_validation::string as regexp_for_validation,
        raw_data:title_in_portal::string as title_in_portal,
        raw_data:visible_in_portal::boolean as visible_in_portal,
        raw_data:editable_in_portal::boolean as editable_in_portal,
        raw_data:required_in_portal::boolean as required_in_portal,
        raw_data:tag::string as tag,
        raw_data:custom_field_options::string as custom_field_options,
        try_to_timestamp_ntz(raw_data:created_at::string) as created_at,
        try_to_timestamp_ntz(raw_data:updated_at::string) as updated_at,
        try_to_timestamp_ntz(raw_data:datalake_updated_at::string) as datalake_updated_at,
        _snowflake_loaded_at
    from source
    where raw_data:id is not null
)
select *
from renamed
qualify row_number() over (partition by field_id order by updated_at desc nulls last, _snowflake_loaded_at desc) = 1
