with source as {{ zendesk_import('zendesk_organizations') }},
renamed as (
    select
        raw_data:id::number as organization_id,
        upper(trim(raw_data:name::string)) as name,
        raw_data:details::string as details,
        raw_data:notes::string as notes,
        raw_data:group_id::number as group_id,
        raw_data:shared_tickets::boolean as shared_tickets,
        raw_data:shared_comments::boolean as shared_comments,
        raw_data:tags::string as tags,
        try_to_timestamp_ntz(raw_data:created_at::string) as created_at,
        try_to_timestamp_ntz(raw_data:updated_at::string) as updated_at,
        try_to_timestamp_ntz(raw_data:datalake_updated_at::string) as datalake_updated_at,
        _snowflake_loaded_at
    from source
    where raw_data:id is not null
)
select *
from renamed
qualify row_number() over (partition by organization_id order by updated_at desc nulls last, _snowflake_loaded_at desc) = 1
