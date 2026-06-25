with source as {{ zendesk_import('zendesk_groups') }},
renamed as (
    select
        raw_data:id::number as group_id,
        upper(trim(raw_data:name::string)) as name,
        raw_data:description::string as description,
        raw_data:default::boolean as is_default,
        raw_data:deleted::boolean as deleted,
        try_to_timestamp_ntz(raw_data:created_at::string) as created_at,
        try_to_timestamp_ntz(raw_data:updated_at::string) as updated_at,
        try_to_timestamp_ntz(raw_data:datalake_updated_at::string) as datalake_updated_at,
        _snowflake_loaded_at
    from source
    where raw_data:id is not null
)
select *
from renamed
qualify row_number() over (partition by group_id order by updated_at desc nulls last, _snowflake_loaded_at desc) = 1
