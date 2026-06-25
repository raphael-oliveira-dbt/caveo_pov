with source as {{ zendesk_import('zendesk_users') }},
renamed as (
    select
        raw_data:id::number as user_id,
        upper(trim(raw_data:name::string)) as name,
        lower(trim(raw_data:email::string)) as email,
        upper(trim(raw_data:role::string)) as role,
        raw_data:locale::string as locale,
        raw_data:time_zone::string as time_zone,
        raw_data:tags::string as tags,
        raw_data:user_fields::string as user_fields,
        raw_data:active::boolean as active,
        raw_data:verified::boolean as verified,
        raw_data:suspended::boolean as suspended,
        raw_data:moderator::boolean as moderator,
        raw_data:restricted_agent::boolean as restricted_agent,
        raw_data:shared::boolean as shared,
        raw_data:shared_agent::boolean as shared_agent,
        raw_data:ticket_restriction::string as ticket_restriction,
        raw_data:organization_id::number as organization_id,
        raw_data:external_id::string as external_id,
        raw_data:phone::string as phone,
        try_to_timestamp_ntz(raw_data:created_at::string) as created_at,
        try_to_timestamp_ntz(raw_data:updated_at::string) as updated_at,
        try_to_timestamp_ntz(raw_data:datalake_updated_at::string) as datalake_updated_at,
        _snowflake_loaded_at
    from source
    where raw_data:id is not null
)
select *
from renamed
qualify row_number() over (partition by user_id order by updated_at desc nulls last, _snowflake_loaded_at desc) = 1
