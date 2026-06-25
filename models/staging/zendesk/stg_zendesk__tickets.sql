with source as {{ zendesk_import('zendesk_tickets') }},
cx_filtered as (
    select *
    from source
    where raw_data:brand_id::number in (34126647548180, 43696510017940)
),
renamed as (
    select
        raw_data:id::number as ticket_id,
        upper(trim(raw_data:subject::string)) as subject,
        raw_data:description::string as description,
        upper(trim(raw_data:status::string)) as status,
        upper(trim(raw_data:priority::string)) as priority,
        raw_data:brand_id::number as brand_id,
        raw_data:assignee_id::number as assignee_id,
        raw_data:requester_id::number as requester_id,
        raw_data:submitter_id::number as submitter_id,
        raw_data:organization_id::number as organization_id,
        raw_data:group_id::number as group_id,
        raw_data:ticket_form_id::number as ticket_form_id,
        raw_data:via::string as via,
        raw_data:tags::string as tags,
        raw_data:satisfaction_rating::string as satisfaction_rating,
        raw_data:is_public::boolean as is_public,
        raw_data:has_incidents::boolean as has_incidents,
        raw_data:from_messaging_channel::boolean as from_messaging_channel,
        raw_data:custom_fields::string as custom_fields_json,
        try_to_timestamp_ntz(raw_data:created_at::string) as created_at,
        try_to_timestamp_ntz(raw_data:updated_at::string) as updated_at,
        try_to_timestamp_ntz(raw_data:datalake_updated_at::string) as datalake_updated_at,
        _snowflake_loaded_at
    from cx_filtered
    where raw_data:id is not null
)
select *
from renamed
qualify row_number() over (partition by ticket_id order by updated_at desc nulls last, _snowflake_loaded_at desc) = 1
