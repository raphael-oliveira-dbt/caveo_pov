with source as {{ zendesk_import('zendesk_ticket_metrics') }},
renamed as (
    select
        raw_data:id::number as ticket_metric_id,
        raw_data:ticket_id::number as ticket_id,
        raw_data:reopens::number as reopens,
        raw_data:replies::number as replies,
        raw_data:assignee_stations::number as assignee_stations,
        raw_data:group_stations::number as group_stations,
        raw_data:agent_wait_time_in_minutes::string as agent_wait_time_in_minutes,
        raw_data:requester_wait_time_in_minutes::string as requester_wait_time_in_minutes,
        raw_data:first_resolution_time_in_minutes::string as first_resolution_time_in_minutes,
        raw_data:full_resolution_time_in_minutes::string as full_resolution_time_in_minutes,
        raw_data:on_hold_time_in_minutes::string as on_hold_time_in_minutes,
        raw_data:reply_time_in_minutes::string as reply_time_in_minutes,
        try_to_timestamp_ntz(raw_data:assigned_at::string) as assigned_at,
        try_to_timestamp_ntz(raw_data:initially_assigned_at::string) as initially_assigned_at,
        try_to_timestamp_ntz(raw_data:solved_at::string) as solved_at,
        try_to_timestamp_ntz(raw_data:latest_comment_added_at::string) as latest_comment_added_at,
        try_to_timestamp_ntz(raw_data:created_at::string) as created_at,
        try_to_timestamp_ntz(raw_data:updated_at::string) as updated_at,
        try_to_timestamp_ntz(raw_data:datalake_updated_at::string) as datalake_updated_at,
        _snowflake_loaded_at
    from source
    where raw_data:id is not null
)
select *
from renamed
qualify row_number() over (partition by ticket_metric_id order by updated_at desc nulls last, _snowflake_loaded_at desc) = 1
