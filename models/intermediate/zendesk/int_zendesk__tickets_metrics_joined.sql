with legacy_metrics as (
    select *
    from {{ ref('stg_zendesk__ticket_metrics') }}
    where reply_time_in_minutes is not null
),
full_metrics as (
    select *
    from {{ ref('stg_zendesk__ticket_metrics_full') }}
),
unioned as (
    select *, 1 as source_priority from legacy_metrics
    union all
    select *, 2 as source_priority from full_metrics
),
deduped as (
    select *
    from unioned
    qualify row_number() over (
        partition by ticket_id
        order by source_priority desc, updated_at desc nulls last, _snowflake_loaded_at desc
    ) = 1
)
select
    ticket_id,
    reopens,
    replies,
    assignee_stations,
    group_stations,
    agent_wait_time_in_minutes,
    requester_wait_time_in_minutes,
    first_resolution_time_in_minutes,
    full_resolution_time_in_minutes,
    on_hold_time_in_minutes,
    reply_time_in_minutes,
    assigned_at,
    initially_assigned_at,
    solved_at,
    latest_comment_added_at,
    created_at as metric_created_at,
    updated_at as metric_updated_at,
    datalake_updated_at as metric_datalake_updated_at,
    _snowflake_loaded_at as metric_snowflake_loaded_at
from deduped
