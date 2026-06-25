with source as {{ zendesk_import('zendesk_tickets_events') }},
cx_filtered as (
    select e.*
    from source e
    where exists (
        select 1
        from {{ ref('stg_zendesk__tickets') }} t
        where t.ticket_id = e.raw_data:ticket_id::number
    )
),
renamed as (
    select
        raw_data:id::number as audit_id,
        raw_data:ticket_id::number as ticket_id,
        raw_data:updater_id::number as updater_id,
        raw_data:author_id::number as legacy_author_id,
        raw_data:field_name::string as legacy_field_name,
        raw_data:value::string as legacy_value,
        raw_data:child_events::string as child_events,
        try_to_timestamp_ntz(raw_data:created_at::string) as created_at,
        try_to_timestamp_ntz(raw_data:datalake_updated_at::string) as datalake_updated_at,
        _snowflake_loaded_at
    from cx_filtered
    where raw_data:id is not null
)
select * from renamed
