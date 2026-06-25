with audits as (
    select *
    from {{ ref('stg_zendesk__tickets_events') }}
),
flat as (
    select
        a.audit_id,
        a.ticket_id,
        a.updater_id,
        a.created_at,
        a.datalake_updated_at,
        a._snowflake_loaded_at,
        ce.value as evt
    from audits a,
        lateral flatten(input => try_parse_json(a.child_events), outer => true) ce
    where ce.value is not null
),
legacy as (
    select
        audit_id as id,
        ticket_id,
        legacy_field_name as field_name,
        legacy_value as value,
        legacy_author_id as author_id,
        created_at,
        datalake_updated_at,
        _snowflake_loaded_at
    from audits
    where legacy_field_name is not null
),
extracted as (
    select evt:id::number as id, ticket_id, 'status' as field_name, evt:status::string as value, updater_id as author_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:status is not null
    union all
    select evt:id::number, ticket_id, 'assignee_id', evt:assignee_id::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:assignee_id is not null
    union all
    select evt:id::number, ticket_id, 'group_id', evt:group_id::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:group_id is not null
    union all
    select evt:id::number, ticket_id, 'priority', evt:priority::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:priority is not null
    union all
    select evt:id::number, ticket_id, 'brand_id', evt:brand_id::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:brand_id is not null
    union all
    select evt:id::number, ticket_id, 'requester_id', evt:requester_id::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:requester_id is not null
    union all
    select evt:id::number, ticket_id, 'organization_id', evt:organization_id::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:organization_id is not null
    union all
    select evt:id::number, ticket_id, 'ticket_form_id', evt:ticket_form_id::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:ticket_form_id is not null
    union all
    select evt:id::number, ticket_id, 'is_public', evt:public::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:public is not null
    union all
    select evt:id::number, ticket_id, 'subject', evt:subject::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:subject is not null
    union all
    select evt:id::number, ticket_id, 'type', evt:type::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:type is not null
    union all
    select evt:id::number, ticket_id, 'custom_status_id', evt:custom_status_id::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:custom_status_id is not null
    union all
    select evt:id::number, ticket_id, 'sla_policy', evt:sla_policy::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:sla_policy is not null
    union all
    select evt:id::number, ticket_id, 'comment_present', evt:body::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:body is not null
    union all
    select evt:id::number, ticket_id, 'comment_public', evt:public::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:event_type::string = 'Comment' and evt:public is not null
    union all
    select evt:id::number, ticket_id, 'tags', evt:tags::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:tags is not null
    union all
    select evt:id::number, ticket_id, '36210587567380', evt:custom_ticket_fields:"36210587567380"::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:custom_ticket_fields:"36210587567380" is not null
    union all
    select evt:id::number, ticket_id, '36627293345556', evt:custom_ticket_fields:"36627293345556"::string, updater_id, created_at, datalake_updated_at, _snowflake_loaded_at from flat where evt:custom_ticket_fields:"36627293345556" is not null
),
combined as (
    select * from legacy
    union all
    select * from extracted
)
select *
from combined
qualify row_number() over (partition by id, coalesce(field_name, '') order by _snowflake_loaded_at desc) = 1
