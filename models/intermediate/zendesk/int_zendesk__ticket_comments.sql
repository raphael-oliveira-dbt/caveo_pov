with audits as (
    select audit_id, ticket_id, datalake_updated_at, _snowflake_loaded_at, child_events
    from {{ ref('stg_zendesk__tickets_events') }}
),
flat as (
    select
        a.audit_id,
        a.ticket_id,
        a.datalake_updated_at,
        a._snowflake_loaded_at,
        ce.value as evt
    from audits a,
        lateral flatten(input => try_parse_json(a.child_events), outer => true) ce
    where ce.value:event_type::string = 'Comment'
      and ce.value:body is not null
)
select
    evt:id::number as id,
    audit_id,
    ticket_id,
    evt:author_id::number as author_id,
    evt:public::boolean as is_public,
    evt:via::string as via,
    evt:type::string as type,
    evt:body::string as body,
    evt:html_body::string as html_body,
    evt:plain_body::string as plain_body,
    try_to_timestamp_ntz(evt:created_at::string) as created_at,
    datalake_updated_at,
    _snowflake_loaded_at
from flat
qualify row_number() over (partition by evt:id::number order by _snowflake_loaded_at desc nulls last) = 1
