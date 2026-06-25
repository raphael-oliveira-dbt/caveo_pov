{{ config(materialized='incremental', unique_key=['id', 'field_name']) }}

select
    id,
    ticket_id,
    field_name,
    value,
    author_id,
    created_at,
    datalake_updated_at,
    _snowflake_loaded_at
from {{ ref('int_zendesk__ticket_events_unpivoted') }}
{% if is_incremental() %}
where _snowflake_loaded_at >= (select coalesce(max(_snowflake_loaded_at), '1900-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
