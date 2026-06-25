{{ config(materialized='incremental', unique_key='id') }}

select
    id,
    audit_id,
    ticket_id,
    author_id,
    is_public,
    via,
    type,
    created_at,
    datalake_updated_at,
    _snowflake_loaded_at
from {{ ref('int_zendesk__ticket_comments') }}
{% if is_incremental() %}
where _snowflake_loaded_at >= (select coalesce(max(_snowflake_loaded_at), '1900-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
