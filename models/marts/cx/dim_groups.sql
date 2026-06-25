select
    group_id,
    name,
    description,
    is_default,
    deleted,
    created_at,
    updated_at,
    datalake_updated_at,
    _snowflake_loaded_at
from {{ ref('stg_zendesk__groups') }}
