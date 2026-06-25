select
    user_id as agent_id,
    name,
    external_id,
    role,
    locale,
    time_zone,
    active,
    verified,
    suspended,
    organization_id,
    created_at,
    updated_at,
    datalake_updated_at,
    _snowflake_loaded_at
from {{ ref('stg_zendesk__users') }}
where role = 'AGENT'
