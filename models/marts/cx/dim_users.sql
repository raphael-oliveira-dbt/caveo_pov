{{ config(
    post_hook=[
        apply_masking_policy(this, 'email', 'mask_zendesk_user_email_pii'),
        apply_masking_policy(this, 'phone', 'mask_zendesk_user_phone_pii')
    ]
) }}

select
    user_id,
    role,
    locale,
    time_zone,
    tags,
    user_fields,
    active,
    verified,
    suspended,
    moderator,
    restricted_agent,
    shared,
    shared_agent,
    ticket_restriction,
    organization_id,
    external_id,
    email,
    phone,
    created_at,
    updated_at,
    datalake_updated_at,
    _snowflake_loaded_at
from {{ ref('stg_zendesk__users') }}
