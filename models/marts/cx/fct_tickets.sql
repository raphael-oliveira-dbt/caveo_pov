{{ config(
    materialized='incremental',
    unique_key='ticket_id',
    post_hook=[apply_masking_policy(this, 'subject', 'mask_zendesk_ticket_subject_pii')]
) }}

with tickets as (
    select *
    from {{ ref('stg_zendesk__tickets') }}
),
custom_fields as (
    select *
    from {{ ref('int_zendesk__tickets_custom_fields') }}
),
metrics as (
    select *
    from {{ ref('int_zendesk__tickets_metrics_joined') }}
),
final as (
    select
        t.ticket_id,
        t.subject,
        t.status,
        t.priority,
        t.brand_id,
        t.assignee_id,
        t.requester_id,
        t.submitter_id,
        t.organization_id,
        t.group_id,
        t.ticket_form_id,
        t.via,
        t.tags,
        t.satisfaction_rating,
        t.is_public,
        t.has_incidents,
        t.from_messaging_channel,
        cf.cf_acionar_contabilidade,
        cf.cf_motivo_de_contato,
        cf.cf_tag_de_atendimento,
        cf.cf_tag_de_atendimento_2,
        cf.cf_adicionar_segundo_periodo_ferias,
        cf.cf_adicionar_terceiro_periodo_ferias,
        cf.cf_conversation_id,
        cf.cf_hsm_data,
        cf.cf_hsm_descricao_template,
        cf.cf_hsm_status,
        cf.cf_hsm_template,
        cf.cf_whatsapp_id,
        cf.cf_migracao_de_area,
        cf.cf_tag_de_atendimento_3,
        cf.cf_renovou_certificado_ecnpj,
        cf.cf_atribuicao_automatica,
        cf.cf_piece_of_cake_disparo,
        t.custom_fields_json,
        m.reopens,
        m.replies,
        m.assignee_stations,
        m.group_stations,
        m.agent_wait_time_in_minutes,
        m.requester_wait_time_in_minutes,
        m.first_resolution_time_in_minutes,
        m.full_resolution_time_in_minutes,
        m.on_hold_time_in_minutes,
        m.reply_time_in_minutes,
        m.assigned_at,
        m.initially_assigned_at,
        m.solved_at,
        m.latest_comment_added_at,
        t.created_at,
        t.updated_at,
        t.datalake_updated_at,
        t._snowflake_loaded_at
    from tickets t
    left join custom_fields cf
        on t.ticket_id = cf.ticket_id
    left join metrics m
        on t.ticket_id = m.ticket_id
)
select *
from final
{% if is_incremental() %}
where _snowflake_loaded_at >= (select coalesce(max(_snowflake_loaded_at), '1900-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
