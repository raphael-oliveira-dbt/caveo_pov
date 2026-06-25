with tickets as (
    select ticket_id, custom_fields_json
    from {{ ref('stg_zendesk__tickets') }}
),
field_map as (
    select field_id::string as field_id, column_name
    from {{ ref('zendesk_custom_field_map') }}
    where used_in = 'tickets'
),
flattened as (
    select
        t.ticket_id,
        f.value:id::string as field_id,
        f.value:value::string as field_value
    from tickets t,
        lateral flatten(input => try_parse_json(t.custom_fields_json)) f
),
filtered as (
    select fl.ticket_id, fl.field_id, fl.field_value, fm.column_name
    from flattened fl
    inner join field_map fm
        on fl.field_id = fm.field_id
),
pivoted as (
    select
        ticket_id,
        max(case when column_name = 'CF_ACIONAR_CONTABILIDADE' then field_value end) as cf_acionar_contabilidade,
        max(case when column_name = 'CF_MOTIVO_DE_CONTATO' then field_value end) as cf_motivo_de_contato,
        max(case when column_name = 'CF_TAG_DE_ATENDIMENTO' then field_value end) as cf_tag_de_atendimento,
        max(case when column_name = 'CF_TAG_DE_ATENDIMENTO_2' then field_value end) as cf_tag_de_atendimento_2,
        max(case when column_name = 'CF_ADICIONAR_SEGUNDO_PERIODO_FERIAS' then field_value end) as cf_adicionar_segundo_periodo_ferias,
        max(case when column_name = 'CF_ADICIONAR_TERCEIRO_PERIODO_FERIAS' then field_value end) as cf_adicionar_terceiro_periodo_ferias,
        max(case when column_name = 'CF_CONVERSATION_ID' then field_value end) as cf_conversation_id,
        max(case when column_name = 'CF_HSM_DATA' then field_value end) as cf_hsm_data,
        max(case when column_name = 'CF_HSM_DESCRICAO_TEMPLATE' then field_value end) as cf_hsm_descricao_template,
        max(case when column_name = 'CF_HSM_STATUS' then field_value end) as cf_hsm_status,
        max(case when column_name = 'CF_HSM_TEMPLATE' then field_value end) as cf_hsm_template,
        max(case when column_name = 'CF_WHATSAPP_ID' then field_value end) as cf_whatsapp_id,
        max(case when column_name = 'CF_MIGRACAO_DE_AREA' then field_value end) as cf_migracao_de_area,
        max(case when column_name = 'CF_TAG_DE_ATENDIMENTO_3' then field_value end) as cf_tag_de_atendimento_3,
        max(case when column_name = 'CF_RENOVOU_CERTIFICADO_ECNPJ' then field_value end) as cf_renovou_certificado_ecnpj,
        max(case when column_name = 'CF_ATRIBUICAO_AUTOMATICA' then field_value end) as cf_atribuicao_automatica,
        max(case when column_name = 'CF_PIECE_OF_CAKE_DISPARO' then field_value end) as cf_piece_of_cake_disparo
    from filtered
    group by 1
)
select * from pivoted
