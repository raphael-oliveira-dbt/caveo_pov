{% macro create_zendesk_masking_policies() %}
  {% if target.type != 'snowflake' %}
    {{ return('select 1') }}
  {% endif %}

  {% set policy_schema = target.schema %}
  {% set policy_prefix = target.database ~ '.' ~ policy_schema %}

  {% set statements = [
    "create masking policy if not exists " ~ policy_prefix ~ ".mask_zendesk_user_name_pii as (val string) returns string -> case when current_role() in ('ROLE_DATA_CX', 'ROLE_DOMAIN_CX_PII', 'ACCOUNTADMIN') then val else '**********' end",
    "create masking policy if not exists " ~ policy_prefix ~ ".mask_zendesk_user_email_pii as (val string) returns string -> case when current_role() in ('ROLE_DATA_CX', 'ROLE_DOMAIN_CX_PII', 'ACCOUNTADMIN') then val else sha2(val) end",
    "create masking policy if not exists " ~ policy_prefix ~ ".mask_zendesk_user_phone_pii as (val string) returns string -> case when current_role() in ('ROLE_DATA_CX', 'ROLE_DOMAIN_CX_PII', 'ACCOUNTADMIN') then val else sha2(val) end",
    "create masking policy if not exists " ~ policy_prefix ~ ".mask_zendesk_ticket_description_pii as (val string) returns string -> case when current_role() in ('ROLE_DATA_CX', 'ROLE_DOMAIN_CX_PII', 'ACCOUNTADMIN') then val else '**********' end",
    "create masking policy if not exists " ~ policy_prefix ~ ".mask_zendesk_ticket_subject_pii as (val string) returns string -> case when current_role() in ('ROLE_DATA_CX', 'ROLE_DOMAIN_CX_PII', 'ACCOUNTADMIN') then val else iff(val is null, null, regexp_replace(val, '.', '*')) end",
    "create masking policy if not exists " ~ policy_prefix ~ ".mask_zendesk_comment_body_pii as (val string) returns string -> case when current_role() in ('ROLE_DATA_CX', 'ROLE_DOMAIN_CX_PII', 'ACCOUNTADMIN') then val else '**********' end"
  ] %}

  {% for statement in statements %}
    {% do run_query(statement) %}
  {% endfor %}

  {{ return('select 1') }}
{% endmacro %}

{% macro apply_masking_policy(relation, column_name, policy_name) %}
  {% if target.type == 'snowflake' %}
    {% set qualified_policy_name = relation.database ~ '.' ~ relation.schema ~ '.' ~ policy_name %}
    alter table {{ relation }} modify column {{ column_name }} set masking policy {{ qualified_policy_name }}
  {% else %}
    select 1
  {% endif %}
{% endmacro %}
