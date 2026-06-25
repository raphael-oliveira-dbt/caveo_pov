{% macro zendesk_relation(name) %}
  {% if var('zendesk_use_seeds', true) %}
    {{ ref(name) }}
  {% else %}
    {{ source('zendesk', name) }}
  {% endif %}
{% endmacro %}

{% macro zendesk_import(name) %}
(
  select
    {% if var('zendesk_use_seeds', true) %}
      parse_json(raw_data) as raw_data,
      _snowflake_loaded_at
    from {{ ref(name) }}
    {% else %}
      raw_data,
      _snowflake_loaded_at
    from {{ source('zendesk', name) }}
    {% endif %}
)
{% endmacro %}
