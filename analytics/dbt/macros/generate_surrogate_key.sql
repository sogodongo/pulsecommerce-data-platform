-- =============================================================================
-- macros/generate_surrogate_key.sql
-- =============================================================================
-- Generates a deterministic surrogate key by hashing a list of field
-- expressions using SHA-2(256). Null values are coalesced to the literal
-- string '_null_' before hashing to avoid NULL propagation.
--
-- Usage:
--   {{ generate_surrogate_key(['user_id_hashed']) }}
--   {{ generate_surrogate_key(['order_id', 'product_sku']) }}
--   {{ generate_surrogate_key(['oc.country']) }}
-- =============================================================================

{% macro generate_surrogate_key(field_list) %}

  sha2(
    concat_ws(
      '||',
      {% for field in field_list %}
        coalesce(cast({{ field }} as string), '_null_')
        {%- if not loop.last %}, {% endif %}
      {% endfor %}
    ),
    256
  )

{% endmacro %}
