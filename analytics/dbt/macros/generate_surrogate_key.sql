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
