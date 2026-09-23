{#
    Reusable range check. Nulls always pass (a missing value isn't an
    out-of-range value); `where` is an optional extra SQL filter, e.g. to
    restrict which rows this particular application of the test applies to.
#}
{% test test_value_between(model, column_name, min_value, max_value, where=none) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})
  {% if where %}
  and ({{ where }})
  {% endif %}

{% endtest %}
