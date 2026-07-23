{% test row_level_with_id (column_name, sourcemodel, destinationmodel) %}

select {{ column_name }} from {{ sourcemodel }}
except
select {{ column_name }} from {{ destinationmodel }}

{% endtest %}