{% test silver_count(sourcemodel, destinationmodel)%}

with src as (
    select count(*) as srccount
    from {{ sourcemodel }}
    where cast(updated_at as date) = current_date
),

dest as (
    select count(*) as destcount
    from {{ destinationmodel }}
    where cast(updated_at as date) = current_date
)

select *
from src
cross join dest
where src.srccount <> dest.destcount

{% endtest %}