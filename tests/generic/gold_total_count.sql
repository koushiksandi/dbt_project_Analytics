{% test gold_total_count(sourcemodel, destinationmodel) %}

with src as (
    select 
        count(*) as srccount
    from {{ sourcemodel }}
),

dest as (
    select count(*) as destcount
    from {{ destinationmodel }}
    where expiry = 'Current details'
)

select *
from src
cross join dest
where src.srccount <> dest.destcount

{% endtest %}