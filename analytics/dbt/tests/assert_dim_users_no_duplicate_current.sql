select
    user_id_hashed,
    count(*) as current_record_count
from {{ ref('dim_users') }}
where is_current = true
group by user_id_hashed
having count(*) > 1
