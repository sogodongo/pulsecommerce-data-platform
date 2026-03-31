-- =============================================================================
-- tests/assert_dim_users_no_duplicate_current.sql
-- =============================================================================
-- Data test: each user_id_hashed must have exactly one is_current=true row
-- in dim_users. Multiple current rows = broken SCD2 merge logic.
-- Returns user_id_hashed values with more than one current record (failing rows).
-- =============================================================================

select
    user_id_hashed,
    count(*) as current_record_count
from {{ ref('dim_users') }}
where is_current = true
group by user_id_hashed
having count(*) > 1
