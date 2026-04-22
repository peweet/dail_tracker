-- mart_member_overview.sql
--
-- Goal:
--   Produce one row per member for a thin Streamlit dashboard.
--
-- Important design rule:
--   Keep business logic here in SQL, not in Streamlit.
--
-- How to use:
--   1) Update the source relation names to match your real silver/gold tables.
--   2) Run this in DuckDB as part of the pipeline.
--   3) Point Streamlit to the resulting mart_member_overview view or table.
--
-- Suggested upstream inputs:
--   dim_member
--   fact_attendance
--   fact_question
--   fact_bill_sponsorship
--   fact_member_interest
--   fact_payment
--   bridge_lobbying_member_contact (or a member-level lobbying fact)
--   fact_revolving_door
--   mart_provenance_summary

create or replace view mart_member_overview as
with member_base as (
    select
        m.member_id,
        m.member_name,
        m.party_name,
        m.constituency,
        m.government_status,
        m.is_active
    from dim_member m
    where coalesce(m.is_active, true) = true
),
attendance as (
    select
        member_id,
        count(*) as attendance_event_count,
        sum(case when attended_flag then 1 else 0 end) as attended_count,
        case
            when count(*) = 0 then null
            else cast(sum(case when attended_flag then 1 else 0 end) as double) / count(*)
        end as attendance_rate
    from fact_attendance
    group by 1
),
questions as (
    select
        asker_member_id as member_id,
        count(distinct question_id) as questions_asked_count
    from fact_question
    group by 1
),
bills as (
    select
        sponsor_member_id as member_id,
        count(distinct bill_id) as bills_sponsored_count
    from fact_bill_sponsorship
    group by 1
),
interests as (
    select
        member_id,
        count(distinct interest_id) as declared_interests_count
    from fact_member_interest
    group by 1
),
payments as (
    select
        member_id,
        sum(amount_eur) as payment_total_eur,
        count(distinct payment_id) as payment_record_count
    from fact_payment
    group by 1
),
lobbying as (
    select
        member_id,
        count(distinct lobbying_interaction_id) as lobbying_interactions_count
    from fact_lobbying_member_contact
    group by 1
),
revolving_doors as (
    select
        member_id,
        max(case when revolving_door_flag then 1 else 0 end) = 1 as revolving_door_flag,
        max(post_public_role_sector) as latest_known_post_public_role_sector
    from fact_revolving_door
    group by 1
),
provenance as (
    select
        relation_name,
        latest_run_id,
        latest_fetch_timestamp_utc,
        source_summary,
        mart_version,
        code_version
    from mart_provenance_summary
    where relation_name = 'mart_member_overview'
)
select
    mb.member_id,
    mb.member_name,
    mb.party_name,
    mb.constituency,
    mb.government_status,
    coalesce(a.attendance_event_count, 0) as attendance_event_count,
    coalesce(a.attended_count, 0) as attended_count,
    a.attendance_rate,
    coalesce(q.questions_asked_count, 0) as questions_asked_count,
    coalesce(b.bills_sponsored_count, 0) as bills_sponsored_count,
    coalesce(i.declared_interests_count, 0) as declared_interests_count,
    coalesce(p.payment_total_eur, 0) as payment_total_eur,
    coalesce(p.payment_record_count, 0) as payment_record_count,
    coalesce(l.lobbying_interactions_count, 0) as lobbying_interactions_count,
    coalesce(r.revolving_door_flag, false) as revolving_door_flag,
    r.latest_known_post_public_role_sector,
    pr.latest_run_id,
    pr.latest_fetch_timestamp_utc,
    pr.source_summary,
    pr.mart_version,
    pr.code_version,
    concat(
        'Built from mart_member_overview | run=', coalesce(pr.latest_run_id, 'unknown'),
        ' | version=', coalesce(pr.mart_version, 'unknown')
    ) as provenance_summary
from member_base mb
left join attendance a on mb.member_id = a.member_id
left join questions q on mb.member_id = q.member_id
left join bills b on mb.member_id = b.member_id
left join interests i on mb.member_id = i.member_id
left join payments p on mb.member_id = p.member_id
left join lobbying l on mb.member_id = l.member_id
left join revolving_doors r on mb.member_id = r.member_id
left join provenance pr on true
;

-- Optional validation query ideas:
-- select count(*) from mart_member_overview;
-- select member_id, count(*) from mart_member_overview group by 1 having count(*) > 1;
-- select * from mart_member_overview order by questions_asked_count desc limit 25;
