---need to build pivot to highlight people by their causes
with user_causes as (
    Select northstar_id, split_part(causes, ',', 1) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 1)
    union all
    Select northstar_id, split_part(causes, ',', 2) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 2)
    union all
    Select northstar_id, split_part(causes, ',', 3) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 3)
    union all
    Select northstar_id, split_part(causes, ',', 4) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 4)
    union all
    Select northstar_id, split_part(causes, ',', 5) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 5)
    union all
    Select northstar_id, split_part(causes, ',', 6) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 6)
    union all
    Select northstar_id, split_part(causes, ',', 7) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 7)
    union all
    Select northstar_id, split_part(causes, ',', 8) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 8)
    union all
    Select northstar_id, split_part(causes, ',', 9) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 9)
    union all
    Select northstar_id, split_part(causes, ',', 10) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 10)
    union all
    Select northstar_id, split_part(causes, ',', 11) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 11)
    union all
    Select northstar_id, split_part(causes, ',', 12) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 12)
    union all
    Select northstar_id, split_part(causes, ',', 13) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 13)
)
Select northstar_id,
       max(case when (cause = 'bullying') then 1 else NULL end)               as Bullying_CauseFlag,
       max(case when (cause = 'gender_rights_equality') then 1 else NULL end) as GenderRightEquality_CauseFlag,
       max(case when (cause = 'education') then 1 else NULL end)              as Education_CauseFlag,
       max(case when (cause = 'homelessness_poverty') then 1 else NULL end)   as HomelessnessPoverty_CauseFlag,
       max(case when (cause = 'mental_health') then 1 else NULL end)          as MentalHealth_CauseFlag,
       max(case when (cause = 'lgbtq_rights_equality') then 1 else NULL end)  as LGBTQRightsEquality_CauseFlag

from user_causes
group by northstar_id

--------Quick campaign query
Select campaign_info.campaign_id,
       campaign_name,
       campaign_info.contentful_id,
       campaign_cause,
       campaign_run_start_date,
       campaign_run_end_date,
       case
           when campaign_run_start_date >= now() then 'Closed'
           when campaign_run_end_date <= now() then 'Closed'
           when campaign_run_end_date IS NULL then 'Perpetual'
           else 'Open' end                                                                   as CampaignStatus,
       concat('https://www.dosomething.org/us/campaigns/', slug)                             as URLlink,
       campaign_action_type,
       campaign_cause_type,
       campaign_noun,
       campaign_verb,
       post_types,
       action_types,
       scholarship,
       sum(case
               when is_volunteer_credit = true then reportbacks.hours_spent
               else null end)                                                                as VolunteerCreditTrueVolunteerHours,
       sum(case
               when is_volunteer_credit = false then reportbacks.hours_spent
               else null end)                                                                as VolunteerCreditFalseVolunteerHours,
       SUM(case
               when post_status = 'pending' then reportbacks.reportback_volume
               else null end)                                                                as PendingReportbacks,
       SUM(case
               when post_status = 'accepted' then reportbacks.reportback_volume
               else null end)                                                                as ApprovedReportbacks,
       sum(reportbacks.reportback_volume)                                                    as TotalReportbacks
from campaign_info
         join contentful_metadata_snapshot cm on campaign_info.contentful_id = cm.contentful_id
         left outer join signups on signups.campaign_id = cast(campaign_info.campaign_id as varchar)
         left outer join posts on signups.id = posts.signup_id
         left outer join reportbacks on posts.id = reportbacks.post_id
Group by campaign_info.campaign_id, campaign_name, campaign_info.contentful_id, campaign_cause, campaign_run_start_date,
         campaign_run_end_date, CampaignStatus, URLlink, campaign_action_type, campaign_cause_type, campaign_noun,
         campaign_verb,
         post_types, action_types, scholarship
Order by campaign_id desc;


---email user facts
    AS
SELECT *,
       LAG(efacts.emails_opened) OVER (PARTITION BY efacts.northstar_id
           ORDER BY efacts.month_year) AS emails_opened_last_month,
       LAG(efacts.emails_clicked) OVER (PARTITION BY efacts.northstar_id
           ORDER BY efacts.month_year) AS emails_clicked_last_month
FROM (
         SELECT customer_id                                                   AS northstar_id,
                DATE_TRUNC('month', TIMESTAMP)                                AS month_year,
                SUM(CASE WHEN event_type = 'email_opened' THEN 1 ELSE 0 END)  AS emails_opened,
                SUM(CASE WHEN event_type = 'email_clicked' THEN 1 ELSE 0 END) AS emails_clicked
         FROM public.cio_email_events
         GROUP BY customer_id,
                  DATE_TRUNC('month', TIMESTAMP)
     ) efacts
    AS
SELECT *,
       LAG(efacts.emails_opened) OVER (PARTITION BY efacts.northstar_id
           ORDER BY efacts.month_year) AS emails_opened_last_month,
       LAG(efacts.emails_clicked) OVER (PARTITION BY efacts.northstar_id
           ORDER BY efacts.month_year) AS emails_clicked_last_month
FROM (
         SELECT customer_id                                                   AS northstar_id,
                DATE_TRUNC('month', TIMESTAMP)                                AS month_year,
                SUM(CASE WHEN event_type = 'email_opened' THEN 1 ELSE 0 END)  AS emails_opened,
                SUM(CASE WHEN event_type = 'email_clicked' THEN 1 ELSE 0 END) AS emails_clicked
         FROM public.cio_email_events
         GROUP BY customer_id,
                  DATE_TRUNC('month', TIMESTAMP)
     ) efacts;


union all

select * from cio_email_events
where cio_campaign_name is not null


select * from gambit_messages_inbound

select * from gambit_messages_outbound

select * from ft_gambit_conversations_api.conversations