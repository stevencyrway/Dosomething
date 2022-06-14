---To break out the comma separated causes
---To break out the comma separated causes
with CausesUnnested as (Select campaign_name,
                               campaign_id,
                               campaign_created_date,
                               unnest(string_to_array(campaign_cause, ',')) as cause
                        from campaign_info
                        group by campaign_name, campaign_id, campaign_created_date, cause),
     CU2 as (Select campaign_name,
                    campaign_id,
                    campaign_created_date,
                    sum(count(cause))
                    over (partition by campaign_name order by cause asc rows between unbounded preceding and current row) as CauseCumSum,
                    cause
             from CausesUnnested
             group by campaign_name, campaign_id, campaign_created_date, cause),
     CampaignsAgg as (Select signups.campaign_id,
                             date_trunc('week', signups.created_at)                 as SignupWeek,
                             posts.location,
                             lower(posts.noun)                                      as noun,
                             lower(posts.verb)                                      as verb,
                             posts.post_class,
                             posts.status,
                             lower(posts.type)                                      as type,
                             count(distinct signups.northstar_id)                   as Particpants,
                             count(distinct signups.id)                             as Signups,
                             count(distinct reportbacks.post_id)                    as Reportbacks,
                             count(distinct signups.campaign_id)                    as Campaigns,
                             max(posts.created_at::DATE - signups.created_at::DATE) as DaystoPost,
                             count(distinct posts.id)                               as Posts,
                             sum(posts.hours_spent)                                 as VolunteerHours,
                             SUM(CASE
                                     WHEN posts.quantity > 10000 THEN 1
                                     WHEN posts.quantity IS NULL THEN 1
                                     ELSE posts.quantity END)                       AS Impact
                      from signups
                               left outer join posts on signups.id = posts.signup_id
                               left outer join reportbacks on posts.id = reportbacks.post_id
                      group by signups.campaign_id, date_trunc('week', signups.created_at), posts.location,
                               lower(posts.noun), lower(posts.verb),
                               posts.post_class, posts.status, lower(posts.type))

Select CampaignsAgg.campaign_id,
       SignupWeek,
       location,
       noun,
       verb,
       post_class,
       status,
       type,
       Particpants,
       Signups,
       Reportbacks,
       Campaigns,
       DaystoPost,
       Posts,
       VolunteerHours,
       Impact,
       campaign_name,
       campaign_created_date,
       cause
from CampaignsAgg
         join CU2 on CampaignsAgg.campaign_id = cast(CU2.campaign_id as varchar)
where CampaignsAgg.campaign_id = '9133'

---User Activity Query
Select northstar_id,
       created_at,
       sms_status,
       email_status,
       num_signups,
       most_recent_signup,
       num_rbs,
       total_quantity,
       most_recent_rb,
       first_rb,
       extract(DAY from avg_time_betw_rbs) as DaysbetweenRB,
       avg_days_next_action_after_rb,
       days_to_next_action_after_last_rb,
       most_recent_mam_action,
       most_recent_email_open,
       most_recent_all_actions,
       last_action_is_rb,
       days_since_last_action,
       extract(DAY from time_to_first_rb)  as DaystoFirstRB,
       sms_unsubscribed_at,
       sms_undeliverable_at,
       email_unsubscribed_at,
       user_unsubscribed_at,
       voter_reg_acquisition
from user_activity

--voter reg Looker query
WITH signups AS (select s.northstar_id,
                        s.id,
                        s.club_id,
                        s.campaign_id,
                        s.campaign_run_id,
                        s.why_participated,
                        s.source,
                        s.source_bucket,
                        s.source_details,
                        s.created_at,
                        s.utm_source,
                        s.utm_medium,
                        s.utm_campaign,
                        s.referrer_user_id,
                        s.group_id,
                        p1.ignore,
                        s.details
                        --,
                        --r.post_created_at post_date,
                        --date_part('day', r.post_created_at - s.created_at) as num_days_rb
                 from public.signups s
                          left join (select distinct northstar_id, campaign_id, 1 as ignore
                                     from public.posts
                                     where signup_id = -1) p1
                                    on s.northstar_id = p1.northstar_id and s.campaign_id = p1.campaign_id
    --left join public.reportbacks r on s.id = r.signup_id
)
SELECT COALESCE(SUM((case
                         when "reportbacks"."post_type" = 'voter-reg'
                             THEN "reportbacks"."reportback_volume"
                         ELSE NULL END)), 0) AS "reportbacks.total_voter_registrations_rbs"
FROM "signups" AS "signups"
         LEFT JOIN "public"."posts" AS "posts"
                   ON "signups"."id" = "posts"."signup_id"
         LEFT JOIN "public"."reportbacks" AS "reportbacks" ON "posts"."id" = "reportbacks"."post_id"
WHERE ((((("reportbacks"."post_created_at")) >= (TIMESTAMP '2020-01-01')
    AND
         (("reportbacks"."post_created_at"))
             < (TIMESTAMP '2020-12-31'))))
  AND "reportbacks"."vr_source" = 'email'
    FETCH NEXT 500 ROWS ONLY
