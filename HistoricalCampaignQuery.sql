-------
--Historical Query for all campaign attributes
Select count(distinct signups.northstar_id)   as User_count,
       count(distinct signups.id)             as Signups,
       Count(distinct reportbacks.post_id)    as Reportbacks,
       SUM(CASE
               WHEN posts.quantity > 10000 THEN 1
               WHEN posts.quantity IS NULL THEN 1
               ELSE posts.quantity END)       AS Posts,
       SUM(posts.hours_spent)                 as VolunteerHours,
       SUM(posts.num_participants)            as NumberofParticipants,
       signups.campaign_id                    as signup_campaign_id,
       campaign_info.campaign_name,
       campaign_info.campaign_cause,
       campaign_info.campaign_run_start_date,
       campaign_info.campaign_run_end_date,
       campaign_info.campaign_created_date,
       campaign_info.online_offline,
       campaign_info.scholarship,
       signups.utm_medium                     as signup_utm_medium,
       signups.utm_source                     as signup_utm_source,
       signups.utm_campaign                   as signup_utm_campaign,
       date_trunc('week', signups.created_at) as SignupWeek,
       date_trunc('week', posts.created_at)   as PostWeek,
       posts.noun                             as post_noun,
       posts.verb                             as post_verb,
       posts.status,
--        posts.tags                           as post_tags,
       posts.post_class
--        reportbacks.tags                     as reportback_tags
from signups
         left outer join posts on signups.id = posts.signup_id
         left outer join reportbacks on posts.id = reportbacks.post_id
         left outer join campaign_info on signups.campaign_id = cast(campaign_info.campaign_id as varchar)
GROUP BY campaign_info.campaign_name, signups.campaign_id, campaign_info.campaign_cause,
         campaign_info.campaign_run_start_date, campaign_info.campaign_run_end_date,
         campaign_info.campaign_created_date, campaign_info.online_offline, campaign_info.scholarship,
         signups.utm_medium, signups.utm_source, signups.utm_campaign, date_trunc('week', signups.created_at),
         date_trunc('week', posts.created_at), posts.noun, posts.verb, posts.status, posts.post_class

