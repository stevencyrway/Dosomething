-------
--Campaign Funnel combined with Campaign Info
--Campaign Activity Data at the Device ID Campaign ID level
--Summarized
with
    --NDIDs 1st Signups
    signup1 as (
        select northstar_id, min(created_at) as min_created_at
        from public.signups
        group by 1
    )
        ,
    --Devices get Categorized as New/Existing per Session
    device_session_time as (
        select dc.device_id,
               dn.northstar_id,
               dc.campaign_id,
               dc.min_view_session_id,
               dc.min_view_datetime,
               case when dc.min_view_datetime > s1.min_created_at then 'Existing' else 'New' end as user_new,
               dc.session_referrer_host,
               dc.session_utm_source,
               dc.session_utm_campaign,
               dc.min_intent_datetime
        from public.device_campaign dc
                 join public.device_northstar dn on (dc.device_id = dn.device_id)
                 left join signup1 s1 on (dn.northstar_id = s1.northstar_id)
    )
        ,
    --NDIDs Signups per Campaign
    signupscte as (
        select northstar_id, campaign_id, id as signup_id, created_at
        from public.signups
        where lower(source_bucket) = 'web'
    )
        ,
    --Combining to get funnel
    campaign_journey as (
        select s.device_id,
               s.northstar_id,
               s.campaign_id,
               s.min_view_session_id,
               s.min_view_datetime,
               s.min_intent_datetime,
               s.user_new,
               s.session_referrer_host,
               s.session_utm_source,
               s.session_utm_campaign,
               su.signup_id,
               coalesce(date_trunc('month', su.created_at), date_trunc('month', s.min_view_datetime)) as signup_month,
               rb.num_rbs,
               rb.post_types,
               rb.action_types,
               rb.online_offline,
               coalesce(date_trunc('month', rb.first_rb), date_trunc('month', su.created_at),
                        date_trunc('month', s.min_view_datetime))                                     as rb_month
        from device_session_time s
                 left join signupscte su on (concat(s.northstar_id, '-', s.campaign_id) =
                                             concat(su.northstar_id, '-', su.campaign_id))
                 left join public.user_rb_summary rb on (su.signup_id = rb.signup_id)
        where (su.created_at is null or su.created_at > s.min_intent_datetime)
          and (rb.num_rbs is null or rb.post_sources like '%web%')
    ),
    finalcampaignjourney as (Select user_new,
                                    campaign_id,
                                    --Step 1 = Campaign Page Visits
                                    count(distinct device_id)                                             as VistingCampaignPage,
                                    --Step 2 = Sign Up Intent
                                    count(distinct case
                                                       when min_intent_datetime is not null
                                                           then device_id end)                            as SignupIntent,
                                    --Step 3 = Sign Confirmed
                                    count(distinct case when signup_id is not null then northstar_id end) as SigningUp,
                                    --Step 4 = Report Back
                                    count(distinct case when num_rbs > 0 then northstar_id end)           as ReportingBack
                             from campaign_journey
--           where (session_utm_source is null or session_utm_source<>'Snapchat')
--                              where min_view_datetime >= (CURRENT_DATE - INTERVAL '90 days')
                             group by 1, 2)

Select campaign_info.campaign_name,
       campaign_info.campaign_cause,
       campaign_info.campaign_run_start_date,
       campaign_info.campaign_run_end_date,
       campaign_info.campaign_created_date,
--        campaign_info.online_offline,
       campaign_info.scholarship,
--        signups.utm_medium                                                 as signup_utm_medium,
--        signups.utm_source                                                 as signup_utm_source,
--        signups.utm_campaign                                               as signup_utm_campaign,
       date_trunc('week', signups.created_at)                             as SignupWeek,
       date_trunc('week', posts.created_at)                               as PostWeek,
       posts.noun                                                         as post_noun,
       posts.verb                                                         as post_verb,
       posts.status,
--        posts.tags                           as post_tags, need to sort tags with Json parsing
       posts.post_class,
--        reportbacks.tags                     as reportback_tags,  need to sort tags with Json parsing
       count(distinct signups.northstar_id)                               as User_count,
       count(distinct signups.id)                                         as Signups,
       Count(distinct reportbacks.post_id)                                as Reportbacks,
       (Count(distinct reportbacks.post_id) / count(distinct signups.id)) as ReportbackRate,
       SUM(CASE
               WHEN posts.quantity > 10000 THEN 1
               WHEN posts.quantity IS NULL THEN 1
               ELSE posts.quantity END)                                   AS Posts,
       SUM(posts.hours_spent)                                             as VolunteerHours,
       SUM(posts.num_participants)                                        as NumberofParticipants,
       (SUM(case when user_new = 'New' then VistingCampaignPage else null end) /
        SUM(VistingCampaignPage))                                         as PercentNewUsers,
       (SUM(case when user_new = 'Existing' then VistingCampaignPage else null end) /
        SUM(VistingCampaignPage))                                         as PercenExistingUsers,
       (SUM(SigningUp) /
        SUM(VistingCampaignPage))                                         as PercenSigningUp,
       (SUM(ReportingBack) /
        SUM(VistingCampaignPage))                                         as PercenReportingBack,
       sum(VistingCampaignPage) as TotalVisitingCampaignPage,
       sum(SignupIntent) as TotalSignupIntent,
       sum(SigningUp) as TotalSigningup,
       sum(ReportingBack) as TotalReportingBack,
       SUM(case when user_new = 'New' then 1 else null end)               as NewuserCount,
       SUM(case when user_new = 'Existing' then 1 else null end)          as ExistinguserCount
from signups
         left outer join posts on signups.id = posts.signup_id
         left outer join reportbacks on posts.id = reportbacks.post_id
         left outer join campaign_info on signups.campaign_id = cast(campaign_info.campaign_id as varchar)
         left outer join finalcampaignjourney on finalcampaignjourney.campaign_id = signups.campaign_id
GROUP BY campaign_info.campaign_name, campaign_info.campaign_cause, campaign_info.campaign_run_start_date,
         campaign_info.campaign_run_end_date, campaign_info.campaign_created_date, campaign_info.scholarship,
         date_trunc('week', signups.created_at), date_trunc('week', posts.created_at), posts.noun, posts.verb,
         posts.status, posts.post_class