--Campaign Activity Data at the Device ID Campaign ID level
--Summarized
with
    --NDIDs 1st Signups
    signup1 as (select northstar_id, min(created_at) as min_created_at
                from public.signups
                group by 1)
        ,
    --Devices get Categorized as New/Existing per Session
    device_session_time as (select dc.device_id,
                                   dn.northstar_id,
                                   dc.campaign_id,
                                   dc.min_view_session_id,
                                   dc.min_view_datetime,
                                   case
                                       when dc.min_view_datetime > s1.min_created_at then 'Existing'
                                       else 'New' end as user_new,
                                   dc.session_referrer_host,
                                   dc.session_utm_source,
                                   dc.session_utm_campaign,
                                   dc.min_intent_datetime
                            from public.device_campaign dc
                                     join public.device_northstar dn on (dc.device_id = dn.device_id)
                                     left join signup1 s1 on (dn.northstar_id = s1.northstar_id))
        ,
    --NDIDs Signups per Campaign
    signups as (select northstar_id, campaign_id, id as signup_id, created_at
                from public.signups
                where lower(source_bucket) = 'web')
        ,
    --Combining to get funnel
    campaign_journey as (select s.device_id,
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
                                coalesce(date_trunc('month', su.created_at),
                                         date_trunc('month', s.min_view_datetime)) as signup_month,
                                rb.num_rbs,
                                rb.post_types,
                                rb.action_types,
                                rb.online_offline,
                                coalesce(date_trunc('month', rb.first_rb), date_trunc('month', su.created_at),
                                         date_trunc('month', s.min_view_datetime)) as rb_month

                         from device_session_time s
                                  left join signups su on (concat(s.northstar_id, '-', s.campaign_id) =
                                                           concat(su.northstar_id, '-', su.campaign_id))
                                  left join public.user_rb_summary rb on (su.signup_id = rb.signup_id)
                         where (su.created_at is null or su.created_at > s.min_intent_datetime)
                           and (rb.num_rbs is null or rb.post_sources like '%web%'))
select session_utm_campaign,
       session_referrer_host,
       session_utm_source,
       user_new,
       campaign_journey.campaign_id,
       campaign_name,
       date_trunc('day',min_view_datetime),
       campaign_journey.post_types,
       campaign_journey.action_types,
       campaign_journey.online_offline,
       campaign_journey.signup_month,
       rb_month,
       --Step 1 = Campaign Page Visits
       count(distinct device_id)                                                    as step_1,
       --Step 2 = Sign Up Intent
       count(distinct case when min_intent_datetime is not null then device_id end) as step_2,
       --Step 3 = Sign Confirmed
       count(distinct case when signup_id is not null then northstar_id end)        as step_3,
       --Step 4 = Report Back
       count(distinct case when num_rbs > 0 then northstar_id end)                  as step_4
from campaign_journey
         join campaign_info on campaign_journey.campaign_id = cast(campaign_info.campaign_id as varchar)
where min_view_datetime >= (CURRENT_DATE - INTERVAL '180 days')
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

