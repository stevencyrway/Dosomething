
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
          select dc.device_id, dn.northstar_id, dc.campaign_id, dc.min_view_session_id,
              dc.min_view_datetime,
              case when dc.min_view_datetime > s1.min_created_at then 'Existing' else 'New' end as user_new,
              dc.session_referrer_host, dc.session_utm_source, dc.session_utm_campaign,
              dc.min_intent_datetime
          from public.device_campaign dc
          join public.device_northstar dn on (dc.device_id=dn.device_id)
          left join signup1 s1 on (dn.northstar_id=s1.northstar_id)
      )
      ,
      --NDIDs Signups per Campaign
      signups as (
          select northstar_id, campaign_id, id as signup_id, created_at
          from public.signups
          where lower(source_bucket)='web'
      )
      ,
      --Combining to get funnel
      campaign_journey as (
          select s.device_id, s.northstar_id, s.campaign_id, s.min_view_session_id,
                 s.min_view_datetime, s.min_intent_datetime,
                 s.user_new,
                 s.session_referrer_host, s.session_utm_source, s.session_utm_campaign,
                 su.signup_id,
                 coalesce(date_trunc('month',su.created_at),date_trunc('month',s.min_view_datetime)) as signup_month,
                 rb.num_rbs, rb.post_types, rb.action_types, rb.online_offline,
                 coalesce(date_trunc('month',rb.first_rb),date_trunc('month',su.created_at),date_trunc('month',s.min_view_datetime)) as rb_month

          from device_session_time s
          left join signups su on ( concat(s.northstar_id,'-',s.campaign_id) = concat(su.northstar_id,'-',su.campaign_id) )
          left join public.user_rb_summary rb on ( su.signup_id = rb.signup_id )
               where (su.created_at is null or su.created_at > s.min_intent_datetime)
               and (rb.num_rbs is null or rb.post_sources like '%web%'))
      , CampaignJourneyFinal as (select
          user_new, campaign_journey.campaign_id,
          campaign_journey.min_view_datetime,
          campaign_journey.session_referrer_host, campaign_journey.session_utm_source, campaign_journey.session_utm_campaign,
          campaign_journey.post_types, campaign_journey.action_types, campaign_journey.online_offline,
          signup_month, rb_month,
          --Step 1 = Campaign Page Visits
          count(distinct device_id) as step_1,
          --Step 2 = Sign Up Intent
          count(distinct case when min_intent_datetime is not null then device_id end) as step_2,
          --Step 3 = Sign Confirmed
          count(distinct case when signup_id is not null then northstar_id end) as step_3,
          --Step 4 = Report Back
          count(distinct case when num_rbs>0 then northstar_id end) as step_4
          from campaign_journey
--           where (session_utm_source is null or session_utm_source<>'Snapchat')
          where min_view_datetime>=(CURRENT_DATE - INTERVAL '180 days')
          group by 1,2,3,4,5,6,7,8,9,10,11)




-----Badge total, for 2021
With badgeparsedas (
Select northstar_id, last_accessed, last_messaged_at, last_logged_in, replace(cast(json_array_elements(badges) as varchar),'"','') as badges from users
where badges is not null);



Select replace(cast(json_array_elements(badges) as varchar),'"','') as badges from users
group by replace(cast(json_array_elements(badges) as varchar),'"','');

Select badges from users


Select campaign_name, unnest(string_to_array(campaign_cause,',')) as campaign_paul from campaign_info
group by campaign_name, unnest(string_to_array(campaign_cause,','));

with parsedtags as (
Select id, replace(cast(json_array_elements(tags) as varchar),'"','') as tags from posts)



Select id,
       case when tags = 'Bulk' then 1 else 0 end as Bulk,
case when tags = 'Good For Brand' then 1 else 0 end as GoodForBrand,
case when tags = 'Good For Sponsor' then 1 else 0 end as GoodforSponsor,
case when tags = 'Good For Storytelling' then 1 else 0 end as GoodforStorytelling,
case when tags = 'Good Quote' then 1 else 0 end as GoodQuote,
case when tags = 'Good Submission' then 1 else 0 end as GoodSubmission,
case when tags = 'Group Photo' then 1 else 0 end as GroupPhoto,
case when tags = 'Hide In Gallery' then 1 else 0 end as HideInGallery,
case when tags = 'Inappropriate' then 1 else 0 end as Inappropriate,
case when tags = 'Incomplete Action' then 1 else 0 end as IncompleteAction,
case when tags = 'Irrelevant' then 1 else 0 end as Irrelevant,
case when tags = 'Social' then 1 else 0 end as Social,
case when tags = 'Test' then 1 else 0 end as Test,
case when tags = 'Unrealistic Hours' then 1 else 0 end as UnrealisticHours,
case when tags = 'Unrealistic Quantity' then 1 else 0 end as UnrealisticQuantity
from parsedtags
group by id, tags;


select * from gambit_messages_outbound
where conversation_id = '5a07282e476662000489b55b'