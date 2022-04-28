create or replace function is_json(str varchar)
returns boolean language plpgsql as $$
declare
    j json;
begin
    j:= str;
    return true;
exception
    when others then return false;
end $$;

SELECT payload::jsonb #>> '{actionId}'      AS action_id,
       payload::jsonb #>> '{blockId}'       AS block_id,
       payload::jsonb #>> '{campaignId}'    AS campaign_id,
       payload::jsonb #>> '{contextSource}' AS context_source,
       payload::jsonb #>> '{value}'         AS context_value,
       event_id,
       payload::jsonb #>> '{name}'          AS event_name,
       _fivetran_synced                     AS ft_timestamp,
       payload::jsonb #>> '{groupId}'       AS group_id,
       payload::jsonb #>> '{modalType}'     AS modal_type,
       payload::jsonb #>> '{pageId}'        AS page_id,
       payload::jsonb #>> '{searchQuery}'   AS search_query,
       payload::jsonb #>> '{utmSource}'     AS utm_source,
       payload::jsonb #>> '{utmMedium}'     AS utm_medium,
       payload::jsonb #>> '{utmCampaign}'   AS utm_campaign,
       payload::jsonb #>> '{url}'           AS url
FROM ft_snowplow.snowplow_event
  where is_json(payload) = true;

{{ source('snowplow', 'snowplow_event') }}

Select * FROM ft_snowplow.snowplow_event
where event_id in ('d0bb5310-258e-466b-bfbc-7e9cb6628026','00653ba0-d327-4f0d-8683-c9f5ff64a575','d8bf040f-006d-4d26-a6c6-a2fca275794c')
where event_id >= '2022-04-23'
and is_json(payload) = false

union all

Select count(*) as "True" FROM ft_snowplow.snowplow_event
-- where event_id in ('d0bb5310-258e-466b-bfbc-7e9cb6628026','00653ba0-d327-4f0d-8683-c9f5ff64a575','d8bf040f-006d-4d26-a6c6-a2fca275794c')
where event_id >= '2022-04-23'
and is_json(payload) = true;


----

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


Select * from CampaignJourneyFinal
left outer join campaign_info on cast(campaign_info.campaign_id as varchar) = CampaignJourneyFinal.campaign_id
where CampaignJourneyFinal.campaign_id = '9148'

