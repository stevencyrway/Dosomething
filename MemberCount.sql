Select count(distinct signups.northstar_id) as Signups,
       count(distinct post_id)              as reportbacks,
       campaign_info.campaign_id,
       campaign_info.campaign_name,
       campaign_info.campaign_created_date
from quasar_prod_warehouse.public.signups
         join quasar_prod_warehouse.public.reportbacks on signups.campaign_id = reportbacks.campaign_id
         join quasar_prod_warehouse.public.campaign_info on signups.campaign_id = cast(campaign_info.campaign_id as varchar)
where signups.campaign_id in ('9136')
group by campaign_info.campaign_id, campaign_info.campaign_name, campaign_info.campaign_created_date;


Select campaign_info.campaign_id,
       campaign_info.campaign_run_id,
       campaign_name,
       campaign_cause,
       campaign_run_start_date,
       campaign_run_end_date,
       campaign_created_date,
       campaign_node_id,
       group_type_id,
       contentful_id,
       contentful_internal_title,
       contentful_title,
       contentful_raf_flag,
       campaign_node_id_title,
       campaign_run_id_title,
       campaign_action_type,
       campaign_cause_type,
       campaign_noun,
       campaign_verb,
       campaign_cta,
       action_types,
       online_offline,
       scholarship,
       post_types,
       northstar_id,
       id,
       campaign_info.campaign_id,
       campaign_info.campaign_run_id,
       club_id,
       why_participated,
       source,
       details,
       referrer_user_id,
       group_id,
       source_bucket,
       created_at,
       source_details,
       utm_medium,
       utm_source,
       utm_campaign,
       signup_rank,
       count(distinct signups.northstar_id) as Signups
from campaign_info
         join quasar_prod_warehouse.public.signups on cast(signups.campaign_id as bigint) = campaign_info.campaign_id
group by campaign_info.campaign_id, campaign_info.campaign_run_id, campaign_name, campaign_cause,
         campaign_run_start_date, campaign_run_end_date, campaign_created_date, campaign_node_id, group_type_id,
         contentful_id, contentful_internal_title, contentful_title, contentful_raf_flag, campaign_node_id_title,
         campaign_run_id_title, campaign_action_type, campaign_cause_type, campaign_noun, campaign_verb, campaign_cta,
         action_types, online_offline, scholarship, post_types, northstar_id, id, campaign_info.campaign_id, campaign_info.campaign_run_id, club_id,
         why_participated, source, details, referrer_user_id, group_id, source_bucket, created_at, source_details,
         utm_medium, utm_source, utm_campaign, signup_rank
order by campaign_run_start_date desc