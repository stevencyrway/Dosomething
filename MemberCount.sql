--main query for campaign stats
Select count(distinct signups.northstar_id) as Signups,
       count(distinct post_id)              as Reportbacks,
       count(distinct users.northstar_id)   as DistinctUsers,
       campaign_info.campaign_id,
       campaign_info.campaign_name,
       campaign_info.campaign_created_date,
       post_action,
       campaign_verb,
       country,
       state

from quasar_prod_warehouse.public.signups
         join quasar_prod_warehouse.public.reportbacks on reportbacks.northstar_id = signups.northstar_id
         join quasar_prod_warehouse.public.campaign_info on signups.campaign_id = (campaign_info.campaign_id::varchar)
         left join users ON users.northstar_id = signups.northstar_id
where signups.campaign_id in ('9136')
group by campaign_info.campaign_id, campaign_info.campaign_name, campaign_info.campaign_created_date, post_action, campaign_verb, country, state