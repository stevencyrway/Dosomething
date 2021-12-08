Select count(distinct signups.northstar_id) as Signups,
       count(distinct post_id)              as reportbacks,
       campaign_info.campaign_id,
       campaign_info.campaign_name,
       campaign_info.campaign_created_date
from quasar_prod_warehouse.public.signups
         join quasar_prod_warehouse.public.reportbacks on signups.campaign_id = reportbacks.campaign_id
         join quasar_prod_warehouse.public.campaign_info on signups.campaign_id = cast(campaign_info.campaign_id as varchar)
where signups.campaign_id in ('9134','9136','9135','9133')
group by campaign_info.campaign_id, campaign_info.campaign_name, campaign_info.campaign_created_date
