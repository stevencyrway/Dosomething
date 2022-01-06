-------
--main query for campaign stats
Select count(distinct signups.id)          as Signups,
       count(distinct reportbacks.post_id) as Reportbacks,
       count(distinct users.northstar_id)  as DistinctUsers,
       count(posts.id)                     as Posts,
       sum(posts.reportback_volume)        as ReportbackVolume,
       campaign_info.campaign_id,
       campaign_info.campaign_name,
       campaign_info.campaign_created_date,
       campaign_info.campaign_run_start_date,
       campaign_info.campaign_run_end_date,
       users.state,
       users.country,
       signups.source                      as signup_source,
       signups.created_at as signup_date,
       posts.created_at as post_date,
       posts.action,
       posts.noun,
       posts.verb,
       posts.source                        as post_source,
       users.birthdate,
       users.last_accessed,
       users.last_logged_in,
       users.last_messaged_at
from quasar_prod_warehouse.public.signups
         join quasar_prod_warehouse.public.reportbacks on reportbacks.northstar_id = signups.northstar_id
         join quasar_prod_warehouse.public.campaign_info on signups.campaign_id = (campaign_info.campaign_id::varchar)
         left join quasar_prod_warehouse.public.posts on posts.northstar_id = signups.northstar_id
         left join users ON users.northstar_id = signups.northstar_id
where signups.campaign_id in ('9136', '9133', '9134')
group by campaign_info.campaign_id,
       campaign_info.campaign_name,
       campaign_info.campaign_created_date,
       campaign_info.campaign_run_start_date,
       campaign_info.campaign_run_end_date,
       users.state,
       users.country,
       signups.source,
       signups.created_at,
       posts.created_at,
       posts.action,
       posts.noun,
       posts.verb,
       posts.source,
       users.birthdate,
       users.last_accessed,
       users.last_logged_in,
       users.last_messaged_at;


---
Select * from user_activity