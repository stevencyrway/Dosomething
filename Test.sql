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
          left join (
              select distinct northstar_id, campaign_id, 1 as ignore
              from public.posts
              where signup_id = -1
          ) p1 on s.northstar_id = p1.northstar_id and s.campaign_id = p1.campaign_id
          --left join public.reportbacks r on s.id = r.signup_id
          )
SELECT
    (DATE(campaign_info.campaign_run_start_date )) AS "campaign_info.campaign_run_start_date",
    campaign_info.campaign_name  AS "campaign_info.campaign_name",
    campaign_info.online_offline  AS "campaign_info.online_offline",
    CASE WHEN campaign_info.scholarship ='Scholarship'
      THEN TRUE ELSE FALSE END  AS "campaign_info.is_scholarship",
    post_actions.noun  AS "post_actions.action_noun",
    post_actions.verb  AS "post_actions.action_verb",
        (CASE WHEN posts.is_reportback  THEN 'Yes' ELSE 'No' END) AS "posts.is_reportback",
    COUNT(DISTINCT posts.northstar_id ) AS "posts.count_of_distinct_members_posting",
    COALESCE(SUM(CASE
--         WHEN  posts.quantity   > 10000 THEN 1
        WHEN  posts.quantity   IS NULL THEN 1
             ELSE  posts.quantity   END  ), 0) AS "posts.quantity_clean"
FROM signups
LEFT JOIN public.posts  AS posts ON signups.id = posts.signup_id
LEFT JOIN public.post_actions  AS post_actions ON posts.action_id = post_actions.id
LEFT JOIN public.campaign_info  AS campaign_info ON signups.campaign_id = (campaign_info.campaign_id::varchar)
WHERE (posts.status ) = 'accepted' AND (campaign_info.campaign_name ) LIKE '%Why We Should Care%'
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7
ORDER BY
    2 DESC
FETCH NEXT 500 ROWS ONLY

-- sql for creating the total and/or determining pivot columns
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
          left join (
              select distinct northstar_id, campaign_id, 1 as ignore
              from public.posts
              where signup_id = -1
          ) p1 on s.northstar_id = p1.northstar_id and s.campaign_id = p1.campaign_id
          --left join public.reportbacks r on s.id = r.signup_id
          )
SELECT
    COUNT(DISTINCT posts.northstar_id ) AS "posts.count_of_distinct_members_posting",
    COALESCE(SUM(CASE
--         WHEN  posts.quantity   > 10000 THEN 1
        WHEN  posts.quantity   IS NULL THEN 1
             ELSE  posts.quantity   END  ), 0) AS "posts.quantity_clean"
FROM signups
LEFT JOIN public.posts  AS posts ON signups.id = posts.signup_id
LEFT JOIN public.post_actions  AS post_actions ON posts.action_id = post_actions.id
LEFT JOIN public.campaign_info  AS campaign_info ON signups.campaign_id = (campaign_info.campaign_id::varchar)
WHERE (posts.status ) = 'accepted' AND (campaign_info.campaign_name ) LIKE '%Why We Should Care%'
FETCH NEXT 1 ROWS ONLY