---Marketing Build out

--Accounts Created
-- raw sql results do not include filled-in values for 'members.created_at_month'


WITH members AS (SELECT
        users.*

      -- DS Campaign Attribution --
      ,case when lower(campaign_info.campaign_name) is not null then lower(campaign_info.campaign_name)
          when lower(source) = 'importer-client' and lower(source_detail) = 'rock-the-vote' then 'rock-the-vote'
          when lower(source) = 'importer-client' and source_detail is null then 'rock-the-vote'
          when lower(source) = 'importer-client' and lower(source_detail) like '%opt_in%' then 'email_signup'
          else 'no attributable DS campaign' end as DS_campaign

        from public.users
          left join campaign_info ON substring(users.source_detail from '(?<=contentful_id\:)(\w*)') = campaign_info.contentful_id

          )
SELECT
    (TO_CHAR(DATE_TRUNC('month', members.created_at ), 'YYYY-MM')) AS "members.created_at_month",
    COUNT(DISTINCT members.northstar_id ) AS "members.count_distinct_northstar_id"
FROM public.user_activity  AS user_activity
LEFT JOIN members ON members.northstar_id = user_activity.northstar_id
WHERE ((( members.created_at  ) >= TIMESTAMP '2019-01-01'))
GROUP BY
    (DATE_TRUNC('month', members.created_at ))
ORDER BY
    1 DESC
FETCH NEXT 500 ROWS ONLY

--Account unsubscribed

SELECT
    (TO_CHAR(DATE_TRUNC('month', CASE WHEN user_activity.sms_status is null and user_activity.email_status is null
      THEN user_activity.created_at
      ELSE user_activity.user_unsubscribed_at END ), 'YYYY-MM')) AS "user_activity.user_unsubscribed_at_month",
    COUNT(*) AS "user_activity.count_users"
FROM
    "public"."user_activity" AS "user_activity"
WHERE (((( CASE WHEN user_activity.sms_status is null and user_activity.email_status is null
      THEN user_activity.created_at
      ELSE user_activity.user_unsubscribed_at END  )) >= TIMESTAMP '2019-01-01'))
GROUP BY
    (DATE_TRUNC('month', CASE WHEN user_activity.sms_status is null and user_activity.email_status is null
      THEN user_activity.created_at
      ELSE user_activity.user_unsubscribed_at END ))
ORDER BY
    1 DESC
FETCH NEXT 500 ROWS ONLY