

--Members by Access Date
select count(northstar_id),
        date_trunc('day',last_accessed) accessed_date,
       state,
       country
from users
group by accessed_date, country, state;

--Members by logged in date
select count(northstar_id),
       state,
       country
from users
group by country, state;

--Members by Created Date
select count(northstar_id),
       date_trunc('year', created_at) createdat_date,
       state,
       country
from users
group by createdat_date, country, state;

----addressable members
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
SELECT * FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering, CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell FROM (
SELECT *, MIN(z___rank) OVER (PARTITION BY "members.voter_registration_status") as z___min_rank FROM (
SELECT *, RANK() OVER (ORDER BY "members.voter_registration_status" ASC, z__pivot_col_rank) AS z___rank FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY CASE WHEN "members.acq_program" IS NULL THEN 1 ELSE 0 END, "members.acq_program") AS z__pivot_col_rank FROM (
SELECT
    case
          when members.source = 'importer-client' and members.source_detail = 'rock-the-vote' then 'voter-reg'
          when members.source = 'importer-client' and members.source_detail is null then 'voter-reg'
          when members.utm_source ilike 'scholarship_%' then 'scholarship'
          when (CASE
      when
        members.utm_medium ILIKE 'schol%' OR members.utm_source ILIKE 'schol%'
      then
        'referral - scholarship'
      else members.utm_medium end) ilike 'scholarship_%' then 'scholarship'
          when members.utm_campaign ilike '%vcredit%' then 'volunteer-credit'
        else 'core' end
     AS "members.acq_program",
    members.voter_registration_status  AS "members.voter_registration_status",
    COALESCE(SUM((CASE WHEN ( case
      -- unsub scenarios
         when (user_activity.user_unsubscribed_at is not null)
          then 'not subscribed'
         when (user_activity.email_status is null and user_activity.sms_status is null)
          then 'not subscribed'
         when (user_activity.email_status is null and user_activity.sms_status in ('stop', 'undeliverable', 'unknown'))
          then 'not subscribed'

      -- sms scenarios
         when ((user_activity.email_status is null or user_activity.email_status = 'customer_unsubscribed')
              and user_activity.sms_status in ('active', 'less', 'pending')
              and date_part('day', now() - coalesce(user_activity.most_recent_mam_action, user_activity.created_at)) >= 365)
          then 'sms inactive'

         when (user_activity.sms_status in ('active', 'less', 'pending')
              and date_part('day', now() - coalesce(user_activity.most_recent_mam_action, user_activity.created_at)) < 365)
          then 'addressable'
      -- email scenarios
         when (user_activity.email_status = 'customer_subscribed'
              and date_part('day', now() - coalesce(user_activity.most_recent_email_open, user_activity.created_at)) >= 120)
         then 'email inactive'

        when (user_activity.email_status = 'customer_subscribed'
              and date_part('day', now() - coalesce(user_activity.most_recent_email_open, user_activity.created_at)) < 120)
        then 'addressable'
      -- this is to catch anything that does not fall into the above rules
        else 'other' end ) = 'addressable' then 1 END) ), 0) AS "user_activity.count_active_subscribed"
FROM members
LEFT JOIN public.user_activity  AS user_activity ON members.northstar_id = user_activity.northstar_id
GROUP BY
    1,
    2) ww
) bb WHERE z__pivot_col_rank <= 16384
) aa
) xx
) zz
 WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 5000) ORDER BY z___pivot_row_rank

-- sql for creating the total and/or determining pivot columns
WITH members AS (SELECT
        users.*, campaign_info.*
        from public.users
          left join campaign_info ON substring(users.source_detail from '(?<=contentful_id\:)(\w*)') = campaign_info.contentful_id

          )
SELECT
    members.utm_source ilike 'scholarship_%' then 'scholarship'
          when (CASE
      when
        members.utm_medium ILIKE 'schol%' OR members.utm_source ILIKE 'schol%'
      then
        'referral - scholarship'
      else members.utm_medium end) ilike 'scholarship_%' then 'scholarship'
          when members.utm_campaign ilike '%vcredit%' then 'volunteer-credit'
        else 'core' end
     AS "members.acq_program",
    COALESCE(SUM((CASE WHEN ( case
      -- unsub scenarios
         when (user_activity.user_unsubscribed_at is not null)
          then 'not subscribed'
         when (user_activity.email_status is null and user_activity.sms_status is null)
          then 'not subscribed'
         when (user_activity.email_status is null and user_activity.sms_status in ('stop', 'undeliverable', 'unknown'))
          then 'not subscribed'

      -- sms scenarios
         when ((user_activity.email_status is null or user_activity.email_status = 'customer_unsubscribed')
              and user_activity.sms_status in ('active', 'less', 'pending')
              and date_part('day', now() - coalesce(user_activity.most_recent_mam_action, user_activity.created_at)) >= 365)
          then 'sms inactive'

         when (user_activity.sms_status in ('active', 'less', 'pending')
              and date_part('day', now() - coalesce(user_activity.most_recent_mam_action, user_activity.created_at)) < 365)
          then 'addressable'
      -- email scenarios
         when (user_activity.email_status = 'customer_subscribed'
              and date_part('day', now() - coalesce(user_activity.most_recent_email_open, user_activity.created_at)) >= 120)
         then 'email inactive'

        when (user_activity.email_status = 'customer_subscribed'
              and date_part('day', now() - coalesce(user_activity.most_recent_email_open, user_activity.created_at)) < 120)
        then 'addressable'
      -- this is to catch anything that does not fall into the above rules
        else 'other' end ) = 'addressable' then 1 END) ), 0) AS "user_activity.count_active_subscribed"
FROM members
LEFT JOIN public.user_activity  AS user_activity ON members.northstar_id = user_activity.northstar_id
GROUP BY
    1
ORDER BY
    1


