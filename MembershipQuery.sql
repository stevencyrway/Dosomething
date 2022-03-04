-- use existing sms_user_facts in looker_scratch.LR$Q62XE1646306348001_sms_user_facts
WITH members AS (SELECT users.*

                      -- DS Campaign Attribution --
                      , case
                            when lower(campaign_info.campaign_name) is not null then lower(campaign_info.campaign_name)
                            when lower(source) = 'importer-client' and lower(source_detail) = 'rock-the-vote'
                                then 'rock-the-vote'
                            when lower(source) = 'importer-client' and source_detail is null then 'rock-the-vote'
                            when lower(source) = 'importer-client' and lower(source_detail) like '%opt_in%'
                                then 'email_signup'
                            else 'no attributable DS campaign' end as DS_campaign

                 from public.users
                          left join campaign_info ON substring(users.source_detail from '(?<=contentful_id\:)(\w*)') =
                                                     campaign_info.contentful_id
)
SELECT case
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
       COUNT(DISTINCT members.northstar_id) AS "members.count_distinct_northstar_id"
FROM members
         LEFT JOIN public.user_activity AS user_activity ON members.northstar_id = user_activity.northstar_id
         LEFT JOIN looker_scratch.LR$Q62XE1646306348001_sms_user_facts AS sms_user_facts
                   ON members.northstar_id = sms_user_facts.user_id
WHERE ((((CASE
    -- UNSUB OR UNDEL (NO EMAIL OR SMS)
              WHEN (case
                  -- unsub scenarios
                        when (user_activity.user_unsubscribed_at is not null)
                            then 'not subscribed'
                        when (user_activity.email_status is null and user_activity.sms_status is null)
                            then 'not subscribed'
                        when (user_activity.email_status is null and
                              user_activity.sms_status in ('stop', 'undeliverable', 'unknown'))
                            then 'not subscribed'

                  -- sms scenarios
                        when ((user_activity.email_status is null or
                               user_activity.email_status = 'customer_unsubscribed')
                            and user_activity.sms_status in ('active', 'less', 'pending')
                            and date_part('day', now() - coalesce(user_activity.most_recent_mam_action,
                                                                  user_activity.created_at)) >= 365)
                            then 'sms inactive'

                        when (user_activity.sms_status in ('active', 'less', 'pending')
                            and date_part('day', now() - coalesce(user_activity.most_recent_mam_action,
                                                                  user_activity.created_at)) < 365)
                            then 'addressable'
                  -- email scenarios
                        when (user_activity.email_status = 'customer_subscribed'
                            and date_part('day', now() - coalesce(user_activity.most_recent_email_open,
                                                                  user_activity.created_at)) >= 120)
                            then 'email inactive'

                        when (user_activity.email_status = 'customer_subscribed'
                            and date_part('day', now() - coalesce(user_activity.most_recent_email_open,
                                                                  user_activity.created_at)) < 120)
                            then 'addressable'
                  -- this is to catch anything that does not fall into the above rules
                        else 'other' end
                       ) IN ('not subscribed')
                  THEN coalesce(user_activity.user_unsubscribed_at, user_activity.created_at)
    -- SMS INACTIVE
              WHEN (case
                  -- unsub scenarios
                        when (user_activity.user_unsubscribed_at is not null)
                            then 'not subscribed'
                        when (user_activity.email_status is null and user_activity.sms_status is null)
                            then 'not subscribed'
                        when (user_activity.email_status is null and
                              user_activity.sms_status in ('stop', 'undeliverable', 'unknown'))
                            then 'not subscribed'

                  -- sms scenarios
                        when ((user_activity.email_status is null or
                               user_activity.email_status = 'customer_unsubscribed')
                            and user_activity.sms_status in ('active', 'less', 'pending')
                            and date_part('day', now() - coalesce(user_activity.most_recent_mam_action,
                                                                  user_activity.created_at)) >= 365)
                            then 'sms inactive'

                        when (user_activity.sms_status in ('active', 'less', 'pending')
                            and date_part('day', now() - coalesce(user_activity.most_recent_mam_action,
                                                                  user_activity.created_at)) < 365)
                            then 'addressable'
                  -- email scenarios
                        when (user_activity.email_status = 'customer_subscribed'
                            and date_part('day', now() - coalesce(user_activity.most_recent_email_open,
                                                                  user_activity.created_at)) >= 120)
                            then 'email inactive'

                        when (user_activity.email_status = 'customer_subscribed'
                            and date_part('day', now() - coalesce(user_activity.most_recent_email_open,
                                                                  user_activity.created_at)) < 120)
                            then 'addressable'
                  -- this is to catch anything that does not fall into the above rules
                        else 'other' end
                       ) IN ('sms inactive')
                  THEN coalesce(user_activity.most_recent_mam_action, user_activity.created_at) + INTERVAL '365 days'
    -- EMAIL INACTIVE
              WHEN (case
                  -- unsub scenarios
                        when (user_activity.user_unsubscribed_at is not null)
                            then 'not subscribed'
                        when (user_activity.email_status is null and user_activity.sms_status is null)
                            then 'not subscribed'
                        when (user_activity.email_status is null and
                              user_activity.sms_status in ('stop', 'undeliverable', 'unknown'))
                            then 'not subscribed'

                  -- sms scenarios
                        when ((user_activity.email_status is null or
                               user_activity.email_status = 'customer_unsubscribed')
                            and user_activity.sms_status in ('active', 'less', 'pending')
                            and date_part('day', now() - coalesce(user_activity.most_recent_mam_action,
                                                                  user_activity.created_at)) >= 365)
                            then 'sms inactive'

                        when (user_activity.sms_status in ('active', 'less', 'pending')
                            and date_part('day', now() - coalesce(user_activity.most_recent_mam_action,
                                                                  user_activity.created_at)) < 365)
                            then 'addressable'
                  -- email scenarios
                        when (user_activity.email_status = 'customer_subscribed'
                            and date_part('day', now() - coalesce(user_activity.most_recent_email_open,
                                                                  user_activity.created_at)) >= 120)
                            then 'email inactive'

                        when (user_activity.email_status = 'customer_subscribed'
                            and date_part('day', now() - coalesce(user_activity.most_recent_email_open,
                                                                  user_activity.created_at)) < 120)
                            then 'addressable'
                  -- this is to catch anything that does not fall into the above rules
                        else 'other' end
                       ) IN ('email inactive')
                  THEN coalesce(user_activity.most_recent_email_open, user_activity.created_at) + INTERVAL '120 days'
              ELSE null END
    )) IS NULL))
  AND (((sms_user_facts.created_at) >=
        ((SELECT (DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                  (-23 || ' month')::INTERVAL))) AND (sms_user_facts.created_at) < ((SELECT ((DATE_TRUNC('month',
                                                                                                         DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                                                                                              (-23 || ' month')::INTERVAL) +
                                                                                             (24 || ' month')::INTERVAL)))))
GROUP BY 1
ORDER BY 2 DESC
    FETCH NEXT 500 ROWS ONLY

-- sql for creating the total and/or determining pivot columns
WITH members AS (SELECT users.*

                      -- DS Campaign Attribution --
                      , case
                            when lower(campaign_info.campaign_name) is not null then lower(campaign_info.campaign_name)
                            when lower(source) = 'importer-client' and lower(source_detail) = 'rock-the-vote'
                                then 'rock-the-vote'
                            when lower(source) = 'importer-client' and source_detail is null then 'rock-the-vote'
                            when lower(source) = 'importer-client' and lower(source_detail) like '%opt_in%'
                                then 'email_signup'
                            else 'no attributable DS campaign' end as DS_campaign

                 from public.users
                          left join campaign_info ON substring(users.source_detail from '(?<=contentful_id\:)(\w*)') =
                                                     campaign_info.contentful_id
)
SELECT COUNT(DISTINCT members.northstar_id) AS "members.count_distinct_northstar_id"
FROM members
         LEFT JOIN public.user_activity AS user_activity ON members.northstar_id = user_activity.northstar_id
         LEFT JOIN looker_scratch.LR$Q62XE1646306348001_sms_user_facts AS sms_user_facts
                   ON members.northstar_id = sms_user_facts.user_id
WHERE ((((CASE
    -- UNSUB OR UNDEL (NO EMAIL OR SMS)
              WHEN (case
                  -- unsub scenarios
                        when (user_activity.user_unsubscribed_at is not null)
                            then 'not subscribed'
                        when (user_activity.email_status is null and user_activity.sms_status is null)
                            then 'not subscribed'
                        when (user_activity.email_status is null and
                              user_activity.sms_status in ('stop', 'undeliverable', 'unknown'))
                            then 'not subscribed'

                  -- sms scenarios
                        when ((user_activity.email_status is null or
                               user_activity.email_status = 'customer_unsubscribed')
                            and user_activity.sms_status in ('active', 'less', 'pending')
                            and date_part('day', now() - coalesce(user_activity.most_recent_mam_action,
                                                                  user_activity.created_at)) >= 365)
                            then 'sms inactive'

                        when (user_activity.sms_status in ('active', 'less', 'pending')
                            and date_part('day', now() - coalesce(user_activity.most_recent_mam_action,
                                                                  user_activity.created_at)) < 365)
                            then 'addressable'
                  -- email scenarios
                        when (user_activity.email_status = 'customer_subscribed'
                            and date_part('day', now() - coalesce(user_activity.most_recent_email_open,
                                                                  user_activity.created_at)) >= 120)
                            then 'email inactive'

                        when (user_activity.email_status = 'customer_subscribed'
                            and date_part('day', now() - coalesce(user_activity.most_recent_email_open,
                                                                  user_activity.created_at)) < 120)
                            then 'addressable'
                  -- this is to catch anything that does not fall into the above rules
                        else 'other' end
                       ) IN ('not subscribed')
                  THEN coalesce(user_activity.user_unsubscribed_at, user_activity.created_at)
    -- SMS INACTIVE
              WHEN (case
                  -- unsub scenarios
                        when (user_activity.user_unsubscribed_at is not null)
                            then 'not subscribed'
                        when (user_activity.email_status is null and user_activity.sms_status is null)
                            then 'not subscribed'
                        when (user_activity.email_status is null and
                              user_activity.sms_status in ('stop', 'undeliverable', 'unknown'))
                            then 'not subscribed'

                  -- sms scenarios
                        when ((user_activity.email_status is null or
                               user_activity.email_status = 'customer_unsubscribed')
                            and user_activity.sms_status in ('active', 'less', 'pending')
                            and date_part('day', now() - coalesce(user_activity.most_recent_mam_action,
                                                                  user_activity.created_at)) >= 365)
                            then 'sms inactive'

                        when (user_activity.sms_status in ('active', 'less', 'pending')
                            and date_part('day', now() - coalesce(user_activity.most_recent_mam_action,
                                                                  user_activity.created_at)) < 365)
                            then 'addressable'
                  -- email scenarios
                        when (user_activity.email_status = 'customer_subscribed'
                            and date_part('day', now() - coalesce(user_activity.most_recent_email_open,
                                                                  user_activity.created_at)) >= 120)
                            then 'email inactive'

                        when (user_activity.email_status = 'customer_subscribed'
                            and date_part('day', now() - coalesce(user_activity.most_recent_email_open,
                                                                  user_activity.created_at)) < 120)
                            then 'addressable'
                  -- this is to catch anything that does not fall into the above rules
                        else 'other' end
                       ) IN ('sms inactive')
                  THEN coalesce(user_activity.most_recent_mam_action, user_activity.created_at) + INTERVAL '365 days'
    -- EMAIL INACTIVE
              WHEN (case
                  -- unsub scenarios
                        when (user_activity.user_unsubscribed_at is not null)
                            then 'not subscribed'
                        when (user_activity.email_status is null and user_activity.sms_status is null)
                            then 'not subscribed'
                        when (user_activity.email_status is null and
                              user_activity.sms_status in ('stop', 'undeliverable', 'unknown'))
                            then 'not subscribed'

                  -- sms scenarios
                        when ((user_activity.email_status is null or
                               user_activity.email_status = 'customer_unsubscribed')
                            and user_activity.sms_status in ('active', 'less', 'pending')
                            and date_part('day', now() - coalesce(user_activity.most_recent_mam_action,
                                                                  user_activity.created_at)) >= 365)
                            then 'sms inactive'

                        when (user_activity.sms_status in ('active', 'less', 'pending')
                            and date_part('day', now() - coalesce(user_activity.most_recent_mam_action,
                                                                  user_activity.created_at)) < 365)
                            then 'addressable'
                  -- email scenarios
                        when (user_activity.email_status = 'customer_subscribed'
                            and date_part('day', now() - coalesce(user_activity.most_recent_email_open,
                                                                  user_activity.created_at)) >= 120)
                            then 'email inactive'

                        when (user_activity.email_status = 'customer_subscribed'
                            and date_part('day', now() - coalesce(user_activity.most_recent_email_open,
                                                                  user_activity.created_at)) < 120)
                            then 'addressable'
                  -- this is to catch anything that does not fall into the above rules
                        else 'other' end
                       ) IN ('email inactive')
                  THEN coalesce(user_activity.most_recent_email_open, user_activity.created_at) + INTERVAL '120 days'
              ELSE null END
    )) IS NULL))
  AND (((sms_user_facts.created_at) >=
        ((SELECT (DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                  (-23 || ' month')::INTERVAL))) AND (sms_user_facts.created_at) < ((SELECT ((DATE_TRUNC('month',
                                                                                                         DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                                                                                              (-23 || ' month')::INTERVAL) +
                                                                                             (24 || ' month')::INTERVAL)))))
    FETCH NEXT 1 ROWS ONLY