---need to build pivot to highlight people by their causes
with user_causes as (
    Select northstar_id, split_part(causes, ',', 1) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 1)
    union all
    Select northstar_id, split_part(causes, ',', 2) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 2)
    union all
    Select northstar_id, split_part(causes, ',', 3) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 3)
    union all
    Select northstar_id, split_part(causes, ',', 4) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 4)
    union all
    Select northstar_id, split_part(causes, ',', 5) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 5)
    union all
    Select northstar_id, split_part(causes, ',', 6) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 6)
    union all
    Select northstar_id, split_part(causes, ',', 7) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 7)
    union all
    Select northstar_id, split_part(causes, ',', 8) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 8)
    union all
    Select northstar_id, split_part(causes, ',', 9) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 9)
    union all
    Select northstar_id, split_part(causes, ',', 10) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 10)
    union all
    Select northstar_id, split_part(causes, ',', 11) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 11)
    union all
    Select northstar_id, split_part(causes, ',', 12) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 12)
    union all
    Select northstar_id, split_part(causes, ',', 13) as cause
    from users
    where causes is not null
    group by northstar_id, split_part(causes, ',', 13)
)
Select northstar_id,
       max(case when (cause = 'bullying') then 1 else NULL end)               as Bullying_CauseFlag,
       max(case when (cause = 'gender_rights_equality') then 1 else NULL end) as GenderRightEquality_CauseFlag,
       max(case when (cause = 'education') then 1 else NULL end)              as Education_CauseFlag,
       max(case when (cause = 'homelessness_poverty') then 1 else NULL end)   as HomelessnessPoverty_CauseFlag,
       max(case when (cause = 'mental_health') then 1 else NULL end)          as MentalHealth_CauseFlag,
       max(case when (cause = 'lgbtq_rights_equality') then 1 else NULL end)  as LGBTQRightsEquality_CauseFlag

from user_causes
group by northstar_id

--------

Select campaign_info.campaign_id,
       campaign_name,
       campaign_info.contentful_id,
       campaign_cause,
       campaign_run_start_date,
       campaign_run_end_date,
       case
           when campaign_run_start_date >= now() then 'Closed'
           when campaign_run_end_date <= now() then 'Closed'
           when campaign_run_end_date IS NULL then 'Perpetual'
           else 'Open' end                                                                   as CampaignStatus,
       concat('https://www.dosomething.org/us/campaigns/', slug)                             as URLlink,
       campaign_action_type,
       campaign_cause_type,
       campaign_noun,
       campaign_verb,
       post_types,
       action_types,
       scholarship,
       sum(case
               when is_volunteer_credit = true then reportbacks.hours_spent
               else null end)                                                                as VolunteerCreditTrueVolunteerHours,
       sum(case
               when is_volunteer_credit = false then reportbacks.hours_spent
               else null end)                                                                as VolunteerCreditFalseVolunteerHours,
       SUM(case
               when post_status = 'pending' then reportbacks.reportback_volume
               else null end)                                                                as PendingReportbacks,
       SUM(case
               when post_status = 'accepted' then reportbacks.reportback_volume
               else null end)                                                                as ApprovedReportbacks,
       sum(reportbacks.reportback_volume)                                                    as TotalReportbacks
from campaign_info
         join contentful_metadata_snapshot cm on campaign_info.contentful_id = cm.contentful_id
         left outer join signups on signups.campaign_id = cast(campaign_info.campaign_id as varchar)
         left outer join posts on signups.id = posts.signup_id
         left outer join reportbacks on posts.id = reportbacks.post_id
Group by campaign_info.campaign_id, campaign_name, campaign_info.contentful_id, campaign_cause, campaign_run_start_date,
         campaign_run_end_date, CampaignStatus, URLlink, campaign_action_type, campaign_cause_type, campaign_noun,
         campaign_verb,
         post_types, action_types, scholarship
Order by campaign_id desc;

-----SMS users facts
AS
SELECT *,
       LAG(sms_facts.clicks) OVER (PARTITION BY sms_facts.user_id
           ORDER BY sms_facts.created_at) AS clicks_last_month,
       LAG(sms_facts.responses) OVER (PARTITION BY sms_facts.user_id
           ORDER BY sms_facts.created_at) AS responses_last_month
FROM (
         SELECT i.user_id,
                DATE_TRUNC('month', i.created_at) as created_at,
                COALESCE(clicks, 0)               as clicks,
                COUNT(i.user_id)                  AS responses
         FROM public.gambit_messages_inbound i
                  LEFT JOIN
              (
                  SELECT c.northstar_id                                                                as user_id,
                         DATE_TRUNC('month', c.click_time)                                             as created_at,
                         SUM(CASE WHEN c.interaction_type IN ('click', 'uncertain') THEN 1 ELSE 0 END) AS clicks
                  FROM public.bertly_clicks c
                  GROUP BY c.northstar_id, DATE_TRUNC('month', c.click_time)
              ) clicks ON clicks.user_id = i.user_id
         WHERE
           -- broadcast responses
             i.broadcast_id IS NOT NULL
           AND
           -- non-stop responses
             i.macro != 'subscriptionStatusStop'
         GROUP BY i.user_id,
                  DATE_TRUNC('month', i.created_at),
                  clicks
     ) sms_facts
------SMS funnel
with sms_funnel as (
    SELECT o.message_id           as event_id,
           o.user_id,
           o.created_at,
           o.macro,
           o.broadcast_id,
           o.campaign_id,
           o.template             as content_type,
           --o.text as message_text,
           'sms_outbound_message' as event_type
    FROM public.gambit_messages_outbound o
    UNION ALL
    SELECT i.message_id          as event_id,
           i.user_id,
           i.created_at,
           i.macro,
           i.broadcast_id,
           i.campaign_id,
           i.template            as content_type,
           --i.text as message_text,
           'sms_inbound_message' as event_type
    FROM public.gambit_messages_inbound i
    UNION ALL
    SELECT c.click_id                                                                                               as event_id,
           c.northstar_id                                                                                           as user_id,
           c.click_time                                                                                             as created_at,
           NULL                                                                                                     AS macro,
           CASE
               WHEN c.broadcast_id LIKE 'id=%' THEN REGEXP_REPLACE(c.broadcast_id, '^id=', '')
               ELSE c.broadcast_id END                                                                              AS broadcast_id,
           null                                                                                                     as campaign_id,
           c.interaction_type                                                                                       AS content_type,
           --null as message_text,
           CASE
               WHEN c.interaction_type IN ('click', 'uncertain') THEN 'sms_link_click'
               ELSE 'sms_link_preview' END                                                                          AS event_type
    FROM public.bertly_clicks c
    WHERE c.source = 'sms'
)


Select *
from sms_funnel


-- use existing sms_funnel in looker_scratch.LR$Q6G9R1647860024923_sms_funnel
SELECT (TO_CHAR(DATE_TRUNC('month', sms_funnel.created_at), 'YYYY-MM')) AS "sms_funnel.created_at_month",
       COUNT(DISTINCT (CASE
                           WHEN sms_funnel.event_type = 'sms_outbound_message'
                               THEN sms_funnel.event_id
                           ELSE NULL
           END))                                                        AS "sms_funnel.count_messages_outbound",
       COUNT(DISTINCT (CASE
                           WHEN sms_funnel.event_type = 'sms_inbound_message'
                               THEN sms_funnel.event_id
                           ELSE NULL
           END))                                                        AS "sms_funnel.count_messages_inbound",
       COUNT(DISTINCT (CASE
                           WHEN sms_funnel.event_type = 'sms_link_click'
                               THEN sms_funnel.event_id
                           ELSE NULL
           END))                                                        AS "sms_funnel.count_link_clicks"
FROM sms_funnel
WHERE ((sms_funnel.macro) <> 'subscriptionStatusStop' OR (sms_funnel.macro) IS NULL)
  AND (((sms_funnel.created_at) >=
        ((SELECT (DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                  (-2 || ' month')::INTERVAL))) AND (sms_funnel.created_at) < ((SELECT ((DATE_TRUNC('month',
                                                                                                    DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                                                                                         (-2 || ' month')::INTERVAL) +
                                                                                        (3 || ' month')::INTERVAL)))))
  AND (((sms_funnel.created_at) >=
        ((SELECT (DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                  (-2 || ' month')::INTERVAL))) AND (sms_funnel.created_at) < ((SELECT ((DATE_TRUNC('month',
                                                                                                    DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                                                                                         (-2 || ' month')::INTERVAL) +
                                                                                        (3 || ' month')::INTERVAL)))))
GROUP BY (DATE_TRUNC('month', sms_funnel.created_at))
ORDER BY 1 DESC

----SMS Messages Sent
SELECT (TO_CHAR(DATE_TRUNC('month', "created_at"), 'YYYY-MM')) AS "sms_outbound.created_at_month",
       COUNT(DISTINCT sms_outbound.message_id)                 AS "sms_outbound.count"
FROM "public"."gambit_messages_outbound" AS "sms_outbound"
WHERE LENGTH("broadcast_id") <> 0
  AND ((("created_at") >= ((SELECT (DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                                    (-2 || ' month')::INTERVAL))) AND ("created_at") < ((SELECT ((DATE_TRUNC('month',
                                                                                                             DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                                                                                                  (-2 || ' month')::INTERVAL) +
                                                                                                 (3 || ' month')::INTERVAL)))))
  AND "broadcast_id" IS NOT NULL
GROUP BY (DATE_TRUNC('month', "created_at"))
ORDER BY 1 DESC

---Bertly Clicks
SELECT (TO_CHAR(DATE_TRUNC('month', "click_time"), 'YYYY-MM')) AS "bertly_clicks.click_month",
       COUNT(DISTINCT bertly_clicks.click_id)                  AS "bertly_clicks.click_count"
FROM "public"."bertly_clicks" AS "bertly_clicks"
WHERE LENGTH("broadcast_id") <> 0
  AND ((("click_time") >= ((SELECT (DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                                    (-2 || ' month')::INTERVAL))) AND ("click_time") < ((SELECT ((DATE_TRUNC('month',
                                                                                                             DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                                                                                                  (-2 || ' month')::INTERVAL) +
                                                                                                 (3 || ' month')::INTERVAL)))))
  AND ((("click_time") >= ((SELECT (DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                                    (-2 || ' month')::INTERVAL))) AND ("click_time") < ((SELECT ((DATE_TRUNC('month',
                                                                                                             DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                                                                                                  (-2 || ' month')::INTERVAL) +
                                                                                                 (3 || ' month')::INTERVAL)))))
  AND "broadcast_id" IS NOT NULL
  AND "broadcast_id" NOT LIKE '%&source=sms%'
GROUP BY (DATE_TRUNC('month', "click_time"))
ORDER BY 2 DESC
    FETCH NEXT 500 ROWS ONLY

---Emails query, sent, opened, clicked
SELECT cio_campaign_name                 as "Email Campaign Name",
       (DATE_TRUNC('week', "timestamp")) AS "email_funnel.created_at_week",
       COUNT(DISTINCT (CASE
                           WHEN email_funnel.event_type = 'email_sent'
                               THEN email_funnel.email_id
                           ELSE NULL
           END))                         AS "Emails sent",
       COUNT(DISTINCT (CASE
                           WHEN email_funnel.event_type = 'email_opened'
                               THEN email_funnel.email_id
                           ELSE NULL
           END))                         AS "Email Opens",
       COUNT(DISTINCT (CASE
                           WHEN "event_type" = 'email_clicked'
                               THEN email_funnel.event_id
                           ELSE NULL
           END))                         AS "Email Clicks",
       COUNT(DISTINCT (CASE
                           WHEN email_funnel.event_type = 'email_unsubscribed'
                               THEN email_funnel.email_id
                           ELSE NULL
           END))                         AS "Email Unsubscribes",
       COUNT(DISTINCT (CASE
                           WHEN email_funnel.event_type = 'email_bounced'
                               THEN email_funnel.email_id
                           ELSE NULL
           END))                         AS "Emails Bounced"
FROM "public"."cio_email_events" AS "email_funnel"
WHERE ((("timestamp") >= ((SELECT (DATE_TRUNC('month', DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                                   (-2 || ' month')::INTERVAL))) AND ("timestamp") < ((SELECT ((DATE_TRUNC('month',
                                                                                                           DATE_TRUNC('day', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')) +
                                                                                                (-2 || ' month')::INTERVAL) +
                                                                                               (3 || ' month')::INTERVAL)))))
GROUP BY (DATE_TRUNC('week', "timestamp")), cio_campaign_name

---email user facts
    AS
SELECT *,
       LAG(efacts.emails_opened) OVER (PARTITION BY efacts.northstar_id
           ORDER BY efacts.month_year) AS emails_opened_last_month,
       LAG(efacts.emails_clicked) OVER (PARTITION BY efacts.northstar_id
           ORDER BY efacts.month_year) AS emails_clicked_last_month
FROM (
         SELECT customer_id                                                   AS northstar_id,
                DATE_TRUNC('month', TIMESTAMP)                                AS month_year,
                SUM(CASE WHEN event_type = 'email_opened' THEN 1 ELSE 0 END)  AS emails_opened,
                SUM(CASE WHEN event_type = 'email_clicked' THEN 1 ELSE 0 END) AS emails_clicked
         FROM public.cio_email_events
         GROUP BY customer_id,
                  DATE_TRUNC('month', TIMESTAMP)
     ) efacts
    AS
SELECT *,
       LAG(efacts.emails_opened) OVER (PARTITION BY efacts.northstar_id
           ORDER BY efacts.month_year) AS emails_opened_last_month,
       LAG(efacts.emails_clicked) OVER (PARTITION BY efacts.northstar_id
           ORDER BY efacts.month_year) AS emails_clicked_last_month
FROM (
         SELECT customer_id                                                   AS northstar_id,
                DATE_TRUNC('month', TIMESTAMP)                                AS month_year,
                SUM(CASE WHEN event_type = 'email_opened' THEN 1 ELSE 0 END)  AS emails_opened,
                SUM(CASE WHEN event_type = 'email_clicked' THEN 1 ELSE 0 END) AS emails_clicked
         FROM public.cio_email_events
         GROUP BY customer_id,
                  DATE_TRUNC('month', TIMESTAMP)
     ) efacts;


union all



---opens clicks etc
select *
from cio_email_events;

---history of subscribes and unsubscribes
select *
from cio_customer_event;

--current status
select *
from cio_latest_status