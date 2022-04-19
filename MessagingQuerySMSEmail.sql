-----Messaging funnel email & sms
with messaging_funnel as (
    SELECT o.message_id           as event_id,
           o.user_id,
           o.created_at,
           o.macro,
           o.broadcast_id,
           cast(o.campaign_id as varchar),
           o.conversation_id,
           o.template             as content_type,
           --o.text as message_text,
           'sms_outbound_message' as event_type,
           o.topic                as topic,
           'SMS'                  as Modality
    FROM public.gambit_messages_outbound o
    UNION ALL
    SELECT i.message_id          as event_id,
           i.user_id,
           i.created_at,
           i.macro,
           i.broadcast_id,
           cast(i.campaign_id as varchar),
           i.conversation_id,
           i.template            as content_type,
           --i.text as message_text,
           'sms_inbound_message' as event_type,
           i.topic               as topic,
           'SMS'                 as Modality
    FROM public.gambit_messages_inbound i
    UNION ALL
    SELECT c.click_id                      as event_id,
           c.northstar_id                  as user_id,
           c.click_time                    as created_at,

           NULL                            AS macro,
           CASE
               WHEN c.broadcast_id LIKE 'id=%' THEN REGEXP_REPLACE(c.broadcast_id, '^id=', '')
               ELSE c.broadcast_id END     AS broadcast_id,
           null                            as campaign_id,
           null                            as conversation_id,
           c.interaction_type              AS content_type,
           CASE
               WHEN c.interaction_type IN ('click', 'uncertain') THEN 'sms_link_click'
               ELSE 'sms_link_preview' END AS event_type,
           null                            as topic,
           'SMS'                           as Modality
    FROM public.bertly_clicks c
    WHERE c.source = 'sms'
    UNION ALL
    SELECT event_id,
           customer_id       as user_id,
           timestamp         as created_at,
           null              as macro,
           null              as broadcast_id,
           cio_campaign_id   as campaign_id,
           email_id          as conversation_id,
           cio_campaign_type as content_type,
           event_type,
           subject           as topic,
           'Email'           as Modality
    from cio_email_events
)

Select count(distinct event_id) as EventCount,
       count(distinct user_id) as UserCount,
       count(distinct northstar_id)                    as Members,
       date_trunc('week', messaging_funnel.created_at) as Date,
       Modality,
       content_type,
       event_type,
       case when date_part('day',(last_logged_in::timestamp - current_date::timestamp)) > 90 then 'Greater than 90 Days'
           when date_part('day',(last_logged_in::timestamp - current_date::timestamp)) < 90 then 'Less than 90 Days' end as DaysSinceLogin,
       case when date_part('day',(last_accessed::timestamp - current_date::timestamp)) > 90 then 'Greater than 90 Days'
           when date_part('day',(last_accessed::timestamp - current_date::timestamp)) < 90 then 'Less than 90 Days' end as DaysSinceAccessedSite,
       case when date_part('day',(last_messaged_at::timestamp - current_date::timestamp)) > 90 then 'Greater than 90 Days'
           when date_part('day',(last_messaged_at::timestamp - current_date::timestamp)) < 90 then 'Less than 90 Days' end as DaysSinceMessaged
from messaging_funnel
         join users on user_id = northstar_id
where messaging_funnel.created_at >= current_date - INTERVAL '90 DAY'
group by Date,
       Modality,
       content_type,
       event_type,
        DaysSinceLogin, DaysSinceAccessedSite, DaysSinceMessaged
