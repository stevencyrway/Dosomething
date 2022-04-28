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
           o.text                as topic,
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
           null               as topic,
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
)


Select count(distinct event_id)                       as EventCount,
       count(distinct users.northstar_id)            as UserCount,
       date_trunc('day', messaging_funnel.created_at) as Date,
       content_type,
       event_type,
       topic,
       campaign_name,
       campaign_cause,
       campaign_cause_type,
       campaign_run_start_date,
       campaign_noun,
       campaign_verb
from messaging_funnel
         left outer join users on user_id = northstar_id
         left outer join campaign_info ci on cast(messaging_funnel.campaign_id as varchar) = cast(ci.campaign_id as varchar)
where messaging_funnel.created_at >= current_date - INTERVAL '90 DAY'
group by date_trunc('day', messaging_funnel.created_at), content_type, event_type, topic, campaign_name, campaign_cause,
         campaign_cause_type, campaign_run_start_date, campaign_noun, campaign_verb



