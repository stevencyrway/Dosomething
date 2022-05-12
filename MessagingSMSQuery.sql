-- -----Messaging funnel email & sms
-- with messaging_funnel as (
--     SELECT o.message_id           as event_id,
--            o.user_id,
--            o.created_at,
--            o.macro,
--            o.broadcast_id,
--            cast(o.campaign_id as varchar),
--            o.conversation_id,
--            o.template             as content_type,
--            --o.text as message_text,
--            'sms_outbound_message' as event_type,
--            o.text                 as topic,
--            'SMS'                  as Modality
--     FROM public.gambit_messages_outbound o
--     UNION ALL
--     SELECT i.message_id          as event_id,
--            i.user_id,
--            i.created_at,
--            i.macro,
--            i.broadcast_id,
--            cast(i.campaign_id as varchar),
--            i.conversation_id,
--            i.template            as content_type,
--            --i.text as message_text,
--            'sms_inbound_message' as event_type,
--            null                  as topic,
--            'SMS'                 as Modality
--     FROM public.gambit_messages_inbound i
--     UNION ALL
--     SELECT c.click_id                      as event_id,
--            c.northstar_id                  as user_id,
--            c.click_time                    as created_at,
--
--            NULL                            AS macro,
--            CASE
--                WHEN c.broadcast_id LIKE 'id=%' THEN REGEXP_REPLACE(c.broadcast_id, '^id=', '')
--                ELSE c.broadcast_id END     AS broadcast_id,
--            null                            as campaign_id,
--            null                            as conversation_id,
--            c.interaction_type              AS content_type,
--            CASE
--                WHEN c.interaction_type IN ('click', 'uncertain') THEN 'sms_link_click'
--                ELSE 'sms_link_preview' END AS event_type,
--            null                            as topic,
--            'SMS'                           as Modality
--     FROM public.bertly_clicks c
--     WHERE c.source = 'sms'
-- )
--
--
-- Select count(distinct event_id)                       as EventCount,
--        count(distinct users.northstar_id)             as UserCount,
--        date_trunc('day', messaging_funnel.created_at) as Date,
--        content_type,
--        event_type,
--        topic,
--        campaign_name,
--        campaign_cause,
--        campaign_cause_type,
--        campaign_run_start_date,
--        campaign_noun,
--        campaign_verb
-- from messaging_funnel
--          left outer join users on user_id = northstar_id
--          left outer join campaign_info ci
--                          on cast(messaging_funnel.campaign_id as varchar) = cast(ci.campaign_id as varchar)
-- where messaging_funnel.created_at >= current_date - INTERVAL '90 DAY'
-- group by date_trunc('day', messaging_funnel.created_at), content_type, event_type, topic, campaign_name, campaign_cause,
--          campaign_cause_type, campaign_run_start_date, campaign_noun, campaign_verb


---new sms query

select date_trunc('week',ob.created_at) as WeekofDate,
       count(distinct ob.broadcast_id) as Broadcasts,
       ob.template,
       ob.topic,
       ob.macro,
       ob.match,
       ob.campaign_id,
       count(distinct ob.conversation_id) as Conversations, --Back and forth conversation level, flag inbound as column True False
       count(ob.message_id) as MessagesSent,
       count(distinct ob.user_id) as UserCount,
       count(distinct ib.message_id) as MessagesReceived,
       case when ib.message_id is not null then True else False end as Responded,
       count(distinct CASE
               WHEN interaction_type IN ('preview') THEN click_id END) AS LinksPreviewed,
       count(distinct CASE
               WHEN interaction_type IN ('click', 'uncertain') THEN click_id
               ELSE 'sms_link_preview' END) AS LinksClicked
from gambit_messages_outbound ob
         left outer join gambit_messages_inbound ib on ib.conversation_id = ob.conversation_id
        left outer join bertly_clicks bc on ib.broadcast_id =  bc.broadcast_id and ib.user_id = bc.northstar_id
where ob.created_at >= '2022-05-01'
group by ob.template, date_trunc('week',ob.created_at), ob.topic, ob.macro, ob.match, ob.campaign_id, case when ib.message_id is not null then True else False end


