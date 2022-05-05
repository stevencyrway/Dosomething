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
           o.text                 as topic,
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
           null                  as topic,
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
       count(distinct users.northstar_id)             as UserCount,
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
         left outer join campaign_info ci
                         on cast(messaging_funnel.campaign_id as varchar) = cast(ci.campaign_id as varchar)
where messaging_funnel.created_at >= current_date - INTERVAL '90 DAY'
group by date_trunc('day', messaging_funnel.created_at), content_type, event_type, topic, campaign_name, campaign_cause,
         campaign_cause_type, campaign_run_start_date, campaign_noun, campaign_verb


---new sms query

select ob.campaign_id,
       ob.conversation_id, --Back and forth conversation level, flag inbound as column True False
       ob.created_at,
       ob.message_id,
       ob.macro,
       ob.match,
       ob.carrier_delivered_at,
       ob.carrier_failure_code,
       ob.platform_message_id, --Message Level, Count as messages sent.
       ob.template,
       ob.text,
       ob.topic,
       ob.user_id as outbounduserid,
       ib.agent_id,
       ib.attachment_content_type,
       ib.attachment_url,
       ib.campaign_id as inboundcampaignid,
       ib.conversation_id as inboundconversationid,
       ib.created_at as inboundcreatedat,
       ib.message_id as inboundmessageid,
       ib.macro as inboundmacro,
       ib.match as inboundmatch,
       ib.carrier_delivered_at as inboundcarrierdeliveredat,
       ib.carrier_failure_code as inboundcarrierfaliurecode,
       ib.total_segments,
       ib.platform_message_id as inboundplatformmessageid,
       ib.template as inboundtemplate,
       ib.text as inboundtext,
       ib.topic as inboundtopic,
       ib.user_id as inbounduserid
--        bc.click_id,
--        bc.shortened,
--        bc.target_url,
--        bc.click_time,
--        bc.user_agent,
--        bc.northstar_id as bertlyclickuserid,
--        bc.source,
--        bc.interaction_type
from gambit_messages_outbound ob
         left outer join gambit_messages_inbound ib on ib.conversation_id = ob.conversation_id
--         left outer join bertly_clicks bc on ib.conversation_id = bc
where ob.created_at >= '2022-04-01'


Select agent_id,
       attachment_content_type,
       attachment_url,
       broadcast_id,
       campaign_id,
       conversation_id,
       created_at,
       direction,
       message_id,
       macro,
       match,
       carrier_delivered_at,
       carrier_failure_code,
       total_segments,
       platform_message_id,
       template,
       text,
       topic,
       user_id
from gambit_messages_inbound
where broadcast_id = '7smlxYT8Q9Yk3otW44UjT5';


Select
from bertly_clicks
