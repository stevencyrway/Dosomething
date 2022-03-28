-- outbound sms
select null as agent_id,
       campaign_id,
       conversation_id,
       broadcast_id,
       null as attachment_url
       created_at,
       direction,
       message_id,
       macro,
       match,
       carrier_delivered_at,
       carrier_failure_code,
       platform_message_id,
       template,
       text,
       topic,
       user_id
from gambit_messages_outbound

--inbound sms
Select agent_id,
       attachment_content_type,
       broadcast_id,
       attachment_url,
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


select count(distinct conversation_id), date_part('week',created_at) , macro, direction from gambit_messages_outbound
group by date_part('week',created_at) , macro, direction;


select date_part('week',created_at) from gambit_messages_outbound
group by date_part('week',created_at)