



Select replace(cast(json_array_elements(badges) as varchar), '"', '') as badges
from users
group by replace(cast(json_array_elements(badges) as varchar), '"', '');


--- to unnest campaign causes
Select campaign_name, unnest(string_to_array(campaign_cause, ',')) as campaign_paul
from campaign_info
group by campaign_name, unnest(string_to_array(campaign_cause, ','));

with parsedtags as (
    Select id, replace(cast(json_array_elements(tags) as varchar), '"', '') as tags from posts)

Select id,
       case when tags = 'Bulk' then 1 else 0 end                  as Bulk,
       case when tags = 'Good For Brand' then 1 else 0 end        as GoodForBrand,
       case when tags = 'Good For Sponsor' then 1 else 0 end      as GoodforSponsor,
       case when tags = 'Good For Storytelling' then 1 else 0 end as GoodforStorytelling,
       case when tags = 'Good Quote' then 1 else 0 end            as GoodQuote,
       case when tags = 'Good Submission' then 1 else 0 end       as GoodSubmission,
       case when tags = 'Group Photo' then 1 else 0 end           as GroupPhoto,
       case when tags = 'Hide In Gallery' then 1 else 0 end       as HideInGallery,
       case when tags = 'Inappropriate' then 1 else 0 end         as Inappropriate,
       case when tags = 'Incomplete Action' then 1 else 0 end     as IncompleteAction,
       case when tags = 'Irrelevant' then 1 else 0 end            as Irrelevant,
       case when tags = 'Social' then 1 else 0 end                as Social,
       case when tags = 'Test' then 1 else 0 end                  as Test,
       case when tags = 'Unrealistic Hours' then 1 else 0 end     as UnrealisticHours,
       case when tags = 'Unrealistic Quantity' then 1 else 0 end  as UnrealisticQuantity
from parsedtags
group by id, tags;

---need to add into dbt
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
       topic
from messaging_funnel
         left outer join users on user_id = northstar_id
where messaging_funnel.created_at >= current_date - INTERVAL '1 YEAR'
group by date_trunc('day', messaging_funnel.created_at), content_type, event_type, topic;


