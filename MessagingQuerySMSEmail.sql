------SMS funnel
with sms_funnel as (
    SELECT o.message_id           as event_id,
           o.user_id,
           o.created_at,
           o.macro,
           o.broadcast_id,
           o.campaign_id,
           o.conversation_id,
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
           i.conversation_id,
           i.template            as content_type,
           --i.text as message_text,
           'sms_inbound_message' as event_type
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
           --null as message_text,
           CASE
               WHEN c.interaction_type IN ('click', 'uncertain') THEN 'sms_link_click'
               ELSE 'sms_link_preview' END AS event_type
    FROM public.bertly_clicks c
    WHERE c.source = 'sms'
)

-- Select count(distinct event_id)                                             as Events,
--        count(distinct user_id)                                              as Users,
--        count(distinct conversation_id)                                      as Messages,
--        date_trunc('week', created_at),
--        SUM(CASE WHEN content_type = 'email_opened' THEN 1 ELSE 0 END)       AS Opened,
--        SUM(CASE WHEN content_type = 'email_clicked' THEN 1 ELSE 0 END)      AS Clicked,
--        SUM(CASE WHEN content_type = 'email_bounced' THEN 1 ELSE 0 END)      AS Undeliverable,
--        SUM(CASE WHEN content_type = 'email_converted' THEN 1 ELSE 0 END)    AS Converted,
--        SUM(CASE WHEN content_type = 'email_unsubscribed' THEN 1 ELSE 0 END) AS Unsubscribed,
--        SUM(CASE WHEN content_type = 'email_sent' THEN 1 ELSE 0 END)         AS Sent,
--        'SMS'                                                                as Modality,
--        campaign_id,
--        content_type
-- from sms_funnel
-- where created_at >= (CURRENT_DATE + INTERVAL '-1 year')
-- group by date_trunc('week', created_at), Modality, campaign_id, content_type


Select content_type,
       SUM(CASE
               WHEN content_type in
                    ('invalidAskContinueResponse', 'invalidAskMultipleChoiceResponse', 'invalidAskSignupResponse',
                     'invalidAskSubscriptionStatusResponse', 'invalidAskVotingPlanStatusResponse',
                     'invalidAskYesNoResponse', 'invalidCaption', 'invalidCompletedMenuCommand', 'invalidHoursSpent',
                     'invalidPhoto', 'invalidQuantity', 'invalidSignupMenuCommand', 'invalidText',
                     'invalidVotingMethod', 'invalidVotingPlanAttendingWith', 'invalidVotingPlanMethodOfTransport',
                     'invalidVotingPlanStatus', 'invalidVotingPlanTimeOfDay', 'invalidWhyParticipated') THEN 1
               ELSE 0 end) as Invalid,
       SUM(CASE
               WHEN content_type in
                    ('askCaption', 'askContinue', 'askHoursSpent', 'askMultipleChoice', 'askPhoto',
                     'askQuantity', 'askSignup', 'askSubscriptionStatus', 'askText',
                     'askVotingPlanStatus', 'askWhyParticipated',
                     'askYesNo') THEN 1
               ELSE 0 END) as Ask,
       SUM(CASE
               WHEN content_type in
                    ('votingMethodEarly', 'votingMethodInPerson', 'votingMethodMail', 'votingPlanAttendingWithAlone',
                     'votingPlanAttendingWithCoWorkers', 'votingPlanAttendingWithFamily',
                     'votingPlanAttendingWithFriends', 'votingPlanMethodOfTransportBike',
                     'votingPlanMethodOfTransportDrive', 'votingPlanMethodOfTransportPublicTransport',
                     'votingPlanMethodOfTransportWalk', 'votingPlanStatusCantVote', 'votingPlanStatusNotVoting',
                     'votingPlanStatusVoted', 'votingPlanStatusVoting', 'votingPlanTimeOfDayAfternoon',
                     'votingPlanTimeOfDayEvening', 'votingPlanTimeOfDayMorning'
                        ) THEN 1
               ELSE 0 END) as Voting,
       SUM(CASE
               WHEN content_type in
                    ('subscriptionStatusStop'
                        ) THEN 1
               ELSE 0 END) as Unsubscribed,
       SUM(CASE
               WHEN content_type in
                    ()
 ) THEN 1
               ELSE 0 END) as Ask
from sms_funnel
group by content_type

Select event_type
from cio_email_events
group by event_type

select *
from gambit_messages_outbound

---email funnel
Select count(distinct customer_id)                                        as Users,
       count(distinct email_id)                                           as Messages,
       count(distinct event_id)                                           as Events,
       SUM(CASE WHEN event_type = 'email_opened' THEN 1 ELSE 0 END)       AS Opened,
       SUM(CASE WHEN event_type = 'email_clicked' THEN 1 ELSE 0 END)      AS Clicked,
       SUM(CASE WHEN event_type = 'email_bounced' THEN 1 ELSE 0 END)      AS Undeliverable,
       SUM(CASE WHEN event_type = 'email_converted' THEN 1 ELSE 0 END)    AS Converted,
       SUM(CASE WHEN event_type = 'email_unsubscribed' THEN 1 ELSE 0 END) AS Unsubscribed,
       SUM(CASE WHEN event_type = 'email_sent' THEN 1 ELSE 0 END)         AS Sent,
       date_trunc('week', timestamp),
       'Email'                                                            as Modality,
       cio_campaign_type,
       cio_campaign_id,
       cio_campaign_name
from cio_email_events
where timestamp >= '2020-01-01'
group by date_trunc('week', timestamp), Modality, cio_campaign_type, cio_campaign_id, cio_campaign_name


