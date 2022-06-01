



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


---
--revised campaign query

-------
With certificatesdownloaded (action_id, certificates_downloaded) as (
    SELECT phoenix_next_events.action_id,
           COUNT(DISTINCT (phoenix_next_events."event_id")) AS "certificates_downloaded"
    FROM quasar_prod_warehouse.public.snowplow_raw_events AS phoenix_next_events
    WHERE phoenix_next_events.event_name = 'phoenix_clicked_download_button_volunteer_credits_table'
    group by phoenix_next_events.action_id)

Select signups.northstar_id             as signup_northstar_id,
       signups.id                       as signup_id,
       signups.campaign_id              as signup_campaign_id,
       signups.why_participated,
       signups.source                   as signup_source,
       signups.details                  as signup_details,
       signups.source_bucket            as signup_source_bucket,
       signups.created_at               as signup_created_at,
       signups.source_details,
       signups.utm_medium               as signup_utm_medium,
       signups.utm_source               as signup_utm_source,
       signups.utm_campaign             as signup_utm_campaign,
       signups.signup_rank,
       posts.action                     as post_action,
       posts.action_id                  as post_action_id,
       posts.campaign_id                as posts_campaign_id,
       posts.created_at                 as post_created_at,
       posts.id                         as post_id,
       posts.is_accepted                as post_is_accepted,
       posts.is_anonymous               as post_is_anonymous,
       posts.is_civic_action            as post_is_civic_action,
       posts.is_online                  as post_is_online,
       posts.is_quiz                    as post_is_quiz,
       posts.is_reportback,
       posts.is_scholarship_entry       as post_is_scholarship_entry,
       posts.time_commitment            as post_is_commitment,
       posts.is_volunteer_credit,
       posts.location                   as post_location,
       posts.northstar_id               as post_northstar_id,
       posts.noun                       as post_noun,
       posts.num_participants,
       posts.postal_code                as post_postal_code,
       posts.post_class,
       CASE
           WHEN posts.quantity > 10000 THEN 1
           WHEN posts.quantity IS NULL THEN 1
           ELSE posts.quantity END      AS post_quantity_clean,
       posts.referrer_user_id           as post_referrer_user_id,
       posts.location                   as location,
       posts.reportback_volume          as post_reportback_volume,
       posts.school_id                  as post_school_id,
       posts.source_bucket              as post_source_bucket,
       posts.signup_id                  as post_signup_id,
       posts.source                     as post_source,
       posts.status                     as post_status,
       posts.text                       as post_text,
       posts.type                       as post_type,
       posts.hours_spent                as post_hours_spent,
       posts.url                        as post_url,
       posts.verb                       as post_verb,
       posts.tags                       as post_tags,
       reportbacks.location             as reportback_location,
       campaign_goals.reportback_goal,
       campaign_goals.signup_goal,
       campaign_goals.volunteerhour_goal,
       campaign_info.campaign_id,
       campaign_info.campaign_run_id,
       campaign_info.campaign_name,
       campaign_info.campaign_cause,
       campaign_info.campaign_run_start_date,
       campaign_info.campaign_run_end_date,
       campaign_info.campaign_created_date,
       campaign_info.campaign_action_type,
       campaign_info.campaign_cause_type,
       campaign_info.campaign_cta,
       campaign_info.action_types,
       campaign_info.online_offline,
       campaign_info.scholarship,
       campaign_info.post_types,
       users.northstar_id,
       users.created_at                    user_created_at,
       users.club_id,
       users.last_logged_in,
       users.last_accessed,
       users.last_messaged_at,
       users.drupal_uid,
       users.source                     as user_source,
       users.email_subscription_topics,
       users.birthdate,
       users.voter_registration_status,
-- DS Campaign Attribution --
                        case
                            when lower(campaign_info.campaign_name) is not null then lower(campaign_info.campaign_name)
                            when lower(users.source) = 'importer-client' and lower(source_detail) = 'rock-the-vote'
                                then 'rock-the-vote'
                            when lower(users.source) = 'importer-client' and source_detail is null then 'rock-the-vote'
                            when lower(users.source) = 'importer-client' and lower(source_detail) like '%opt_in%'
                                then 'email_signup'
                            else 'no attributable DS campaign' end                     as DS_campaign,
                        -- Acquisition Channel
                        case
                            when users.utm_medium = 'referral' and users.utm_source like 'scholarship_fea%'
                                then 'paid-scholarship partner'
                            when users.utm_medium = 'referral' and users.utm_source ilike 'scholarship_list%'
                                then 'referral-scholarship partner'
                            when users.utm_source = 'scholarship_fea%' then 'paid-scholarship partner'
                            when users.utm_source = 'scholarship_list%' then 'referral-scholarship partner'
                            when users.utm_medium = 'digital_ads' then 'paid-ads'
                            when users.utm_medium = 'referral' and users.utm_source = 'partner' then 'referral-other partner'
                            when users.utm_medium in ('sms', 'email') then 'email-or-sms'
                            when users.utm_medium is null and users.referrer_user_id is not null then 'referral-friend'
                            else 'no assigned attribution'
                            end                                                        as AcquisitionChannel,
                        --Acquisition Motivation
                        case
                            when users.source = 'importer-client' and users.source_detail = 'rock-the-vote' then 'voter-reg'
                            when users.source = 'importer-client' and users.source_detail is null then 'voter-reg'
                            when users.utm_source ilike 'scholarship_%' then 'scholarship'
                            when users.utm_medium ilike 'scholarship_%' then 'scholarship'
                            when users.utm_campaign ilike '%vcredit%' then 'volunteer-credit'
                            else 'core' end                                            as AcquisitionMotivation,
                        (extract(year from age(now(), users.created_at)) * 12) +
                        (extract(month from age(now(), users.created_at)))                   as TenureinMonths,
                        case
                            when (extract(year from age(now(), users.created_at)) * 12) +
                                 (extract(month from age(now(), users.created_at))) < 4 then 'a 0-3 mo'
                            when (extract(year from age(now(), users.created_at)) * 12) +
                                 (extract(month from age(now(), users.created_at))) < 7 then 'b 4-6 mo'
                            when (extract(year from age(now(), users.created_at)) * 12) +
                                 (extract(month from age(now(), users.created_at))) < 10 then 'c 7-9 mo'
                            when (extract(year from age(now(), users.created_at)) * 12) +
                                 (extract(month from age(now(), users.created_at))) < 13 then 'd 10-12 mo'
                            when (extract(year from age(now(), users.created_at)) * 12) +
                                 (extract(month from age(now(), users.created_at))) < 19 then 'e 13-18 mo'
                            when (extract(year from age(now(), users.created_at)) * 12) +
                                 (extract(month from age(now(), users.created_at))) < 25 then 'f 19-24 mo'
                            else 'g 24+ mo' end                                        as TenureGroup,
                        (DATE_PART('year', users.created_at) - DATE_PART('year', users.birthdate)) as AgeAtAccountCreation,
                        (DATE_PART('year', now()) - DATE_PART('year', users.birthdate))      as Age,
                        case
                            when DATE_PART('year', users.birthdate) <= '1945' then 'Traditionalists/ Silent'
                            when DATE_PART('year', users.birthdate) >= '1946' and DATE_PART('year', users.birthdate) <= '1964'
                                then 'Baby Boomers'
                            when DATE_PART('year', users.birthdate) >= '1965' and DATE_PART('year', users.birthdate) <= '1976'
                                then 'Gen X'
                            when DATE_PART('year', users.birthdate) >= '1977' and DATE_PART('year', users.birthdate) <= '1995'
                                then 'Millenials'
                            when DATE_PART('year', users.birthdate) >= '1996' and DATE_PART('year', users.birthdate) <= '2015'
                                then 'Gen Z'
                            else 'Likely Made up Age or blank' end                     as GenerationGroup,
                        case
                            when sms_status in ('active', 'less', 'pending') and
                                 users.cio_status in ('customer_subscribed') then 'email & sms'
                            when users.cio_status in ('customer_subscribed') then 'email only'
                            when sms_status in ('active', 'less', 'pending') then 'sms only'
                            else 'none' end                                            as MemberAddressableStatus,
       certificatesdownloaded.action_id,
       certificatesdownloaded.certificates_downloaded
from signups
         left outer join posts on signups.id = posts.signup_id
         left outer join reportbacks on posts.id = reportbacks.post_id
         left outer join campaign_info on signups.campaign_id = cast(campaign_info.campaign_id as varchar)
         left outer join ft_google_sheets.campaign_goals on campaign_goals.campaign_id = campaign_info.campaign_id
         left outer join users on users.northstar_id = signups.northstar_id
         left outer join certificatesdownloaded on cast(posts.action_id as varchar) = certificatesdownloaded.action_id
where campaign_created_date >= current_date - INTERVAL '2 YEARS';