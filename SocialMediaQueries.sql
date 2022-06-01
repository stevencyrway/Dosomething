--twitter query
Select date_trunc('week', created_at)                                       as WeekofDate,
       'Twitter'                                                            as Platform,
       rtrim(split_part(source, '>', 2), '</a')                             as source,
       full_text,
       lang,
       sum(retweet_count)                                                   as Retweets,
       sum(favorite_count)                                                  as Favorites,
       sum(case when in_reply_to_screen_name is not null then 1 else 0 end) as replys
from twitter.tweet
where tweet_type <> 'DRAFT'
group by WeekofDate, Platform, source, full_text, lang;

Select mh.ig_id,
       mh.permalink,
       mh.shortcode,
       mh.caption,
       mh.is_comment_enabled,
       mh.media_type,
       mh.created_time,
       mh._fivetran_synced,
       mi._fivetran_id,
       mi.id,
       mi.like_count,
       mi.comment_count,
       mi.video_photo_impressions,
       mi.video_photo_reach,
       mi.video_views,
       mi.carousel_album_engagement,
       mi.carousel_album_impressions,
       mi.carousel_album_reach,
       mi.carousel_album_saved,
       mi.carousel_album_video_views,
       mi.story_impressions,
       mi.story_reach,
       mi.video_photo_saved,
       mi.video_photo_engagement,
       mi.story_exits,
       mi.story_replies,
       mi.story_taps_back,
       mi.story_taps_forward,
       mi._fivetran_synced
from instagram_business.media_history mh
         join instagram_business.media_insights mi on mh.id = mi.id

Select *
from tiktok_ads.campaign_report_daily;

select c.customer_id, t.*
from customer_turnover c
         cross join lateral (
    values (c.q1, 'Q1'),
           (c.q2, 'Q2'),
           (c.q3, 'Q3'),
           (c.q4, 'Q4')
    ) as t(turnover, quarter)
order by customer_id, quarter;


-- Select *
-- from tiktok_ads.ad_report_daily


Select survey_response_id,
       nps_score,
       net_promoter_cat,
       nps_reason,
       fct_nps_web_responses.created_at as SurveyDate,
       surveyed_on_url,
       legacy_campaign_id,
       users.created_at as UserCreationDate,
       club_id,
       last_logged_in,
       last_accessed,
       last_messaged_at,
       email_subscription_topics,
       voter_registration_status,
       zipcode,
       country,
       language,
       causes,
       case
                            when source = 'importer-client' and source_detail = 'rock-the-vote' then 'voter-reg'
                            when source = 'importer-client' and source_detail is null then 'voter-reg'
                            when utm_source ilike 'scholarship_%' then 'scholarship'
                            when utm_medium ilike 'scholarship_%' then 'scholarship'
                            when utm_campaign ilike '%vcredit%' then 'volunteer-credit'
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
                        (DATE_PART('year', users.created_at) - DATE_PART('year', birthdate)) as AgeAtAccountCreation,
                        (DATE_PART('year', now()) - DATE_PART('year', birthdate))      as Age,
                        case
                            when DATE_PART('year', birthdate) <= '1945' then 'Traditionalists/ Silent'
                            when DATE_PART('year', birthdate) >= '1946' and DATE_PART('year', birthdate) <= '1964'
                                then 'Baby Boomers'
                            when DATE_PART('year', birthdate) >= '1965' and DATE_PART('year', birthdate) <= '1976'
                                then 'Gen X'
                            when DATE_PART('year', birthdate) >= '1977' and DATE_PART('year', birthdate) <= '1995'
                                then 'Millenials'
                            when DATE_PART('year', birthdate) >= '1996' and DATE_PART('year', birthdate) <= '2015'
                                then 'Gen Z'
                            else 'Likely Made up Age or blank' end                     as GenerationGroup,
                        case
                            when sms_status in ('active', 'less', 'pending') and
                                 users.cio_status in ('customer_subscribed') then 'email & sms'
                            when users.cio_status in ('customer_subscribed') then 'email only'
                            when sms_status in ('active', 'less', 'pending') then 'sms only'
                            else 'none' end                                            as MemberAddressableStatus
from fct_nps_web_responses
         left outer join users on fct_nps_web_responses.northstar_id = users.northstar_id;


Select causes from users
where causes is not null



Select replace(cast(json_array_elements(causes) as varchar), '"', '') as badges
from users
group by replace(cast(json_array_elements(causes) as varchar), '"', '');


--- to unnest campaign causes
Select count(distinct northstar_id) as usercount, unnest(string_to_array(causes, ',')) as campaign_paul
from users
group by unnest(string_to_array(causes, ','));



