--twitter query
Select date_trunc('week', created_at) as WeekofDate,
       'Twitter'                      as Platform,
       rtrim(split_part(source,'>',2),'</a') as source,
       full_text,
       lang,
       sum(retweet_count)             as Retweets,
       sum(favorite_count)            as Favorites,
       sum(case when in_reply_to_screen_name is not null then 1 else 0 end) as replys
from twitter.tweet
where tweet_type <> 'DRAFT'
group by WeekofDate, Platform, source, full_text, lang;

Select
       mh.ig_id,
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

-- Select *
-- from tiktok_ads.campaign_report_daily;
--
-- Select *
-- from tiktok_ads.ad_report_daily

