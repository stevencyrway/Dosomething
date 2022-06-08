--normalizing stats for weeks and only displaying engagement stats
--twitter query
Select date_trunc('week', created_at)                                       as WeekofDate,
       'Twitter'                                                            as Platform,
       rtrim(split_part(source, '>', 2), '</a')                             as source,
       'Tweet'                                                              as MediaType,
       full_text                                                            as PostText,
       sum(retweet_count)                                                   as Shares,
       sum(favorite_count)                                                  as Likes,
       sum(case when in_reply_to_screen_name is not null then 1 else 0 end) as Comments,
       null                                                                 as reach,
       null                                                                 as Impressions
from twitter.tweet
where tweet_type <> 'DRAFT'
group by WeekofDate, Platform, source, MediaType, full_text

union all

--instagram query
Select date_trunc('week', mh.created_time) as WeekofDate,
       'Instagram'                         as Platform,
       null                                as Source,
       mh.media_type                       as MediaType,
       mh.caption                          as PostText,
       null                                as Shares,
       sum(mi.like_count)                  as Likes,
       sum(mi.comment_count)               as Comments,
       sum(video_photo_reach)              as reach,
       sum(video_photo_impressions)        as Impressions
from instagram_business.media_history mh
         join instagram_business.media_insights mi on mh.id = mi.id
group by date_trunc('week', mh.created_time), source, mh.caption, mh.media_type

union all

--Tiktok query
Select date_trunc('week', stat_time_day) as WeekofDate,
       'Tiktok'                          as Platform,
       null                              as Source,
       null                              as MediaType,
       null                              as PostText,
       suM(likes)                        as Likes,
       null                              as Shares,
--        sum(follows)                      as follows,
       sum(comments)                     as Comments,
       sum(reach)                        as Reach,
       sum(impressions)                  as Impressions
from tiktok_ads.campaign_report_daily
group by WeekofDate, Platform, source, MediaType, PostText

--Facebook Query

union all

Select date_trunc('week', created_time)                                as WeekofDate,
       'Facebook'                                                      as Platform,
       null                                                            as Source,
       status_type                                                     as MediaType,
       message                                                         as PostText,
       sum(post_reactions_like_total) + sum(post_reactions_love_total) as Likes,
       suM(share_count)                                                as Shares,
       null                                                            as Comments,
       sum(post_engaged_users)                                         as Reach,
       sum(post_impressions)                                           as Impresssions
from facebook_pages.post_history
         join facebook_pages.lifetime_post_metrics_total on post_id = id
group by WeekofDate, Platform, Source, status_type, MediaType, PostText



