--normalizing stats for weeks and only displaying engagement stats
--twitter query
Select date_trunc('week', created_at)                                       as WeekofDate,
       'Twitter'                                                            as Platform,
       id                                                                   as id,
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
group by WeekofDate, Platform, id, source, MediaType, full_text

union all

--instagram query
Select date_trunc('week', mh.created_time) as WeekofDate,
       'Instagram'                         as Platform,
       cast(mh.id as varchar)              as Id,
       null                                as Source,
       mh.media_type                       as MediaType,
       mh.caption                          as PostText,
       null                                as Shares,
       max(mi.like_count)                  as Likes,
       max(mi.comment_count)               as Comments,
       max(video_photo_reach)              as reach,
       max(video_photo_impressions)        as Impressions
from instagram_business.media_history mh
         join instagram_business.media_insights mi on mh.id = mi.id
group by WeekofDate, Platform, mh.Id, source, PostText, media_type

union all

--Tiktok query
Select date_trunc('week', stat_time_day) as WeekofDate,
       'Tiktok'                          as Platform,
       cast(campaign_id as varchar)      as id,
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
group by WeekofDate, Platform, campaign_id, source, MediaType, PostText

--Facebook Query

union all

Select date_trunc('week', created_time)                                as WeekofDate,
       'Facebook'                                                      as Platform,
       post_id                                                         as id,
       null                                                            as Source,
       status_type                                                     as MediaType,
       message                                                         as PostText,
       max(post_reactions_like_total) + sum(post_reactions_love_total) as Likes,
       max(share_count)                                                as Shares,
       null                                                            as Comments,
       max(post_engaged_users)                                         as Reach,
       max(post_impressions)                                           as Impresssions
from facebook_pages.post_history
         join facebook_pages.lifetime_post_metrics_total on post_id = id
group by WeekofDate, Platform, post_id, Source, status_type, MediaType, PostText


