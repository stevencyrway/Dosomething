-- Select action,
--        action_id,
--        campaign_id,
--        club_id,
--        created_at,
--        group_id,
--        id,
--        is_accepted,
--        is_anonymous,
--        is_civic_action,
--        is_online,
--        is_quiz,
--        is_reportback,
--        is_scholarship_entry,
--        time_commitment,
--        is_volunteer_credit,
--        location,
--        northstar_id,
--        noun,
--        num_participants,
--        postal_code,
--        post_class,
--        quantity,
--        referrer_user_id,
--        reportback_volume,
--        school_id,
--        source_bucket,
--        signup_id,
--        source,
--        status,
--        text,
--        type,
--        hours_spent,
--        url,
--        verb,
--        vr_source,
--        vr_source_details,
--        tags
-- from posts
-- where campaign_id in ('6716', '6691', '6690', '6460','7713');
--
-- select campaign_info.campaign_name, signups.campaign_id, why_participated from signups
-- left outer join campaign_info on signups.campaign_id = cast(campaign_info.campaign_id as varchar)
-- where signups.campaign_id in ('6716', '6691', '6690', '6460','7713')
-- and why_participated is not null;
--
--
-- Select *
-- from campaign_info
-- where campaign_id in ('6716', '6691', '6690', '6460','7713');
--
-- select id, northstar_id, text from posts where created_at >= '2022-03-01' and text is not null
--
-- ---query for nltk sentiment analysis posts
-- Select posts.campaign_id, text from posts
--          left outer join campaign_info on posts.campaign_id = cast(campaign_info.campaign_id as varchar)
-- where created_at >= '2022-03-01' and text is not null
-- group by posts.campaign_id, text;
--
-- --query for nltk sentiment analysis signups
-- Select signups.campaign_id, why_participated from signups
--          left outer join campaign_info on signups.campaign_id = cast(campaign_info.campaign_id as varchar)
-- where created_at >= '2022-03-01' and why_participated is not null
-- group by signups.campaign_id, why_participated;

---To break out the comma separated causes
with results as (
    Select northstar_id, unnest(string_to_array(causes, ',')) as cause
    from users
    group by northstar_id, unnest(string_to_array(causes, ','))
),

     results2 as (Select northstar_id,
                         count(distinct northstar_id) as                                      UserCount,
                         SUM(case when cause = 'animal_welfare' then 1 else 0 end)            AnimalWelfare,
                         SUM(case when cause = 'bullying' then 1 else 0 end)                  Bullying,
                         SUM(case when cause = 'environment' then 1 else 0 end)               Environment,
                         SUM(case when cause = 'gender_rights_equality' then 1 else 0 end)    gender_rights_equality,
                         SUM(case when cause = 'homelessness_poverty' then 1 else 0 end)      Ehomelessness_poverty,
                         SUM(case when cause = 'immigration_refugees' then 1 else 0 end)      immigration_refugees,
                         SUM(case when cause = 'lgbtq_rights_equality' then 1 else 0 end)     lgbtq_rights_equality,
                         SUM(case when cause = 'mental_health' then 1 else 0 end)             mental_health,
                         SUM(case when cause = 'physical_health' then 1 else 0 end)           physical_health,
                         SUM(case when cause = 'racial_justice_equity' then 1 else 0 end)     racial_justice_equity,
                         SUM(case when cause = 'sexual_harassment_assault' then 1 else 0 end) sexual_harassment_assault
                  from results
                  group by northstar_id)

Select *
from results;



Select count(distinct device_campaign.device_id) as distinctdevices,
       count(distinct dn.device_id)              as distinctdevicemembers,
       count(distinct u.northstar_id)            as distinctusers
from device_campaign
         left outer join device_northstar dn on device_campaign.device_id = dn.device_id
         left outer join users u on dn.northstar_id = u.northstar_id;


Select * from campaign_info
where campaign_created_date >= '2022=06-01';


Select * from rock_the_vote
where started_registration >= '2021-01-01';


Select * from snowplow_base_event
where se_label = 'voter_registration_status'


select * from instagram_business.media_history mh
         join instagram_business.media_insights mi on mh.id = mi.id


Select * from twitter.tweet