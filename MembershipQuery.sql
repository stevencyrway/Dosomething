With Members as (SELECT users.*,
        -- DS Campaign Attribution --
        case
            when lower(campaign_info.campaign_name) is not null then lower(campaign_info.campaign_name)
            when lower(source) = 'importer-client' and lower(source_detail) = 'rock-the-vote'
                then 'rock-the-vote'
            when lower(source) = 'importer-client' and source_detail is null then 'rock-the-vote'
            when lower(source) = 'importer-client' and lower(source_detail) like '%opt_in%'
                then 'email_signup'
            else 'no attributable DS campaign' end   as DS_campaign,
        -- Acquisition Channel
        case
            when utm_medium = 'referral' and utm_source like 'scholarship_fea%'
                then 'paid-scholarship partner'
            when utm_medium = 'referral' and utm_source ilike 'scholarship_list%'
                then 'referral-scholarship partner'
            when utm_source = 'scholarship_fea%' then 'paid-scholarship partner'
            when utm_source = 'scholarship_list%' then 'referral-scholarship partner'
            when utm_medium = 'digital_ads' then 'paid-ads'
            when utm_medium = 'referral' and utm_source = 'partner' then 'referral-other partner'
            when utm_medium in ('sms', 'email') then 'email-or-sms'
            when utm_medium is null and referrer_user_id is not null then 'referral-friend'
            else 'no assigned attribution'
            end                                      as AcquisitionChannel,
        --Acquisition Motivation
        case
            when source = 'importer-client' and source_detail = 'rock-the-vote' then 'voter-reg'
            when source = 'importer-client' and source_detail is null then 'voter-reg'
            when utm_source ilike 'scholarship_%' then 'scholarship'
            when utm_medium ilike 'scholarship_%' then 'scholarship'
            when utm_campaign ilike '%vcredit%' then 'volunteer-credit'
            else 'core' end                          as AcquisitionMotivation,
        (extract(year from age(now(), created_at)) * 12) +
        (extract(month from age(now(), created_at))) as Tenure,
        case
            when (extract(year from age(now(), created_at)) * 12) +
                 (extract(month from age(now(), created_at))) < 4 then 'a 0-3 mo'
            when (extract(year from age(now(), created_at)) * 12) +
                 (extract(month from age(now(), created_at))) < 7 then 'b 4-6 mo'
            when (extract(year from age(now(), created_at)) * 12) +
                 (extract(month from age(now(), created_at))) < 10 then 'c 7-9 mo'
            when (extract(year from age(now(), created_at)) * 12) +
                 (extract(month from age(now(), created_at))) < 13 then 'd 10-12 mo'
            when (extract(year from age(now(), created_at)) * 12) +
                 (extract(month from age(now(), created_at))) < 19 then 'e 13-18 mo'
            when (extract(year from age(now(), created_at)) * 12) +
                 (extract(month from age(now(), created_at))) < 25 then 'f 19-24 mo'
            else 'g 24+ mo' end                      as TenureGroup,
        CASE
            WHEN DATE_PART('year', birthdate) < 2010 AND DATE_PART('year', birthdate) > 1971
                then (DATE_PART('year', created_at) - DATE_PART('year', birthdate))
            else null end                            as AgeAtAccountCreation

 from public.users
          left join campaign_info ON substring(users.source_detail from '(?<=contentful_id\:)(\w*)') =
                                     campaign_info.contentful_id)

--Main Query for Membership metrics
SELECT *,
    CASE WHEN ( case  when user_activity.sms_status in ('active', 'less', 'pending') and user_activity.email_status in ('customer_subscribed') then 'email & sms'
              when user_activity.email_status in ('customer_subscribed') then 'email only'
              when user_activity.sms_status in ('active', 'less', 'pending')  then 'sms only'
            else 'none' end  ) = 'none' then 0 ELSE 1 END AS "user_activity.count_subscribed",
    CASE WHEN ( case
      -- unsub scenarios
         when (user_activity.user_unsubscribed_at is not null)
          then 'not subscribed'
         when (user_activity.email_status is null and user_activity.sms_status is null)
          then 'not subscribed'
         when (user_activity.email_status is null and user_activity.sms_status in ('stop', 'undeliverable', 'unknown'))
          then 'not subscribed'

      -- sms scenarios
         when ((user_activity.email_status is null or user_activity.email_status = 'customer_unsubscribed')
              and user_activity.sms_status in ('active', 'less', 'pending')
              and date_part('day', now() - coalesce(user_activity.most_recent_mam_action, user_activity.created_at)) >= 365)
          then 'sms inactive'

         when (user_activity.sms_status in ('active', 'less', 'pending')
              and date_part('day', now() - coalesce(user_activity.most_recent_mam_action, user_activity.created_at)) < 365)
          then 'addressable'
      -- email scenarios
         when (user_activity.email_status = 'customer_subscribed'
              and date_part('day', now() - coalesce(user_activity.most_recent_email_open, user_activity.created_at)) >= 120)
         then 'email inactive'

        when (user_activity.email_status = 'customer_subscribed'
              and date_part('day', now() - coalesce(user_activity.most_recent_email_open, user_activity.created_at)) < 120)
        then 'addressable'
      -- this is to catch anything that does not fall into the above rules
        else 'other' end ) = 'addressable' then 1 END AS "user_activity.count_active_subscribed",
    CASE WHEN ( date_part('day', now() - user_activity.most_recent_mam_action)   ) <= 120 THEN  user_activity.northstar_id   END AS "user_activity.count_mel_past_120",
    case when date_part('day', now()- ( DATE(user_activity.most_recent_rb ) )) <= 120 then  user_activity.northstar_id   end AS "user_activity.count_impact_past_120"
FROM members
LEFT JOIN public.user_activity  AS user_activity ON members.northstar_id = user_activity.northstar_id
