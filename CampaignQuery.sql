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
       posts.vr_source                  as post_vr_source,
       posts.vr_source_details          as post_vr_source_details,
       posts.tags                       as post_tags,
       reportbacks.post_action          as reportback_action,
       reportbacks.campaign_id          as reportback_campaign_id,
       reportbacks.post_id              as reportback_id,
       reportbacks.is_civic_action      as reportback_is_civic_action,
       reportbacks.is_scholarship_entry as reportsback_is_scholarship_entry,
       reportbacks.location             as reportback_location,
       reportbacks.northstar_id         as reportback_northstar_id,
       reportbacks.post_type            as reportback_type,
       reportbacks.post_class           as reportback_class,
       reportbacks.signup_id            as reportback_signup_id,
       reportbacks.post_source          as reportback_source,
       reportbacks.post_source_bucket   as reportback_source_bucket,
       reportbacks.post_status          as reportback_status,
       reportbacks.hours_spent          as reportback_hours_spent,
       reportbacks.post_created_at      as reportback_created_at,
       reportbacks.postal_code          as reportback_postal_code,
       reportbacks.post_bucket          as reportback_bucket,
       reportbacks.reportback_volume,
       reportbacks.vr_source            as reportback_vr_source,
       reportbacks.vr_source_details    as reportback_vr_source_details,
       reportbacks.tags                 as reportback_tags,
       campaign_info.campaign_id,
       campaign_info.campaign_run_id,
       campaign_info.campaign_name,
       campaign_info.campaign_cause,
       campaign_info.campaign_run_start_date,
       campaign_info.campaign_run_end_date,
       campaign_info.campaign_created_date,
       campaign_info.campaign_node_id,
       campaign_info.group_type_id,
       campaign_info.contentful_id,
       campaign_info.contentful_internal_title,
       campaign_info.contentful_title,
       campaign_info.contentful_raf_flag,
       campaign_info.campaign_node_id_title,
       campaign_info.campaign_run_id_title,
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
       users.email,
       users.email_subscription_topics,
       users.facebook_id,
       users.mobile,
       users.birthdate,
       users.first_name,
       users.last_name,
       users.voter_registration_status,
       users.city                       as user_city,
       users.state,
       case
               when lower(trim(users.state)) = 'al - alabama' then 'AL'
               when lower(trim(users.state)) = 'alabama' then 'AL'
               when lower(trim(users.state)) = 'ariz' then 'AZ'
               when lower(trim(users.state)) = 'arizon' then 'AZ'
               when lower(trim(users.state)) = 'arizona' then 'AZ'
               when lower(trim(users.state)) = 'az -' then 'AZ'
               when lower(trim(users.state)) = 'az.' then 'AZ'
               when lower(trim(users.state)) = 'az' then 'AZ'
               when lower(trim(users.state)) = 'az - arizona' then 'AZ'
               when lower(trim(users.state)) = 'arkansas' then 'AR'
               when lower(trim(users.state)) = 'arka' then 'AR'
               when lower(trim(users.state)) = 'ark' then 'AR'
               when lower(trim(users.state)) = 'ca - california' then 'CA'
               when lower(trim(users.state)) = 'ca - california [usa]' then 'CA'
               when lower(trim(users.state)) = 'ca-c' then 'CA'
               when lower(trim(users.state)) = 'ca' then 'CA'
               when lower(trim(users.state)) = 'ca.' then 'CA'
               when lower(trim(users.state)) = 'ca -' then 'CA'
               when lower(trim(users.state)) = 'cali' then 'CA'
               when lower(trim(users.state)) = 'calif' then 'CA'
               when lower(trim(users.state)) = 'california' then 'CA'
               when lower(trim(users.state)) = 'califorinia' then 'CA'
               when lower(trim(users.state)) = 'californa' then 'CA'
               when lower(trim(users.state)) = 'californi' then 'CA'
               when lower(trim(users.state)) = 'califorina' then 'CA'
               when lower(trim(users.state)) = 'california ca' then 'CA'
               when lower(trim(users.state)) = 'califronia' then 'CA'
               when lower(trim(users.state)) = 'carlifornia' then 'CA'
               when lower(trim(users.state)) = 'california (ca)' then 'CA'
               when lower(trim(users.state)) = 'ca   -   californi' then 'CA'
               when lower(trim(users.state)) = 'connecticurt' then 'CT'
               when lower(trim(users.state)) = 'connecticut' then 'CT'
               when lower(trim(users.state)) = 'conneticut' then 'CT'
               when lower(trim(users.state)) = 'co' then 'CO'
               when lower(trim(users.state)) = 'co -' then 'CO'
               when lower(trim(users.state)) = 'co.' then 'CO'
               when lower(trim(users.state)) = 'colorado' then 'CO'
               when lower(trim(users.state)) = 'delaware' then 'DE'
               when lower(trim(users.state)) = 'guam' then 'GU'
               when lower(trim(users.state)) = 'district of columbia' then 'DC'
               when lower(trim(users.state)) = 'fl' then 'FL'
               when lower(trim(users.state)) = 'fl -' then 'FL'
               when lower(trim(users.state)) = 'fl.' then 'FL'
               when lower(trim(users.state)) = 'fl florida' then 'FL'
               when lower(trim(users.state)) = 'fl - florida' then 'FL'
               when lower(trim(users.state)) = 'fl: florida' then 'FL'
               when lower(trim(users.state)) = 'flor' then 'FL'
               when lower(trim(users.state)) = 'flordia' then 'FL'
               when lower(trim(users.state)) = 'florida' then 'FL'
               when lower(trim(users.state)) = 'florida fl' then 'FL'
               when lower(trim(users.state)) = 'fl florida (fl)' then 'FL'
               when lower(trim(users.state)) = 'ga - georgia' then 'GA'
               when lower(trim(users.state)) = 'ga' then 'GA'
               when lower(trim(users.state)) = 'ga -' then 'GA'
               when lower(trim(users.state)) = 'georgia' then 'GA'
               when lower(trim(users.state)) = 'ga - georgia' then 'GA'
               when lower(trim(users.state)) = 'ga - georgia' then 'GA'
               when lower(trim(users.state)) = 'hawalli' then 'HI'
               when lower(trim(users.state)) = 'hawaii' then 'HI'
               when lower(trim(users.state)) = 'hi - hawaii' then 'HI'
               when lower(trim(users.state)) = 'hi - hawaii [usa]' then 'HI'
               when lower(trim(users.state)) = 'hawaii' then 'HI'
               when lower(trim(users.state)) = 'hawaii' then 'HI'
               when lower(trim(users.state)) = 'idaho' then 'ID'
               when lower(trim(users.state)) = 'idah' then 'ID'
               when lower(trim(users.state)) = 'iowa' then 'IA'
               when lower(trim(users.state)) = 'il - illinois' then 'IL'
               when lower(trim(users.state)) = 'il-illinois' then 'IL'
               when lower(trim(users.state)) = 'illinios' then 'IL'
               when lower(trim(users.state)) = 'illinoi' then 'IL'
               when lower(trim(users.state)) = 'illinois' then 'IL'
               when lower(trim(users.state)) = 'illinoise' then 'IL'
               when lower(trim(users.state)) = 'illinois il' then 'IL'
               when lower(trim(users.state)) = 'illinois' then 'IL'
               when lower(trim(users.state)) = 'indiana' then 'IN'
               when lower(trim(users.state)) = 'indiana' then 'IN'
               when lower(trim(users.state)) = 'in - indiana' then 'IN'
               when lower(trim(users.state)) = 'indiana (in)' then 'IN'
               when lower(trim(users.state)) = 'kentucky' then 'KY'
               when lower(trim(users.state)) = 'kentucky (ky)' then 'KY'
               when lower(trim(users.state)) = 'kansas' then 'KS'
               when lower(trim(users.state)) = 'kansas [ks]' then 'KS'
               when lower(trim(users.state)) = 'la - louisiana' then 'LA'
               when lower(trim(users.state)) = 'louisianna' then 'LA'
               when lower(trim(users.state)) = 'lousiiana' then 'LA'
               when lower(trim(users.state)) = 'louisisana' then 'LA'
               when lower(trim(users.state)) = 'lousiana' then 'LA'
               when lower(trim(users.state)) = 'louisiana' then 'LA'
               when lower(trim(users.state)) = 'Iowa' then 'IA'
               when lower(trim(users.state)) = 'massachussetts' then 'MA'
               when lower(trim(users.state)) = 'massachusetts' then 'MA'
               when lower(trim(users.state)) = 'maryland' then 'MD'
               when lower(trim(users.state)) = 'maryland md' then 'MD'
               when lower(trim(users.state)) = 'maryland (md)' then 'MD'
               when lower(trim(users.state)) = 'marylang' then 'MD'
               when lower(trim(users.state)) = 'md - maryland' then 'MD'
               when lower(trim(users.state)) = 'md - maryland [usa]' then 'MD'
               when lower(trim(users.state)) = 'michigan' then 'MI'
               when lower(trim(users.state)) = 'michian' then 'MI'
               when lower(trim(users.state)) = 'michigan' then 'MI'
               when lower(trim(users.state)) = 'michigan (mi)' then 'MI'
               when lower(trim(users.state)) = 'michigan [mi]' then 'MI'
               when lower(trim(users.state)) = 'mn - minnesota' then 'MN'
               when lower(trim(users.state)) = 'minnesota' then 'MN'
               when lower(trim(users.state)) = 'minnesota (mn)' then 'MN'
               when lower(trim(users.state)) = 'missisippi' then 'MP'
               when lower(trim(users.state)) = 'ms - mississippi' then 'MP'
               when lower(trim(users.state)) = 'mississippi' then 'MP'
               when lower(trim(users.state)) = 'misouri' then 'MS'
               when lower(trim(users.state)) = 'missouri' then 'MS'
               when lower(trim(users.state)) = 'mo - missouri' then 'MS'
               when lower(trim(users.state)) = 'mt' then 'MT'
               when lower(trim(users.state)) = 'mt - montana' then 'MT'
               when lower(trim(users.state)) = 'montana' then 'MT'
               when lower(trim(users.state)) = 'maine' then 'ME'
               when lower(trim(users.state)) = 'n.c' then 'NC'
               when lower(trim(users.state)) = 'n.c.' then 'NC'
               when lower(trim(users.state)) = 'n. carolina' then 'NC'
               when lower(trim(users.state)) = 'nc - north carolina' then 'NC'
               when lower(trim(users.state)) = 'nc north carolina' then 'NC'
               when lower(trim(users.state)) = 'nebraska' then 'NE'
               when lower(trim(users.state)) = 'nd - north dakota' then 'ND'
               when lower(trim(users.state)) = 'ne -' then 'NE'
               when lower(trim(users.state)) = 'new hampshire' then 'NH'
               when lower(trim(users.state)) = 'n.dak' then 'ND'
               when lower(trim(users.state)) = 'nevada' then 'NV'
               when lower(trim(users.state)) = 'newjersey' then 'NJ'
               when lower(trim(users.state)) = 'new jersey' then 'NJ'
               when lower(trim(users.state)) = 'new-jersey' then 'NJ'
               when lower(trim(users.state)) = 'new jersey (nj)' then 'NJ'
               when lower(trim(users.state)) = 'new mexcio' then 'NM'
               when lower(trim(users.state)) = 'new mexico' then 'NM'
               when lower(trim(users.state)) = 'new mexico (nm)' then 'NM'
               when lower(trim(users.state)) = 'newyork' then 'NY'
               when lower(trim(users.state)) = 'new york' then 'NY'
               when lower(trim(users.state)) = 'new york (ny)' then 'NY'
               when lower(trim(users.state)) = 'new mexico' then 'NM'
               when lower(trim(users.state)) = 'n.h.' then 'NH'
               when lower(trim(users.state)) = 'nh -' then 'NH'
               when lower(trim(users.state)) = 'nh - new hampshire' then 'NH'
               when lower(trim(users.state)) = 'nj - new jersey' then 'NJ'
               when lower(trim(users.state)) = 'nj-new jersey' then 'NJ'
               when lower(trim(users.state)) = 'nm - new mexico' then 'NM'
               when lower(trim(users.state)) = 'north caralina' then 'NC'
               when lower(trim(users.state)) = 'north carolina' then 'NC'
               when lower(trim(users.state)) = 'north carolina nc' then 'NC'
               when lower(trim(users.state)) = 'north carolina (nc)' then 'NC'
               when lower(trim(users.state)) = 'north carolinas' then 'NC'
               when lower(trim(users.state)) = 'north caroline' then 'NC'
               when lower(trim(users.state)) = 'north carolinia' then 'NC'
               when lower(trim(users.state)) = 'north dakota' then 'ND'
               when lower(trim(users.state)) = 'ny - new york' then 'NY'
               when lower(trim(users.state)) = 'ny new york' then 'NY'
               when lower(trim(users.state)) = 'oh -' then 'OH'
               when lower(trim(users.state)) = 'ohio' then 'OH'
               when lower(trim(users.state)) = 'ohio oh' then 'OH'
               when lower(trim(users.state)) = 'ohip' then 'OH'
               when lower(trim(users.state)) = 'oh ohio' then 'OH'
               when lower(trim(users.state)) = 'oh - ohio' then 'OH'
               when lower(trim(users.state)) = 'ok -' then 'OK'
               when lower(trim(users.state)) = 'okahoma' then 'OK'
               when lower(trim(users.state)) = 'oklahoma' then 'OK'
               when lower(trim(users.state)) = 'oklahoma (ok)' then 'OK'
               when lower(trim(users.state)) = 'ok - oklahoma' then 'OK'
               when lower(trim(users.state)) = 'oregon' then 'OR'
               when lower(trim(users.state)) = 'or' then 'OR'
               when lower(trim(users.state)) = 'or - oregon' then 'OR'
               when lower(trim(users.state)) = 'pa - pennsylvania' then 'PA'
               when lower(trim(users.state)) = 'pa - pennsylvania [usa]' then 'PA'
               when lower(trim(users.state)) = 'pennslyvania' then 'PA'
               when lower(trim(users.state)) = 'pennsylvaia' then 'PA'
               when lower(trim(users.state)) = 'pennsylvainia' then 'PA'
               when lower(trim(users.state)) = 'pennsylvania (pa)' then 'PA'
               when lower(trim(users.state)) = 'pennsylvannia' then 'PA'
               when lower(trim(users.state)) = 'pennsylvvania' then 'PA'
               when lower(trim(users.state)) = 'rhode island' then 'RI'
               when lower(trim(users.state)) = 'rhode island ri' then 'RI'
               when lower(trim(users.state)) = 'ri - rhode island' then 'RI'
               when lower(trim(users.state)) = 'south caorlina' then 'SC'
               when lower(trim(users.state)) = 'south carolina' then 'SC'
               when lower(trim(users.state)) = 'south dakota' then 'SD'
               when lower(trim(users.state)) = 'teas' then 'TX'
               when lower(trim(users.state)) = 'teaxas' then 'TX'
               when lower(trim(users.state)) = 'tenessee' then 'TN'
               when lower(trim(users.state)) = 'tenn' then 'TN'
               when lower(trim(users.state)) = 'tennessee' then 'TN'
               when lower(trim(users.state)) = 'tennesse' then 'TN'
               when lower(trim(users.state)) = 'tennessee (tn)' then 'TN'
               when lower(trim(users.state)) = 'texa' then 'TX'
               when lower(trim(users.state)) = 'texas' then 'TX'
               when lower(trim(users.state)) = 'texastx' then 'TX'
               when lower(trim(users.state)) = 'texas tx' then 'TX'
               when lower(trim(users.state)) = 'texas / tx' then 'TX'
               when lower(trim(users.state)) = 'texas (tx)' then 'TX'
               when lower(trim(users.state)) = 'tx -' then 'TX'
               when lower(trim(users.state)) = 'tx' then 'TX'
               when lower(trim(users.state)) = 'tx-t' then 'TX'
               when lower(trim(users.state)) = 'txg' then 'TX'
               when lower(trim(users.state)) = 'tx texas' then 'TX'
               when lower(trim(users.state)) = 'tx - texas' then 'TX'
               when lower(trim(users.state)) = 'tx (texas)' then 'TX'
               when lower(trim(users.state)) = 'tx-texas' then 'TX'
               when lower(trim(users.state)) = 'tx - texas [usa]' then 'TX'
               when lower(trim(users.state)) = 'tx w' then 'TX'
               when lower(trim(users.state)) = 'twxas' then 'TX'
               when lower(trim(users.state)) = 'texas' then 'TX'
               when lower(trim(users.state)) = 'utah' then 'UT'
               when lower(trim(users.state)) = 'utah (ut)' then 'UT'
               when lower(trim(users.state)) = '{value: al}' then 'AL'
               when lower(trim(users.state)) = '{value: ar}' then 'AR'
               when lower(trim(users.state)) = '{value: az}' then 'AZ'
               when lower(trim(users.state)) = '{value: ca}' then 'CA'
               when lower(trim(users.state)) = '{value: co}' then 'CO'
               when lower(trim(users.state)) = '{value: ct}' then 'CT'
               when lower(trim(users.state)) = '{value: de}' then 'DE'
               when lower(trim(users.state)) = '{value: fl}' then 'FL'
               when lower(trim(users.state)) = '{value: ga}' then 'GA'
               when lower(trim(users.state)) = '{value: ia}' then 'IA'
               when lower(trim(users.state)) = '{value: il}' then 'IL'
               when lower(trim(users.state)) = '{value: in}' then 'IN'
               when lower(trim(users.state)) = '{value: ks}' then 'KS'
               when lower(trim(users.state)) = '{value: ky}' then 'KY'
               when lower(trim(users.state)) = '{value: la}' then 'LA'
               when lower(trim(users.state)) = '{value: ma}' then 'MA'
               when lower(trim(users.state)) = '{value: md}' then 'MD'
               when lower(trim(users.state)) = '{value: mi}' then 'MI'
               when lower(trim(users.state)) = '{value: mn}' then 'MN'
               when lower(trim(users.state)) = '{value: mo}' then 'MO'
               when lower(trim(users.state)) = '{value: ms}' then 'MS'
               when lower(trim(users.state)) = '{value: nc}' then 'NC'
               when lower(trim(users.state)) = '{value: ne}' then 'NE'
               when lower(trim(users.state)) = '{value: nj}' then 'NJ'
               when lower(trim(users.state)) = '{value: nv}' then 'NV'
               when lower(trim(users.state)) = '{value: ny}' then 'NY'
               when lower(trim(users.state)) = '{value: oh}' then 'OH'
               when lower(trim(users.state)) = '{value: ok}' then 'OK'
               when lower(trim(users.state)) = '{value: or}' then 'OR'
               when lower(trim(users.state)) = '{value: pa}' then 'PA'
               when lower(trim(users.state)) = '{value: sc}' then 'SC'
               when lower(trim(users.state)) = '{value: sd}' then 'SD'
               when lower(trim(users.state)) = '{value: tn}' then 'TN'
               when lower(trim(users.state)) = '{value: tx}' then 'TX'
               when lower(trim(users.state)) = '{value: ut}' then 'UT'
               when lower(trim(users.state)) = '{value: va}' then 'VA'
               when lower(trim(users.state)) = '{value: wa}' then 'WA'
               when lower(trim(users.state)) = '{value: wi}' then 'WI'
               when lower(trim(users.state)) = '{value: wv}' then 'WV'
               when lower(trim(users.state)) = '{value: wy}' then 'WY'
               when lower(trim(users.state)) = 'va virginia' then 'VA'
               when lower(trim(users.state)) = 'va - virginia' then 'VA'
               when lower(trim(users.state)) = 'vermont' then 'VT'
               when lower(trim(users.state)) = 'virgina' then 'VA'
               when lower(trim(users.state)) = 'virgini' then 'VA'
               when lower(trim(users.state)) = 'virginia, hickory high school' then 'VA'
               when lower(trim(users.state)) = 'virginia' then 'VA'
               when lower(trim(users.state)) = 'virginia va' then 'VA'
               when lower(trim(users.state)) = 'virginia (va)' then 'VA'
               when lower(trim(users.state)) = 'virgnia' then 'VA'
               when lower(trim(users.state)) = 'west virginia' then 'WV'
               when lower(trim(users.state)) = 'wisc' then 'WI'
               when lower(trim(users.state)) = 'wisconsin' then 'WI'
               when lower(trim(users.state)) = 'wi - wisconsin' then 'WI'
               when lower(trim(users.state)) = 'wyom' then 'WY'
               when lower(trim(users.state)) = 'wyoming' then 'WY'
               when lower(trim(users.state)) = 'puerto rico' then 'PR'
               when lower(trim(users.state)) = 'pr' then 'WY'
               when lower(trim(users.state)) = 'washington' then 'WA'
               when lower(trim(users.state)) = 'pennsylvania' then 'PA'
               when lower(trim(users.state)) = 'alaska' then 'AK'
               else lower(trim(users.state)) end as User_State,
       users.zipcode                    as user_zipcode,
       users.country                    as user_country,
       users.language                   as user_language,
       users.cio_status,
       users.cio_status_timestamp,
       users.sms_status,
       users.source_detail,
       users.utm_medium                 as user_utm_medium,
       users.utm_source                 as user_utm_source,
       users.utm_campaign               as user_utm_campaign,
       users.contentful_id              as user_contentful_id,
       users.legacy_badges_flag,
       users.badges,
       users.total_badges,
       users.refer_friends,
       users.refer_friends_scholarship,
       users.subscribed_member,
       users.last_updated_at,
       users.school_id                  as user_school_id,
       users.causes,
       users.referrer_user_id,
       certificatesdownloaded.action_id,
       certificatesdownloaded.certificates_downloaded
from signups
         left outer join posts on signups.id = posts.signup_id
         left outer join reportbacks on posts.id = reportbacks.post_id
         left outer join campaign_info on signups.campaign_id = cast(campaign_info.campaign_id as varchar)
         left outer join users on users.northstar_id = signups.northstar_id
         left outer join certificatesdownloaded on cast(posts.action_id as varchar) = certificatesdownloaded.action_id
