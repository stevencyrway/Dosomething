----- The general campaign query used for All Campaigns and campaign history,
----- some fields from joins have been reduced due to non use.

-- Only for specific campaigns that used certificates
With certificatesdownloaded (action_id, certificates_downloaded) as (
    SELECT phoenix_next_events.action_id,
           COUNT(DISTINCT (phoenix_next_events."event_id")) AS "certificates_downloaded"
    FROM quasar_prod_warehouse.public.snowplow_raw_events AS phoenix_next_events
    WHERE phoenix_next_events.event_name = 'phoenix_clicked_download_button_volunteer_credits_table'
    group by phoenix_next_events.action_id),
     -- to make sense of the tags for posts to make them easy to view in tableau
     parsedtags as (
         Select id, replace(cast(json_array_elements(tags) as varchar), '"', '') as tags from posts),
     ---to make sense of the causes that are comma seperated and make them flaggable in tableau
     parsedcauses as (
         Select northstar_id, unnest(string_to_array(causes, ',')) as cause
         from users
         group by northstar_id, unnest(string_to_array(causes, ',')))

Select signups.northstar_id                                                  as     signup_northstar_id,
       signups.id                                                            as     signup_id,
       signups.campaign_id                                                   as     signup_campaign_id,
       signups.why_participated,
       signups.source                                                        as     signup_source,
       signups.created_at                                                    as     signup_created_at,
       posts.action                                                          as     post_action,
       posts.action_id                                                       as     post_action_id,
       posts.created_at                                                      as     post_created_at,
       posts.id                                                              as     post_id,
       case when parsedtags.tags = 'Bulk' then 1 else 0 end                  as     Bulk,
       case when parsedtags.tags = 'Good For Brand' then 1 else 0 end        as     GoodForBrand,
       case when parsedtags.tags = 'Good For Sponsor' then 1 else 0 end      as     GoodforSponsor,
       case when parsedtags.tags = 'Good For Storytelling' then 1 else 0 end as     GoodforStorytelling,
       case when parsedtags.tags = 'Good Quote' then 1 else 0 end            as     GoodQuote,
       case when parsedtags.tags = 'Good Submission' then 1 else 0 end       as     GoodSubmission,
       case when parsedtags.tags = 'Group Photo' then 1 else 0 end           as     GroupPhoto,
       case when parsedtags.tags = 'Hide In Gallery' then 1 else 0 end       as     HideInGallery,
       case when parsedtags.tags = 'Inappropriate' then 1 else 0 end         as     Inappropriate,
       case when parsedtags.tags = 'Incomplete Action' then 1 else 0 end     as     IncompleteAction,
       case when parsedtags.tags = 'Irrelevant' then 1 else 0 end            as     Irrelevant,
       case when parsedtags.tags = 'Social' then 1 else 0 end                as     Social,
       case when parsedtags.tags = 'Test' then 1 else 0 end                  as     Test,
       case when parsedtags.tags = 'Unrealistic Hours' then 1 else 0 end     as     UnrealisticHours,
       case when parsedtags.tags = 'Unrealistic Quantity' then 1 else 0 end  as     UnrealisticQuantity,
       posts.location                                                        as     post_location,
       posts.noun                                                            as     post_noun,
       posts.num_participants,
       posts.postal_code                                                     as     post_postal_code,
       CASE
           WHEN posts.quantity > 10000 THEN 1
           WHEN posts.quantity IS NULL THEN 1
           ELSE posts.quantity END                                           AS     post_quantity_clean,
       posts.reportback_volume                                               as     post_reportback_volume,
       posts.school_id                                                       as     post_school_id,
       posts.source_bucket                                                   as     post_source_bucket,
       posts.signup_id                                                       as     post_signup_id,
       posts.status                                                          as     post_status,
       posts.text                                                            as     post_text,
       posts.type                                                            as     post_type,
       posts.hours_spent                                                     as     post_hours_spent,
       posts.verb                                                            as     post_verb,
       reportbacks.location                                                  as     reportback_location,
       reportbacks.northstar_id                                              as     reportback_northstar_id,
       reportbacks.signup_id                                                 as     reportback_signup_id,
       reportbacks.post_status                                               as     reportback_status,
       reportbacks.hours_spent                                               as     reportback_hours_spent,
       reportbacks.post_created_at                                           as     reportback_created_at,
       reportbacks.post_bucket                                               as     reportback_bucket,
       reportbacks.reportback_volume,
       campaign_goals.reportback_goal,
       campaign_goals.signup_goal,
       campaign_goals.volunteerhour_goal,
       campaign_info.campaign_id,
       campaign_info.campaign_name,
       campaign_info.campaign_cause,
       campaign_info.campaign_run_start_date,
       campaign_info.campaign_run_end_date,
       campaign_info.campaign_created_date,
       campaign_info.contentful_id,
       campaign_info.campaign_cause_type,
       campaign_info.campaign_cta,
       campaign_info.action_types,
       campaign_info.online_offline,
       campaign_info.scholarship,
       campaign_info.post_types,
       users.created_at                                                             user_created_at,
       users.club_id,
       users.email,
       users.email_subscription_topics,
       case
           when users.source = 'importer-client' and users.source_detail = 'rock-the-vote' then 'voter-reg'
           when users.source = 'importer-client' and source_detail is null then 'voter-reg'
           when users.utm_source ilike 'scholarship_%' then 'scholarship'
           when users.utm_medium ilike 'scholarship_%' then 'scholarship'
           when users.utm_campaign ilike '%vcredit%' then 'volunteer-credit'
           else 'core' end                                                   as     SignupMotivation,
       (extract(year from age(now(), users.created_at)) * 12) +
       (extract(month from age(now(), users.created_at)))                    as     TenureinMonths,
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
           else 'g 24+ mo' end                                               as     TenureGroup,
       (DATE_PART('year', users.created_at) - DATE_PART('year', birthdate))  as     AgeAtAccountCreation,
       (DATE_PART('year', now()) - DATE_PART('year', birthdate))             as     Age,
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
           else 'Likely Made up Age or blank' end                            as     GenerationGroup,
       case
           when users.sms_status in ('active', 'less', 'pending') and
                users.cio_status in ('customer_subscribed') then 'email & sms'
           when users.cio_status in ('customer_subscribed') then 'email only'
           when users.sms_status in ('active', 'less', 'pending') then 'sms only'
           else 'none' end                                                   as     MemberAddressableStatus,
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
           else lower(trim(users.state)) end                                 as     User_State,
       users.zipcode                                                         as     user_zipcode,
       users.country                                                         as     user_country,
       users.language                                                        as     user_language,
       case when parsedcauses.cause = 'animal_welfare' then 1 else 0 end            AnimalWelfare,
       case when parsedcauses.cause = 'bullying' then 1 else 0 end                  Bullying,
       case when parsedcauses.cause = 'environment' then 1 else 0 end               Environment,
       case when parsedcauses.cause = 'gender_rights_equality' then 1 else 0 end    gender_rights_equality,
       case when parsedcauses.cause = 'homelessness_poverty' then 1 else 0 end      Ehomelessness_poverty,
       case when parsedcauses.cause = 'immigration_refugees' then 1 else 0 end      immigration_refugees,
       case when parsedcauses.cause = 'lgbtq_rights_equality' then 1 else 0 end     lgbtq_rights_equality,
       case when parsedcauses.cause = 'mental_health' then 1 else 0 end             mental_health,
       case when parsedcauses.cause = 'physical_health' then 1 else 0 end           physical_health,
       case when parsedcauses.cause = 'racial_justice_equity' then 1 else 0 end     racial_justice_equity,
       case when parsedcauses.cause = 'sexual_harassment_assault' then 1 else 0 end sexual_harassment_assault,
       users.referrer_user_id,
       certificatesdownloaded.action_id,
       certificatesdownloaded.certificates_downloaded
from signups
         left outer join posts on signups.id = posts.signup_id
         left outer join reportbacks on posts.id = reportbacks.post_id
         left outer join campaign_info on signups.campaign_id = cast(campaign_info.campaign_id as varchar)
         left outer join ft_google_sheets.campaign_goals on campaign_goals.campaign_id = campaign_info.campaign_id
         left outer join users on users.northstar_id = signups.northstar_id
         left outer join certificatesdownloaded on cast(posts.action_id as varchar) = certificatesdownloaded.action_id
         left outer join parsedtags on parsedtags.id = posts.id
         left outer join parsedcauses on parsedcauses.northstar_id = users.northstar_id
where campaign_created_date >= current_date - INTERVAL '2 YEARS';


