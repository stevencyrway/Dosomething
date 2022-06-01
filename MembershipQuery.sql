With Members as (SELECT users.northstar_id,
                        users.created_at,
                        club_id,
                        last_logged_in,
                        last_accessed,
                        last_messaged_at,
                        drupal_uid,
                        source,
                        email,
                        email_subscription_topics,
                        facebook_id,
                        mobile,
                        birthdate,
                        first_name,
                        last_name,
                        voter_registration_status,
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
                            else lower(trim(users.state)) end                          as State,
                        zipcode,
                        country,
                        language,
                        cio_status,
                        cio_status_timestamp,
                        sms_status,
                        source_detail,
                        utm_medium,
                        utm_source,
                        utm_campaign,
                        users.contentful_id,
                        legacy_badges_flag,
                        badges,
                        total_badges,
                        refer_friends,
                        refer_friends_scholarship,
                        subscribed_member,
                        last_updated_at,
                        school_id,
                        causes,
                        referrer_user_id,
                        -- DS Campaign Attribution --
                        case
                            when lower(campaign_info.campaign_name) is not null then lower(campaign_info.campaign_name)
                            when lower(source) = 'importer-client' and lower(source_detail) = 'rock-the-vote'
                                then 'rock-the-vote'
                            when lower(source) = 'importer-client' and source_detail is null then 'rock-the-vote'
                            when lower(source) = 'importer-client' and lower(source_detail) like '%opt_in%'
                                then 'email_signup'
                            else 'no attributable DS campaign' end                     as DS_campaign,
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
                            end                                                        as AcquisitionChannel,
                        --Acquisition Motivation
                        case
                            when source = 'importer-client' and source_detail = 'rock-the-vote' then 'voter-reg'
                            when source = 'importer-client' and source_detail is null then 'voter-reg'
                            when utm_source ilike 'scholarship_%' then 'scholarship'
                            when utm_medium ilike 'scholarship_%' then 'scholarship'
                            when utm_campaign ilike '%vcredit%' then 'volunteer-credit'
                            else 'core' end                                            as AcquisitionMotivation,
                        (extract(year from age(now(), created_at)) * 12) +
                        (extract(month from age(now(), created_at)))                   as TenureinMonths,
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
                            else 'g 24+ mo' end                                        as TenureGroup,
                        (DATE_PART('year', created_at) - DATE_PART('year', birthdate)) as AgeAtAccountCreation,
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
                 from public.users
                          left join campaign_info ON substring(users.source_detail from '(?<=contentful_id\:)(\w*)') =
                                                     campaign_info.contentful_id),

--Main Query for Membership metrics
     MemberswithQualifiers as (SELECT State,
                                      country,
                                      language,
                                      DS_campaign,
                                      AcquisitionChannel,
                                      AcquisitionMotivation,
                                      TenureinMonths,
                                      TenureGroup,
                                      AgeAtAccountCreation,
                                      total_badges,
                                      members.northstar_id,
                                      user_unsubscribed_at,
                                      members.created_at,
                                      most_recent_mam_action,
                                      most_recent_email_open,
                                      memberaddressablestatus,
                                      Age,
                                      GenerationGroup,
                                      CASE
                                          WHEN (case
                                                    when user_activity.sms_status in ('active', 'less', 'pending') and
                                                         user_activity.email_status in ('customer_subscribed')
                                                        then 'email & sms'
                                                    when user_activity.email_status in ('customer_subscribed')
                                                        then 'email only'
                                                    when user_activity.sms_status in ('active', 'less', 'pending')
                                                        then 'sms only'
                                                    else 'none' end) = 'none' then 0
                                          ELSE 1 END                              AS count_subscribed,
                                      CASE
                                          WHEN (case
                                              -- unsub scenarios
                                                    when (user_activity.user_unsubscribed_at is not null)
                                                        then 'not subscribed'
                                                    when (user_activity.email_status is null and user_activity.sms_status is null)
                                                        then 'not subscribed'
                                                    when (user_activity.email_status is null and
                                                          user_activity.sms_status in
                                                          ('stop', 'undeliverable', 'unknown'))
                                                        then 'not subscribed'
                                              -- sms scenarios
                                                    when ((user_activity.email_status is null or
                                                           user_activity.email_status = 'customer_unsubscribed')
                                                        and user_activity.sms_status in ('active', 'less', 'pending')
                                                        and date_part('day', now() - coalesce(
                                                                user_activity.most_recent_mam_action,
                                                                user_activity.created_at)) >= 365)
                                                        then 'sms inactive'
                                                    when (user_activity.sms_status in ('active', 'less', 'pending')
                                                        and date_part('day', now() - coalesce(
                                                                user_activity.most_recent_mam_action,
                                                                user_activity.created_at)) < 365)
                                                        then 'addressable'
                                              -- email scenarios
                                                    when (user_activity.email_status = 'customer_subscribed'
                                                        and date_part('day', now() - coalesce(
                                                                user_activity.most_recent_email_open,
                                                                user_activity.created_at)) >= 120)
                                                        then 'email inactive'
                                                    when (user_activity.email_status = 'customer_subscribed'
                                                        and date_part('day', now() - coalesce(
                                                                user_activity.most_recent_email_open,
                                                                user_activity.created_at)) < 120)
                                                        then 'addressable'
                                              -- this is to catch anything that does not fall into the above rules
                                                    else 'other' end) = 'addressable'
                                              then 1 END                          AS count_active_subscribed,
                                      CASE
                                          WHEN (date_part('day', now() - user_activity.most_recent_mam_action)) <= 120
                                              THEN 1 END AS count_mel_past_120,
                                      case
                                          when date_part('day', now() - (DATE(user_activity.most_recent_rb))) <= 120
                                              then 1 end AS count_impact_past_120,
                                      case
                                          -- unsub scenarios
                                          when (user_unsubscribed_at is not null)
                                              then 'not subscribed'
                                          when (email_status is null and user_activity.sms_status is null)
                                              then 'not subscribed'
                                          when (email_status is null and
                                                user_activity.sms_status in ('stop', 'undeliverable', 'unknown'))
                                              then 'not subscribed'
                                          -- sms scenarios
                                          when ((email_status is null or email_status = 'customer_unsubscribed')
                                              and user_activity.sms_status in ('active', 'less', 'pending')
                                              and date_part('day', now() -
                                                                   coalesce(most_recent_mam_action, user_activity.created_at)) >=
                                                  365)
                                              then 'sms inactive'

                                          when (user_activity.sms_status in ('active', 'less', 'pending')
                                              and date_part('day', now() -
                                                                   coalesce(most_recent_mam_action, user_activity.created_at)) <
                                                  365)
                                              then 'addressable'
                                          -- email scenarios
                                          when (email_status = 'customer_subscribed'
                                              and date_part('day', now() -
                                                                   coalesce(most_recent_email_open, user_activity.created_at)) >=
                                                  120)
                                              then 'email inactive'

                                          when (email_status = 'customer_subscribed'
                                              and date_part('day', now() -
                                                                   coalesce(most_recent_email_open, user_activity.created_at)) <
                                                  120)
                                              then 'addressable'
                                          -- this is to catch anything that does not fall into the above rules
                                          else 'other' end                        as MessagingStatus
                               FROM members
                                        LEFT JOIN public.user_activity AS user_activity
                                                  ON members.northstar_id = user_activity.northstar_id),
--- to add in churn qualifications and case statements
     Memberswithqualifiersandchurn as (Select *
                                            , Case
                                                  when Age >= 0 and Age <= 14 then 'Children 0-14'
                                                  when Age >= 15 and Age <= 24 then 'Youth 15-24'
                                                  when Age >= 25 and Age <= 64 then 'Adults 25-64'
                                                  when Age >= 65 then 'Seniors 65+'
                                                  else 'Likely Made Up age' end     as AgeGroup
                                            , Case
                                                  when Ageataccountcreation >= 0 and Ageataccountcreation <= 14
                                                      then 'Children 0-14'
                                                  when Ageataccountcreation >= 15 and Ageataccountcreation <= 24
                                                      then 'Youth 15-24'
                                                  when Ageataccountcreation >= 25 and Ageataccountcreation <= 64
                                                      then 'Adults 25-64'
                                                  when Ageataccountcreation >= 65 then 'Seniors 65+'
                                                  else 'Likely Made Up Age' end     as AgeGroupatAccountCreation
                                            , Date_trunc('Week', CASE
             -- UNSUB OR UNDEL (NO EMAIL OR SMS)
                                                                     WHEN messagingstatus IN ('not subscribed')
                                                                         THEN coalesce(user_unsubscribed_at, created_at)
             -- SMS INACTIVE
                                                                     WHEN messagingstatus IN ('sms inactive')
                                                                         THEN coalesce(most_recent_mam_action, created_at) + INTERVAL '365 days'
             -- EMAIL INACTIVE
                                                                     WHEN messagingstatus IN ('email inactive')
                                                                         THEN coalesce(most_recent_email_open, created_at) + INTERVAL '120 days'
                                                                     ELSE null END) as ChurnDate

                                       from MemberswithQualifiers)


Select state,
       country,
       language,
       date_trunc('week',created_at),
       ds_campaign,
       acquisitionchannel,
       acquisitionmotivation,
       tenuregroup,
       AgeGroup,
       GenerationGroup,
       agegroupataccountcreation,
       messagingstatus,
       memberaddressablestatus,
       Case when churndate is null then 'Not Churned' else 'Churned' end as ChurnedFlag,
       date_trunc('week',created_at)                                     as CreatedDateWeek,
       date_trunc('week', churndate)                                     as ChurnDateWeek,
       avg(tenureinMonths),
       sum(total_badges)                                                 as "Total Badges",
       count(distinct northstar_id)                                      as "Member Count",
       sum(count_subscribed)                                             as "Subscribed Members",
       sum(count_active_subscribed)                                      as "Active Addressable Members",
       sum(count_mel_past_120)                                           as "Members Engaging (Last 120 days",
       sum(count_impact_past_120)                                        as "Members Making Impact (Last 120 day)"
from Memberswithqualifiersandchurn
Group by state, country, language, ds_campaign, acquisitionchannel, acquisitionmotivation, tenuregroup, AgeGroup,
         GenerationGroup, agegroupataccountcreation, messagingstatus, memberaddressablestatus,
         Case when churndate is null then 'Not Churned' else 'Churned' end, date_trunc('week',created_at), date_trunc('week', churndate)


