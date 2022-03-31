
select
     f.session_id
    ,f.device_id
    ,f.northstar_id
    ,1 as journeys
    ,f.journey_begin_date
    ,f.referring_domain
    ,f.utm_medium
    ,f.utm_source
    ,f.utm_campaign
    ,f.landing_page
    ,f.previous_page
    --If users clicked both buttons to the registration page, we only thake the last one
    ,case
      when f.last_join_button_used=f.join_nav then 'Top Nav'
      when f.last_join_button_used=f.join_page then 'Sign Up'
    end as source_button
    ,f.view_register_page
    --There are 3 ways in the registration page in which users can register, Email, Google and Facebook
    --When the account is created with email, we get confirmation tracking (submitted_register=1)
    --But when the account is created with Google or Facebook we only know the user has been authenticated
    --but there's no difference between Google vs Facebook or new registration vs login from pre-existing user

    --Users trigger the tracking event to initiate registration with Google or Facebook which takes users to Google or Facebook to confirm
    --If we detect both intents to register (with Google and with Facebook) in he same session we evaluate for the one that occurred last
    --whether the user ends up registering or not
    ,case when f.submited_register_email=1 or f.last_register_event=f.start_register_email then 'Email'
        when f.last_register_event=f.start_register_fb then 'Facebok'
        when f.last_register_event=f.start_register_google then 'Google'
        else null
     end as registration_credentials
    ,case when f.northstar_id is not null then 1 else 0 end as account_created
    ,f.view_about_page
    ,f.view_subpref_page
  from (


      --This are the multiple steps in the funnel tracked as yes/no or by timestamp
      select
        psc.device_id
      , psc.session_id
      , sm.northstar_id
      --The start of the session/journey
      , min(psc.landing_datetime::date) as journey_begin_date
      , max(psc.landing_page) as landing_page
      , max(case when psc.session_referrer_host is not null then psc.session_referrer_host end) as referring_domain
      , max(case when pec.utm_medium is not null then pec.utm_medium end) as utm_medium
      , max(case when psc.session_utm_source is not null then psc.session_utm_source end) as utm_source
      , max(case when psc.session_utm_campaign is not null then psc.session_utm_campaign end) as utm_campaign
      , max(case when pec.event_name='view' and pec.path='/register' then pec.referrer_path else null end) as previous_page
      -- Whether they clicked join in top nav and/or page (both can happen in the session so) we take the timestamp and use the latest event in the outer query
      , max(case when pec.event_name='phoenix_clicked_nav_link_join_now' then pec.event_datetime else null end) as join_nav
      , max(case when pec.event_name='phoenix_clicked_signup' then pec.event_datetime else null end) as join_page
      , max(case when pec.event_name in ('phoenix_clicked_nav_link_join_now','phoenix_clicked_signup') then pec.event_datetime else null end) as last_join_button_used
        --Sees the registration page
      , max(case when pec.path='/register' and pec.event_name='view' then 1 else 0 end) as view_register_page
      , max(case when pec.path='/register' and pec.event_name='northstar_focused_field_email' then pec.event_datetime else null end) as start_register_email
      -- Whether they clicked join with Google, Facebook or Email (either intention to use one of the 3 types of registration credentials can happen in the session so) we take the timestamp and use the latest event in the outer query
        --tbd -- in a future version we may want to look at cases when users start by entering email and end up using google/facebook credentials
      , max(case when pec.event_name='northstar_clicked_login_google' then pec.event_datetime else null end) as start_register_google
      , max(case when pec.event_name='northstar_clicked_login_facebook' then pec.event_datetime else null end) as start_register_fb
      , max(case when pec.event_name in ('northstar_clicked_login_google','northstar_clicked_login_facebook','northstar_focused_field_email','northstar_submitted_register')
            then pec.event_datetime
          end) as last_register_event
      , max(case when pec.event_name='northstar_submitted_register' then 1 else 0 end) as submited_register_email
      , max(case when pec.event_name='view' and pec.path='/profile/about' then 1 else 0 end) as view_about_page
      , max(case when pec.event_name='view' and pec.path='/profile/subscriptions' then 1 else 0 end) as view_subpref_page

      from public.snowplow_sessions psc
      join public.snowplow_raw_events pec on (psc.session_id=pec.session_id and psc.device_id=pec.device_id and pec.event_datetime between psc.landing_datetime and psc.ending_datetime)
      left join looker_scratch.LR$Q6VL21648552726874_web_sessions_members sm on (psc.session_id=sm.session_id and psc.device_id=sm.device_id and psc.landing_datetime=sm.landing_datetime)
      where psc.landing_datetime::date > (
          select max(journey_begin_date) from analyst_sandbox.es_registration_funnel
      )
      and psc.landing_datetime::date <= (
          select max(landing_datetime) from looker_scratch.LR$Q6VL21648552726874_web_sessions_members
      )
      and pec.event_datetime between psc.landing_datetime and psc.ending_datetime

      and pec.event_name in ('view','phoenix_clicked_nav_link_join_now','phoenix_clicked_signup',
      'northstar_focused_field_email','northstar_clicked_login_google','northstar_clicked_login_facebook',
      'northstar_submitted_register')
      --This part excludes sessions that started authenticated
      --This part removes sessions from pre-existing users who authenticated in the session
      --Only anonymous sessions or new member sessions (table sessions_members has all authenticated sessions)
      and (sm.landing_datetime < sm.user_created_at or sm.session_id is null)
      group by
        psc.session_id
      , psc.device_id
      , sm.northstar_id

  ) f

    union all

    select * from analyst_sandbox.es_registration_funnel