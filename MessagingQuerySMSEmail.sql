--email Query
    SELECT count(distinct event_id) as Eventcount,
           count(distinct customer_id)     as UserCount,
           date_trunc('day', timestamp) as Date,
           cio_campaign_name  as campaign,
           cio_campaign_type as content_type,
           event_type,
           subject           as topic
    from cio_email_events
    where timestamp  >= (current_date - interval '180 days')
group by
customer_id, date_trunc('day', timestamp), cio_campaign_name, cio_campaign_type, event_type, subject;

--SMS Query
select * from gambit_messages_outbound
where conversation_id = '5daa5b0e2df5830038b383d4'


select current_date - interval '180 days'