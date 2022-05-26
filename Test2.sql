Select action,
       action_id,
       campaign_id,
       club_id,
       created_at,
       group_id,
       id,
       is_accepted,
       is_anonymous,
       is_civic_action,
       is_online,
       is_quiz,
       is_reportback,
       is_scholarship_entry,
       time_commitment,
       is_volunteer_credit,
       location,
       northstar_id,
       noun,
       num_participants,
       postal_code,
       post_class,
       quantity,
       referrer_user_id,
       reportback_volume,
       school_id,
       source_bucket,
       signup_id,
       source,
       status,
       text,
       type,
       hours_spent,
       url,
       verb,
       vr_source,
       vr_source_details,
       tags
from posts
where campaign_id in ('6716', '6691', '6690', '6460','7713');

select campaign_info.campaign_name, signups.campaign_id, why_participated from signups
left outer join campaign_info on signups.campaign_id = cast(campaign_info.campaign_id as varchar)
where signups.campaign_id in ('6716', '6691', '6690', '6460','7713')
and why_participated is not null;


Select *
from campaign_info
where campaign_id in ('6716', '6691', '6690', '6460','7713');

