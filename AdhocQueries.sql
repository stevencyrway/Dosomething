-----Query for Age, voter reg, email mobile status, and counts
Select count(distinct northstar_id),
       case
           when date_part('year', age(birthdate)) >= 18 and date_part('year', age(birthdate)) <= 28
               then cast(date_part('year', age(birthdate)) as varchar)
           when date_part('year', age(birthdate)) > 28 then 'Older than 28' end as Age,
       case when email is not null or mobile is not null then True else False end as Hasemailormobile,
       voter_registration_status
from users
where subscribed_member = 'true'
group by voter_registration_status, case when email is not null or mobile is not null then True else False end, case
           when date_part('year', age(birthdate)) >= 18 and date_part('year', age(birthdate)) <= 28
               then cast(date_part('year', age(birthdate)) as varchar)
           when date_part('year', age(birthdate)) > 28 then 'Older than 28' end;

--Badges query
With badgeparsedas as (
    Select northstar_id,
           last_accessed,
           last_messaged_at,
           last_logged_in,
           replace(cast(json_array_elements(badges) as varchar), '"', '') as badges
    from users
    where badges is not null)

Select 'MessagedYearLast' as DataInfo,
       date_part('year', last_messaged_at),
       count(distinct case when badges = 'news-subscription' then northstar_id end)          as NewsSubscription,
       count(distinct case when badges = 'signup' then northstar_id end)                     as signup,
       count(distinct case when badges = 'one-post' then northstar_id end)                   as OnePost,
       count(distinct case when badges = 'one-staff-fave' then northstar_id end)             as OneStaffFave,
       count(distinct case when badges = 'two-posts' then northstar_id end)         as TwoPosts,
       count(distinct case when badges = 'two-staff-faves' then northstar_id end)   as TwoStaffFaves,
       count(distinct case when badges = 'three-posts' then northstar_id end)       as ThreePosts,
       count(distinct case when badges = 'three-staff-faves' then northstar_id end) as ThreeStaffFaves,
       count(distinct case when badges = 'four-posts' then northstar_id end)        as FourPosts
from badgeparsedas
group by date_part('year', last_messaged_at)

union all

Select 'AccessedYearLast' as DataInfo,
       date_part('year', last_accessed),
       count(distinct case when badges = 'news-subscription' then northstar_id end)          as NewsSubscription,
       count(distinct case when badges = 'signup' then northstar_id end)                     as signup,
       count(distinct case when badges = 'one-post' then northstar_id end)                   as OnePost,
       count(distinct case when badges = 'one-staff-fave' then northstar_id end)             as OneStaffFave,
       count(distinct case when badges = 'two-posts' then northstar_id end)         as TwoPosts,
       count(distinct case when badges = 'two-staff-faves' then northstar_id end)   as TwoStaffFaves,
       count(distinct case when badges = 'three-posts' then northstar_id end)       as ThreePosts,
       count(distinct case when badges = 'three-staff-faves' then northstar_id end) as ThreeStaffFaves,
       count(distinct case when badges = 'four-posts' then northstar_id end)        as FourPosts
from badgeparsedas
group by date_part('year', last_accessed)

union all

Select 'LoggedInYearLast' as DataInfo,
       date_part('year', last_logged_in),
       count(distinct case when badges = 'news-subscription' then northstar_id end)          as NewsSubscription,
       count(distinct case when badges = 'signup' then northstar_id end)                     as signup,
       count(distinct case when badges = 'one-post' then northstar_id end)                   as OnePost,
       count(distinct case when badges = 'one-staff-fave' then northstar_id end)             as OneStaffFave,
       count(distinct case when badges = 'two-posts' then northstar_id end)         as TwoPosts,
       count(distinct case when badges = 'two-staff-faves' then northstar_id end)   as TwoStaffFaves,
       count(distinct case when badges = 'three-posts' then northstar_id end)       as ThreePosts,
       count(distinct case when badges = 'three-staff-faves' then northstar_id end) as ThreeStaffFaves,
       count(distinct case when badges = 'four-posts' then northstar_id end)        as FourPosts
from badgeparsedas
group by date_part('year', last_logged_in)
