

--Members by Access Date
select count(northstar_id),
        date_trunc('day',last_accessed) accessed_date,
       state,
       country
from users
group by accessed_date, country, state;

--Members by logged in date
select count(northstar_id),
       state,
       country
from users
group by country, state;

--Members by Created Date
select count(northstar_id),
       date_trunc('year', created_at) createdat_date,
       state,
       country
from users
group by createdat_date, country, state;

