-- Question 4
-- top 3 companies driving from the most popular locations (more than 5000 trips from there)
with trips_per_company as (
    select pickupregionid,company, count(*) ntrips
    from taxi_trips
    where company is not null and company <> ''
        and pickupregionid is not null and pickupregionid <> ''
    group by pickupregionid,company),
ranked_trips_per_company as (
    select pickupregionid,company, ntrips,
           row_number() over (partition by pickupregionid order by ntrips desc) r,
           sum(ntrips) over (partition by pickupregionid) total_sum
    from trips_per_company),
filtered_data as (
    select pickupregionid,company, total_sum,ntrips,r
    from ranked_trips_per_company
    where r <= 3 and total_sum >= 5000
    order by total_sum desc, pickupregionid asc, r asc)
select pickupregionid,company,ntrips
from filtered_data;