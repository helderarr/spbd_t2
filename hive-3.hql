-- Question 3
-- What is the expected charge/cost of a taxi ride, given the pickup region ID, the weekday
-- (0=Monday, 6=Sunday) and time in format “hour AM/PM”?
-- Output is expected to have two columns: (month_number, avg_total_trip_cost).
-- We used <<pickup>> as first column name instead of <<month_number>> as <<month_number>> is meaningless
select concat(PickupRegionID,"_",cast(date_format(tripstarttimestamp, "u") as INT) -1,
    date_format(tripstarttimestamp, "_hh_aaa")) as pickup,
       round(avg (TripTotal),2) as avg_total_trip_cost
from taxi_trips
where PickupRegionID is not null and PickupRegionID <> ''
group by PickupRegionID,tripstarttimestamp;

-- variant without Weekday just for measuring processing time
select concat(PickupRegionID, date_format(tripstarttimestamp, "_hh_aaa")) as pickup,
       round(avg (TripTotal),2) as avg_total_trip_cost
from taxi_trips
where PickupRegionID is not null and PickupRegionID <> ''
group by PickupRegionID,tripstarttimestamp;
