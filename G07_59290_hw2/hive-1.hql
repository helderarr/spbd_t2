
---------------------------------------------------------------------
-- Question 1
-- What is the accumulated number of taxi trips per month?
-- Output is expected to have two columns: (month_number, #total_trips).
select lpad(month(TripStartTimestamp), 2, "0") as month_number,
    count(*) as `#total_trips`
from taxi_trips
group by month(TripStartTimestamp);

