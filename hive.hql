DROP TABLE IF EXISTS taxi_trips;

-- creating the data table
CREATE TABLE `taxi_trips`(
  `tripid` string,
  `taxiid` string,
  `tripstarttimestamp` timestamp,
  `tripendtimestamp` timestamp,
  `tripseconds` float,
  `tripmiles` float,
  `pickupregionid` string,
  `dropoffregionid` string,
  `pickupcommunity` float,
  `dropoffcommunity` float,
  `fare` float,
  `tips` float,
  `tolls` float,
  `extras` float,
  `triptotal` float,
  `paymenttype` string,
  `company` string,
  `pickupcentroidlatitude` float,
  `pickupcentroidlongitude` float,
  `pickupcentroidlocation` string,
  `dropoffcentroidlatitude` float,
  `dropoffcentroidlongitude` float,
  `dropoffcentroidlocation` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE;

-- change date time text format
ALTER TABLE taxi_trips SET SERDEPROPERTIES ("timestamp.formats"="MM/dd/yyyy hh:mm:ss aaa");

-- load data into table
LOAD DATA LOCAL INPATH "/root/work/Taxi_Trips_151MB.csv" INTO TABLE taxi_trips;

---------------------------------------------------------------------
-- Question 1
-- What is the accumulated number of taxi trips per month?
-- Output is expected to have two columns: (month_number, #total_trips).
select lpad(month(TripStartTimestamp), 2, "0") as month_number,
    count(*) as `#total_trips`
from taxi_trips
group by lpad(month(TripStartTimestamp), 2, "0");


-- Question 2
-- For each pickup region, report the list of unique dropoff regions?
-- Output is expected to have two columns: (pickup_region_ID, list_of_dropoff_region_ID).
select PickupRegionID as pickup_region_ID,
    collect_set(DropoffRegionID) as list_of_dropoff_region_ID
from   (select distinct PickupRegionID,DropoffRegionID  from taxi_trips
        where PickupRegionID is not null and DropoffRegionID is not null) x
group by PickupRegionID;

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
group by concat(PickupRegionID,"_",cast(date_format(tripstarttimestamp, "u") as INT) -1,
    date_format(tripstarttimestamp, "_hh_aaa"));

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
    order by total_sum desc, r asc)
select pickupregionid,company,ntrips
from filtered_data;