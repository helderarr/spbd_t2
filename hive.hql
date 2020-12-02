DROP TABLE IF EXISTS taxi_trips;

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

LOAD DATA LOCAL INPATH "/root/work/Taxi_Trips_151MB.csv" INTO TABLE taxi_trips;

--How many trips were started in each year present in the data set?

select year(tripstarttimestamp) as `year`, count(*) as `Trips count`
from taxi_trips
group by year(tripstarttimestamp);


--For each of the 24 hours of the day, how many taxi trips there were,
--what was their average trip miles and trip total cost?
--Non-integer values should be printed with two decimal places.

select date_format(tripstarttimestamp,'HH aaa') as `hour`,
       count(*) as `Trips count`,
       round(sum(tripmiles)/count(*),2) `average trip miles`,
       round(sum(triptotal)/count(*),2) `trip total cost`
from taxi_trips
group by date_format(tripstarttimestamp,'HH aaa');

--For each of the 24 hours of the day, which are the (up to) 5 most popular routes (pairs
--pickup/dropoff regions) according to the the total number of taxi trips? Also report
--and the average fare (total trip cost).
--Non-integer values should be printed with two decimal places.
with data as (
    select *,
       row_number() over (partition by hour order by `trip total cost` desc) r
    from (
             select date_format(tripstarttimestamp, 'HH aaa') as `hour`,
                    pickupregionid,
                    dropoffregionid,
                    count(*)                                  as `Trips count`,
                    round(sum(tripmiles) / count(*), 2)          `average trip miles`,
                    round(sum(triptotal) / count(*), 2)          `trip total cost`
             from taxi_trips
             where pickupregionid is not null and pickupregionid <> ''
               and dropoffregionid is not null and dropoffregionid <> ''
             group by date_format(tripstarttimestamp, 'HH aaa'), pickupregionid, dropoffregionid
         ) x
    )
select hour,pickupregionid,dropoffregionid,
    `Trips count`,`average trip miles`,`trip total cost`
from data
where r <= 5;


--What is the accumulated number of taxi trips per month?
select lpad(month(tripstarttimestamp), 2, "0") as `month`, count(*) as `Trips count`
from taxi_trips
group by month(tripstarttimestamp);

---------------------------------------------------------------------

-- What is the accumulated number of taxi trips per month?
-- Output is expected to have two columns: (month_number, #total_trips).
select lpad(month(TripStartTimestamp), 2, "0") as month_number,
    count(*) as `#total_trips`
from taxi_trips
group by lpad(month(TripStartTimestamp), 2, "0");

select PickupRegionID as pickup_region_ID,
    collect_set(DropoffRegionID) as list_of_dropoff_region_ID
from   (select distinct PickupRegionID,DropoffRegionID  from taxi_trips
        where PickupRegionID is not null and DropoffRegionID is not null) x
group by PickupRegionID;


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

