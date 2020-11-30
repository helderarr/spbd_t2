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

-- change date time format
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
with data as (select *,
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
     ) x)
select hour,r,pickupregionid,dropoffregionid,
    `Trips count`,`average trip miles`,`trip total cost`
from data
where r <= 5;
