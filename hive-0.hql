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
