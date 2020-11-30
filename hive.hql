CREATE TABLE `taxi_trips`(
  `tripid` string,
  `taxiid` string,
  `tripstarttimestamp` string,
  `tripendtimestamp` string,
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
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://ec30b6ade50f:9000/apps/hive/warehouse/taxi_trips'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true',
  'numFiles'='1',
  'totalSize'='158754777',
  'transient_lastDdlTime'='1606727336')
