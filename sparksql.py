from pyspark.sql import *
from pyspark.sql.types import *

spark = SparkSession.builder.master('local[*]') \
    .appName('Exercice 1').getOrCreate()
sc = spark.sparkContext

fields = [
    StructField('TripID', StringType(), True),
    StructField('TaxiID', StringType(), True),
    StructField('TripStartTimestamp', TimestampType(), True),
    StructField('TripEndTimestamp', TimestampType(), True),
    StructField('TripSeconds', DoubleType(), True),
    StructField('TripMiles', DoubleType(), True),
    StructField('PickupRegionID', StringType(), True),
    StructField('DropoffRegionID', StringType(), True),
    StructField('PickupCommunity', DoubleType(), True),
    StructField('DropoffCommunity', DoubleType(), True),
    StructField('Fare', DoubleType(), True),
    StructField('Tips', DoubleType(), True),
    StructField('Tolls', DoubleType(), True),
    StructField('Extras', DoubleType(), True),
    StructField('TripTotal', DoubleType(), True),
    StructField('PaymentType', StringType(), True),
    StructField('Company', StringType(), True),
    StructField('PickupCentroidLatitude', DoubleType(), True),
    StructField('PickupCentroidLongitude', DoubleType(), True),
    StructField('PickupCentroidLocation', StringType(), True),
    StructField('DropoffCentroidLatitude', DoubleType(), True),
    StructField('DropoffCentroidLongitude', DoubleType(), True),
    StructField('DropoffCentroidLocation', StringType(), True)]

# https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
df = spark.read.csv('Taxi_Trips_151MB.csv', sep=';', schema=StructType(fields), timestampFormat="M/d/y h:m:s a")
df.createOrReplaceTempView("taxi_trips")

# What is the accumulated number of taxi trips per month?
# Output is expected to have two columns: (month_number, #total_trips).
spark.sql('select lpad(month(TripStartTimestamp), 2, "0") as month_number,'
          '     count(*) as `#total_trips` '
          'from taxi_trips '
          'group by lpad(month(TripStartTimestamp), 2, "0")')\
    .cache().show(truncate=False)

# For each pickup region, report the list of unique dropoff regions?
# Output is expected to have two columns: (pickup_region_ID, list_of_dropoff_region_ID)
spark.sql('select PickupRegionID as pickup_region_ID,'
          'collect_list(DropoffRegionID) as list_of_dropoff_region_ID '
          'from (select distinct PickupRegionID,DropoffRegionID '
          '      from taxi_trips'
          '      where PickupRegionID is not null and DropoffRegionID is not null) '
          'group by PickupRegionID') \
    .cache().show(truncate=False)


# What is the expected charge/cost of a taxi ride, given the pickup region ID, the weekday
# (0=Monday, 6=Sunday) and time in format “hour AM/PM”?
# Output is expected to have two columns: (month_number, avg_total_trip_cost).
# We used <<pickup>> as first column name instead of <<month_number>> as <<month_number>> is meaningless

# define an UDF as the date_format function doesn't allow to extract the day of the week as a numeric value
def day_off_week(d):
    return d.weekday()


# registering the UDF so it can be used inside the SQL statements
spark.udf.register("day_off_week", day_off_week)

spark.sql('select concat(PickupRegionID,"_",day_off_week(tripstarttimestamp),date_format(tripstarttimestamp, "_hh_a")) '
          'as pickup, round(avg (TripTotal),2) as avg_total_trip_cost '
          'from taxi_trips '
          'where PickupRegionID is not null and PickupRegionID <> "" '
          'group by PickupRegionID,tripstarttimestamp ') \
    .cache().show(truncate=False)


# Question 4
# top 3 companies driving from the most popular locations (more than 5000 trips from there)
spark.sql('with trips_per_company as ( '
          '    select pickupregionid,company, count(*) ntrips '
          '    from taxi_trips '
          '    where company is not null and company <> "" '
          '       and pickupregionid is not null and pickupregionid <> "" '
          '    group by pickupregionid,company), '
          'ranked_trips_per_company as ( '
          '    select pickupregionid,company, ntrips, '
          '           row_number() over (partition by pickupregionid order by ntrips desc) r, '
          '           sum(ntrips) over (partition by pickupregionid) total_sum '
          '    from trips_per_company), '
          'filtered_data as ( '
          '    select pickupregionid,company, total_sum,ntrips,r '
          '    from ranked_trips_per_company '
          '    where r <= 3 and total_sum >= 5000 '
          '    order by total_sum desc, r asc) '
          'select pickupregionid,company,ntrips '
          'from filtered_data') \
    .cache().show(truncate=False)

spark.stop()
