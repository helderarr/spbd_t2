from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]') \
    .appName('Exercice 1').getOrCreate()
sc = spark.sparkContext
try:

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

    # How many trips were started in each year present in the data set?
    # (year, #trips)
    df.groupBy(year("TripStartTimestamp").alias("year")) \
        .count() \
        .withColumnRenamed("count", "#trips") \
        .show()

    # For each of the 24 hours of the day, how many taxi trips there were, what was their average
    # trip miles and trip total cost?
    # Non-integer values should be printed with two decimal places.
    df.withColumn("time_slot", date_format("TripStartTimestamp", "hh a")) \
        .groupBy("time_slot").agg({"TripMiles": "avg", "TripTotal": "avg", "*": "count"}) \
        .withColumnRenamed("count(1)", "#trips") \
        .withColumn("avg_trip_miles", format_number("avg(TripMiles)", 2)) \
        .withColumn("avg_trip_total", format_number("avg(TripTotal)", 2)) \
        .select("time_slot", "#trips", "avg_trip_miles", "avg_trip_total") \
        .show()

    # For each of the 24 hours of the day, which are the(up to) 5 most popular routes(pairs
    # pickup / dropoff regions) according to the the total number of taxi trips? Also report
    # and the average fare(total trip cost).
    # Non - integer values should be printed with two decimal places.

    windowSpec = Window.partitionBy("hour").orderBy(col("count(1)").desc())

    df.where(col("PickupRegionID").isNotNull() & col("DropoffRegionID").isNotNull()) \
        .withColumn("hour", date_format("TripStartTimestamp", "hh a")) \
        .groupBy("hour", "PickupRegionID", "DropoffRegionID").agg(
        {"TripMiles": "avg", "TripTotal": "avg", "*": "count"}) \
        .withColumn("row_number", row_number().over(windowSpec)) \
        .where("row_number <= 5") \
        .withColumnRenamed("count(1)", "#trips") \
        .withColumn("avg_trip_miles", format_number("avg(TripMiles)", 2)) \
        .withColumn("avg_trip_total", format_number("avg(TripTotal)", 2)) \
        .withColumn("pickup_location", concat_ws(" ", "hour", "PickupRegionID", "DropoffRegionID")) \
        .select("pickup_location", "#trips", "avg_trip_miles", "avg_trip_total") \
        .show(truncate=False)


except Exception as err:
    print(err)
    spark.stop()
