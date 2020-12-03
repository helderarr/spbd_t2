from pyspark import StorageLevel
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
        .show(truncate=False)

    # For each of the 24 hours of the day, how many taxi trips there were, what was their average
    # trip miles and trip total cost?
    # Non-integer values should be printed with two decimal places.
    df.withColumn("time_slot", date_format("TripStartTimestamp", "hh a")) \
        .groupBy("time_slot").agg({"TripMiles": "avg", "TripTotal": "avg", "*": "count"}) \
        .withColumnRenamed("count(1)", "#trips") \
        .withColumn("avg_trip_miles", format_number("avg(TripMiles)", 2)) \
        .withColumn("avg_trip_total", format_number("avg(TripTotal)", 2)) \
        .select("time_slot", "#trips", "avg_trip_miles", "avg_trip_total") \
        .show(truncate=False)

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

    # EXTRA

    # https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=pet&s=emm_epm0_pte_nus_dpg&f=m
    gas_df = spark.read.csv("data/gas_price.csv", sep=';', inferSchema=True, header=True)
    gas_df.show()

    # https://sparkbyexamples.com/pyspark/pyspark-pivot-and-unpivot-dataframe
    unpivotExpr = "stack(12, 'Jan',Jan,'Feb',Feb,'Mar',Mar,'Apr',Apr,'May',May,'Jun',Jun,'Jul',Jul," \
                  "'Aug',Aug,'Sep',Sep,'Oct',Oct,'Nov',Nov,'Dec',Dec) as (Month,Price)"

    gas_df = gas_df.select('Year', expr(unpivotExpr))
    gas_df.show()
    gas_df.printSchema()

    gas_df = gas_df.withColumn("DateKey", concat_ws(" ", "Year", "Month"))\
        .persist(storageLevel=StorageLevel.MEMORY_ONLY)

    gas_df.show()

    df = df.withColumn("DateKey", date_format("TripStartTimestamp", "y LLL"))
    df.show()

    trips = df.alias("trips")
    gas = gas_df.alias('gas')

    joined = trips.join(gas, gas.DateKey == trips.DateKey)

    # https://en.wikipedia.org/wiki/Fuel_economy_in_automobiles
    # United States (L/100 km) – 'combined' 9.8, 'city' 11.2, 'highway' 8.1[21]
    # 26.3 mpg

    agg_costs = joined.withColumn("gas_cost", joined.TripMiles * joined.Price / 26.3) \
        .groupBy(year('TripStartTimestamp').alias('year')) \
        .agg({"gas_cost": "sum", "TripTotal": "sum", "Price": "sum", "*": "count", "TripMiles": "sum"}) \
        .withColumnRenamed("sum(gas_cost)", "sum_gas_cost") \
        .withColumnRenamed("sum(TripTotal)", "sum_trip_total") \
        .withColumnRenamed("sum(TripMiles)", "sum_trip_miles") \
        .withColumnRenamed("sum(Price)", "sum_price") \
        .withColumnRenamed("count(1)", "cnt")

    agg_costs.withColumn("% gas cost on total", round(agg_costs.sum_gas_cost * 100 / agg_costs.sum_trip_total, 2)) \
        .withColumn("avg gas price $/g", round(agg_costs.sum_price / agg_costs.cnt, 2)) \
        .withColumn("$/mile", round(agg_costs.sum_trip_miles / agg_costs.cnt, 2)) \
        .drop("sum_trip_total", "sum_gas_cost", "sum_price", "cnt", "sum_trip_miles") \
        .orderBy(agg_costs.year) \
        .show()


except Exception as err:
    print(err)
    spark.stop()
