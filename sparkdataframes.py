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

    #How many trips were started in each year present in the data set?
    df.groupBy(year("TripStartTimestamp").alias("year")).count().show()

    print(df.dtypes)


except Exception as err:
    print(err)
    spark.stop()
