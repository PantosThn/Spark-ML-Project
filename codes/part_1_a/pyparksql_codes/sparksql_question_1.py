from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import pyspark.sql.functions as F
import time

spark = SparkSession \
    .builder \
    .appName("AverageLatLong_SparkSQL") \
    .getOrCreate()

#rdd1 =spark.read.format("csv").option("header", "false").load('../../../yellow_tripvendors_1m_100k.csv')
rdd1 =spark.read.format("csv").option("header", "false").load('hdfs://master:9000/input/yellow_tripvendors_1m.csv')
df1 = rdd1.toDF("id", "vendor")

#rdd2 =spark.read.format("csv").option("header", "false").load('../../../yellow_tripdata_1m_100k.csv')
rdd2 =spark.read.format("csv").option("header", "false").load('hdfs://master:9000/input/yellow_tripdata_1m.csv')
df2 = rdd2.toDF("id","start","end","lon_start","lat_start","lon_end","lat_end","cost")

df = df1.join(df2, "id")
df = df.filter((df.lon_start!=0)&(df.lat_start!=0)&(df.lon_end!=0)&(df.lon_end!=0))

time_start = time.time()
df.registerTempTable("TripData") #use sql queries

aver_lat_long_start = spark.sql("select hour(start) as HourOfDay, AVG(lat_start) as AverageLatitudeStart, AVG(lon_start) as AverageLongtitudeStart  from TripData group by hour(start) order by hour(start)")

aver_lat_long_start.show(24)
time_end = time.time()
print("The program took:: %.6f seconds to execute\n"%(time_end-time_start))
