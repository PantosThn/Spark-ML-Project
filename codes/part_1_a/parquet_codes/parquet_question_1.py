from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import pyspark.sql.functions as F
import time


spark = SparkSession \
    .builder \
    .appName("AverageLatLong_Parquet") \
    .getOrCreate()


#rdd1 =spark.read.format("csv").option("header", "false").load('../../../yellow_tripvendors_1m_100k.csv')
rdd1 =spark.read.format("csv").option("header", "false").load('hdfs://master:9000/input/yellow_tripvendors_1m.csv')
df1 = rdd1.toDF("id", "vendor")

#rdd2 =spark.read.format("csv").option("header", "false").load('../../../yellow_tripdata_1m_100k.csv')
rdd2 =spark.read.format("csv").option("header", "false").load('hdfs://master:9000/input/yellow_tripdata_1m.csv')
df2 = rdd2.toDF("id","start","end","lon_start","lat_start","lon_end","lat_end","cost")

df_ = df1.join(df2, "id")
df_ = df_.filter((df_.lon_start!=0)&(df_.lat_start!=0)&(df_.lon_end!=0)&(df_.lon_end!=0))

t_parquet_start = time.time()
df_.write.mode('overwrite').parquet("hdfs://master:9000/input/newd1f10_.parquet")
t_parquet_end = time.time()
print("Parquet conversion took:: %.6f seconds to execute\n"%(t_parquet_end-t_parquet_start))

time_start = time.time()
df = spark.read.parquet("hdfs://master:9000/input/newd1f10_.parquet")

df.registerTempTable("TripDat1a")

aver_lat_long_start = spark.sql("select hour(start) as HourOfDay, AVG(lat_start) as AverageLatitudeStart, AVG(lon_start) as AverageLongtitudeStart  from TripDat1a group by hour(start) order by hour(start)")

aver_lat_long_start.show(24)
time_end = time.time()
print("The program took:: %.6f seconds to execute\n"%(time_end-time_start))
