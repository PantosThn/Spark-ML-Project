from pyspark.sql import SparkSession
import time
from math import radians, cos, sin, asin, sqrt, atan2
from pyspark.sql.functions import udf
from datetime import datetime

def calculate_duration(start_time, end_time):

    duration = (datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S') - datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')).total_seconds()

    return duration
udf_duration = udf(calculate_duration)

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [float(lon1), float(lat1), float(lon2), float(lat2)])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return c * 6371 ## give in kms
udf_haversine = udf(haversine)

spark = SparkSession \
        .builder \
        .appName("MaxDistance_SQL") \
        .getOrCreate()

#rdd1 =spark.read.format("csv").option("header", "false").load('../../../yellow_tripvendors_1m_100k.csv')
rdd1 =spark.read.format("csv").option("header", "false").load('hdfs://master:9000/input/yellow_tripvendors_1m.csv')
df1 = rdd1.toDF("id", "vendor")

#rdd2 =spark.read.format("csv").option("header", "false").load('../../../yellow_tripdata_1m_100k.csv')
rdd2 =spark.read.format("csv").option("header", "false").load('hdfs://master:9000/input/yellow_tripdata_1m.csv')
df2 = rdd2.toDF("id","start","end","lon_start","lat_start","lon_end","lat_end","cost")

df = df1.join(df2, "id")
df = df.filter((df.lon_start!=0)&(df.lat_start!=0)&(df.lon_end!=0)&(df.lon_end!=0))

#add new column with distance
df = df.withColumn("distance", udf_haversine("lon_start","lat_start","lon_end","lat_end") )
df = df.withColumn("duration", udf_duration("start","end") )

start = time.time()
df.registerTempTable("TripData")

result = spark.sql("select vendor,  max(distance) as MaxDistance, first(duration) as Duration from TripData group by vendor")
result.show()

end = time.time()
print("The program took: %.6f seconds to execute\n"%(end-start))
