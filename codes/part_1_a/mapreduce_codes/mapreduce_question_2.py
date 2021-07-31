from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import time
from math import radians, cos, sin, asin, sqrt, atan2
from datetime import datetime


def calculate_duration(start_time, end_time):

    duration = (datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S') - datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')).total_seconds()

    if duration==0:
        return 100000

    return (datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S') - datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')).total_seconds()


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r # in km

spark = SparkSession\
        .builder\
        .appName("MaxDistance")\
        .getOrCreate()

data1 = spark.read.text('hdfs://master:9000/input/yellow_tripdata_1m.csv').rdd.map(lambda r: r[0])
rdd1 = data1.flatMap(lambda line: line.split("\n"))\
             .map(lambda word: word.split(','))\
             .filter(lambda entry: (float(entry[3]) != 0 and float(entry[4]) != 0 and float(entry[5]) != 0 and float(entry[6]) != 0))\
             .map(lambda entry: (entry[0],(entry[1], entry[2], float(entry[3]), float(entry[4]), float(entry[5]) ,float(entry[6]))))\
             .map(lambda entry: (entry[0], (haversine(entry[1][2],entry[1][3],entry[1][4],entry[1][5]), calculate_duration(entry[1][0],entry[1][1])) ) )

vendors = spark.read.text('hdfs://master:9000/input/yellow_tripvendors_1m.csv').rdd.map(lambda r: r[0])
rdd2 = vendors.flatMap(lambda line: line.split("\n"))\
             .map(lambda word: word.split(','))\
             .map(lambda entry: (entry[0],int(entry[1])))

rdd_join = rdd1.join(rdd2)
out = rdd_join.map(lambda entry: (entry[1][1],entry[1][0]))\
	.reduceByKey(lambda x,y: (max(x[0], y[0]), x[1] if x[0]>y[0] else y[1] ) )
start = time.time()
final = out.collect()
end = time.time()
print("The program took: %.6f seconds to execute\n"%(end-start))
print('Vendor',"\t",'MaxDistance', '\t', 'Duration(s)')
for x in final:
  print(x[0],'\t',x[1][0],'\t',x[1][1] )


