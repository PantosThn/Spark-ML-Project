import sys
from operator import add
import time
from pyspark.sql import SparkSession
from datetime import datetime

#wget http://www.cslab.ntua.gr/courses/atds/yellow_trip_data.zip
#unzip file.zip

#hadoop fs -put yellow_tripdata_1m.csv hdfs://master:9000/input
#hadoop fs -put yellow_tripvendors_1m.csv hdfs://master:9000/input

spark = SparkSession\
        .builder\
        .appName("AverageLatitudeLongtitude")\
        .getOrCreate()

lines = spark.read.text('hdfs://master:9000/input/yellow_tripdata_1m.csv').rdd.map(lambda r: r[0])
splitted_data = lines.flatMap(lambda many_lines: many_lines.split("\n"))\
                        .map(lambda line: line.split(','))\
                        .filter(lambda entry: (float(entry[3]) != 0 and float(entry[4]) != 0 and float(entry[5]) != 0 and float(entry[6]) != 0 ) ) \
                        .map(lambda entry: ((datetime.strptime(entry[1], '%Y-%m-%d %H:%M:%S').strftime("%H")),(float(entry[3]),float(entry[4]),1))) \
                        .reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1], x[2]+y[2])) \
                        .map(lambda x: (x[0],x[1][0]/x[1][2], x[1][1]/x[1][2]))

start = time.time()
result=sorted(splitted_data.collect(), key=lambda x:x[0])
end = time.time()

print("The program took: %.6f seconds to execute\n"%(end-start))
print("--------------------------------------------------")
print("HourOfDay","\t","Longtitude", "\t", "Latitude")
for x in result:
  print(x[0],"\t",x[1], "\t",x[2])
print("--------------------------------------------------")

