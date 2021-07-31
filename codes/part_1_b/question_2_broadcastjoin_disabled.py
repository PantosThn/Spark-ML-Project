from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import pyspark.sql.functions as F
import time

spark = SparkSession \
	.builder \
	.appName("OptimizerTest2") \
	.getOrCreate()

rdd1 = spark.read.format("csv").option("header", "false").load('hdfs://master:9000/input/yellow_tripvendors_1m.csv')
df1 = rdd1.toDF("id", "vendor")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

rdd2 = spark.read.format("csv").option("header", "false").load('hdfs://master:9000/input/yellow_tripdata_1m.csv')
df2 = rdd2.toDF("id", "start", "end", "lon_start", "lat_start", "lon_end",  "lat_end", "cost")

start = time.time()
df1.write.mode('overwrite').parquet("hdfs://master:9000/input/new_df1_temp.parquet")
df2.write.mode('overwrite').parquet("hdfs://master:9000/input/new_df2_temp.parquet")

df1_parquet = spark.read.parquet("hdfs://master:9000/input/new_df1_temp.parquet")
df2_parquet = spark.read.parquet("hdfs://master:9000/input/new_df2_temp.parquet")

df1_parquet.registerTempTable("ParquetD1F")

tempOut = spark.sql("select * from ParquetD1F limit 100")

join_opt = df2_parquet.join(tempOut, "id")
end = time.time()
join_opt.explain("extended")

print("The programm took %.6f seconds to execute\n" %(end-start))
