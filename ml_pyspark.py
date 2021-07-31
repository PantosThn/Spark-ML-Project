from __future__ import print_function
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.linalg import SparseVector
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression, MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF

import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import math
import numpy

stop_words = set(stopwords.words('english'))
lexicon_size = 2**7 
spark = SparkSession.builder.appName("spam").getOrCreate() 
sc = spark.sparkContext 

comps = sc.textFile('hdfs://master:9000/input/customer_complaints.csv')

#clean and count the complaints
comps = comps.map(lambda x : x.split(",") )\
             .filter( lambda x: len(x) > 2 ) \
             .filter( lambda x: (x[0].startswith("201")) and (x[2] != '') ) \
             .map(lambda x: (x[0], x[1].lower(), x[2].lower() ) ) \
             .map(lambda x: (x[0], x[1], re.sub(r'[^a-zA-Z ]+', ' ', x[2]) ) )\
             .map(lambda x: (x[0], x[1], word_tokenize(x[2])) ) \
             .map(lambda x: (x[0], x[1] , [word2 for word2 in x[2] if not word2 in stop_words]))
n=comps.count()

#get the most Common Words
words = comps.flatMap(lambda x : [w for w in x[2]] ) \
             .map(lambda x : (x, 1))\
             .reduceByKey(lambda x, y: x + y)\
			 .sortBy(lambda x : x[1], ascending = False).map(lambda x : x[0]).take(lexicon_size)

#broadcast words
broad_words = sc.broadcast(words)

rdd = comps.map( lambda x : (x[0], x[1], [broad_words.value.index(y) for y in x[2] if y in broad_words.value]) )\
    .filter(lambda x : len(x[1]) != 0) \
    .zipWithIndex()\
    .flatMap(lambda x : [((y, x[0][1], x[1], len(x[0][2])), 1) for y in x[0][2]])\
    .reduceByKey(lambda x, y : x + y )

#create the DF rdd
rdd1= rdd.map( lambda x : ( x[0][0], (x[0][2], x[0][1], x[1]/x[0][3])) )

#create the IDF rdd
rdd2 = rdd.map( lambda x : ( x[0][0], 1  ) )\
    .reduceByKey(lambda x, y : x + y)\
    .map(lambda x: (x[0], math.log(n/x[1]) )   )

#join them
rddF = rdd1.join(rdd2)

#create the final rdd to feed the algorithm
rddF = rddF.map(lambda x : ( (x[1][0][0], x[1][0][1]), [(x[0], x[1][0][2]*x[1][1])] ) ) \
    .reduceByKey(lambda x, y : x + y)\
    .map(lambda x : (x[0][1], sorted(x[1], key = lambda y : y[0])))\
    .map(lambda x : (x[0], SparseVector(lexicon_size, [y[0] for y in x[1]], [y[1] for y in x[1]])))

#RDD -> Spark Dataframe
df = rddF.toDF(["string_label", "features"])

#StringIndexer -> String Labels to Integer
stringIndexer = StringIndexer(inputCol="string_label", outputCol="label")
stringIndexer.setHandleInvalid("skip")
stringIndexerModel = stringIndexer.fit(df)
newDF = stringIndexerModel.transform(df)
newDF = newDF["label", "features"]


#Train - Test Stratified Split
train_size = 0.75
fractions_dict = {}
for i in newDF.dropDuplicates((['label'])).select("label").sort("label").collect():#newDF.select('label').distinct().collect():
    fractions_dict[i[0]] = train_size
train = newDF.sampleBy("label", fractions = fractions_dict, seed=1234)
output_size = len(fractions_dict)

input_size = len(newDF.select("features").collect())

test = newDF.subtract(train)
#train = train.cache()


#Perceptron
start = time.time()
mlp = MultilayerPerceptronClassifier(layers=[lexicon_size, output_size], seed=1234)
model = mlp.fit(train)

result = model.transform(test)

predictionAndLabels = result.select("prediction", "label")
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
end = time.time()
print("Test set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))
print("The programm took %.6f seconds to get trained\n" %(end-start))

print("--------------------------------------------------")
