from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType


train_df = spark.read.csv('hdfs:///tmp/train.csv', header=True)
test_df = spark.read.csv('hdfs:///tmp/test.csv', header=True)

logireg = LogisticRegression(featuresCol='features', labelCol='label')
model = logireg.fit(train_df)

print(model.evaluate(test_df).accuracy)