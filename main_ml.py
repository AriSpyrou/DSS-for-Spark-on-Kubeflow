from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

spark = SparkSession.builder.appName("Predict Person Salary").getOrCreate()

schema = StructType([
    StructField("age", IntegerType(), True),
    StructField("workclass", StringType(), True),
    StructField("fnlwgt", FloatType(), True),
    StructField("education", StringType(), True),
    StructField("education-num", FloatType(), True),
    StructField("marital-status", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("relationship", StringType(), True),
    StructField("race", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("capital-gain", FloatType(), True),
    StructField("capital-loss", FloatType(), True),
    StructField("hours-per-week", FloatType(), True),
    StructField("native-country", StringType(), True),
    StructField("salary", StringType(), True),
    StructField("workclass-index", FloatType(), True),
    StructField("education-index", FloatType(), True),
    StructField("marital-status-index", FloatType(), True),
    StructField("occupation-index", FloatType(), True),
    StructField("relationship-index", FloatType(), True),
    StructField("race-index", FloatType(), True),
    StructField("sex-index", FloatType(), True),
    StructField("native-country-index", FloatType(), True)
])

train_df = spark.read.csv('hdfs:///tmp/train_temp.csv', header=True, schema=schema)
test_df = spark.read.csv('hdfs:///tmp/test_temp.csv', header=True, schema=schema)

encoder = OneHotEncoder(
    inputCols=train_df.columns[-8:],
    outputCols=["{0}-encoded".format(col) for col in train_df.columns[-8:]]
)

assembler = VectorAssembler(
    inputCols=encoder.getOutputCols(),
    outputCol="categorical-features"
)

pipeline = Pipeline(stages=[encoder, assembler])

train_df = pipeline.fit(train_df).transform(train_df)
test_df = pipeline.fit(test_df).transform(test_df)

continuous_variables = ['age', 'fnlwgt', 'education-num', 'capital-gain', 'capital-loss', 'hours-per-week']
assembler = VectorAssembler(
    inputCols=['categorical-features', *continuous_variables],
    outputCol='features'
)

train_df = assembler.transform(train_df)
test_df = assembler.transform(test_df)

indexer = StringIndexer(inputCol='salary', outputCol='label')
train_df = indexer.fit(train_df).transform(train_df)
test_df = indexer.fit(test_df).transform(test_df)

logireg = LogisticRegression(featuresCol='features', labelCol='label')
model = logireg.fit(train_df)

print(f'Model Accuracy on test set: {model.evaluate(test_df).accuracy}')

spark.stop()