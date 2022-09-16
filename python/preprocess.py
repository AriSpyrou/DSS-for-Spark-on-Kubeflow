from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType


def strip_str(a_string):
    if a_string.dtype == 'string':
        return a_string.strip()
    
strip_udf = udf(strip_str, StringType())


column_names = [
    'age',
    'workclass',
    'fnlwgt',
    'education',
    'education-num',
    'marital-status',
    'occupation',
    'relationship',
    'race',
    'sex',
    'capital-gain',
    'capital-loss',
    'hours-per-week',
    'native-country',
    'salary'
]

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
    StructField("salary", StringType(), True)
])

train_df = spark.read.csv('hdfs:///data/adult.data', header=False, schema=schema)
test_df = spark.read.csv('hdfs:///data/adult.test', header=False, schema=schema)

train_df = train_df.filter(train_df['native-country'] != ' Holand-Netherlands')
test_df = test_df.filter(test_df['native-country'] != ' Holand-Netherlands')

categorical_variables = ['workclass', 'education', 'marital-status', 'occupation', 'relationship', 'race', 'sex', 'native-country']

indexers = [StringIndexer(inputCol=column, outputCol=column+"-index") for column in categorical_variables]

encoder = OneHotEncoder(
    inputCols=[indexer.getOutputCol() for indexer in indexers],
    outputCols=["{0}-encoded".format(indexer.getOutputCol()) for indexer in indexers]
)

pipeline = Pipeline(stages=indexers)
train_df = pipeline.fit(train_df).transform(train_df)
test_df = pipeline.fit(test_df).transform(test_df)

train_df.write.mode('overwrite').csv('hdfs:///tmp/train_temp.csv', header=True)
test_df.write.mode('overwrite').csv('hdfs:///tmp/test_temp.csv', header=True)

spark.stop()