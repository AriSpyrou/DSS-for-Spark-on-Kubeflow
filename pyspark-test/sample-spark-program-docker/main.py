# Code snippet, showcases a simple spark program running

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("capstone").getOrCreate()

a = spark.range(1000 * 1000 * 1000).count()
print(a)
print('END_OUT')