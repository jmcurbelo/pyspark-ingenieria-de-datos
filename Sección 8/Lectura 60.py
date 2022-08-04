# Explorando los datos

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data/')

df.printSchema()

df.show(20, truncate=False)

