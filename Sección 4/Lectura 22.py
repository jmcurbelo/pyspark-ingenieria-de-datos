# Transformaciones: función coalesce

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([1,2,3.4,5], 10)

rdd.getNumPartitions()

rdd.coalesce(5)
rdd.getNumPartitions()

rdd5 = rdd.coalesce(5)

rdd5.getNumPartitions()