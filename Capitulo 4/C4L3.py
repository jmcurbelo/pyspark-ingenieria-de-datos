# Acciones: función reduce

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([2,4,6,8])

rdd.reduce(lambda x,y: x + y)

rdd1 = sc.parallelize([1,2,3,4])

rdd1.reduce(lambda x,y: x * y)