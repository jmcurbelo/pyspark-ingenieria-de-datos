# Broadcast variables

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([item for item in range(10)])

uno = 1

br_uno = sc.broadcast(uno)

rdd1 = rdd.map(lambda x: x + br_uno.value)

rdd1.collect()

br_uno.unpersist()

rdd1  = rdd.map(lambda x: x + br_uno.value)

rdd1.collect()

br_uno.destroy()

rdd1  = rdd.map(lambda x: x + br_uno.value)

rdd1.take(5)

