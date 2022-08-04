# Acciones: funciones take, max y saveAsTextFile

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# take

rdd = sc.parallelize('La programación es bella'.split(' '))

rdd.take(2)

rdd.take(4)

# max

rdd1 = sc.parallelize([item/(item + 1) for item in range(10)])

rdd1.max()

rdd1.collect()

# saveAsTextFile

rdd.collect()

rdd.saveAsTextFile('./rdd')

rdd.coalesce(1).saveAsTextFile('./rdd1')