# Acumuladores

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

acumulador = sc.accumulator(0)

rdd = sc.parallelize([2,4,6,8,10])

rdd.foreach(lambda x: acumulador.add(x))

print(acumulador.value)

rdd1 = sc.parallelize('Mi nombre es Jose Miguel y me siento genial'.split(' '))

acumulador1 = sc.accumulator(0)

rdd1.foreach(lambda x: acumulador1.add(1))

print(acumulador1.value)