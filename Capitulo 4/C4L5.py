# Acciones: función collect

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize('Hola Apache Spark!'.split(' '))

rdd.collect()

rdd1 = sc.parallelize([(item, item ** 2) for item in range(20)])

rdd1.collect()

