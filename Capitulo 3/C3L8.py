# Transformaciones: función reduceByKey

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize(
    [('casa', 2),
     ('parque', 1),
     ('que', 5),
     ('casa', 1),
     ('escuela', 2),
     ('casa', 1),
     ('que', 1)]
)

rdd.collect()

rdd_reduciodo = rdd.reduceByKey(lambda x,y: x + y)

rdd_reduciodo.collect()