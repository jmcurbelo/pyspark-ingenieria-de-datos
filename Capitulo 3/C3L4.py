# Transformaciones: función flatMap

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext

rdd = sc.parallelize([1,2,3,4,5])

rdd_cuadrado = rdd.map(lambda x: (x, x ** 2))

rdd_cuadrado.collect()

rdd_cuadrado_flat = rdd.flatMap(lambda x: (x, x ** 2))

rdd_cuadrado_flat.collect()

rdd_texto = sc.parallelize(['jose', 'juan', 'lucia'])

rdd_mayuscula = rdd_texto.flatMap(lambda x: (x, x.upper()))

rdd_mayuscula.collect()