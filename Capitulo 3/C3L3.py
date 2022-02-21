# Transformaciones: función map

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext

rdd = sc.parallelize([1,2,3,4,5])

rdd_resta = rdd.map(lambda x: x - 1)
rdd_resta.collect()

rdd_par = rdd.map(lambda x: x % 2 == 0)

rdd_par.collect()

rdd_texto = sc.parallelize(['jose', 'juan', 'lucia'])

rdd_mayuscula = rdd_texto.map(lambda x: x.upper())

rdd_mayuscula.collect()

rdd_hola = rdd_texto.map(lambda x: 'Hola ' + x)

rdd_hola.collect()