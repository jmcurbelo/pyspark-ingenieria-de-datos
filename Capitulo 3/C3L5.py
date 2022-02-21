# Transformaciones: función filter

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9])

rdd_par = rdd.filter(lambda x: x % 2 == 0)

rdd_par.collect()

rdd_impar = rdd.filter(lambda x: x % 2 != 0)

rdd_impar.collect()

rdd_texto = sc.parallelize(['jose', 'juaquin', 'juan', 'lucia', 'karla', 'katia'])

rdd_k = rdd_texto.filter(lambda x: x.startswith('k'))

rdd_k.collect()

rdd_filtro = rdd_texto.filter(lambda x: x.startswith('j') and x.find('u') == 1)

rdd_filtro.collect()