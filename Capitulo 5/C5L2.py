# HashPartitioner

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize(['x', 'y', 'z'])

hola = 'Hola'

hash(hola)

num_particiones = 6

# indice = hash(item) % num_particiones

hash('x') % num_particiones

hash('y') % num_particiones

hash('z') % num_particiones

