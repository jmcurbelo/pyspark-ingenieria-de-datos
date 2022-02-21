# Diferentes formas de crear un RDD

import findspark

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext

# Crear un RDD vacío

rdd_vacio = sc.emptyRDD

# Crear un RDD con parallelize

rdd_vacio3 = sc.parallelize([], 3)

rdd_vacio3.getNumPartitions()

rdd = sc.parallelize([1,2,3,4,5])

rdd.collect()

# Crear un RDD desde un archivo de texto

rdd_texto = sc.textFile('./rdd_source.txt')

rdd_texto.collect()

rdd_texto_completo = sc.wholeTextFiles('./rdd_source.txt')

rdd_texto_completo.collect()

rdd_suma = rdd.map(lambda x: x +1)

rdd_suma.collect()

df = spark.createDataFrame([(1, 'jose'), (2, 'juan')], ['id', 'nombre'])

df.show()

rdd_df = df.rdd

rdd_df.collect()