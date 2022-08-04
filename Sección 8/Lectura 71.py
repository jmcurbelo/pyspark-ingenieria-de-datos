# Right Outer Join

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

empleados = spark.read.parquet('./data/empleados/')

departamentos = spark.read.parquet('./data/departamentos/')

from pyspark.sql.functions import col

empleados.join(departamentos, col('num_dpto') == col('id'), 'rightouter').show()

empleados.join(departamentos, col('num_dpto') == col('id'), 'right_outer').show()

empleados.join(departamentos, col('num_dpto') == col('id'), 'right').show()

