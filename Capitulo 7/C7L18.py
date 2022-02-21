# Shuffle Hash Join y Broadcast Hash Join

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

empleados = spark.read.parquet('./data/empleados/')

departamentos = spark.read.parquet('./data/departamentos/')

from pyspark.sql.functions import col, broadcast

empleados.join(broadcast(departamentos), col('num_dpto') == col('id')).show()

empleados.join(broadcast(departamentos), col('num_dpto') == col('id')).explain()