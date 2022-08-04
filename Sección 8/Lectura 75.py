# Cross Join

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

empleados = spark.read.parquet('./data/empleados/')

departamentos = spark.read.parquet('./data/departamentos/')

from pyspark.sql.functions import col

df = empleados.crossJoin(departamentos)

df.show()

df.count()