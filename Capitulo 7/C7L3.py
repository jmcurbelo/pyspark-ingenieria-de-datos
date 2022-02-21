# Funciones min y max

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

vuelos = spark.read.parquet('./data')

vuelos.printSchema()

from pyspark.sql.functions import min, max, col

vuelos.select(
    min('AIR_TIME').alias('menor_timepo'),
    max('AIR_TIME').alias('mayor_tiempo')
).show()

vuelos.select(
    min('AIRLINE_DELAY'),
    max('AIRLINE_DELAY')
).show()

