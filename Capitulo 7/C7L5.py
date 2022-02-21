# Agregación con agrupación

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

vuelos = spark.read.parquet('./data/')

vuelos.printSchema()

from pyspark.sql.functions import desc

(vuelos.groupBy('ORIGIN_AIRPORT')
    .count()
    .orderBy(desc('count'))
).show()

(vuelos.groupBy('ORIGIN_AIRPORT', 'DESTINATION_AIRPORT')
    .count()
    .orderBy(desc('count'))
).show()

