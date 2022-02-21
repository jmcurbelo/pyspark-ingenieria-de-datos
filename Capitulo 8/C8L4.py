# Funciones when, coalesce y lit

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = spark.read.parquet('./data/')

data.show()

from pyspark.sql.functions import col, when, lit, coalesce

data.select(
    col('nombre'),
    when(col('pago') == 1, 'pagado').when(col('pago') == 2, 'sin pagar').otherwise('sin iniciar').alias('pago')
).show()

data.select(
    coalesce(col('nombre'), lit('sin nombre')).alias('nombre')
).show()

