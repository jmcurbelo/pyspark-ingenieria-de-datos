# Funciones de ventana

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

data = spark.read.parquet('./data/')

data.show()

windowSpec = Window.partitionBy('departamento').orderBy('puntos')

data.withColumn('row_number', row_number().over(windowSpec)).show(truncate=False)

windowSpecAgg = Window.partitionBy('departamento')

from pyspark.sql.functions import min, max, avg, col

(data.withColumn('row_number', row_number().over(windowSpec))
    .withColumn('min', min(col('puntos')).over(windowSpecAgg))
    .withColumn('max', max(col('puntos')).over(windowSpecAgg))
    .withColumn('avg', avg(col('puntos')).over(windowSpecAgg))
).where(col('row_number') == 1).select('departamento', 'min', 'max', 'avg').show()

