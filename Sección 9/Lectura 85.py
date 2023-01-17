# Funciones de ventana

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data/')

df.show()

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, row_number, rank, dense_rank, col

windowSpec = Window.partitionBy('departamento').orderBy(desc('evaluacion'))

# row_number

df.withColumn('row_number', row_number().over(windowSpec)).filter(col('row_number').isin(1,2)).show()

# rank

df.withColumn('rank', rank().over(windowSpec)).show()

# dense_rank

df.withColumn('dense_rank', dense_rank().over(windowSpec)).show()

# Agregaciones con especificaciones de ventana

windowSpecAgg = Window.partitionBy('departamento')

from pyspark.sql.functions import min, max, avg

(df.withColumn('min', min(col('evaluacion')).over(windowSpecAgg))
.withColumn('max', max(col('evaluacion')).over(windowSpecAgg))
.withColumn('avg', avg(col('evaluacion')).over(windowSpecAgg))
.withColumn('row_number', row_number().over(windowSpec))
 ).show()