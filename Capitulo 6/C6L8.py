# Transformaciones - funciones distinct y dropDuplicates

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data')

# distinct

df_sin_duplicados = df.distinct()

print('El conteo del dataframe original es {}'.format(df.count()))
print('El conteo del dataframe sin duplicados es {}'.format(df_sin_duplicados.count()))

# función dropDuplicates

dataframe = spark.createDataFrame([(1, 'azul', 567), (2, 'rojo', 487), (1, 'azul', 345), (2, 'verde', 783)]).toDF('id', 'color', 'importe')

dataframe.show()

dataframe.dropDuplicates(['id', 'color']).show()