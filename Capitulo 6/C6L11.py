# Transformaciones - funciones drop, sample y randomSplit

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data')

# drop

df.printSchema()

df_util = df.drop('comments_disabled')

df_util.printSchema()

df_util = df.drop('comments_disabled', 'ratings_disabled', 'thumbnail_link')

df_util.printSchema()

df_util = df.drop('comments_disabled', 'ratings_disabled', 'thumbnail_link', 'cafe')

df_util.printSchema()

# sample

df_muestra = df.sample(0.8)

num_filas = df.count()
num_filas_muestra = df_muestra.count()

print('El 80% de filas del dataframe original es {}'.format(num_filas - (num_filas*0.2)))
print('El numero de filas del dataframe muestra es {}'.format(num_filas_muestra))

df_muestra = df.sample(fraction=0.8, seed=1234)

df_muestra = df.sample(withReplacement=True, fraction=0.8, seed=1234)

# randomSplit

train, test = df.randomSplit([0.8, 0.2], seed=1234)

train, validation, test = df.randomSplit([0.6, 0.2, 0.2], seed=1234)

train.count()

validation.count()

test.count()
