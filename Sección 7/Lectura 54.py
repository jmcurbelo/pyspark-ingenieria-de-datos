# Trabajo con datos incorrectos o faltantes

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data/')

df.count()

df.na.drop().count()

df.na.drop('any').count()

df.dropna().count()

df.na.drop(subset=['views']).count()

df.na.drop(subset=['views', 'dislikes']).count()

from pyspark.sql.functions import col

df.orderBy(col('views')).select(col('views'), col('likes'), col('dislikes')).show()

df.fillna(0).orderBy(col('views')).select(col('views'), col('likes'), col('dislikes')).show()

df.fillna(0, subset=['likes', 'dislikes']).orderBy(col('views')).select(col('views'), col('likes'), col('dislikes')).show()

