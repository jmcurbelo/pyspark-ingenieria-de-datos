# Transformaciones - funciones filter y where

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data/datos.parquet')

# filter

from pyspark.sql.functions import col

df.show()

df.filter(col('video_id') == '2kyS6SvSYSE').show()

df1 = spark.read.parquet('./data/datos.parquet').where(col('trending_date') != '17.14.11')

df1.show()

df2 = spark.read.parquet('./data/datos.parquet').where(col('likes') > 5000)

df2.filter((col('trending_date') != '17.14.11') & (col('likes') > 7000)).show()

df2.filter(col('trending_date') != '17.14.11').filter(col('likes') > 7000).show()

