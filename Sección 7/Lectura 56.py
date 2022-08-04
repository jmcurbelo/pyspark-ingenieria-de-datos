# Escritura de DataFrames

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data/')

df1 = df.repartition(2)

df1.write.format('csv').option('sep', '|').save()

df1.coalesce(1).write.format('csv').option('sep', '|').save('./output/csv1')

df.printSchema()

df.select('comments_disabled').distinct().show()

from pyspark.sql.functions import col

df_limpio = df.filter(col('comments_disabled').isin('True', 'False'))

df_limpio.write.partitionBy('comments_disabled').parquet('./output/parquet')

