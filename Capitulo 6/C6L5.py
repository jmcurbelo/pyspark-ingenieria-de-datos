# Trabajo con columnas

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data/dataPARQUET.parquet')

df.printSchema()

# Primera alternativa para referirnos a las columnas

df.select('title').show()

# Segunda alternativa

from pyspark.sql.functions import col

df.select(col('title')).show()

