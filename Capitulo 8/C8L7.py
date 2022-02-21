# Catalyst Optimizer

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = spark.read.parquet('./data/')

data.printSchema()

data.show()

from pyspark.sql.functions import col

nuevo_df = (data.filter(col('MONTH').isin(6,7,8))
            .withColumn('dis_tiempo_aire', col('DISTANCE') / col('AIR_TIME'))
).select(
    col('AIRLINE'),
    col('dis_tiempo_aire')
).where(col('AIRLINE').isin('AA', 'DL', 'AS'))

nuevo_df.explain(True)

