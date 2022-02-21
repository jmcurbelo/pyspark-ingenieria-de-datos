# Transformaciones - funciones sort y orderBy

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import col

df = (spark.read.parquet('./data')
    .select(col('likes'), col('views'), col('video_id'), col('dislikes'))
    .dropDuplicates(['video_id'])
)

df.show()

# sort

df.sort('likes').show()

from pyspark.sql.functions import desc

df.sort(desc('likes')).show()

# función orderBy

df.orderBy(col('views')).show()

df.orderBy(col('views').desc()).show()

dataframe = spark.createDataFrame([(1, 'azul', 568), (2, 'rojo', 235), (1, 'azul', 456), (2, 'azul', 783)]).toDF('id', 'color', 'importe')

dataframe.show()

dataframe.orderBy(col('color').desc(), col('importe')).show()

# funcion limit 

top_10 = df.orderBy(col('views').desc()).limit(10)

top_10.show()
