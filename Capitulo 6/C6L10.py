# Transformaciones - funciones withColumn y withColumnRenamed

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data')

# withColumn

from pyspark.sql.functions import col

df_valoracion = df.withColumn('valoracion', col('likes') - col('dislikes'))

df_valoracion.printSchema()

df_valoracion1 = (df.withColumn('valoracion', col('likes') - col('dislikes'))
                    .withColumn('res_div', col('valoracion') % 10)
)

df_valoracion1.printSchema()

df_valoracion1.select(col('likes'), col('dislikes'), col('valoracion'), col('res_div')).show()

# withColumnRenamed

df_renombrado = df.withColumnRenamed('video_id', 'id')

df_renombrado.printSchema()

df_error = df.withColumnRenamed('nombre_que_no_existe', 'otro_nombre')

df_error.printSchema()
