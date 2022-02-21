# Funciones para trabajo con strings

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = spark.read.parquet('./data/')

data.show()

from pyspark.sql.functions import ltrim, rtrim, trim

data.select(
    ltrim('nombre').alias('ltrim'),
    rtrim('nombre').alias('rtrim'),
    trim('nombre').alias('trim')
).show()

from pyspark.sql.functions import col, lpad, rpad

data.select(
    trim(col('nombre')).alias('trim')
).select(
    lpad(col('trim'), 8, '-').alias('lpad'),
    rpad(col('trim'), 8, '=').alias('rpad')
).show()

df1 = spark.createDataFrame([('Spark', 'es', 'maravilloso')], ['sujeto', 'verbo', 'adjetivo'])

df1.show()

from pyspark.sql.functions import concat_ws, lower, upper, initcap, reverse

df1.select(
    concat_ws(' ', col('sujeto'), col('verbo'), col('adjetivo')).alias('frase')
).select(
    col('frase'),
    lower(col('frase')).alias('minuscula'),
    upper(col('frase')).alias('mayuscula'),
    initcap(col('frase')).alias('initcap'),
    reverse(col('frase')).alias('reversa')
).show()

from pyspark.sql.functions import regexp_replace

df2 = spark.createDataFrame([(' voy a casa por mis llaves',)], ['frase'])

df2.show(truncate=False)

df2.select(
    regexp_replace(col('frase'), 'voy|por', 'ir').alias('nueva_frase')
).show(truncate=False)

