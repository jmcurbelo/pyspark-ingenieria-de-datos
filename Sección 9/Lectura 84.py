# Funciones definidas por el usuario UDF

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def cubo(n):
    return n * n * n

from pyspark.sql.types import LongType

spark.udf.register('cubo', cubo, LongType())

df = spark.range(1, 10)

from pyspark.sql.functions import col

df.show()

df.select(
    col('id'),
    cubo(col('id')).alias('cubo')
).show()

def binvenida(nombre):
    return('Hola {}'.format(nombre))

from pyspark.sql.functions import

from pyspark.sql.types import StringType

binvenida_udf = udf(lambda x: binvenida(x), StringType())

df_nombre = spark.createDataFrame([('Jose',), ('Julia',), ('Katia',)], ['nombre'])

df_nombre.show()

df_nombre.select(
    col('nombre'),
    binvenida_udf(col('nombre')).alias('bienvenida')
).show()

@udf(returnType=StringType())
def mayuscula(s):
    return s.upper()

df_nombre.select(
    col('nombre'),
    mayuscula(col('nombre')).alias('mayuscula')
).show()
