# Manejo de nombres de columnas duplicados

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

empleados = spark.read.parquet('./data/empleados/')

departamentos = spark.read.parquet('./data/departamentos/')

from pyspark.sql.functions import col

depa = departamentos.withColumn('num_dpto', col('id'))

depa.printSchema()

empleados.printSchema()

# Devuelve un error

empleados.join(depa, col('num_dpto') == col('num_dpto'))

# Forma correcta

df_con_duplicados = empleados.join(depa, empleados['num_dpto'] == depa['num_dpto'])

df_con_duplicados.printSchema()

df_con_duplicados.select(empleados['num_dpto']).show()

df2 = empleados.join(depa, 'num_dpto')

df2.printSchema()

empleados.join(depa, ['num_dpto']).printSchema()



