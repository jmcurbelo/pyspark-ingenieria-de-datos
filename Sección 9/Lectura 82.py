# Funciones para trabajo con colecciones

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = spark.read.parquet('./data/parquet/')

data.show(truncate=False)

data.printSchema()

from pyspark.sql.functions import col, size, sort_array, array_contains

data.select(
    size(col('tareas')).alias('tamaño'),
    sort_array(col('tareas')).alias('arreglo_ordenado'),
    array_contains(col('tareas'), 'buscar agua').alias('buscar_agua')
).show(truncate=False)

from pyspark.sql.functions import explode

data.select(
    col('dia'),
    explode(col('tareas')).alias('tareas')
).show()

# Formato JSON

json_df_str = spark.read.parquet('./data/JSON')

json_df_str.show(truncate=False)

json_df_str.printSchema()

from pyspark.sql.types import StructType, StructField, StringType, ArrayType

schema_json = StructType(
    [
     StructField('dia', StringType(), True),
     StructField('tareas', ArrayType(StringType()), True)
    ]
)

from pyspark.sql.functions import from_json, to_json

json_df = json_df_str.select(
    from_json(col('tareas_str'), schema_json).alias('por_hacer')
)

json_df.printSchema()

json_df.select(
    col('por_hacer').getItem('dia'),
    col('por_hacer').getItem('tareas'),
    col('por_hacer').getItem('tareas').getItem(0).alias('primer_tarea')
).show(truncate=False)

json_df.select(
    to_json(col('por_hacer'))
).show(truncate=False)

