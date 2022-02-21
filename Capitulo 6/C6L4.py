# Crear DataFrames a partir de fuentes de datos en la práctica

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Crear un DataFrame mediante la lectura de un archivo de texto

df = spark.read.text('./data/dataTXT.txt')

df.show()

df.show(truncate=False)

# Crear un DataFrame mediante la lectura de un archivo csv

df1 = spark.read.csv('./data/dataCSV.csv')

df1.show()

df1 = spark.read.option('header', 'true').csv('./data/dataCSV.csv')

df1.show()

# Leer un archivo de texto con un delimitador diferente

df2 = spark.read.option('header', 'true').option('delimiter', '|').csv('./data/dataTab.txt')

df2.show()

# Crear un DataFrame a partir de un json proporcionando un schema

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

json_schema =  StructType(
    [
     StructField('color', StringType(), True),
     StructField('edad', IntegerType(), True),
     StructField('fecha', DateType(), True),
     StructField('pais', StringType(), True)
    ]
)

df4 = spark.read.schema(json_schema).json('./data/dataJSON.json')

df4.show()

df4.printSchema()

# Crear un DataFrame a partir de un archivo parquet

df5 = spark.read.parquet('./data/dataPARQUET.parquet')

df5.show()

# Otra alternativa para leer desde una fuente de datos parquet en este caso

df6 = spark.read.format('parquet').load('./data/dataPARQUET.parquet')

df6.printSchema()