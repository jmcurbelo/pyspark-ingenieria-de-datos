# Creando DataFrames

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([item for item in range(10)]).map(lambda x: (x, x ** 2))

rdd.collect()

df = rdd.toDF(['numero', 'cudrado'])

df.printSchema()

df.show()

# Crear un DataFrame a partir de un RDD con schema

rdd1 = sc.parallelize([(1, 'Jose', 35.5), (2, 'Teresa', 54.3), (3, 'Katia', 12.7)])

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Primera vía

esquema1 = StructType(
    [
     StructField('id', IntegerType(), True),
     StructField('nombre', StringType(), True),
     StructField('saldo', DoubleType(), True)
    ]
)

# Segunda vía

esquema2 = "`id` INT, `nombre` STRING, `saldo` DOUBLE"

df1 = spark.createDataFrame(rdd1, schema=esquema1)

df1.printSchema()

df1.show()

df2 = spark.createDataFrame(rdd1, schema=esquema2)

df2.printSchema()

df2.show()

# Crear un DataFrame a partir de un rango de números

spark.range(5).toDF('id').show()

spark.range(3, 15).toDF('id').show()

spark.range(0, 20, 2).toDF('id').show()

