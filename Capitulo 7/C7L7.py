# Agregación con pivote

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

estudiantes = spark.read.parquet('./data/')

estudiantes.show()

from pyspark.sql.functions import min, max, avg, col

estudiantes.groupBy('graduacion').pivot('sexo').agg(avg('peso')).show()

estudiantes.groupBy('graduacion').pivot('sexo').agg(avg('peso'), min('peso'), max('peso')).show()

estudiantes.groupBy('graduacion').pivot('sexo', ['M']).agg(avg('peso'), min('peso'), max('peso')).show()

estudiantes.groupBy('graduacion').pivot('sexo', ['F']).agg(avg('peso'), min('peso'), max('peso')).show()

