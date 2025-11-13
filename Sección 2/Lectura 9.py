# Instalar SDK java 17

!apt-get install openjdk-17-jdk-headless -qq > /dev/null

# Descargar Spark 4.0.1

!wget -q https://downloads.apache.org/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz

# Descomprimir la version de Spark

!tar xf spark-4.0.1-bin-hadoop3.tgz

# Establecer las variables de entorno

import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-4.0.1-bin-hadoop3"

# Instalar la librería findspark

!pip install -q findspark

# Instalar pyspark

!pip install -q pyspark

# Verificar la instalación

import findspark

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

# Probando la sesión de Spark

df = spark.createDataFrame([{"Hola": "Mundo"} for x in range(10)])

df.show(10, False)
