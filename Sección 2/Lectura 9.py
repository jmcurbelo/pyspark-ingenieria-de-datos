# Configurar Java
# Google Colab ya incluye Java preinstalado (actualmente Java 21, compatible con Spark 4.x).
# Detectamos su ubicación automáticamente para no depender de una versión concreta.

import os
import subprocess

java_bin = subprocess.check_output(["readlink", "-f", "/usr/bin/java"]).decode().strip()
os.environ["JAVA_HOME"] = java_bin.replace("/bin/java", "")

# Instalar Spark
# El paquete pyspark de PyPI ya incluye Spark completo, así que no hace falta descargar
# el archivo .tgz desde la web de Apache (esos enlaces dejan de funcionar con cada versión nueva).

SPARK_VERSION = "4.0.2"

!pip install -q pyspark=={SPARK_VERSION}

# Verificar la instalación

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

print("Versión de Spark:", spark.version)
print("JAVA_HOME:", os.environ["JAVA_HOME"])

# Probando la sesión de Spark

df = spark.createDataFrame([{"Hola": "Mundo"} for x in range(10)])

df.show(10, False)
