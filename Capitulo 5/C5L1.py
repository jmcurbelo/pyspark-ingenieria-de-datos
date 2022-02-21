# Almacenamiento en caché

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([item for item in range(10)])

from pyspark.storagelevel import StorageLevel

rdd.persist(StorageLevel.MEMORY_ONLY)

rdd.unpersist()

rdd.persist(StorageLevel.DISK_ONLY)

rdd.unpersist()

rdd.cache()