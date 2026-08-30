import sys

from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("WorkerTest")
    .master("local[1]")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

print("Python:", sys.executable)
print("Spark:", spark.version)

result = spark.sparkContext.parallelize([1, 2, 3], 1).map(lambda x: x * 2).collect()

print("Result:", result)

spark.stop()