from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PythonWorkerTest")
    .master("local[*]")
    .config("spark.driver.memory", "6g")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.python.worker.reuse", "true")
    .getOrCreate()
)

df = spark.range(10000)

print("Count:", df.count())

spark.stop()