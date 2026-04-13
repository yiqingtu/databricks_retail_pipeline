from pyspark.sql import SparkSession
import csv
from pathlib import Path

print("Starting Spark...")

spark = (
    SparkSession.builder
    .appName("test")
    .master("local[1]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

#df= spark.createDataFrame([("Alice", 25)], ["name", "age"])
#df.show()

df = spark.read.csv("data/superstore.csv", header=True, inferSchema=True)
print("Row count:", df.count())
df.show(5, truncate=False)
df.printSchema()



output_root = Path("data") / "output"
output_root.mkdir(parents=True, exist_ok=True)

df.write.mode("overwrite").parquet(str(output_root / "people_parquet"))
print("Saved to data/output/people_parquet")

print("test completed successfully.")

spark.stop()