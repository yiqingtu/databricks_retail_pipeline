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

df = spark.createDataFrame([("Alice", 25)], ["name", "age"])
df.show()

output_root = Path("data") / "output"
output_root.mkdir(parents=True, exist_ok=True)

try:
    df.write.mode("overwrite").parquet(str(output_root / "people_parquet"))
    print("Saved to data/output/people_parquet")
except Exception as err:
    # On some Windows setups, Spark parquet write needs HADOOP_HOME/winutils.
    print(f"Parquet write failed: {err}")
    csv_path = output_root / "people.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        writer.writerows((row["name"], row["age"]) for row in df.collect())
    print(f"Saved fallback CSV to {csv_path}")

print("test comple`ted successfully.")

spark.stop()