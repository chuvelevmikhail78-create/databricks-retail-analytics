from pyspark.sql import functions as F

raw_path = "/Volumes/workspace/default/koerber_retail_raw"

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")


sales_bronze = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{raw_path}/sales_raw.csv")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("sales_raw.csv"))
)

sales_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.sales")


customers_bronze = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{raw_path}/customers_raw.csv")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("customers_raw.csv"))
)

customers_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.customers")


products_bronze = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{raw_path}/products_raw.csv")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("products_raw.csv"))
)

products_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.products")


returns_bronze = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{raw_path}/returns_raw.csv")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("returns_raw.csv"))
)

returns_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.returns")


monthly_targets_bronze = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{raw_path}/monthly_targets_raw.csv")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("monthly_targets_raw.csv"))
)

monthly_targets_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.monthly_targets")
