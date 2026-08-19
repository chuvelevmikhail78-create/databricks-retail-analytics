from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

sales = spark.table("workspace.silver.sales")
products = spark.table("workspace.silver.products")
returns = spark.table("workspace.silver.returns")
targets = spark.table("workspace.silver.monthly_targets")


returns_by_line = (
    returns
    .groupBy("OrderLineID")
    .agg(
        F.sum("ReturnQty").alias("ReturnedQty"),
        F.sum("RefundAmount").alias("RefundAmount")
    )
)


fact_sales = (
    sales
    .join(
        products.select(
            "ProductID",
            "ProductCategory",
            "StandardUnitCost"
        ),
        on="ProductID",
        how="left"
    )
    .join(
        returns_by_line,
        on="OrderLineID",
        how="left"
    )
    .fillna({
        "ReturnedQty": 0,
        "RefundAmount": 0
    })
    .withColumn(
        "GrossRevenue",
        F.col("Units") * F.col("UnitPrice")
    )
    .withColumn(
        "DiscountAmount",
        F.col("GrossRevenue") * F.col("DiscountPct")
    )
    .withColumn(
        "SalesNetRevenue",
        F.col("GrossRevenue") - F.col("DiscountAmount")
    )
    .withColumn(
        "NetRevenue",
        F.col("SalesNetRevenue") - F.col("RefundAmount")
    )
    .withColumn(
        "NetUnits",
        F.col("Units") - F.col("ReturnedQty")
    )
    .withColumn(
        "COGS",
        F.col("NetUnits") * F.col("StandardUnitCost")
    )
    .withColumn(
        "GrossProfit",
        F.col("NetRevenue") - F.col("COGS")
    )
    .withColumn(
        "Month",
        F.trunc(F.col("OrderDate"), "month")
    )
)

fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.gold.fact_sales")


monthly_performance = (
    fact_sales
    .groupBy("Month", "Channel")
    .agg(
        F.sum("NetRevenue").alias("NetRevenue"),
        F.sum("GrossProfit").alias("GrossProfit"),
        F.sum(
            F.when(
                F.col("StandardUnitCost").isNotNull(),
                F.col("NetRevenue")
            )
        ).alias("RevenueWithKnownCost"),
        F.countDistinct("OrderID").alias("Orders"),
        F.sum("NetUnits").alias("NetUnits"),
        F.countDistinct(
            F.when(
                F.col("ReturnedQty") > 0,
                F.col("OrderID")
            )
        ).alias("ReturnedOrders")
    )
    .withColumn(
        "GrossMargin",
        F.col("GrossProfit") / F.col("RevenueWithKnownCost")
    )
    .withColumn(
        "AverageOrderValue",
        F.col("NetRevenue") / F.col("Orders")
    )
    .withColumn(
        "ReturnRate",
        F.col("ReturnedOrders") / F.col("Orders")
    )
)


targets_for_gold = (
    targets
    .withColumn(
        "ChannelKey",
        F.lower(F.trim(F.col("Channel")))
    )
    .select(
        "Month",
        "ChannelKey",
        "RevenueTarget",
        "GrossMarginTargetPct"
    )
)


monthly_channel_performance = (
    monthly_performance
    .withColumn(
        "ChannelKey",
        F.lower(F.trim(F.col("Channel")))
    )
    .join(
        targets_for_gold,
        on=["Month", "ChannelKey"],
        how="left"
    )
    .withColumn(
        "RevenueTargetAttainment",
        F.col("NetRevenue") / F.col("RevenueTarget")
    )
    .withColumn(
        "GrossMarginVariance",
        F.col("GrossMargin") - F.col("GrossMarginTargetPct")
    )
    .drop("ChannelKey")
)


monthly_channel_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.gold.monthly_channel_performance")


display(
    spark.table("workspace.gold.monthly_channel_performance")
    .orderBy("Month", "Channel")
)
