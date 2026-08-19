from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")


# Sales

sales_silver = spark.table("workspace.bronze.sales")

sales_silver = (
    sales_silver
    .withColumn(
        "Channel",
        F.when(F.lower(F.trim(F.col("Channel"))) == "market place", "marketplace")
        .when(F.lower(F.trim(F.col("Channel"))) == "store", "retail store")
        .otherwise(F.lower(F.trim(F.col("Channel"))))
    )
    .withColumn(
        "OrderDate",
        F.coalesce(
            F.try_to_date(F.col("OrderDate"), "dd.MM.yyyy"),
            F.try_to_date(F.col("OrderDate"), "yyyy-MM-dd"),
            F.try_to_date(F.col("OrderDate"), "yyyy/MM/dd")
        )
    )
    .withColumn("Units", F.col("Units").cast("int"))
    .withColumn("UnitPrice", F.col("UnitPrice").cast("decimal(10,2)"))
    .withColumn(
        "_discount_clean",
        F.regexp_replace(F.trim(F.col("DiscountPct")), ",", ".")
    )
    .withColumn(
        "DiscountPct",
        F.when(
            F.col("_discount_clean").contains("%"),
            F.regexp_replace(
                F.col("_discount_clean"), "%", ""
            ).cast("decimal(10,4)") / 100
        ).otherwise(
            F.col("_discount_clean").cast("decimal(10,4)")
        )
    )
    .drop("_discount_clean")
    .withColumn("Currency", F.upper(F.trim(F.col("Currency"))))
)

invalid_sales = (
    F.col("OrderDate").isNull()
    | F.col("Units").isNull()
    | (F.col("Units") <= 0)
    | F.col("UnitPrice").isNull()
    | (F.col("UnitPrice") <= 0)
    | F.col("DiscountPct").isNull()
    | (F.col("DiscountPct") < 0)
    | (F.col("DiscountPct") > 1)
)

sales_silver = (
    sales_silver
    .filter(~invalid_sales)
    .dropDuplicates()
)

sales_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.sales")


# Customers

customers_silver = spark.table("workspace.bronze.customers")

segment_clean = F.lower(F.trim(F.col("CustomerSegment")))
region_clean = F.lower(F.trim(F.col("Region")))

customers_silver = (
    customers_silver
    .withColumn(
        "CustomerSegment",
        F.when(segment_clean == "new", "New")
        .when(segment_clean == "returning", "Returning")
        .when(segment_clean == "vip", "VIP")
        .otherwise(F.col("CustomerSegment"))
    )
    .withColumn(
        "Region",
        F.when(region_clean == "hh", "Hamburg")
        .when(region_clean == "hamburg", "Hamburg")
        .when(region_clean == "berlin", "Berlin")
        .when(region_clean == "nord", "Nord")
        .when(region_clean == "north", "Nord")
        .when(region_clean.isin("sued", "süd"), "Süd")
        .when(region_clean == "west", "West")
        .otherwise(F.col("Region"))
    )
    .withColumn(
        "SignupDate",
        F.coalesce(
            F.try_to_date(F.col("SignupDate"), "dd.MM.yyyy"),
            F.try_to_date(F.col("SignupDate"), "yyyy-MM-dd"),
            F.try_to_date(F.col("SignupDate"), "yyyy/MM/dd")
        )
    )
    .withColumn("Country", F.upper(F.trim(F.col("Country"))))
    .dropDuplicates()
)

conflicting_customers = (
    customers_silver
    .groupBy("CustomerID")
    .count()
    .filter(F.col("count") > 1)
)

customers_silver = (
    customers_silver
    .join(
        conflicting_customers.select("CustomerID"),
        on="CustomerID",
        how="left_anti"
    )
)

customers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.customers")


# Products

products_silver = spark.table("workspace.bronze.products")

category_clean = F.lower(F.trim(F.col("ProductCategory")))
active_clean = F.lower(F.trim(F.col("ActiveFlag")))

products_silver = (
    products_silver
    .withColumn(
        "ProductCategory",
        F.when(category_clean == "electronics", "Electronics")
        .when(category_clean.isin("home", "home & living"), "Home")
        .when(category_clean == "lifestyle", "Lifestyle")
        .when(category_clean == "office", "Office")
        .otherwise(F.col("ProductCategory"))
    )
    .withColumn(
        "ActiveFlag",
        F.when(active_clean.isin("1", "y", "yes"), True)
        .when(active_clean.isin("0", "n", "no"), False)
        .otherwise(None)
    )
    .withColumn(
        "StandardUnitCost",
        F.col("StandardUnitCost").cast("decimal(10,2)")
    )
    .withColumn(
        "ListPrice",
        F.col("ListPrice").cast("decimal(10,2)")
    )
    .dropDuplicates()
)

conflicting_products = (
    products_silver
    .groupBy("ProductID")
    .count()
    .filter(F.col("count") > 1)
)

products_silver = (
    products_silver
    .join(
        conflicting_products.select("ProductID"),
        on="ProductID",
        how="left_anti"
    )
)

products_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.products")


# Returns

returns_silver = spark.table("workspace.bronze.returns")

returns_silver = (
    returns_silver
    .withColumn(
        "ReturnDate_parsed",
        F.coalesce(
            F.try_to_date(F.col("ReturnDate"), "dd.MM.yyyy"),
            F.try_to_date(F.col("ReturnDate"), "yyyy-MM-dd"),
            F.try_to_date(F.col("ReturnDate"), "yyyy/MM/dd")
        )
    )
    .withColumn("ReturnQty", F.col("ReturnQty").cast("int"))
    .withColumn("RefundAmount", F.col("RefundAmount").cast("decimal(10,2)"))
    .withColumn("ReturnDate", F.col("ReturnDate_parsed"))
    .drop("ReturnDate_parsed")
)

sales_check = spark.table("workspace.silver.sales")

returns_checked = (
    returns_silver
    .join(
        sales_check.select("OrderLineID", "Units"),
        on="OrderLineID",
        how="left"
    )
)

returns_silver = (
    returns_checked
    .filter(
        F.col("Units").isNotNull()
        & (F.col("ReturnQty") > 0)
        & (F.col("ReturnQty") <= F.col("Units"))
        & F.col("ReturnDate").isNotNull()
        & F.col("RefundAmount").isNotNull()
    )
    .drop("Units")
    .dropDuplicates()
)

returns_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.returns")


# Monthly targets

monthly_targets_silver = spark.table("workspace.bronze.monthly_targets")

monthly_targets_silver = (
    monthly_targets_silver
    .withColumn(
        "Month_parsed",
        F.when(
            F.col("Month").contains("/"),
            F.try_to_date(
                F.concat(F.lit("01/"), F.col("Month")),
                "dd/MM/yyyy"
            )
        ).otherwise(
            F.try_to_date(
                F.concat(F.col("Month"), F.lit("-01")),
                "yyyy-MM-dd"
            )
        )
    )
    .drop("Month")
    .withColumnRenamed("Month_parsed", "Month")
    .withColumn(
        "RevenueTarget",
        F.col("RevenueTarget").cast("decimal(12,2)")
    )
    .withColumn(
        "GrossMarginTargetPct",
        F.col("GrossMarginTargetPct").cast("decimal(10,4)")
    )
)

monthly_targets_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.monthly_targets")
