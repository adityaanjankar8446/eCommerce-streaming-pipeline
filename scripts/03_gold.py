from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .appName("Gold_Aggregations")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session started")

# ── Read Silver Tables (batch) ───────────────────────────
silver_orders   = spark.read.format("delta").load("/opt/data/delta/silver/orders")
silver_customers = spark.read.format("delta").load("/opt/data/delta/silver/customers")
silver_products  = spark.read.format("delta").load("/opt/data/delta/silver/products")

print("✅ Silver tables loaded")


# ── Gold 1: Revenue by Customer ──────────────────────────
print("Building gold_customer_revenue...")

orders_with_price = (
    silver_orders
    .join(
        silver_products.select("product_id", "price"),
        on="product_id", how="left"
    )
    .withColumn("line_total", col("quantity") * col("price"))
)

gold_customer_revenue = (
    orders_with_price
    .groupBy("customer_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("quantity").alias("total_items"),
        round(sum("line_total"), 2).alias("total_revenue")
    )
)

gold_customer_revenue.show(5)

(
    gold_customer_revenue.write
    .format("delta")
    .mode("overwrite")
    .save("/opt/data/delta/gold/customer_revenue")
)
print("✅ gold_customer_revenue complete")



# ── Gold 2: Popular Products ─────────────────────────────
print("Building gold_popular_products...")

gold_popular_products = (
    silver_orders
    .groupBy("product_id")
    .agg(
        sum("quantity").alias("total_quantity_sold"),
        countDistinct("order_id").alias("total_orders")
    )
    .join(
        silver_products.select("product_id", "title", "category", "price"),
        on="product_id", how="left"
    )
    .withColumn("total_revenue", round(col("total_quantity_sold") * col("price"), 2))
    .orderBy(col("total_quantity_sold").desc())
)

gold_popular_products.show(5)

(
    gold_popular_products.write
    .format("delta")
    .mode("overwrite")
    .save("/opt/data/delta/gold/popular_products")
)
print("✅ gold_popular_products complete")

# ── Gold 3: Revenue by Category ──────────────────────────
print("Building gold_category_revenue...")

gold_category_revenue = (
    silver_orders
    .join(
        silver_products.select("product_id", "price", "category"),
        on="product_id", how="left"
    )
    .withColumn("line_total", col("quantity") * col("price"))
    .groupBy("category")
    .agg(
        round(sum("line_total"), 2).alias("total_revenue"),
        sum("quantity").alias("total_units_sold"),
        countDistinct("order_id").alias("total_orders")
    )
    .orderBy(col("total_revenue").desc())
)

gold_category_revenue.show(5)

(
    gold_category_revenue.write
    .format("delta")
    .mode("overwrite")
    .save("/opt/data/delta/gold/category_revenue")
)
print("✅ gold_category_revenue complete")

print("✅ All gold tables complete")
spark.stop()