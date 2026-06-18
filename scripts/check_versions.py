from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("CheckVersions").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

for table in ["bronze/orders", "bronze/customers", "bronze/products",
              "silver/orders", "silver/customers", "silver/products",
              "gold/customer_revenue", "gold/popular_products", "gold/category_revenue"]:
    try:
        dt = DeltaTable.forPath(spark, f"/opt/data/delta/{table}")
        latest = dt.history(1).select("version", "timestamp", "operation").collect()[0]
        print(f"✅ {table:35} version={latest.version} | {latest.timestamp} | {latest.operation}")
    except Exception as e:
        print(f"❌ {table:35} {str(e)[:50]}")

spark.stop()