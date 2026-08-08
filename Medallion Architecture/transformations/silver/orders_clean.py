from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Silver layer: Validated orders with enriched metrics",
    cluster_by=["order_date"]
)
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_drop("valid_status", "status IN ('completed', 'pending', 'cancelled')")
@dp.expect("completed_orders", "status = 'completed'")
def silver_orders_clean():
    """
    Validate and enrich order data:
    - Join with products to get prices
    - Calculate order totals
    - Filter out invalid orders
    - Add time-based dimensions
    """
    
    orders = spark.read.table("bronze_orders")
    products = spark.read.table("silver_products_clean")
    
    return (
        orders
        .join(
            products,
            orders.product_id == products.product_id,
            "inner"
        )
        .withColumn(
            "order_amount",
            (F.col("quantity") * F.col("price")).cast("decimal(10,2)")
        )
        .withColumn(
            "order_year",
            F.year(F.col("order_date"))
        )
        .withColumn(
            "order_month",
            F.month(F.col("order_date"))
        )
        .withColumn(
            "order_quarter",
            F.quarter(F.col("order_date"))
        )
        .withColumn(
            "is_completed",
            F.when(F.col("status") == "completed", F.lit(1)).otherwise(F.lit(0))
        )
        .select(
            orders.order_id,
            orders.customer_id,
            orders.product_id,
            products.product_name_clean.alias("product_name"),
            products.category,
            orders.quantity,
            products.price.alias("unit_price"),
            "order_amount",
            orders.order_date,
            "order_year",
            "order_month",
            "order_quarter",
            orders.status,
            "is_completed",
            orders.created_at
        )
    )
