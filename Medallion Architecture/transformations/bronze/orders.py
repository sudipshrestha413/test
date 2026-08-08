from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Bronze layer: Raw order transactions"
)
def bronze_orders():
    """
    Generate synthetic order data for the bronze layer.
    In production, this would read from your PostgreSQL database.
    """
    
    # Generate 5000 orders
    orders_data = spark.range(1, 5001).select(
        F.col("id").alias("order_id"),
        ((F.col("id") % 1000) + 1).alias("customer_id"),
        ((F.col("id") % 100) + 1).alias("product_id"),
        (F.rand() * 5 + 1).cast("int").alias("quantity"),
        F.date_sub(F.current_date(), (F.col("id") % 90).cast("int")).alias("order_date"),
        F.when(F.col("id") % 10 == 0, F.lit("cancelled"))
         .when(F.col("id") % 20 == 0, F.lit("pending"))
         .otherwise(F.lit("completed")).alias("status"),
        F.current_timestamp().alias("created_at")
    )
    
    return orders_data
