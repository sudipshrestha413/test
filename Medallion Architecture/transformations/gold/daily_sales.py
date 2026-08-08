from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    comment="Gold layer: Daily sales metrics and revenue trends",
    cluster_by=["order_year", "order_month"]
)
def gold_daily_sales():
    """
    Business-ready daily sales analytics:
    - Daily revenue and order counts
    - Running totals
    - Growth metrics
    - Product category breakdown
    """
    
    orders = spark.read.table("silver_orders_clean")
    
    # Calculate daily metrics
    daily_metrics = (
        orders
        .filter(F.col("is_completed") == 1)
        .groupBy(
            "order_date",
            "order_year",
            "order_month",
            "order_quarter"
        )
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_amount").alias("total_revenue"),
            F.avg("order_amount").alias("avg_order_value"),
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("product_id").alias("unique_products")
        )
    )
    
    # Add window functions for trends
    window_spec = Window.orderBy("order_date")
    
    return (
        daily_metrics
        .withColumn(
            "cumulative_revenue",
            F.sum("total_revenue").over(window_spec)
        )
        .withColumn(
            "cumulative_orders",
            F.sum("total_orders").over(window_spec)
        )
        .withColumn(
            "revenue_7day_moving_avg",
            F.avg("total_revenue").over(
                window_spec.rowsBetween(-6, 0)
            )
        )
        .withColumn(
            "revenue_per_customer",
            (F.col("total_revenue") / F.col("unique_customers")).cast("decimal(10,2)")
        )
        .withColumn(
            "revenue_per_order",
            (F.col("total_revenue") / F.col("total_orders")).cast("decimal(10,2)")
        )
        .select(
            "order_date",
            "order_year",
            "order_month",
            "order_quarter",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "total_units_sold",
            "unique_customers",
            "unique_products",
            "cumulative_revenue",
            "cumulative_orders",
            "revenue_7day_moving_avg",
            "revenue_per_customer",
            "revenue_per_order"
        )
        .orderBy("order_date")
    )
