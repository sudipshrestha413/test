from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Gold layer: Customer lifetime value and key metrics",
    cluster_by=["customer_segment"]
)
def gold_customer_summary():
    """
    Business-ready customer analytics:
    - Customer lifetime value (CLV)
    - Total orders and revenue per customer
    - Average order value
    - Customer segmentation
    """
    
    customers = spark.read.table("silver_customers_clean")
    orders = spark.read.table("silver_orders_clean")
    
    # Calculate customer metrics
    customer_metrics = (
        orders
        .filter(F.col("is_completed") == 1)
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_amount").alias("total_revenue"),
            F.avg("order_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("product_id").alias("unique_products_purchased")
        )
    )
    
    # Join with customer data
    return (
        customers
        .join(customer_metrics, "customer_id", "left")
        .withColumn(
            "total_orders",
            F.coalesce(F.col("total_orders"), F.lit(0))
        )
        .withColumn(
            "total_revenue",
            F.coalesce(F.col("total_revenue"), F.lit(0)).cast("decimal(10,2)")
        )
        .withColumn(
            "avg_order_value",
            F.coalesce(F.col("avg_order_value"), F.lit(0)).cast("decimal(10,2)")
        )
        .withColumn(
            "customer_lifetime_value",
            F.col("total_revenue")
        )
        .withColumn(
            "days_since_last_order",
            F.when(
                F.col("last_order_date").isNotNull(),
                F.datediff(F.current_date(), F.col("last_order_date"))
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "customer_status",
            F.when(F.col("total_orders") == 0, F.lit("inactive"))
             .when(F.col("days_since_last_order") <= 30, F.lit("active"))
             .when(F.col("days_since_last_order") <= 90, F.lit("at_risk"))
             .otherwise(F.lit("churned"))
        )
        .select(
            "customer_id",
            "email_normalized",
            "first_name",
            "last_name",
            "country",
            "customer_segment",
            "customer_status",
            "signup_date",
            "customer_tenure_days",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "customer_lifetime_value",
            "unique_products_purchased",
            "first_order_date",
            "last_order_date",
            "days_since_last_order"
        )
    )
