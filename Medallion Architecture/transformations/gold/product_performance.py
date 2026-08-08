from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    comment="Gold layer: Product performance metrics and rankings",
    cluster_by=["category"]
)
def gold_product_performance():
    """
    Business-ready product analytics:
    - Product revenue and sales volume
    - Category performance
    - Product rankings
    - Profitability metrics
    """
    
    orders = spark.read.table("silver_orders_clean")
    products = spark.read.table("silver_products_clean")
    
    # Calculate product metrics
    product_metrics = (
        orders
        .filter(F.col("is_completed") == 1)
        .groupBy(
            "product_id",
            "product_name",
            "category"
        )
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_units_sold"),
            F.sum("order_amount").alias("total_revenue"),
            F.avg("order_amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers")
        )
    )
    
    # Join with product details
    enriched = (
        product_metrics
        .join(
            products.select(
                "product_id",
                "price",
                "tier",
                "estimated_margin",
                "margin_percent"
            ),
            "product_id",
            "inner"
        )
    )
    
    # Add rankings
    revenue_window = Window.orderBy(F.col("total_revenue").desc())
    category_window = Window.partitionBy("category").orderBy(F.col("total_revenue").desc())
    
    return (
        enriched
        .withColumn(
            "total_profit",
            (F.col("total_units_sold") * F.col("estimated_margin")).cast("decimal(10,2)")
        )
        .withColumn(
            "revenue_rank_overall",
            F.row_number().over(revenue_window)
        )
        .withColumn(
            "revenue_rank_in_category",
            F.row_number().over(category_window)
        )
        .withColumn(
            "revenue_per_unit",
            (F.col("total_revenue") / F.col("total_units_sold")).cast("decimal(10,2)")
        )
        .withColumn(
            "profit_per_unit",
            (F.col("total_profit") / F.col("total_units_sold")).cast("decimal(10,2)")
        )
        .select(
            "product_id",
            "product_name",
            "category",
            "tier",
            "price",
            "total_orders",
            "total_units_sold",
            "total_revenue",
            "total_profit",
            "avg_order_value",
            "unique_customers",
            "revenue_per_unit",
            "profit_per_unit",
            "margin_percent",
            "revenue_rank_overall",
            "revenue_rank_in_category"
        )
        .orderBy("revenue_rank_overall")
    )
