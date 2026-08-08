from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Silver layer: Enriched and validated product catalog",
    cluster_by=["category"]
)
@dp.expect_or_drop("valid_price", "price > 0")
@dp.expect_or_fail("required_product_id", "product_id IS NOT NULL")
@dp.expect("reasonable_price", "price < 10000")
def silver_products_clean():
    """
    Enrich and validate product data:
    - Standardize product names
    - Add price tiers and margins
    - Calculate discount flags
    """
    
    return (
        spark.read.table("bronze_products")
        .withColumn(
            "product_name_clean",
            F.trim(F.col("product_name"))
        )
        .withColumn(
            "price_tier_flag",
            F.when(F.col("price") >= 1000, F.lit("high"))
             .when(F.col("price") >= 500, F.lit("medium"))
             .otherwise(F.lit("low"))
        )
        .withColumn(
            "estimated_cost",
            (F.col("price") * 0.6).cast("decimal(10,2)")
        )
        .withColumn(
            "estimated_margin",
            (F.col("price") - F.col("estimated_cost")).cast("decimal(10,2)")
        )
        .withColumn(
            "margin_percent",
            F.round((F.col("estimated_margin") / F.col("price") * 100), 2)
        )
        .select(
            "product_id",
            "product_name_clean",
            "category",
            "tier",
            "price",
            "price_tier_flag",
            "estimated_cost",
            "estimated_margin",
            "margin_percent",
            "created_at"
        )
    )
