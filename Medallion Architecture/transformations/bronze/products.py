from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Bronze layer: Raw product catalog"
)
def bronze_products():
    """
    Generate synthetic product data for the bronze layer.
    In production, this would read from your PostgreSQL database.
    """
    
    # Generate 100 products
    products_data = spark.range(1, 101).select(
        F.col("id").alias("product_id"),
        F.concat(
            F.when(F.col("id") % 5 == 0, F.lit("Laptop"))
             .when(F.col("id") % 5 == 1, F.lit("Phone"))
             .when(F.col("id") % 5 == 2, F.lit("Tablet"))
             .when(F.col("id") % 5 == 3, F.lit("Monitor"))
             .otherwise(F.lit("Keyboard")),
            F.lit(" Model "),
            F.col("id")
        ).alias("product_name"),
        F.when(F.col("id") % 5 == 0, F.lit("Electronics"))
         .when(F.col("id") % 5 == 1, F.lit("Mobile"))
         .when(F.col("id") % 5 == 2, F.lit("Computing"))
         .when(F.col("id") % 5 == 3, F.lit("Accessories"))
         .otherwise(F.lit("Peripherals")).alias("category"),
        (F.col("id") * 10 + 99).cast("decimal(10,2)").alias("price"),
        F.when(F.col("id") % 10 == 0, F.lit("Premium"))
         .when(F.col("id") % 3 == 0, F.lit("Standard"))
         .otherwise(F.lit("Budget")).alias("tier"),
        F.current_timestamp().alias("created_at")
    )
    
    return products_data
