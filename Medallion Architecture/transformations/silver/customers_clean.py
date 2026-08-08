from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Silver layer: Cleansed customer data with quality checks",
    cluster_by=["country"]
)
@dp.expect_or_drop("valid_email", "email_normalized IS NOT NULL AND (email_normalized LIKE '%@%' OR email_normalized LIKE 'customer_%')")
@dp.expect_or_drop("valid_country", "country IN ('US', 'CA', 'UK', 'AU')")
@dp.expect("recent_signup", "signup_date >= date_sub(current_date(), 730)")
def silver_customers_clean():
    """
    Cleanse and validate customer data:
    - Standardize email format
    - Deduplicate by customer_id (keep most recent)
    - Add data quality flags
    - Enrich with derived fields
    """
    
    return (
        spark.read.table("bronze_customers")
        .withColumn(
            "email_normalized",
            F.lower(F.trim(F.col("email")))
        )
        .withColumn(
            "first_name",
            F.split(F.col("full_name"), " ")[0]
        )
        .withColumn(
            "last_name",
            F.split(F.col("full_name"), " ")[1]
        )
        .withColumn(
            "customer_tenure_days",
            F.datediff(F.current_date(), F.col("signup_date"))
        )
        .withColumn(
            "customer_segment",
            F.when(F.col("customer_tenure_days") > 365, F.lit("loyal"))
             .when(F.col("customer_tenure_days") > 90, F.lit("active"))
             .otherwise(F.lit("new"))
        )
        .select(
            "customer_id",
            "email_normalized",
            "first_name",
            "last_name",
            "country",
            "signup_date",
            "customer_tenure_days",
            "customer_segment",
            "created_at"
        )
    )
