from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType

@dp.table(
    comment="Bronze layer: Raw customer data with synthetic generation"
)
def bronze_customers():
    """
    Generate synthetic customer data for the bronze layer.
    In production, this would read from your PostgreSQL database:
    
    jdbc_url = "jdbc:postgresql://host:port/database"
    properties = {
        "user": spark.conf.get("postgres.username"),
        "password": spark.conf.get("postgres.password"),
        "driver": "org.postgresql.Driver"
    }
    return spark.read.jdbc(jdbc_url, "customers", properties=properties)
    """
    
    # Generate 1000 synthetic customers
    customers_data = spark.range(1, 1001).select(
        F.col("id").alias("customer_id"),
        F.concat(
            F.lit("customer_"),
            F.col("id")
        ).alias("email"),
        F.concat(
            F.when(F.col("id") % 2 == 0, F.lit("John"))
             .when(F.col("id") % 3 == 0, F.lit("Jane"))
             .otherwise(F.lit("Alex")),
            F.lit(" "),
            F.when(F.col("id") % 5 == 0, F.lit("Smith"))
             .when(F.col("id") % 7 == 0, F.lit("Johnson"))
             .otherwise(F.lit("Williams"))
        ).alias("full_name"),
        F.when(F.col("id") % 4 == 0, F.lit("US"))
         .when(F.col("id") % 4 == 1, F.lit("CA"))
         .when(F.col("id") % 4 == 2, F.lit("UK"))
         .otherwise(F.lit("AU")).alias("country"),
        F.date_sub(F.current_date(), (F.col("id") % 365).cast("int")).alias("signup_date"),
        F.current_timestamp().alias("created_at")
    )
    
    return customers_data
