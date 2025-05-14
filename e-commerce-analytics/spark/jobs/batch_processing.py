#!/usr/bin/env python3
"""
E-commerce Batch Processing Job
------------------------------
This Spark job processes historical e-commerce data from the data lake,
transforms it, and loads it into the data warehouse.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, TimestampType

def create_spark_session():
    """Create and return a Spark session."""
    return (
        SparkSession.builder
        .appName("E-commerce Batch Processing")
        .config("spark.sql.warehouse.dir", "/opt/spark-data/warehouse")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.3.3")
        .getOrCreate()
    )

def read_data_from_lake(spark, data_path):
    """Read data from the data lake (local directory simulating S3)."""
    # Define schema for orders data
    orders_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("order_date", StringType(), False),
        StructField("status", StringType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("items", ArrayType(
            StructType([
                StructField("product_id", StringType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("price", DoubleType(), False)
            ])
        ), False)
    ])
    
    # Read orders data
    orders_df = (
        spark.read.format("json")
        .schema(orders_schema)
        .load(f"{data_path}/orders/*.json")
    )
    
    # Convert string timestamp to timestamp type
    orders_df = orders_df.withColumn(
        "order_date", 
        to_timestamp(col("order_date"))
    )
    
    return orders_df

def transform_data(orders_df):
    """Transform the data for analytics purposes."""
    # Extract date parts for time-based analysis
    orders_with_time_df = (
        orders_df
        .withColumn("year", year(col("order_date")))
        .withColumn("month", month(col("order_date")))
        .withColumn("day", dayofmonth(col("order_date")))
        .withColumn("hour", hour(col("order_date")))
    )
    
    # Create order summary
    order_summary_df = (
        orders_with_time_df
        .select(
            "order_id",
            "user_id",
            "order_date",
            "status",
            "total_amount",
            "year",
            "month",
            "day",
            "hour"
        )
    )
    
    # Explode items to get order details
    order_items_df = (
        orders_with_time_df
        .select(
            "order_id",
            "order_date",
            "year",
            "month",
            "day",
            explode(col("items")).alias("item")
        )
        .select(
            "order_id",
            "order_date",
            "year",
            "month",
            "day",
            col("item.product_id").alias("product_id"),
            col("item.quantity").alias("quantity"),
            col("item.price").alias("price"),
            (col("item.quantity") * col("item.price")).alias("item_total")
        )
    )
    
    return order_summary_df, order_items_df

def write_to_warehouse(df, table_name, jdbc_url, properties):
    """Write the transformed data to PostgreSQL warehouse."""
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="overwrite",
        properties=properties
    )

def main():
    """Main function to run the Spark job."""
    spark = create_spark_session()
    
    # Define connection properties
    jdbc_url = "jdbc:postgresql://postgres:5432/ecommerce_dwh"
    conn_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    
    try:
        # Read data from the data lake
        orders_df = read_data_from_lake(spark, "/opt/spark-data/lake")
        
        # Transform the data
        order_summary_df, order_items_df = transform_data(orders_df)
        
        # Write to the data warehouse
        write_to_warehouse(
            order_summary_df,
            "order_summary",
            jdbc_url,
            conn_properties
        )
        
        write_to_warehouse(
            order_items_df,
            "order_items",
            jdbc_url,
            conn_properties
        )
        
        print("Batch processing completed successfully")
        
    except Exception as e:
        print(f"Error in batch processing: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 