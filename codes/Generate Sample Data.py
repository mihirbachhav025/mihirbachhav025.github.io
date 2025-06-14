# Databricks notebook source
from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.appName("SalesWindowPractice").getOrCreate()

# 8-row sample data with variety
data = [
    (1, "2025-06-01", "Laptop", "New York", 1200),
    (2, "2025-06-01", "Phone", "Los Angeles", 850),
    (3, "2025-06-02", "Tablet", "Chicago", 400),
    (4, "2025-06-02", "Laptop", "Chicago", 1100),
    (5, "2025-06-03", "Phone", "New York", 850),
    (6, "2025-06-03", "Tablet", "Los Angeles", 420),
    (7, "2025-06-04", "Laptop", "Los Angeles", 1250),
    (8, "2025-06-04", "Phone", "Chicago", 780)
]

columns = ["id", "Date", "Product", "City", "Sales"]

# Create DataFrame
sales_df = spark.createDataFrame(data, columns)

# Show DataFrame
sales_df.display()
sales_df.createOrReplaceTempView('sales_df')