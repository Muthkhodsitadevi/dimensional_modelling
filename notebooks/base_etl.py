# Use these two lines if not running in Databricks
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("DataModellingAndETL").getOrCreate()

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from dq_checks import *

df_bronze_base = spark.read.csv("/data/SampleData_Base.csv", header = True)

clean_base, invalid_base = data_quality_check(df_bronze_base)

# Standardize valid records
std_df = clean_base.withColumn("amount", F.col("amount").cast("double")) \
                         .withColumn("quantity", F.col("quantity").cast("int")) \
                         .withColumn("sale_date", F.to_date(F.col("sale_date"), "M/d/yyyy"))

# Extract unique customers and products from the raw data.
df_customer = std_df.select("customer_name","region","segment").distinct()
df_product = std_df.select("product_name").distinct()

# Build dimension tables with surrogate keys.

df_product = df_product
    .withColumn("product_key",rowNumber().over(Window.orderBy("product_name"))
    
df_customer = df_customer
    .withColumn("customer_key",rowNumber().over(Window.orderBy("customer_name"))
    .withColumn("start_date",current_date())
    .withColumn("end_date",F.to_date('12/31/9999', "M/d/yyyy")) 
    
#order the columns

df_date = (
    std_df
    .select("sale_date")
    .distinct()
    .withColumn("date_key", date_format(col("sale_date"), "yyyyMMdd"))
    .withColumn("year", year(col("sale_date")))
    .withColumn("quarter", quarter(col("sale_date")))
    .withColumn("month", month(col("sale_date")))
)

df_product.write.saveAsTable("dim_product")
df_customer.write.saveAsTable("dim_customer")
df_date.write.saveAsTable("dim_date")

#Transform the transaction data into a fact table referencing these keys.

df_sales = (
    std_df
    .join(
        df_customer,
        ["customer_name", "region",  "segment"],
        "left"
    )
    .join(
        df_product,
        ["product_name"],
        "left"
    )
    .join(
        df_date,
        ["sale_date"],
        "left"
    )
    .select(
        "customer_key",
        "product_key",
        "date_key",
        "sale_id",
        "amount",
        "quantity"
    )
)

df_sales.write.saveAsTable("fact_sales")
