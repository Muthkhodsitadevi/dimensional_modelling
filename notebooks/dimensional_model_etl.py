# Use these two lines if not running in Databricks
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("DataModellingAndETL").getOrCreate()

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from dq_checks import *
from delta.tables import DeltaTable

# Read source file
df_bronze_updates = spark.read.csv("/data/SampleData_Updates.csv", header = True)

# Perform DQ checkss
clean_updates, invalid_updates = data_quality_check(df_bronze_updates)

# Standardize valid records
std_df = clean_updates.withColumn("amount", col("amount").cast("double")) \
                      .withColumn("quantity", col("quantity").cast("int")) \
                      .withColumn("sale_date", to_date(col("sale_date"), "M/d/yyyy"))



# Read tables
product_delta = DeltaTable.forName(spark, "dim_product")
customer_delta = DeltaTable.forName(spark, "dim_customer")
df_date = spark.table("dim_date")

#Process incremental data, updating fact and dimension tables.

#---------------------------------------
# Product table ------------------------
# get distinct new products
new_products = (std_df .select("product_name").distinct())

#write to dimension table product
product_delta.alias("target")
    .merge(
    new_products.alias("source"),
    'target.product_name = source.product_name'
    )
    .whenNotMatchedInsert(
        values={
            "product_name":
                "source.product_name"
        }
    )
    .execute()
)

#---------------------------------------
# Customer table -----------------------
customer_updates = (
    std_df
    .select(
        "customer_name",
        "region",
        "segment"
    )
    .distinct()
)

#write to dimension table customer
customer_delta.alias("target")
    .merge(
    customer_updates.alias("source"),
    "target.customer_name = source.customer_name AND target.end_date = '9999-12-31'"
).whenMatchedUpdate(
    condition="target.region <> source.region OR target.segment <> source.segment",
    set={
        "end_date": current_date()
    }
).execute()

# insert new customers and scd2 latest version records
changed_customers = (
    customer_updates.alias("source")
    .join(
        spark.table("dim_customer")
        .filter(col("is_current") == True)
        .alias("target"),
        "customer_name",
        "left"
    )
    .filter(
        col("target.customer_name").isNull()
        |
        (col("source.region")
            != col("target.region"))
        |
        (col("source.segment")
            != col("target.segment"))
    )
    .select(
        "source.customer_name",
        "source.region",
        "source.segment"
    )
)

changed_customers
    .withColumn(
        "start_date",
        current_date()
    )
    .withColumn(
        "end_date",
        to_date(
            lit("9999-12-31")
        )
    )
    .write
    .mode("append")
    .saveAsTable("dim_customer")
    
#---------------------------------------
# Fact sales table ---------------------

# Read target tables
df_customer = spark.table("dim_customer").filter(col("end_date") == '9999-12-31')
df_product = spark.table("dim_product")
df_date = spark.table("dim_date")

sales_delta = DeltaTable.forName(spark,"fact_sales")
df_sales_tgt = spark.table("fact_sales")

#get last record from target
max_sale_id = df_sales_tgt.agg(max("sale_id")).collect()[0][0]

#get latest records
incremental_df = std_df.filter(col("sale_id") > max_sale_id)

#Create intermediate df to merge into target

df_sales = (
    incremental_df
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

#Write to fact table sales

sales_delta.alias("target")
    .merge(
        df_sales.alias("source"),
        """
        target.sale_id =
        source.sale_id
        """
    )
    .whenNotMatchedInsertAll()
    .execute()
    
    
#----------------------

print( f"Valid Records: {clean_updates.count()}")

print(f"Invalid Records: {invalid_updates.count()}")

print(f"Fact Records Loaded: {df_sales.count()}")
