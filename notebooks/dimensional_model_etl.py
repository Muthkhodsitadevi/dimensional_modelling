# Use these two lines if not running in Databricks
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("DataModellingAndETL").getOrCreate()


from pyspark.sql.functions import *
from pyspark.sql.window import Window

df_bronze_base = spark.read.csv("/data/SampleData_Base.csv", header = True)
df_bronze_updates = spark.read.csv("/data/SampleData_Updates.csv", header = True)

# Write PySpark code to detect and report data quality issues.
def data_quality_check(df):
	"""Checks for invalid records in input dataframe: find any duplicates, nulls, datatype mismatch. 
	Returns invalid_records as a dataframe."""
	
	# find missing customers
	missing_customers = df.filter(
    col("customer_name").isNull()
	)
	
	# find invalid amounts
	invalid_amounts = df.filter(
		col("amount") <= 0
	)
	
	# find invalid quantities
	invalid_quantities = df.filter(
		col("quantity") <= 0
	)
	
	# find duplicate sales
	duplicate_sales = (
		df
		.groupBy("sale_id")
		.count()
		.filter(col("count") > 1)
	)
	
	# find invalid dates
	invalid_dates = df.filter(
		to_date(col("sale_date"), "M/d/yyyy").isNull()
	)
	
	# combine all invalid records with a column "check_failed"
	
	
	return invalid_records
	
clean_base = data_quality_check(df_bronze_base)

#Describe how you would handle invalid or missing data in your ETL pipeline.
Load valid records and write the invalid_records to a file invalid.csv and send as email attachment to the client or concerned team. 

# Standardize valid records
std_df = df_base.withColumn("amount", F.col("amount").cast("double")) \
                         .withColumn("quantity", F.col("quantity").cast("int")) \
                         .withColumn("sale_date", F.to_date(F.col("sale_date"), "M/d/yyyy"))

# Extract unique customers and products from the raw data.
df_customer = df.select("customer_name","region","segment").distinct()
df_product = df.select("product_name").distinct()

# Build dimension tables with surrogate keys.

df_customer = df_customer.withColumn("customer_id",rowNumber().over(Window.orderBy("customer_name")) #use hash for parallelism
df_product = df_product.withColumn("product_id",rowNumber().over(Window.orderBy("product_name"))

spark.table("customer")
spark.table("product")

#Transform the transaction data into a fact table referencing these keys.
spark.table("sales")


#Process incremental data, updating fact and dimension tables as needed.


#Identify potential data quality issues in the sample data.
# nulls in first name and quantity columns, 
# datatype mismatch for amount column, does negative value reflect a refund or is it a bad record?
