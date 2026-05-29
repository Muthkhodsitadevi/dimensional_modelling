# dimensional_modelling
dimensional modelling for sales data

# Explain your approach and any assumptions made.
-Approach:
1. Create Customer and product tables as dimension tables
2. Use row nnumber as surrogate key
3. Considering base data for day0 load, created one-time base_etl.py for the same
4. Created dimensional_model_etl.py for daily incremental load
5. Performed DQ checks, deduplicate records and quarantine invalid records in a separate file
6. Since there is no timestamp column in source file, used latest processed sale_id for incremental load assuming it is unique
7. New products are inserted into Product table
8. SCD2 implemented for Customer table
9. Sales table is updated using merge

-Assumptions:
1. We received Base dataset on day0 and Updates dataset from next day onwards
2. sale_id is unique. Hence used for incremental load
3. customer_name is unique.
4. product_name is unique.
5. segment is a tier of a customer since it stays consistent with customer name.
6. We need to maintain historical updates for customer
7. We are using Databricks.
8. Considering this small dataset, segment and region columns are a part of customer table. For larger datasets, dim_region and dim_segment tables can be creaeted separately.

# Identify potential data quality issues in the sample data.
There were several data quality issues found listed as below:
 -customer_name: null value
 -quantity: null and zero values
 -amount: string and negative values (if negative values are refunds, need to handle differently)

# Describe how you would handle invalid or missing data in your ETL pipeline.
Load valid records and write the invalid_records to a file invalid.csv and send as email attachment to the client or concerned team. 
