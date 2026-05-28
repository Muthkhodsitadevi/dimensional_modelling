-----------------ASSUMPTIONS---------------------
-We received Base dataset on day0 and Updates dataset from next day onwards.
-sale_id is unique.
-customer_name is unique.
-product_name is unique. 
-segment is a tier of a customer since it stays consistent with customer name.
-We need to maintain historical updates.
-We are using Databricks.
-Considering this small dataset, segment and region columns are a part of customer table. For larger datasets, dim_region and dim_segment tables can be creaeted separately.

-----------Doubts---------------
Unit price is not uniform when derived as amount/quantity.
