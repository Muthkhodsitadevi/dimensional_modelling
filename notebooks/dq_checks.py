# PySpark code to detect data quality issues.
def data_quality_check(df):
	"""Checks for invalid records in input dataframe: find any duplicates, nulls, datatype mismatch. 
	Returns invalid_records as a dataframe."""
	
	validation_df = (
        df.withColumn("check_failed",
        
            when(col("customer_name").isNull() |
                 (trim(col("customer_name")) == ""),
                 "Missing Customer")
            
            #datatype check
            .when(col("amount").cast("double").isNull() & col("amount").isNotNull(),
                  "Invalid Amount Datatype")

            .when(col("quantity").cast("int").isNull() & col("quantity").isNotNull(),
                  "Invalid Quantity Datatype")

            .when(col("sale_id").cast("int").isNull() & col("sale_id").isNotNull(),
                  "Invalid Sale ID Datatype")
            
            #value checks
            .when(to_date(col("sale_date"), "M/d/yyyy").isNull(),
                  "Invalid Date")

            .when(col("amount").cast("double") <= 0,
                  "Invalid Amount")

            .when(col("quantity").cast("int") <= 0,
                  "Invalid Quantity")
        )
    )

	
	# find duplicate sales
    duplicate_ids = (
        validation_df
        .groupBy("sale_id")
        .count()
        .filter(col("count") > 1)
        .select("sale_id")
    )
	
    #populate the check_failed column for duplicate records if they have not failed other checks
    validation_df = (
        validation_df.alias("v")
        .join(
            duplicate_ids.alias("d"), "sale_id", "left"
        )
        .withColumn(
            "check_failed",
            when(
                col("d.sale_id").isNotNull() & col("check_failed").isNull(),
                "Duplicate Sale ID"
            ).otherwise(col("check_failed"))
        )
    )
	
	#get invalid records and valid records in separate dataframes
    invalid_records = validated_df.filter(
        col("check_failed").isNotNull()
    )

    valid_records = validated_df.filter(
        col("check_failed").isNull()
    ).drop("check_failed")

    return valid_records, invalid_records
