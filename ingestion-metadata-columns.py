%python

from pyspark.sql.functions import col, from_unixtime, current_timestamp
from pyspark.sql.types import DateType

# 1. Read parquet files in cloud storage into a Spark DataFrame
df = (spark
      .read
      .format("parquet")
      .load("/Volumes/dbacademy_ecommerce/v01/raw/users-historical")
    )


# 2. Add metadata columns
df_with_metadata = (
    df.withColumn("first_touch_date", from_unixtime(col("user_first_touch_timestamp") / 1_000_000).cast(DateType()))
      .withColumn("file_modification_time", col("_metadata.file_modification_time"))
      .withColumn("source_file", col("_metadata.file_name"))
      .withColumn("ingestion_time", current_timestamp())
)


# 3. Save as a Delta table
(df_with_metadata
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"dbacademy.{DA.schema_name}.historical_users_bronze_python_metadata")
)


# 4. Read and display the table
historical_users_bronze_python_metadata = spark.table(f"dbacademy.{DA.schema_name}.historical_users_bronze_python_metadata")

display(historical_users_bronze_python_metadata)
