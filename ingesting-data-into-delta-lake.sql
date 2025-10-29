-- Change the default catalog/schema
USE CATALOG dbacademy;
USE SCHEMA IDENTIFIER(DA.schema_name);

-- 1. Create table (CTAS)
-- It processes all records each time it runs.
-- read_files documentation: https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files
DROP TABLE IF EXISTS current_employees_ctas;

CREATE TABLE current_employees_ctas
AS
SELECT ID, FirstName, Country, Role
FROM read_files(
  'Volumes/dbacademy/' || DA.schema_name || '/myfiles/',
  format => 'csv',
  header => true,
  inferSchema => true
);

%python
# 1. Read the Parquet files from the volume into a Spark DataFrame
df = (spark
      .read
      .format("parquet")
      .load("/Volumes/dbacademy_ecommerce/v01/raw/users-historical")
    )

# 2. Write to the DataFrame to a Delta table (overwrite the table if it exists)
(df
 .write
 .mode("overwrite")
 .saveAsTable(f"dbacademy.{DA.schema_name}.historical_users_bronze_python")
)

## 3. Read and view the table
users_bronze_table = spark.table(f"dbacademy.{DA.schema_name}.historical_users_bronze_python")
users_bronze_table.display()

  
-- 2. Upload UI

-- 3. Copy into
-- Incremental batch ingestion by default.
DROP TABLE IF EXISTS current_employees_copyinto;

CREATE TABLE current_employees_copyinto (
  ID INT,
  FirstName STRING,
  Country STRING,
  Role STRING
);

spark.sql(f'''
COPY INTO current_employees_copyinto
  FROM 'Volumes/dbacademy/{DA.schema_name}/myfiles/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true'
  )
  ''').display()

DESCRIBE HISTORY current_employees_copyinto;

-- Example 2: Common Schema Mismatch Error
-- Create an empty table with the specified table schema (only 2 out of the 3 columns)
CREATE TABLE historical_users_bronze_ci (
  user_id STRING,
  user_first_touch_timestamp BIGINT
);

COPY INTO historical_users_bronze_ci
  FROM '/Volumes/dbacademy_ecommerce/v01/raw/users-historical'
  FILEFORMAT = parquet
  COPY_OPTIONS ('mergeSchema' = 'true');     -- Merge the schema of each file

-- Example 3: Preemptively Handling Schema Evolution
-- Another way to ingest the same files into a Delta table is to start by creating an empty table named historical_users_bronze_ci_no_schema.
-- Then, add the COPY_OPTIONS ('mergeSchema' = 'true') option to enable schema evolution for the table.

CREATE TABLE historical_users_bronze_ci_no_schema;

-- Use COPY INTO to populate Delta table
COPY INTO historical_users_bronze_ci_no_schema
  FROM '/Volumes/dbacademy_ecommerce/v01/raw/users-historical'
  FILEFORMAT = parquet
  COPY_OPTIONS ('mergeSchema' = 'true');


-- 4. Auto Loader
-- Use streaming tables in Databricks SQL https://docs.databricks.com/aws/en/ldp/dbsql/streaming#gsc.tab=0
-- What is Auto Loader? https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/#gsc.tab=0
-- When you create a streaming table using the CREATE OR REFRESH STREAMING TABLE statement, the initial data refresh and 
-- population begin immediately. These operations do not consume DBSQL warehouse compute. Instead, streaming table rely 
-- on serverless DLT for both creation and refresh. A dedicated serverless DLT pipeline is automatically created and managed 
-- by the system for each streaming table.
CREATE OR REFRESH STREAMING TABLE sql_csv_autoloader
SCHEDULE EVERY 1 WEEK     -- Scheduling the refresh is optional
AS
SELECT *
FROM STREAM read_files(
  '/Volumes/dbacademy/your-labuser-name/csv_files_autoloader_source',  -- Insert the path to you csv_files_autoloader_source volume (example shown)
  format => 'CSV',
  sep => '|',
  header => true
);

REFRESH STREAMING TABLE sql_csv_autoloader;
