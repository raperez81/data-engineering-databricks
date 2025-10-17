-- Change the default catalog/schema
USE CATALOG dbacademy;
USE SCHEMA IDENTIFIER(DA.schema_name);

-- 1. Create table (CTAS)
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

-- 2. Upload UI

-- 3. Copy into
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

-- 4. Auto Loader
