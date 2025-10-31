/*
Example Output Formatted

{
    "key": "VUEwMDAwMDAxMDczOTgwNTQ=",
    "offset": 219255030,
    "partition": 0,
    "timestamp": 1593880885085,
    "topic": "clickstream",
    "value": "eyJkZXZpY2UiOiJBbmRyb2lkIiwiZWNvbW1lcmNlIjp7fSwiZXZlbnRfbmFtZSI6Im1haW4iLCJldmVudF90aW1lc3R
    hbXAiOjE1OTM4ODA4ODUwMzYxMjksImdlbyI6eyJjaXR5IjoiTmV3IFlvcmsiLCJzdGF0ZSI6Ik5ZIn0sIml0ZW1zIjp
    bXSwidHJhZmZpY19zb3VyY2UiOiJnb29nbGUiLCJ1c2VyX2ZpcnN0X3RvdWNoX3RpbWVzdGFtcCI6MTU5Mzg4MDg4NTA
    zNjEyOSwidXNlcl9pZCI6IlVBMDAwMDAwMTA3Mzk4MDU0In0=",
}
*/

SELECT * 
FROM text.`/Volumes/dbacademy_ecommerce/v01/raw/events-kafka`
LIMIT 5;

CREATE TABLE kafka_events_bronze_raw AS
SELECT *
FROM read_files(
  "/Volumes/dbacademy_ecommerce/v01/raw/events-kafka",
  format => "json"
);


-- Display the table
SELECT *
FROM kafka_events_bronze_raw
LIMIT 10;

-- Decoding base64 Strings for the Bronze Table
SELECT
  key AS encoded_key,
  unbase64(key) AS decoded_key,
  value AS encoded_value,
  unbase64(value) AS decoded_value
FROM kafka_events_bronze_raw
LIMIT 5;

SELECT
  key AS encoded_key,
  cast(unbase64(key) AS STRING) AS decoded_key,
  value AS encoded_value,
  cast(unbase64(value) AS STRING) AS decoded_value
FROM kafka_events_bronze_raw
LIMIT 5;


SELECT
  key AS encoded_key,
  cast(unbase64(key) AS STRING) AS decoded_key,
  value AS encoded_value,
  cast(unbase64(value) AS STRING) AS decoded_value
FROM kafka_events_bronze_raw
LIMIT 5;

/*
Example JSON string pulled from a row in the column decoded_value:
{
    "device": "iOS",
    "ecommerce": {},
    "event_name": "add_item",
    "event_previous_timestamp": 1593880300696751,
    "event_timestamp": 1593880892251310,
    "geo": {
      "city": "Westbrook", 
      "state": "ME"
      },
    "items": [
        {
            "item_id": "M_STAN_T",
            "item_name": "Standard Twin Mattress",
            "item_revenue_in_usd": 595.0,
            "price_in_usd": 595.0,
            "quantity": 1,
        }
    ],
    "traffic_source": "google",
    "user_first_touch_timestamp": 1593880300696751,
    "user_id": "UA000000107392458",
}
*/

-- Flattening JSON String Columns
CREATE OR REPLACE TABLE kafka_events_bronze_string_flattened AS
SELECT
  decoded_key,
  offset,
  partition,
  timestamp,
  topic,
  decoded_value:device,
  decoded_value:traffic_source,
  decoded_value:geo,       ----- Contains another JSON formatted string
  decoded_value:items      ----- Contains a nested-array of JSON formatted strings
FROM kafka_events_bronze_decoded;


-- Display the table
SELECT *
FROM kafka_events_bronze_string_flattened;

-- Flattening JSON Formatting Strings via STRUCT Conversion
SELECT schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}')
AS schema
;

--  The from_json() function parses a column containing a JSON-formatted string into a STRUCT type using the specified schema.
CREATE OR REPLACE TABLE kafka_events_bronze_struct AS
SELECT 
  * EXCEPT (decoded_value),
  from_json(
      decoded_value,    -- JSON formatted string column
      'STRUCT<device: STRING, ecommerce: STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name: STRING, event_previous_timestamp: BIGINT, event_timestamp: BIGINT, geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source: STRING, user_first_touch_timestamp: BIGINT, user_id: STRING>') 
  AS value -- Struct datatype
FROM kafka_events_bronze_decoded;


-- View the new table.
SELECT *
FROM kafka_events_bronze_struct
LIMIT 5;

-- Extract fields, nested fields, and nested arrays from STRUCT columns
SELECT 
  decoded_key,
  value.device as device,  -- <----- Field
  value.geo.city as city,  -- <----- Nested-field from geo field
  value.items as items,
  array_size(items) AS number_elements_in_array -- <----- Count the number of elements in the array column items
FROM kafka_events_bronze_struct
ORDER BY number_elements_in_array DESC;

-- Explode Arrays
CREATE OR REPLACE TABLE bronze_explode_array AS
SELECT
  decoded_key,
  array_size(value.items) AS number_elements_in_array,
  explode(value.items) AS item_in_array,
  value.items
FROM kafka_events_bronze_struct
ORDER BY number_elements_in_array DESC;


-- Display table
SELECT *
FROM bronze_explode_array;

-- Working with a VARIANT column
-- The parse_json function returns a VARIANT value from the JSON formatted string.
CREATE OR REPLACE TABLE kafka_events_bronze_variant AS
SELECT
  decoded_key,
  offset,
  partition,
  timestamp,
  topic,
  parse_json(decoded_value) AS json_variant_value   -- Convert the decoded_value column to a variant data type
FROM kafka_events_bronze_decoded;

-- View the table
SELECT *
FROM kafka_events_bronze_variant
LIMIT 5;

SELECT
  json_variant_value,
  json_variant_value:device :: STRING,  -- Obtain the value of device and cast to a string
  json_variant_value:items
FROM kafka_events_bronze_variant
LIMIT 10;
