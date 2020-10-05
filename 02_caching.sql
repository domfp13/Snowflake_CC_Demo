/***** Case 1: Understanding Cloud Services Layer cache *****/

-- Create temporary table 
CREATE OR REPLACE TEMPORARY TABLE SUPPLIER AS
SELECT * 
FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."SUPPLIER";

-- Check execution plan
SELECT * FROM SUPPLIER; 

ALTER WAREHOUSE TALEND_XS SUSPEND;

SELECT * FROM SUPPLIER;

/***** Case 2: Understanding VWH cache *****/

-- Disabling the cloud services layer cache
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
SELECT * FROM SUPPLIER;
-- Rerun the query immediatly, data will be fetched from cache virtual warehouses
SELECT * FROM SUPPLIER;

ALTER WAREHOUSE TALEND_XS SUSPEND;

SELECT * FROM SUPPLIER;

/***** Case 3: Changing query *****/

-- Suspend warehouse execute
ALTER WAREHOUSE TALEND_XS SUSPEND;
SELECT * FROM SUPPLIER;
-- Run but different query
SELECT * FROM SUPPLIER LIMIT 1000;

/***** Case 4: What happens when we delete a record? *****/

ALTER SESSION SET USE_CACHED_RESULT = TRUE;

SELECT * FROM SUPPLIER;

DELETE FROM SUPPLIER
WHERE S_SUPPKEY = 716639;
SELECT * FROM SUPPLIER;

SELECT * FROM SUPPLIER;

/***** More info about VWH *****/
SELECT * FROM TABLE(information_schema.warehouse_load_history(date_range_start=>dateadd('hour',-1,current_timestamp())));

SELECT * FROM TABLE(information_schema.warehouse_metering_history(dateadd('sec',-500,current_date()),current_date()));

SELECT * FROM TABLE(information_schema.warehouse_metering_history(current_date()));