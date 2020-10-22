-- -*- coding: utf-8 -*-
-- Created by Luis Enrique Fuentes Plata

/***** 1. Creating the table Nation that is comming from the share snowflake_sample_data
       in the TALENDTEST database
*****/
USE TALENDTEST;
USE WAREHOUSE TALEND_XS;

CREATE OR REPLACE TRANSIENT TABLE NATION 
AS SELECT * FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."NATION";

SELECT * FROM "TALENDTEST"."PUBLIC"."NATION";

/***** 2. Cloning the Nation table into another database
*****/
CREATE DATABASE TALENDTEST_DEV;

USE TALENDTEST_DEV;
USE WAREHOUSE TALEND_XS;

CREATE OR REPLACE TRANSIENT TABLE NATION_CLONE CLONE "TALENDTEST"."PUBLIC"."NATION";

SELECT * FROM "TALENDTEST_DEV"."PUBLIC"."NATION_CLONE";

/***** 3. Changing data: What happends when we change the data on any of the tables?
*****/

SELECT * FROM "TALENDTEST_DEV"."PUBLIC"."NATION_CLONE" WHERE TRIM(LOWER(n_name)) LIKE '%mexico%';

DESC TABLE NATION_CLONE;

INSERT INTO "TALENDTEST_DEV"."PUBLIC"."NATION_CLONE" (n_nationkey, n_name) VALUES ('25','MEXICO'); 

SELECT * FROM "TALENDTEST_DEV"."PUBLIC"."NATION_CLONE";

--Now lets try to select the other table

SELECT * FROM "TALENDTEST"."PUBLIC"."NATION" WHERE TRIM(LOWER(n_name)) LIKE '%mexico%';

/***** 4. Checking a table is a clone of another table
*****/
SELECT * FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS WHERE TABLE_NAME LIKE 'NATION'
AND TABLE_CATALOG='TALENDTEST' AND TABLE_DROPPED IS NULL
UNION ALL
SELECT * FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS WHERE TABLE_NAME LIKE 'NATION_CLONE'
AND TABLE_CATALOG='TALENDTEST_DEV' AND TABLE_DROPPED IS NULL

/***** 5. Promoting changes
*****/

SELECT * FROM "TALENDTEST_DEV"."PUBLIC"."NATION_CLONE";

SELECT * FROM "TALENDTEST"."PUBLIC"."NATION";

ALTER TABLE "TALENDTEST"."PUBLIC"."NATION" SWAP WITH "TALENDTEST_DEV"."PUBLIC"."NATION_CLONE";

/***** 6. It is posible to clone Databases, Schemas, Tables, Streams and Others
*****/
CREATE [ OR REPLACE ] { DATABASE | SCHEMA | TABLE | STREAM } [ IF NOT EXISTS ] <object_name>
  CLONE <source_object_name>
        [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> } ) ]

CREATE [ OR REPLACE ] { STAGE | FILE FORMAT | SEQUENCE | TASK } [ IF NOT EXISTS ] <object_name>
  CLONE <source_object_name>
