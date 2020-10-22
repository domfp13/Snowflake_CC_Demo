-- -*- coding: utf-8 -*-
-- Created by Luis Enrique Fuentes Plata

USE TALENDTEST;
USE WAREHOUSE TALEND_XS;

/***** 1. Creating share TALENDTEST_SHARE in which we will add the customer_clustered table
*****/
CREATE SHARE TALENDTEST_SHARE;
GRANT USAGE ON DATABASE TALENDTEST TO SHARE TALENDTEST_SHARE;
GRANT USAGE ON SCHEMA TALENDTEST.PUBLIC TO SHARE TALENDTEST_SHARE;
GRANT SELECT ON TABLE TALENDTEST.PUBLIC.CUSTOMER_CLUSTERED TO SHARE TALENDTEST_SHARE;

SHOW GRANTS TO SHARE TALENDTEST_SHARE;

ALTER SHARE TALENDTEST_SHARE ADD ACCOUNTS=xsa52638;

DESC SHARE TALENDTEST_SHARE;

/***** 2. Adding another table to TALENDTEST_SHARE
*****/
-- Sharing the table will be instantly available to the other account -> See the other account
GRANT SELECT ON TABLE TALENDTEST.PUBLIC.EMPLOYEE TO SHARE TALENDTEST_SHARE;

-- Altering the table -> See the other account
BEGIN TRANSACTION NAME t1;
    INSERT INTO TALENDTEST.PUBLIC.EMPLOYEE (NAME, LAST_NAME, COUNTRY, INSERT_DT) 
    VALUES ('Fernando','Garcia', 'USA', current_date());
COMMIT;

-- What happens when we try to grant DML operations to the object
GRANT ALL ON TABLE TALENDTEST.PUBLIC.EMPLOYEE TO SHARE TALENDTEST_SHARE;

/***** 3. Working with Views and Secure views: Suppose we want to share data to the consumer 
       account but we do not want to expose all the fields from a table. 
*****/

DESC TABLE "TALENDTEST"."PUBLIC"."CUSTOMER_CLUSTERED"
 
CREATE OR REPLACE VIEW "TALENDTEST"."PUBLIC"."CUSTOMER_DATA"
AS 
SELECT C_NAME, C_MKTSEGMENT, C_ACCTBAL
FROM "TALENDTEST"."PUBLIC"."CUSTOMER_CLUSTERED";

--What happens when we run the following command?
GRANT SELECT ON VIEW "TALENDTEST"."PUBLIC"."CUSTOMER_DATA" TO
SHARE TALENDTEST_SHARE;

--Recreating the view now as a SECURE VIEW
CREATE OR REPLACE SECURE VIEW "TALENDTEST"."PUBLIC"."CUSTOMER_DATA"
AS SELECT C_NAME, C_MKTSEGMENT, C_ACCTBAL
FROM "TALENDTEST"."PUBLIC"."CUSTOMER_CLUSTERED";

GRANT SELECT ON VIEW "TALENDTEST"."PUBLIC"."CUSTOMER_DATA" TO SHARE TALENDTEST_SHARE;

/***** 4. Understanding why a normal view cannot be add to a share  
*****/

CREATE OR REPLACE SECURE VIEW "TALENDTEST"."PUBLIC"."CUSTOMER_DATA_SECURE"
AS SELECT c_name, c_mktsegment, c_acctbal
FROM "TALENDTEST"."PUBLIC"."CUSTOMER_CLUSTERED"
WHERE c_mktsegment='AUTOMOBILE';

CREATE OR REPLACE VIEW "TALENDTEST"."PUBLIC"."CUSTOMER_DATA_NORMAL"
AS SELECT c_name, c_mktsegment, c_acctbal
FROM "TALENDTEST"."PUBLIC"."CUSTOMER_CLUSTERED"
WHERE c_mktsegment='AUTOMOBILE';

SHOW VIEWS LIKE  '%CUST%'

GRANT USAGE ON DATABASE TALENDTEST TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA TALENDTEST.PUBLIC TO ROLE PUBLIC;

GRANT SELECT ON "TALENDTEST"."PUBLIC"."CUSTOMER_DATA_SECURE" TO ROLE PUBLIC;
GRANT SELECT ON "TALENDTEST"."PUBLIC"."CUSTOMER_DATA_NORMAL" TO ROLE PUBLIC;

-- Run this with a PUBLIC profile and check the execution plan
-- The execution plan will expose the filter condition in the view, therefore a user can write a complex UDF and get to knwo the view statement
SELECT * 
FROM "TALENDTEST"."PUBLIC"."CUSTOMER_DATA_NORMAL"
WHERE 1/IFF(c_mktsegment = 'HOUSEHOLD',1,0)=1;

-- Run this with a PUBLIC profile and check the execution plan
SELECT * 
FROM "TALENDTEST"."PUBLIC"."CUSTOMER_DATA_SECURE"
WHERE 1/IFF(c_mktsegment = 'HOUSEHOLD',1,0)=1;

/***** 5. Understanding why a normal view cannot be add to a share  
*****/

DROP SHARE TALENDTEST_SHARE;