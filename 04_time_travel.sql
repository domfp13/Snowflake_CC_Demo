USE TALENDTEST;
USE WAREHOUSE TALEND_XS;

/***** 1.IF NOT SPECIFIED: The standard retention period is 1 day (24 hours) and is automatically 
         enabled for all Snowflake accounts 
*****/
CREATE OR REPLACE TEMPORARY TABLE TEST_NO_EXPLICIT_TIMETRAVEL (
    row_id NUMBER(38,0) IDENTITY (1,1) NOT NULL,
    user_name VARCHAR(50) NOT NULL,
    insert_date DATE DEFAULT CURRENT_DATE() NOT NULL
)

BEGIN;
 INSERT INTO TEST_NO_EXPLICIT_TIMETRAVEL (user_name) VALUES ('Luis'),('Barry'),('Shradha');
COMMIT;

SELECT * FROM TEST_NO_EXPLICIT_TIMETRAVEL;

BEGIN;
 INSERT INTO TEST_NO_EXPLICIT_TIMETRAVEL (user_name) VALUES ('Other1'),('Other2'),('Other3');
COMMIT;

SHOW TABLES HISTORY;

SELECT * FROM TEST_NO_EXPLICIT_TIMETRAVEL AT(OFFSET => -60*0.8)

/***** 2.Time Travel allows to drop objects and restore them back again. 
         Restore dropped objects with no retention period only persists for the 
         length of the session in which the object was dropped.
*****/
DROP TABLE TEST_NO_EXPLICIT_TIMETRAVEL;
SELECT * FROM TEST_NO_EXPLICIT_TIMETRAVEL;
UNDROP TABLE TEST_NO_EXPLICIT_TIMETRAVEL;

/***** 3.Specifying the Data Retention Period for an Object, explicit key word DATA_RETENTION_TIME_IN_DAYS up to 90 
         with Enterprise edition +. TRASIENT tables are the exeption since there is no Fail-Safe for them
*****/
CREATE OR REPLACE TABLE TEST_EXPLICIT_TIMETRAVEL (
    row_id NUMBER(38,0) IDENTITY (1,1) NOT NULL,
    user_name VARCHAR(50) NOT NULL,
    insert_date DATE DEFAULT CURRENT_DATE() NOT NULL
) DATA_RETENTION_TIME_IN_DAYS=3; 

DROP TABLE TEST_EXPLICIT_TIMETRAVEL;

CREATE OR REPLACE TEMPORARY TABLE TEST_EXPLICIT_TIMETRAVEL (row_id NUMBER(38,0) IDENTITY (1,1) NOT NULL) DATA_RETENTION_TIME_IN_DAYS=3;
CREATE OR REPLACE TRANSIENT TABLE TEST_EXPLICIT_TIMETRAVEL (row_id NUMBER(38,0) IDENTITY (1,1) NOT NULL) DATA_RETENTION_TIME_IN_DAYS=3;

/***** 4. Creating new table from an specific point in time, If you create or replace 
          the table you are time traveling you will drop hidden object id in backend. 
          Doing this snowflake will loose its pointers to metadata of that table.
*****/
CREATE OR REPLACE TABLE TEST_NO_EXPLICIT_TIMETRAVEL_2
SELECT * 
FROM TEST_NO_EXPLICIT_TIMETRAVEL AT(OFFSET => -60*);

SELECT * FROM TEST_NO_EXPLICIT_TIMETRAVEL_2;

/***** 5. Understanding retention period
*****/

SHOW TABLES LIKE 'TEST_EXPLICIT_TIMETRAVEL' IN TALENDTEST.PUBLIC;

SELECT * FROM TEST_EXPLICIT_TIMETRAVEL;

ALTER TABLE TEST_EXPLICIT_TIMETRAVEL SET DATA_RETENTION_TIME_IN_DAYS = 0;

DROP TABLE TEST_EXPLICIT_TIMETRAVEL;
UNDROP TABLE TEST_EXPLICIT_TIMETRAVEL;

/***** 6. Restored db based on transaction
*****/
RESTORED_DB CLONE MY_DB BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');

/***** 7. Create schema based on another schema with time travel.
*****/
create schema restored_schema clone my_schema at(offset => -3600);

/***** 8. Sintax for time travel
*****/

--creates a clone of a table as of the date and time represented by the specified timestamp:
CREATE TABLE RESTORED_TABLE CLONE MY_TABLE
  AT(TIMESTAMP => 'MON, 09 MAY 2015 01:01:00 +0300'::TIMESTAMP_TZ);

--The following CREATE SCHEMA command creates a clone of a schema and all its 
--objects as they existed 1 hour before the current time:
CREATE SCHEMA RESTORED_SCHEMA CLONE MY_SCHEMA AT(OFFSET => -3600);

 --The following CREATE DATABASE command creates a clone of a database and 
 --all its objects as they existed prior to the completion of the specified statement:
CREATE DATABASE RESTORED_DB CLONE MY_DB
  BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');