USE TALENDTEST;
USE WAREHOUSE TALEND_XS;

--1.- Creating two schemas LUIS_DATALAKE & LUIS_METADATA
CREATE OR REPLACE SCHEMA LUIS_DATALAKE
    WITH MANAGED ACCESS; --WITH MANAGED ACCESS centralize privilege management with the schema owner
    
CREATE OR REPLACE SCHEMA LUIS_METADATA
    WITH MANAGED ACCESS; --WITH MANAGED ACCESS centralize privilege management with the schema owner

USE SCHEMA LUIS_DATALAKE;

--2.- Creating a reference table (This will allow us to use generic jobs)
CREATE OR REPLACE TABLE JOB_MASTER_REFERENCE (
    id NUMBER(38,0) IDENTITY NOT NULL PRIMARY KEY,
    tables_name VARCHAR(50) NOT NULL,
    insert_date DATE DEFAULT CURRENT_DATE() NOT NULL
);

BEGIN TRANSACTION NAME t01;

    INSERT INTO JOB_MASTER_REFERENCE (tables_name) VALUES ('Server');
    INSERT INTO JOB_MASTER_REFERENCE (tables_name) VALUES ('Incident');
    
COMMIT;

SELECT A.id AS id, 
       A.tables_name AS tables_name,
       B.tables_name AS tables_name2,
       B.stages_name AS stages_name,
       B.files_format_name AS files_format_name,
       B.related_sp_name AS related_sp_name
FROM JOB_MASTER_REFERENCE AS A
INNER JOIN JOB_SNOWFLAKE_OBJECT_REFERENCE AS B
 ON A.id = B.job_master_reference_id

-- 3.- Creating job_master_loader in luis_metadata schema, this will hold metadata information about processes
USE SCHEMA LUIS_METADATA;

CREATE OR REPLACE TABLE JOB_MASTER_LOADER (
    id NUMBER(38,0) IDENTITY NOT NULL PRIMARY KEY,
    job_name VARCHAR(50) NOT NULL,
    t_name VARCHAR(50) NOT NULL,
    bucket_name VARCHAR(50) NOT NULL,
    bucket_source VARCHAR(50) NOT NULL,
    bucket_prefix VARCHAR(50) NOT NULL,
    file_name VARCHAR(50) NOT NULL,
    insert_date TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(9), --insert_date DATE DEFAULT CURRENT_DATE() NOT NULL
    end_date TIMESTAMP_NTZ(9)
);

-- This query will be used in Talend to update the current file that is being upload in the metadata table
UPDATE JOB_MASTER_LOADER 
SET end_date = CURRENT_TIMESTAMP(9) 
WHERE id = (
    SELECT id FROM JOB_MASTER_LOADER 
    WHERE insert_date = (SELECT MAX(insert_date) 
                         FROM JOB_MASTER_LOADER 
                         WHERE file_name = 'Server_202011061409.json'));

-- 4.- Creating table for landing zone
USE SCHEMA LUIS_DATALAKE;

-- creating file format
CREATE OR REPLACE FILE FORMAT FILE_T_SERVER_STG_S3 
    TYPE = 'JSON' 
    --COMPRESSION = 'AUTO ' 
    ENABLE_OCTAL = FALSE 
    ALLOW_DUPLICATE = TRUE 
    STRIP_OUTER_ARRAY = TRUE 
    STRIP_NULL_VALUES = TRUE 
    IGNORE_UTF8_ERRORS = FALSE;

SHOW FILE FORMATS;

-- creating stage
--REMOVE @T_JORDERS_STG;
CREATE OR REPLACE STAGE T_SERVER_STG_S3 url='s3://luis-s3-talend/'
  credentials=(aws_key_id='' aws_secret_key='')
  FILE_FORMAT = FILE_T_SERVER_STG_S3;

SHOW STAGES;

-- Creating Table with no fail safe
CREATE OR REPLACE TABLE T_SERVER_STG (
    V VARIANT,
    file_name VARCHAR(100) NOT NULL  
);

LIST @T_SERVER_STG_S3;

-- Loading data
BEGIN

  TRUNCATE TABLE T_SERVER_STG;

  COPY INTO T_SERVER_STG
    FROM @T_SERVER_STG_S3
    FILE_FORMAT = (format_name = FILE_T_SERVER_STG_S3)
    PATTERN = 'Server/2020/11/11/Server_202011061349.json'
    ON_ERROR = 'skip_file'
    PURGE = FALSE;
    
   --COPY INTO T_SERVER_STG(V, file_name)
    --FROM (SELECT t.$1, '<your_text_here>' FROM @T_SERVER_STG_S3 t)
    --PATTERN = 'Server/2020/11/11/Server_202011061349.json'
    --ON_ERROR = 'skip_file'
    --PURGE = FALSE;
       
COMMIT;

SELECT * FROM T_SERVER_STG;

-- 5.- Creating a Loading table reference table (This creates a data model)
USE SCHEMA LUIS_DATALAKE;

CREATE OR REPLACE TABLE JOB_SNOWFLAKE_OBJECT_REFERENCE (
    id NUMBER(38,0) IDENTITY NOT NULL PRIMARY KEY,
    tables_name VARCHAR(100) NOT NULL,
    stages_name VARCHAR(100) NOT NULL,
    files_format_name VARCHAR(100) NOT NULL,
    related_sp_name VARCHAR(100) NOT NULL,
    job_master_reference_id NUMBER(38,0) NOT NULL,
    insert_date DATE DEFAULT CURRENT_DATE() NOT NULL,
    constraint fkey_1 foreign key (job_master_reference_id) references JOB_MASTER_REFERENCE (id) not enforced
);

BEGIN TRANSACTION NAME t1;

    INSERT INTO JOB_SNOWFLAKE_OBJECT_REFERENCE (tables_name, stages_name, files_format_name, related_sp_name, job_master_reference_id) 
     VALUES ('T_SERVER_STG', 'T_SERVER_STG_S3', 'FILE_T_SERVER_STG_S3', 'SPW_T_SERVER', 1);
    
COMMIT;

SELECT * FROM JOB_SNOWFLAKE_OBJECT_REFERENCE;

-- 6.- Logic could go into a Stored Procedure
CREATE OR REPLACE TABLE T_SERVER AS 
SELECT substr(parse_json($1):Id, 0, LEN(parse_json($1):Id)) AS Id,
       substr(parse_json($1):Data_Type, 0, LEN(parse_json($1):Data_Type)) AS Data_Type,
       substr(parse_json($1):Reconciliation_Identity, 0, LEN(parse_json($1):Reconciliation_Identity)) AS Reconciliation_Identity,
       substr(parse_json($1):Class_ID, 0, LEN(parse_json($1):Class_ID)) AS Class_ID,
       substr(parse_json($1):Asset_ID, 0, LEN(parse_json($1):Asset_ID)) AS Asset_ID,
       substr(parse_json($1):Name, 0, LEN(parse_json($1):Name)) AS Name,
       substr(parse_json($1):Status, 0, LEN(parse_json($1):Status)) AS Status,
       substr(parse_json($1):Status_Reason, 0, LEN(parse_json($1):Status_Reason)) AS Status_Reason,
       substr(parse_json($1):Allocated_Cluster, 0, LEN(parse_json($1):Allocated_Cluster)) AS Allocated_Cluster,
       substr(parse_json($1):Owner_Cluster, 0, LEN(parse_json($1):Owner_Cluster)) AS Owner_Cluster,
       substr(parse_json($1):Owner_Company, 0, LEN(parse_json($1):Owner_Company)) AS Owner_Company,
       substr(parse_json($1):Product_Category_Tier_1, 0, LEN(parse_json($1):Product_Category_Tier_1)) AS Product_Category_Tier_1,
       substr(parse_json($1):Product_Category_Tier_2, 0, LEN(parse_json($1):Product_Category_Tier_2)) AS Product_Category_Tier_2,
       substr(parse_json($1):Product_Category_Tier_3, 0, LEN(parse_json($1):Product_Category_Tier_3)) AS Product_Category_Tier_3,
       substr(parse_json($1):Manufacturer, 0, LEN(parse_json($1):Manufacturer)) AS Manufacturer,
       substr(parse_json($1):Product_Name, 0, LEN(parse_json($1):Product_Name)) AS Product_Name,
       substr(parse_json($1):Product_Model_Version, 0, LEN(parse_json($1):Product_Model_Version)) AS Product_Model_Version,
       substr(parse_json($1):Site, 0, LEN(parse_json($1):Site)) AS Site,
       substr(parse_json($1):Environment_Information, 0, LEN(parse_json($1):Environment_Information)) AS Environment_Information,
       substr(parse_json($1):Operating_System, 0, LEN(parse_json($1):Operating_System)) AS Operating_System,
       substr(parse_json($1):Location, 0, LEN(parse_json($1):Location)) AS Location,
       substr(parse_json($1):Zone, 0, LEN(parse_json($1):Zone)) AS Zone,
       substr(parse_json($1):Server_Group, 0, LEN(parse_json($1):Server_Group)) AS Server_Group,
       substr(parse_json($1):VCentre, 0, LEN(parse_json($1):VCentre)) AS VCentre,
       substr(parse_json($1):CreatedDate, 0, LEN(parse_json($1):CreatedDate)) AS CreatedDate,
       substr(parse_json($1):UpdatedDate, 0, LEN(parse_json($1):UpdatedDate)) AS UpdatedDate,
       substr(parse_json($1):CreatedBy, 0, LEN(parse_json($1):CreatedBy)) AS CreatedBy,
       substr(parse_json($1):UpdatedBy, 0, LEN(parse_json($1):UpdatedBy)) AS UpdatedBy,
       substr(parse_json($1):SourceKey, 0, LEN(parse_json($1):SourceKey)) AS SourceKey,
       substr(parse_json($1):Source_Desc, 0, LEN(parse_json($1):Source_Desc)) AS Source_Desc,
       substr(parse_json($1):Customer_Desc, 0, LEN(parse_json($1):Customer_Desc)) AS Customer_Desc,
       substr(parse_json($1):Environment_Details, 0, LEN(parse_json($1):Environment_Details)) AS Environment_Details,
       substr(parse_json($1):Version, 0, LEN(parse_json($1):Version)) AS Version,
       substr(parse_json($1):IsManaged, 0, LEN(parse_json($1):IsManaged)) AS IsManaged,
       substr(parse_json($1):DataCenter, 0, LEN(parse_json($1):DataCenter)) AS DataCenter,
       substr(parse_json($1):ServiceType, 0, LEN(parse_json($1):ServiceType)) AS ServiceType,
       substr(parse_json($1):SerialNumber, 0, LEN(parse_json($1):SerialNumber)) AS SerialNumber
FROM T_SERVER_STG;

USE SCHEMA LUIS_METADATA;
TRUNCATE TABLE JOB_MASTER_LOADER;
SELECT * FROM JOB_MASTER_LOADER;

USE SCHEMA LUIS_DATALAKE;
  
TRUNCATE TABLE T_SERVER;
SELECT * FROM T_SERVER;

TRUNCATE TABLE T_SERVER_STG;
SELECT * FROM T_SERVER_STG;

