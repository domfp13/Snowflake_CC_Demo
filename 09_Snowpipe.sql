-- -*- coding: utf-8 -*-
-- Created by Luis Enrique Fuentes Plata

-- **************************************UP**************************************

USE ROLE ACCOUNTADMIN;
USE DATABASE CC_TEST;
CREATE OR REPLACE SCHEMA ENRIQUE_TEST;
USE SCHEMA ENRIQUE_TEST;
USE WAREHOUSE BI_WH_XS;

-- creating file format
CREATE OR REPLACE FILE FORMAT FILE_GENERIC_JSON
    TYPE = 'JSON'
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = TRUE
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = TRUE
    IGNORE_UTF8_ERRORS = FALSE;

SHOW FILE FORMATS;

CREATE OR REPLACE STAGE STAGE_SERVER
  url='s3://'
  credentials=(aws_key_id='' aws_secret_key='')
  FILE_FORMAT = FILE_GENERIC_JSON;

LIST @STAGE_SERVER;

--aws s3 cp Server s3://luis-s3-talend/server/ --recursive --exclude "*.jsp"

CREATE OR REPLACE TABLE T_SERVER_STG (
    V VARIANT,
    FILENAME VARCHAR(255) NOT NULL,
    INSERTED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

LIST @STAGE_SERVER;

COPY INTO T_SERVER_STG(V, FILENAME)
    FROM (SELECT t.$1, metadata$filename FROM @STAGE_SERVER t)
    FILE_FORMAT = (format_name = FILE_GENERIC_JSON)
    PATTERN = '.*.json'
    ON_ERROR = 'skip_file'
    PURGE = FALSE; -- What if I run this command again?

SELECT count(*) FROM T_SERVER_STG;

SELECT * FROM T_SERVER_STG;

CREATE OR REPLACE PIPE PIPE_T_SERVER_STG AUTO_INGEST=TRUE AS
COPY INTO T_SERVER_STG(V, FILENAME)
    FROM (SELECT t.$1, metadata$filename FROM @STAGE_SERVER t)
    FILE_FORMAT = (format_name = FILE_GENERIC_JSON)
    PATTERN = '.*.json'
    ON_ERROR = 'skip_file';

SHOW PIPES;

-- S3 event notification
-- ** Prefix - optional: server/
-- ** Suffix - optional: .json
-- All object create events: Put, Post

--aws s3 rm s3://luis-s3-talend/server/ --recursive --exclude "*.jsp"

--Lets look at one of the files

CREATE OR REPLACE TABLE T_SERVER
(
    ID                      NUMBER PRIMARY KEY,
    DATA_TYPE               VARCHAR(500),
    RECONCILIATION_IDENTITY VARCHAR(500),
    CLASS_ID                VARCHAR(500),
    ASSET_ID                VARCHAR(500),
    NAME                    VARCHAR(500),
    STATUS                  VARCHAR(500),
    STATUS_REASON           VARCHAR(500),
    ALLOCATED_CLUSTER       VARCHAR(500),
    OWNER_CLUSTER           VARCHAR(500),
    OWNER_COMPANY           VARCHAR(500),
    PRODUCT_CATEGORY_TIER_1 VARCHAR(500),
    PRODUCT_CATEGORY_TIER_2 VARCHAR(500),
    PRODUCT_CATEGORY_TIER_3 VARCHAR(500),
    MANUFACTURER            VARCHAR(500),
    PRODUCT_NAME            VARCHAR(500),
    PRODUCT_MODEL_VERSION   VARCHAR(500),
    SITE                    VARCHAR(500),
    ENVIRONMENT_INFORMATION VARCHAR(500),
    OPERATING_SYSTEM        VARCHAR(500),
    LOCATION                VARCHAR(500),
    ZONE                    VARCHAR(500),
    SERVER_GROUP            VARCHAR(500),
    VCENTRE                 VARCHAR(500),
    CREATEDDATE             VARCHAR(500),
    UPDATEDDATE             VARCHAR(500),
    CREATEDBY               VARCHAR(500),
    UPDATEDBY               VARCHAR(500),
    SOURCEKEY               VARCHAR(500),
    SOURCE_DESC             VARCHAR(500),
    CUSTOMER_DESC           VARCHAR(500),
    ENVIRONMENT_DETAILS     VARCHAR(500),
    VERSION                 VARCHAR(500),
    ISMANAGED               VARCHAR(500),
    DATACENTER              VARCHAR(500),
    SERVICETYPE             VARCHAR(500),
    SERIALNUMBER            VARCHAR(500),
    FILENAME                VARCHAR(500)
);

SELECT * FROM T_SERVER;

BEGIN TRANSACTION NAME t1;
    MERGE INTO T_SERVER AS TARGET
      USING (
        SELECT substr(parse_json($1):Id, 0, LEN(parse_json($1):Id))::INT AS ID,
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
           substr(parse_json($1):SerialNumber, 0, LEN(parse_json($1):SerialNumber)) AS SerialNumber,
           FILENAME
        FROM T_SERVER_STG
      ) AS SRC
      ON TARGET.ID = SRC.ID
      WHEN MATCHED THEN UPDATE SET
          TARGET.DATA_TYPE = SRC.DATA_TYPE,
          TARGET.RECONCILIATION_IDENTITY = SRC.RECONCILIATION_IDENTITY,
          TARGET.CLASS_ID = SRC.CLASS_ID,
          TARGET.ASSET_ID = SRC.ASSET_ID,
          TARGET.NAME = SRC.NAME,
          TARGET.STATUS = SRC.STATUS,
          TARGET.STATUS_REASON = SRC.STATUS_REASON,
          TARGET.ALLOCATED_CLUSTER = SRC.ALLOCATED_CLUSTER,
          TARGET.OWNER_CLUSTER = SRC.OWNER_CLUSTER,
          TARGET.OWNER_COMPANY = SRC.OWNER_COMPANY,
          TARGET.PRODUCT_CATEGORY_TIER_1 = SRC.PRODUCT_CATEGORY_TIER_1,
          TARGET.PRODUCT_CATEGORY_TIER_2 = SRC.PRODUCT_CATEGORY_TIER_2,
          TARGET.PRODUCT_CATEGORY_TIER_3 = SRC.PRODUCT_CATEGORY_TIER_3,
          TARGET.MANUFACTURER = SRC.MANUFACTURER,
          TARGET.PRODUCT_NAME = SRC.PRODUCT_NAME,
          TARGET.PRODUCT_MODEL_VERSION = SRC.PRODUCT_MODEL_VERSION,
          TARGET.SITE = SRC.SITE,
          TARGET.ENVIRONMENT_INFORMATION = SRC.ENVIRONMENT_INFORMATION,
          TARGET.OPERATING_SYSTEM = SRC.OPERATING_SYSTEM,
          TARGET.LOCATION = SRC.LOCATION,
          TARGET.ZONE = SRC.ZONE,
          TARGET.SERVER_GROUP = SRC.SERVER_GROUP,
          TARGET.VCENTRE = SRC.VCENTRE,
          TARGET.CREATEDDATE = SRC.CREATEDDATE,
          TARGET.UPDATEDDATE = SRC.UPDATEDDATE,
          TARGET.CREATEDBY = SRC.CREATEDBY,
          TARGET.UPDATEDBY = SRC.UPDATEDBY,
          TARGET.SOURCEKEY = SRC.SOURCEKEY,
          TARGET.SOURCE_DESC = SRC.SOURCE_DESC,
          TARGET.CUSTOMER_DESC = SRC.CUSTOMER_DESC,
          TARGET.ENVIRONMENT_DETAILS = SRC.ENVIRONMENT_DETAILS,
          TARGET.VERSION = SRC.VERSION,
          TARGET.ISMANAGED = SRC.ISMANAGED,
          TARGET.DATACENTER = SRC.DATACENTER,
          TARGET.SERVICETYPE = SRC.SERVICETYPE,
          TARGET.SERIALNUMBER = SRC.SERIALNUMBER,
          TARGET.FILENAME = SRC.FILENAME
      WHEN NOT MATCHED THEN INSERT (
            ID,
            DATA_TYPE,
            RECONCILIATION_IDENTITY,
            CLASS_ID,
            ASSET_ID,
            NAME,
            STATUS,
            STATUS_REASON,
            ALLOCATED_CLUSTER,
            OWNER_CLUSTER,
            OWNER_COMPANY,
            PRODUCT_CATEGORY_TIER_1,
            PRODUCT_CATEGORY_TIER_2,
            PRODUCT_CATEGORY_TIER_3,
            MANUFACTURER,
            PRODUCT_NAME,
            PRODUCT_MODEL_VERSION,
            SITE,
            ENVIRONMENT_INFORMATION,
            OPERATING_SYSTEM,
            LOCATION,
            ZONE,
            SERVER_GROUP,
            VCENTRE,
            CREATEDDATE,
            UPDATEDDATE,
            CREATEDBY,
            UPDATEDBY,
            SOURCEKEY,
            SOURCE_DESC,
            CUSTOMER_DESC,
            ENVIRONMENT_DETAILS,
            VERSION,
            ISMANAGED,
            DATACENTER,
            SERVICETYPE,
            SERIALNUMBER,
            FILENAME
          ) VALUES (
            SRC.ID,
            SRC.DATA_TYPE,
            SRC.RECONCILIATION_IDENTITY,
            SRC.CLASS_ID,
            SRC.ASSET_ID,
            SRC.NAME,
            SRC.STATUS,
            SRC.STATUS_REASON,
            SRC.ALLOCATED_CLUSTER,
            SRC.OWNER_CLUSTER,
            SRC.OWNER_COMPANY,
            SRC.PRODUCT_CATEGORY_TIER_1,
            SRC.PRODUCT_CATEGORY_TIER_2,
            SRC.PRODUCT_CATEGORY_TIER_3,
            SRC.MANUFACTURER,
            SRC.PRODUCT_NAME,
            SRC.PRODUCT_MODEL_VERSION,
            SRC.SITE,
            SRC.ENVIRONMENT_INFORMATION,
            SRC.OPERATING_SYSTEM,
            SRC.LOCATION,
            SRC.ZONE,
            SRC.SERVER_GROUP,
            SRC.VCENTRE,
            SRC.CREATEDDATE,
            SRC.UPDATEDDATE,
            SRC.CREATEDBY,
            SRC.UPDATEDBY,
            SRC.SOURCEKEY,
            SRC.SOURCE_DESC,
            SRC.CUSTOMER_DESC,
            SRC.ENVIRONMENT_DETAILS,
            SRC.VERSION,
            SRC.ISMANAGED,
            SRC.DATACENTER,
            SRC.SERVICETYPE,
            SRC.SERIALNUMBER,
            SRC.FILENAME
    );
COMMIT;

SELECT * FROM ENRIQUE_TEST.T_SERVER;

-- **************************************DOWN**************************************

DROP PIPE IF EXISTS PIPE_T_SERVER_STG;
DROP STAGE IF EXISTS T_SERVER_STG;
DROP FILE FORMAT IF EXISTS FILE_GENERIC_JSON;

DROP SCHEMA ENRIQUE_TEST;

