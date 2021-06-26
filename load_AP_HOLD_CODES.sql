-- -*- coding: utf-8 -*-
-- Created by Luis Fuentes & Shradha

USE ROLE ETL_ROLE;
USE DATABASE EBS;
USE SCHEMA EBS_ARCHIVE;
USE WAREHOUSE BI_WH_XS;

-- 1.- Creating Internal Stage
CREATE OR REPLACE STAGE STAGE_AP_HOLD_CODES
  FILE_FORMAT = TEST_CSV;

SHOW STAGES;

-- 2.- Load files to stage, It does not work with the UI, use snowsql, etc.
PUT file:////Users/enriquep/Downloads/AP_HOLD_CODES.csv @STAGE_AP_HOLD_CODES;

LIST @STAGE_AP_HOLD_CODES;

-- 3.- Loading and transforming timestamp at load time
COPY INTO AP_HOLD_CODES
  FROM (select t.$1 as HOLD_LOOKUP_CODE,
        t.$2 as HOLD_TYPE,
        t.$3 as DESCRIPTION,
        to_date(t.$4, 'dd-mon-yy HH:mi:ss')::TIMESTAMP_NTZ as LAST_UPDATE_DATE,
        t.$5 as LAST_UPDATED_BY,
        t.$6 as USER_RELEASEABLE_FLAG,
        t.$7 as USER_UPDATEABLE_FLAG,
        to_date(t.$8, 'dd-mon-yy HH:mi:ss')::TIMESTAMP_NTZ as INACTIVE_DATE,
        t.$9 as POSTABLE_FLAG,
        t.$10 as LAST_UPDATE_LOGIN,
        to_date(t.$11, 'dd-mon-yy HH:mi:ss')::TIMESTAMP_NTZ as CREATION_DATE,
        t.$12 as CREATED_BY,
        t.$13 as EXTERNAL_DESCRIPTION,
        t.$14 as HOLD_INSTRUCTION,
        t.$15 as WAIT_BEFORE_NOTIFY_DAYS,
        t.$16 as REMINDER_DAYS,
        t.$17 as INITIATE_WORKFLOW_FLAG
from @STAGE_AP_HOLD_CODES (file_format => 'TEST_CSV', PATTERN => '.*.gz') t)
  FILE_FORMAT = (format_name = TEST_CSV)
  PATTERN = '.*.gz'
  ON_ERROR = 'skip_file'
  PURGE = FALSE;
