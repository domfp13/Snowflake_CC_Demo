USE TALENDTEST;
USE WAREHOUSE TALEND_XS;
USE SCHEMA LUIS_DATALAKE;

-- 1.- SPW_T_SERVER

CREATE OR REPLACE PROCEDURE SPW_T_SERVER(FILENAME STRING)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS     
$$
    try{

    // 01- Setting up operation variables
    var cmd, stmt, result_set1, result, file_name
    
    file_name = FILENAME
      
    // 04- Inserting data
    cmd = "INSERT INTO T_SERVER\
           SELECT substr(parse_json($1):Id, 0, LEN(parse_json($1):Id)) AS Id,\
                  substr(parse_json($1):Data_Type, 0, LEN(parse_json($1):Data_Type)) AS Data_Type,\
                  substr(parse_json($1):Reconciliation_Identity, 0, LEN(parse_json($1):Reconciliation_Identity)) AS Reconciliation_Identity,\
                  substr(parse_json($1):Class_ID, 0, LEN(parse_json($1):Class_ID)) AS Class_ID,\
                  substr(parse_json($1):Asset_ID, 0, LEN(parse_json($1):Asset_ID)) AS Asset_ID,\
                  substr(parse_json($1):Name, 0, LEN(parse_json($1):Name)) AS Name,\
                  substr(parse_json($1):Status, 0, LEN(parse_json($1):Status)) AS Status,\
                  substr(parse_json($1):Status_Reason, 0, LEN(parse_json($1):Status_Reason)) AS Status_Reason,\
                  substr(parse_json($1):Allocated_Cluster, 0, LEN(parse_json($1):Allocated_Cluster)) AS Allocated_Cluster,\
                  substr(parse_json($1):Owner_Cluster, 0, LEN(parse_json($1):Owner_Cluster)) AS Owner_Cluster,\
                  substr(parse_json($1):Owner_Company, 0, LEN(parse_json($1):Owner_Company)) AS Owner_Company,\
                  substr(parse_json($1):Product_Category_Tier_1, 0, LEN(parse_json($1):Product_Category_Tier_1)) AS Product_Category_Tier_1,\
                  substr(parse_json($1):Product_Category_Tier_2, 0, LEN(parse_json($1):Product_Category_Tier_2)) AS Product_Category_Tier_2,\
                  substr(parse_json($1):Product_Category_Tier_3, 0, LEN(parse_json($1):Product_Category_Tier_3)) AS Product_Category_Tier_3,\
                  substr(parse_json($1):Manufacturer, 0, LEN(parse_json($1):Manufacturer)) AS Manufacturer,\
                  substr(parse_json($1):Product_Name, 0, LEN(parse_json($1):Product_Name)) AS Product_Name,\
                  substr(parse_json($1):Product_Model_Version, 0, LEN(parse_json($1):Product_Model_Version)) AS Product_Model_Version,\
                  substr(parse_json($1):Site, 0, LEN(parse_json($1):Site)) AS Site,\
                  substr(parse_json($1):Environment_Information, 0, LEN(parse_json($1):Environment_Information)) AS Environment_Information,\
                  substr(parse_json($1):Operating_System, 0, LEN(parse_json($1):Operating_System)) AS Operating_System,\
                  substr(parse_json($1):Location, 0, LEN(parse_json($1):Location)) AS Location,\
                  substr(parse_json($1):Zone, 0, LEN(parse_json($1):Zone)) AS Zone,\
                  substr(parse_json($1):Server_Group, 0, LEN(parse_json($1):Server_Group)) AS Server_Group,\
                  substr(parse_json($1):VCentre, 0, LEN(parse_json($1):VCentre)) AS VCentre,\
                  substr(parse_json($1):CreatedDate, 0, LEN(parse_json($1):CreatedDate)) AS CreatedDate,\
                  substr(parse_json($1):UpdatedDate, 0, LEN(parse_json($1):UpdatedDate)) AS UpdatedDate,\
                  substr(parse_json($1):CreatedBy, 0, LEN(parse_json($1):CreatedBy)) AS CreatedBy,\
                  substr(parse_json($1):UpdatedBy, 0, LEN(parse_json($1):UpdatedBy)) AS UpdatedBy,\
                  substr(parse_json($1):SourceKey, 0, LEN(parse_json($1):SourceKey)) AS SourceKey,\
                  substr(parse_json($1):Source_Desc, 0, LEN(parse_json($1):Source_Desc)) AS Source_Desc,\
                  substr(parse_json($1):Customer_Desc, 0, LEN(parse_json($1):Customer_Desc)) AS Customer_Desc,\
                  substr(parse_json($1):Environment_Details, 0, LEN(parse_json($1):Environment_Details)) AS Environment_Details,\
                  substr(parse_json($1):Version, 0, LEN(parse_json($1):Version)) AS Version,\
                  substr(parse_json($1):IsManaged, 0, LEN(parse_json($1):IsManaged)) AS IsManaged,\
                  substr(parse_json($1):DataCenter, 0, LEN(parse_json($1):DataCenter)) AS DataCenter,\
                  substr(parse_json($1):ServiceType, 0, LEN(parse_json($1):ServiceType)) AS ServiceType,\
                  substr(parse_json($1):SerialNumber, 0, LEN(parse_json($1):SerialNumber)) AS SerialNumber\
            FROM T_SERVER_STG\
            WHERE file_name = :1\
          "

    stmt = snowflake.createStatement({sqlText: cmd, binds: [file_name]});
    result_set1 = stmt.execute();
    
    result =  "COMPLETED SUCCESSFULLY!";
  } catch(err)
  {
    result = "FAILED " + err;
    var time_st = snowflake.execute( {sqlText: "SELECT CURRENT_TIMESTAMP;"} );
  }
  return result
$$
;


CALL SPW_T_SERVER('Server/2020/11/11/Server_202011061349.json');

-- 2.- 

CREATE OR REPLACE PROCEDURE SPW_DEVICE_TOPOLOGY(FILENAME STRING)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS     
$$
    try{

    // 01- Setting up operation variables
    var cmd, stmt, result_set1, result, file_name
    
    file_name = FILENAME
      
    // 04- Inserting data
    cmd = "INSERT INTO T_DEVICE_TOPOLOGY\
           SELECT substr(parse_json($1):RelationshipId, 0, LEN(parse_json($1):RelationshipId)) AS RelationshipId,\
                  substr(parse_json($1):CI_Name, 0, LEN(parse_json($1):CI_Name)) AS CI_Name,\
                  substr(parse_json($1):Absolute_CI_Name, 0, LEN(parse_json($1):Absolute_CI_Name)) AS Absolute_CI_Name,\
                  substr(parse_json($1):CI_RejectFlag, 0, LEN(parse_json($1):CI_RejectFlag)) AS CI_RejectFlag,\
                  substr(parse_json($1):Class_Desc, 0, LEN(parse_json($1):Class_Desc)) AS Class_Desc,\
                  substr(parse_json($1):Topo_Class, 0, LEN(parse_json($1):Topo_Class)) AS Topo_Class,\
                  substr(parse_json($1):Operation, 0, LEN(parse_json($1):Operation)) AS Operation,\
                  substr(parse_json($1):From_SysName, 0, LEN(parse_json($1):From_SysName)) AS From_SysName,\
                  substr(parse_json($1):FromPort, 0, LEN(parse_json($1):FromPort)) AS FromPort,\
                  substr(parse_json($1):To_SysName, 0, LEN(parse_json($1):To_SysName)) AS To_SysName,\
                  substr(parse_json($1):ToPort, 0, LEN(parse_json($1):ToPort)) AS ToPort,\
                  substr(parse_json($1):FromIP, 0, LEN(parse_json($1):FromIP)) AS FromIP,\
                  substr(parse_json($1):FromIP_RejectFlag, 0, LEN(parse_json($1):FromIP_RejectFlag)) AS FromIP_RejectFlag,\
                  substr(parse_json($1):ToIP, 0, LEN(parse_json($1):ToIP)) AS ToIP,\
                  substr(parse_json($1):ToIP_RejectFlag, 0, LEN(parse_json($1):ToIP_RejectFlag)) AS ToIP_RejectFlag,\
                  substr(parse_json($1):RelationshipCategory, 0, LEN(parse_json($1):RelationshipCategory)) AS RelationshipCategory,\
                  substr(parse_json($1):RelationshipType, 0, LEN(parse_json($1):RelationshipType)) AS RelationshipType,\
                  substr(parse_json($1):Source, 0, LEN(parse_json($1):Source)) AS Source,\
                  substr(parse_json($1):PayloadType, 0, LEN(parse_json($1):PayloadType)) AS PayloadType,\
                  substr(parse_json($1):CreatedBy, 0, LEN(parse_json($1):CreatedBy)) AS CreatedBy,\
                  substr(parse_json($1):CreatedDate, 0, LEN(parse_json($1):CreatedDate)) AS CreatedDate,\
                  substr(parse_json($1):UpdatedBy, 0, LEN(parse_json($1):UpdatedBy)) AS UpdatedBy,\
                  substr(parse_json($1):UpdatedDate, 0, LEN(parse_json($1):UpdatedDate)) AS UpdatedDate\
          FROM T_DEVICE_TOPOLOGY_STG\
          WHERE file_name = :1\
          "

    stmt = snowflake.createStatement({sqlText: cmd, binds: [file_name]});
    result_set1 = stmt.execute();
    
    result =  "COMPLETED SUCCESSFULLY!";
  } catch(err)
  {
    result = "FAILED " + err;
    var time_st = snowflake.execute( {sqlText: "SELECT CURRENT_TIMESTAMP;"} );
  }
  return result
$$
;


CALL SPW_DEVICE_TOPOLOGY('OTHER');
