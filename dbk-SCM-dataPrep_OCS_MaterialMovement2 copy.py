# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC #### This notebook is used to load the `Scrap2.0 Report` table from RESB /AFKO /MSEG in the scm_gold_db from the scm_landing database tables.

# COMMAND ----------

# DBTITLE 1,Install Database Utilities Package
# New cell added 

# COMMAND ----------

# DBTITLE 1,Install Database Utilities Package
# MAGIC %pip install delta-utilities --extra-index-url https://install:Gmh2H3b9sLcTtcsKKzwH@gitlab.toolchain.corning.com/api/v4/projects/620/packages/pypi/simple -U

# COMMAND ----------

__DBXX__ = {
  "job_name": "dbk-SCM-dataPrep_OCS_MaterialMovement2",
  "flavor": "SCALE",
  "spark_version": "10.4.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "driver_node_type_id": "i3.xlarge",
  "autoscale.max_workers": 2,
  "timeout_seconds": 3300,
  "max_retries": 0,
  "nonprod": {
    "databricks_environment": "NP",
    "notebook_task": {
      "base_parameters": {
        "01.DB_BRONZE": "c_ocs_ecc_pr",
        "02.TBL_BRONZE": "x",
        "03.DB_GOLD": "scm_gold_np",
        "04.TBL_GOLD": "r_ocs_scrap_final",
        "05.DB_PATH": "/mnt/scm-dl-np/data-lake/gold-np/scm_gold_np/",
        "06.TBL2_GOLD": "r_ocs_scrap_rts_final",
        "07.TBL3_GOLD":"r_ocs_scrap_rts_final_ordtotals"
      }
    }
  },
  "prod": {
    "databricks_environment": "PR",
    "notebook_task": {
      "base_parameters": {
        "01.DB_BRONZE": "c_ocs_ecc_pr",
        "02.TBL_BRONZE": "x",
        "03.DB_GOLD": "scm_gold_pr",
        "04.TBL_GOLD": "r_ocs_scrap2",
        "05.DB_PATH": "/mnt/scm-dl-pr/data-lake/gold-pr/scm_gold_pr/",
        "06.TBL2_GOLD": "r_ocs_scrap_rts_final",
        "07.TBL3_GOLD":"r_ocs_scrap_rts_final_ordtotals"        
      }
    }
  }
}


# COMMAND ----------

import os
import sys
import datetime
from utils.db_maintenance import * #use delta-utilities package
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, IntegerType, DateType, TimestampType , DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.functions import trim, col, regexp_replace
from pyspark.sql import functions as f
import pyspark

# COMMAND ----------

# DBTITLE 1,Create Widgets

# Set widgets for input
# dbutils.widgets.removeAll()

dbutils.widgets.text("01.DB_BRONZE",  "c_ocs_ecc_pr")
dbutils.widgets.text("02.TBL_BRONZE", "x")
dbutils.widgets.text("03.DB_GOLD",  "scm_gold_np")
dbutils.widgets.text("04.TBL_GOLD", "r_ocs_scrap_final")
dbutils.widgets.text("05.DB_PATH", "/mnt/scm-dl-np/data-lake/gold-db/scm_gold_np/")
dbutils.widgets.text("06.TBL2_GOLD", "r_ocs_scrap_rts_final")
dbutils.widgets.text("07.TBL3_GOLD", "r_ocs_scrap_rts_final_ordtotals")

# COMMAND ----------

# DBTITLE 1,Assign Values from Widgets
# Get widget input
db_bronze      = dbutils.widgets.get("01.DB_BRONZE")
tbl_bronze     = dbutils.widgets.get("02.TBL_BRONZE")
db_gold      = dbutils.widgets.get("03.DB_GOLD")
tbl_gold    = dbutils.widgets.get("04.TBL_GOLD")
tbl2_gold    = dbutils.widgets.get("06.TBL2_GOLD")
tbl3_gold    = dbutils.widgets.get("07.TBL3_GOLD")
db_path        = dbutils.widgets.get("05.DB_PATH")

databasePath = db_path+tbl_gold
databasePath2 = db_path+tbl2_gold
databasePath3 = db_path+tbl3_gold

print('{} INPUT VALUES. \ndb_bronze ->\t {} \ntbl_bronze ->\t {} \ndb_gold ->\t {} \ntbl_gold ->\t {}  \ntbl2_gold ->\t {} \ntbl3_gold ->\t {} \ndatabasePath ->\t {} \ndatabasePath2 ->\t {} \ndatabasePath3 ->\t {} '.format(datetime.datetime.now(),  db_bronze, tbl_bronze, db_gold, tbl_gold,tbl2_gold,tbl3_gold, databasePath,databasePath2,databasePath3))

# COMMAND ----------

# DBTITLE 1,Function - Create DataFrame
# prepareDF - takes a sql statement, runs the statement and returns a dataframe
# tablename is informational - it's not used in the query.  function will add prefix [returnFieldsPrefix] if bAddPrefix=True to all fields
def prepareDF(sqlStmt, tableName, bAddPrefix, fieldPrefix, bPrintInfo):
  this_function_name = sys._getframe().f_code.co_name
  ok = False
  msg = this_function_name
  
  try:
    # get dataframe from sql statement
    if bPrintInfo: print("TableName ->\t {}. Prefix DF fields? -> {}. DF fields prefix -> {}".format(tableName, bAddPrefix, fieldPrefix))
    if bPrintInfo: print("SQL To Execute -> {}".format(sqlStmt))
    df = spark.sql(sqlStmt)
    df.cache()
    if bPrintInfo: print("DF records returned :\t {}".format(df.count()))
    
    if bAddPrefix:
      # rename columns with prefix
      orig_col_names = df.columns
      new_col_names  = []
      new_col_names = ['{}{}'.format(fieldPrefix,item) for item in orig_col_names]
      # Update old column names with new column names
      df = df.toDF(*new_col_names)
      if bPrintInfo: print("DF fields renamed to : {}".format(df.columns))
    else:
      # don't rename columns
      if bPrintInfo: print("Did not add DF field prefix")
    
    ok = True
  except Exception as e:
    df = "null"
    msg = "Error message ->\t {}".format(str(e)[0:250])
  
  if not ok:
    print("Error loading DF {} ->\t {}".format(this_function_name, msg))
  
  return ok, df

# COMMAND ----------

# DBTITLE 1,Frame SQL Statement for all Movement Types (Excl 261 and 262) - MSEG with No PO and No Reservation Inv Adjustments
def frameSQL(IssMovement,CancelMovement,db_bronze,tableName1,tableName2, bPrintInfo):
  this_function_name = sys._getframe().f_code.co_name
  ok = False
  dfok = False
  msg = this_function_name
  
  try:
    # Prepare sql statement from MovementTypes
    if bPrintInfo: print("IssMovementType ->\t {}. CancelMovementType -> {}. ".format(IssMovement, CancelMovement))

       #sqlStmt = "with cancelmovement as (select  mandt as Client,trim(leading '0' from aufnr)  as MSEG_PO, werks as MSEG_Plant,trim(leading '0' from matnr) as MSEG_Material,rspos as mseg_ItemNoOfDepReq,rsnum as mseg_NoOfDepReq,meins as MSEG_UOM,max(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Max_ReversalDate,min(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Min_ReversalDate,sum(menge) as MSEG_Quantity , (LGNUM||' - '||LGTYP ) as Warehouse_Bin, sum(cast( DMBTR as double)) AS mseg_val_abs_local from  {0}.{1} where bwart = {2}  group by mandt,trim(leading '0' from aufnr) , werks,trim(leading '0' from matnr),meins,rspos,rsnum, (LGNUM||' - '||LGTYP ) ) ,confirm_mvmt as (select  bwart as mseg_movement_type,mandt as Client,trim(leading '0' from aufnr) as MSEG_PO, werks as MSEG_Plant,trim(leading '0' from matnr) as MSEG_Material,rspos as mseg_ItemNoOfDepReq,rsnum as mseg_NoOfDepReq,meins as MSEG_UOM,max(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Max_ConsumedDate,min(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Min_ConsumedDate, sum(menge) as MSEG_Quantity,  sum(cast( DMBTR as double)) AS mseg_val_abs_local ,(LGNUM||' - '||LGTYP ) as Warehouse_Bin from {0}.{1} where bwart = {3}  group by mandt,bwart,trim(leading '0' from aufnr), werks,trim(leading '0' from matnr),meins,rspos,rsnum, (LGNUM||' - '||LGTYP )) select a.Client, a.MSEG_PO,a.MSEG_Plant,a.MSEG_Material,a.mseg_movement_type,c.MAKTG as mseg_desc,a.Warehouse_Bin,a.MSEG_UOM ,a.mseg_NoOfDepReq,a.mseg_ItemNoOfDepReq, a.MSEG_Max_ConsumedDate as MSEG_Max_PostingDate,a.MSEG_Min_ConsumedDate as MSEG_Min_PostingDate,cast(a.mseg_val_abs_local - nvl(b.mseg_val_abs_local,'0') as decimal(23,2)) as mseg_val_abs_local, cast(a.MSEG_Quantity - nvl(b.MSEG_Quantity,'0') as decimal(23,2)) as MSEG_Quantity from confirm_mvmt a left join cancelmovement b on ( a.Client = b.Client and a.MSEG_PO = b.MSEG_PO  and a.MSEG_Plant = b.MSEG_Plant and a.MSEG_Material = b.MSEG_Material and a.MSEG_UOM = b.MSEG_UOM and a.mseg_NoOfDepReq= b.mseg_NoOfDepReq and a.mseg_ItemNoOfDepReq = b.mseg_ItemNoOfDepReq  and a.Warehouse_Bin = b.Warehouse_Bin) inner join c_ocs_ecc_pr.makt c on (a.Client = c.mandt and trim(a.mseg_NoOfDepReq) ='0000000000' and trim(a.mseg_ItemNoOfDepReq) = '0000'and trim(a.MSEG_PO) == '' and a.MSEG_Material = trim(leading '0' from c.matnr) and c.spras = 'E' ) ".format(db_bronze, tableName,CancelMovement,IssMovement) 
  
    
    sqlStmt = " with movementtrans as (select  mandt as Client,trim(leading '0' from aufnr)  as MSEG_PO, werks as MSEG_Plant,trim(leading '0' from matnr) as MSEG_Material,rspos as mseg_ItemNoOfDepReq,rsnum as mseg_NoOfDepReq,meins as MSEG_UOM, max(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Max_PostingDate,min(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Min_PostingDate,max(nvl(date_format(budat_mkpf,'y-MM-dd'),''))as MSEG_Max_ReversalDate, min(nvl(date_format(budat_mkpf,'y-MM-dd'),''))as MSEG_Min_ReversalDate,(case when bwart = {2} then sum(menge) * -1 else   sum(menge) end) as MSEG_Quantity , (LGNUM||' - '||LGTYP ) as Warehouse_Bin, (case when bwart = {2} then sum(cast( DMBTR as double) ) * -1 else sum(cast( DMBTR as double) ) end)  AS mseg_val_abs_local, bwart as movement_type,grund as movement_reason from  {0}.{1} where bwart in( {3}, {2}) and date_format( budat_mkpf, 'yyyyMMdd') >= '20210101'  group by mandt,trim(leading '0' from aufnr) , werks,trim(leading '0' from matnr),meins,rspos,rsnum, (LGNUM||' - '||LGTYP ) ,nvl(date_format(budat_mkpf,'y-MM-dd'),''), bwart,grund )  select a.Client,a.MSEG_PO,a.MSEG_Plant,a.MSEG_Material,c.MAKTG as mseg_desc,a.Warehouse_Bin,a.MSEG_UOM ,a.mseg_NoOfDepReq,a.mseg_ItemNoOfDepReq, (case when a.movement_type = {3} then a.MSEG_Max_PostingDate when a.movement_type = {2} then a.MSEG_Max_ReversalDate else '' end) as MSEG_Max_PostingDate ,(case when a.movement_type = {3} then a.MSEG_Min_PostingDate  when a.movement_type = {2} then a.MSEG_Min_ReversalDate else '' end) as MSEG_Min_PostingDate ,cast(a.mseg_val_abs_local  as decimal(23,2)) as mseg_val_abs_local, a.movement_type as mseg_movement_type,a.movement_reason as mseg_movement_reason, cast( nvl(a.MSEG_Quantity,'0') as decimal(23,2)) as MSEG_Quantity from movementtrans a inner join {0}.{4} c on (a.Client = c.mandt and trim(a.mseg_NoOfDepReq) ='0000000000' and trim(a.mseg_ItemNoOfDepReq) = '0000' and trim(a.MSEG_PO) == '' and a.MSEG_Material = trim(leading '0' from c.matnr) and c.spras = 'E' ) ". format(db_bronze, tableName1,CancelMovement,IssMovement,tableName2)   
  
    if bPrintInfo: print("SQL To Execute -> {}".format(sqlStmt))    
    #df = spark.sql(sqlStmt)
    #df.cache()
    #if bPrintInfo: print("DF records returned :\t {}".format(df.count()))
    
    ok = True
  except Exception as e:
    sqlStmt = "null"
    msg = "Error message ->\t {}".format(str(e)[0:250])
  
  if not ok:
    print("Error Preparing SQLStmt {} ->\t {}".format(this_function_name, msg))
 
  return ok, sqlStmt

# COMMAND ----------

# DBTITLE 1,Frame SQL for Movement Types (261, 262, 201, 202, 551, 552, 711, 712) - MSEG with PO(with AND without AFKO) and No Reservation 
def frameSQL_NOTAFKO(IssMovement,CancelMovement,db_bronze,tableName1,tableName2,tableName3, bPrintInfo):
  this_function_name = sys._getframe().f_code.co_name
  ok = False
  dfok = False
  msg = this_function_name
  
  try:
    # Prepare sql statement from MovementTypes
    if bPrintInfo: print("IssMovementType ->\t {}. CancelMovementType -> {}. ".format(IssMovement, CancelMovement))

   
    #sqlStmt = " with movementtrans as (select  mandt as Client,trim(leading '0' from aufnr)  as MSEG_PO, werks as MSEG_Plant,trim(leading '0' from matnr) as MSEG_Material,rspos as mseg_ItemNoOfDepReq,rsnum as mseg_NoOfDepReq,meins as MSEG_UOM, max(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Max_PostingDate,min(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Min_PostingDate,max(nvl(date_format(budat_mkpf,'y-MM-dd'),''))as MSEG_Max_ReversalDate, min(nvl(date_format(budat_mkpf,'y-MM-dd'),''))as MSEG_Min_ReversalDate,(case when bwart = {2} then sum(menge) * -1 else   sum(menge) end) as MSEG_Quantity , (LGNUM||' - '||LGTYP ) as Warehouse_Bin, (case when bwart = {2} then sum(cast( DMBTR as double) ) * -1 else sum(cast( DMBTR as double) ) end)  AS mseg_val_abs_local, bwart as movement_type, grund as mseg_movement_reason from  {0}.{1} where bwart in( {3}, {2}) and date_format( budat_mkpf, 'yyyyMMdd') >= '20210101'  group by mandt,trim(leading '0' from aufnr) , werks,trim(leading '0' from matnr),meins,rspos,rsnum, (LGNUM||' - '||LGTYP ) ,nvl(date_format(budat_mkpf,'y-MM-dd'),''), bwart,grund )  select a.Client,a.MSEG_PO,a.MSEG_Plant,a.MSEG_Material,c.MAKTG as mseg_desc,a.Warehouse_Bin,a.MSEG_UOM ,a.mseg_NoOfDepReq,a.mseg_ItemNoOfDepReq, (case when a.movement_type = {3} then a.MSEG_Max_PostingDate when a.movement_type = {2} then a.MSEG_Max_ReversalDate else '' end) as MSEG_Max_PostingDate ,(case when a.movement_type = {3} then a.MSEG_Min_PostingDate  when a.movement_type = {2} then a.MSEG_Min_ReversalDate else '' end) as MSEG_Min_PostingDate ,cast(a.mseg_val_abs_local  as decimal(23,2)) as mseg_val_abs_local, a.movement_type as mseg_movement_type,a.mseg_movement_reason,cast( nvl(a.MSEG_Quantity,'0') as decimal(23,2)) as MSEG_Quantity from movementtrans a ,{0}.{4} c where a.Client = c.mandt and trim(a.mseg_NoOfDepReq) ='0000000000' and trim(a.mseg_ItemNoOfDepReq) == '0000' and trim(a.MSEG_PO) != '' and a.MSEG_Material = trim(leading '0' from c.matnr) and c.spras = 'E' and not exists (select 1 from  {0}.{5} b where b.mandt = a.Client and  trim(leading '0' from b.aufnr) = a.MSEG_PO) ". format(db_bronze, tableName1,CancelMovement,IssMovement,tableName2,tableName3) 

    sqlStmt = " with movementtrans as (select  mandt as Client,trim(leading '0' from aufnr)  as MSEG_PO, werks as MSEG_Plant,trim(leading '0' from matnr) as MSEG_Material,rspos as mseg_ItemNoOfDepReq,rsnum as mseg_NoOfDepReq,meins as MSEG_UOM, max(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Max_PostingDate,min(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Min_PostingDate,max(nvl(date_format(budat_mkpf,'y-MM-dd'),''))as MSEG_Max_ReversalDate, min(nvl(date_format(budat_mkpf,'y-MM-dd'),''))as MSEG_Min_ReversalDate,(case when bwart = {2} then sum(menge) * -1 else   sum(menge) end) as MSEG_Quantity , (LGNUM||' - '||LGTYP ) as Warehouse_Bin, (case when bwart = {2} then sum(cast( DMBTR as double) ) * -1 else sum(cast( DMBTR as double) ) end)  AS mseg_val_abs_local, bwart as movement_type, grund as mseg_movement_reason from  {0}.{1} where bwart in( {3}, {2}) and date_format( budat_mkpf, 'yyyyMMdd') >= '20210101'  group by mandt,trim(leading '0' from aufnr) , werks,trim(leading '0' from matnr),meins,rspos,rsnum, (LGNUM||' - '||LGTYP ) ,nvl(date_format(budat_mkpf,'y-MM-dd'),''), bwart,grund )  select a.Client,a.MSEG_PO,a.MSEG_Plant,a.MSEG_Material,c.MAKTG as mseg_desc,a.Warehouse_Bin,a.MSEG_UOM ,a.mseg_NoOfDepReq,a.mseg_ItemNoOfDepReq, (case when a.movement_type = {3} then a.MSEG_Max_PostingDate when a.movement_type = {2} then a.MSEG_Max_ReversalDate else '' end) as MSEG_Max_PostingDate ,(case when a.movement_type = {3} then a.MSEG_Min_PostingDate  when a.movement_type = {2} then a.MSEG_Min_ReversalDate else '' end) as MSEG_Min_PostingDate ,cast(a.mseg_val_abs_local  as decimal(23,2)) as mseg_val_abs_local, a.movement_type as mseg_movement_type,a.mseg_movement_reason,cast( nvl(a.MSEG_Quantity,'0') as decimal(23,2)) as MSEG_Quantity from movementtrans a ,{0}.{4} c where a.Client = c.mandt and trim(a.mseg_NoOfDepReq) ='0000000000' and trim(a.mseg_ItemNoOfDepReq) == '0000' and trim(a.MSEG_PO) != '' and a.MSEG_Material = trim(leading '0' from c.matnr) and c.spras = 'E' ". format(db_bronze, tableName1,CancelMovement,IssMovement,tableName2)    
  
    if bPrintInfo: print("SQL To Execute -> {}".format(sqlStmt))    
    #df = spark.sql(sqlStmt)
    #df.cache()
    #if bPrintInfo: print("DF records returned :\t {}".format(df.count()))
    
    ok = True
  except Exception as e:
    sqlStmt = "null"
    msg = "Error message ->\t {}".format(str(e)[0:250])
  
  if not ok:
    print("Error Preparing SQLStmt {} ->\t {}".format(this_function_name, msg))
 
  return ok, sqlStmt

# COMMAND ----------

# DBTITLE 1,Prep MSEG - Movement Type 201, 202 GI by CostCenter (Inv Adjust & PO w/o Reservation)
tableName1 = "mseg"
tableName2 = "makt"
tableName3 = "afko"
db_bronze = "c_ocs_ecc_pr"
IssMovement = "201"
CancelMovement = "202"
bAddPrefix=False
tablePrefix = ""

ok, df_CS = frameSQL( IssMovement, CancelMovement,db_bronze,tableName1, tableName2,bPrintInfo=True)


if (not ok):
  msg = '{} Error Preparing SQL STMT {}'.format(datetime.datetime.now(), tableName1)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName1))
  
dfok, df_CC = prepareDF(df_CS, tableName1, bAddPrefix, tablePrefix, bPrintInfo=True)  
  
print("Total no. of records from df_CC -> {}".format( df_CC.count()))    
#df_CC.show()

ok, df_CS_NAFS = frameSQL_NOTAFKO( IssMovement, CancelMovement,db_bronze,tableName1, tableName2,tableName3,bPrintInfo=True)


if (not ok):
  msg = '{} Error Preparing SQL STMT {}'.format(datetime.datetime.now(), tableName3)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName3))
  
dfok, df_CS_NAF = prepareDF(df_CS_NAFS, tableName1, bAddPrefix, tablePrefix, bPrintInfo=True)  
  
print("Total no. of records from df_CC -> {}".format( df_CS_NAF.count()))    
df_CS_NAF.show()

# COMMAND ----------

# DBTITLE 1,Prep MSEG - Movement Type 261, 262 GI by CostCenter ( PO w/o Reservation)
tableName1 = "mseg"
tableName2 = "makt"
tableName3 = "afko"
db_bronze = "c_ocs_ecc_pr"
IssMovement = "261"
CancelMovement = "262"
bAddPrefix=False
tablePrefix = ""

ok, df_CS_PO_NAFS = frameSQL_NOTAFKO( IssMovement, CancelMovement,db_bronze,tableName1, tableName2,tableName3,bPrintInfo=True)


if (not ok):
  msg = '{} Error Preparing SQL STMT {}'.format(datetime.datetime.now(), tableName3)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName3))
  
dfok, df_CS_PO_NAF = prepareDF(df_CS_PO_NAFS, tableName1, bAddPrefix, tablePrefix, bPrintInfo=True)  
  
print("Total no. of records from df_CS_PO_NAF -> {}".format( df_CS_PO_NAF.count()))    
df_CS_PO_NAF.show()

# COMMAND ----------

df_CS_PO_NAF.filter(col("MSEG_PO")=="9005209402").display()

# COMMAND ----------

# DBTITLE 1,Prep - MSEG Movement Type 221 222 - Goods Receipt/Cancellation
tableName1 = "mseg"
tableName2 = "makt"
db_bronze = "c_ocs_ecc_pr"
IssMovement = "221"
CancelMovement = "222"
bAddPrefix=False
tablePrefix = ""

ok, df_GRS = frameSQL( IssMovement, CancelMovement,db_bronze,tableName1, tableName2,bPrintInfo=True)


if (not ok):
  msg = '{} Error Preparing SQL STMT {}'.format(datetime.datetime.now(), tableName1)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName1))
  
dfok, df_GR = prepareDF(df_GRS, tableName1, bAddPrefix, tablePrefix, bPrintInfo=True)  
  
print("Total no. of records from df_GR -> {}".format(df_GR.count())) 
df_GR.show()

# COMMAND ----------

# DBTITLE 1,Prep - MSEG Movement 551,552 - GI Scrapping (Inv Adjust & PO w/o Reserv)
tableName1 = "mseg"
tableName2 = "makt"
tableName3 = "afko"
db_bronze = "c_ocs_ecc_pr"
IssMovement = "551"
CancelMovement = "552"
bAddPrefix=False
tablePrefix = ""

ok, df_GI = frameSQL( IssMovement, CancelMovement,db_bronze,tableName1, tableName2,bPrintInfo=True)


if (not ok):
  msg = '{} Error Preparing SQL STMT {}'.format(datetime.datetime.now(), tableName1)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName1))
  
dfok, df_GI = prepareDF(df_GI, tableName1, bAddPrefix, tablePrefix, bPrintInfo=True)  
  
print("Total no. of records from df_GI  -> {}".format( df_GI.count())) 
df_GI.show()

ok, df_GI_NAFS = frameSQL_NOTAFKO( IssMovement, CancelMovement,db_bronze,tableName1, tableName2,tableName3,bPrintInfo=True)


if (not ok):
  msg = '{} Error Preparing SQL STMT {}'.format(datetime.datetime.now(), tableName3)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName3))
  
dfok, df_GI_NAF = prepareDF(df_GI_NAFS, tableName1, bAddPrefix, tablePrefix, bPrintInfo=True)  
  
print("Total no. of records from df_GI_NAF -> {}".format( df_GI_NAF.count()))    
df_GI_NAF.show()

# COMMAND ----------

# DBTITLE 1,Prep - MSEG Movement 553,554 - GI Scrapping QI
tableName1 = "mseg"
tableName2 = "makt"
db_bronze = "c_ocs_ecc_pr"
IssMovement = "553"
CancelMovement = "554"
bAddPrefix=False
tablePrefix = ""

ok, df_GIQS = frameSQL( IssMovement, CancelMovement,db_bronze,tableName1,tableName2, bPrintInfo=True)


if (not ok):
  msg = '{} Error Preparing SQL STMT {}'.format(datetime.datetime.now(), tableName1)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName1))
  
dfok, df_GIQ = prepareDF(df_GIQS, tableName1, bAddPrefix, tablePrefix, bPrintInfo=True)  
  
print("Total no. of records from {} table -> {}".format(tableName1, df_GIQ.count()))  
df_GIQ.show()

# COMMAND ----------

# DBTITLE 1,Prep - MSEG Movement 555, 556 - GI scrapping blocked
tableName1 = "mseg"
tableName2 = "makt"
db_bronze = "c_ocs_ecc_pr"
IssMovement = "555"
CancelMovement = "556"
bAddPrefix=False
tablePrefix = ""

ok, df_GISS = frameSQL( IssMovement, CancelMovement,db_bronze,tableName1,tableName2, bPrintInfo=True)


if (not ok):
  msg = '{} Error Preparing SQL STMT {}'.format(datetime.datetime.now(), tableName1)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName1))
  
dfok, df_GIS = prepareDF(df_GISS, tableName1, bAddPrefix, tablePrefix, bPrintInfo=True)  
  
print("Total no. of records from {} table -> {}".format(tableName1, df_GIS.count()))  
df_GIS.show()

# COMMAND ----------

# DBTITLE 1,Prep - MSEG Movement 711,712 - GI InvDiff.: warehouse  (PO w/o Reservation)
tableName1 = "mseg"
tableName2 = "makt"
tableName3 = "afko"
db_bronze = "c_ocs_ecc_pr"
IssMovement = "711"
CancelMovement = "712"
bAddPrefix=False
tablePrefix = ""

ok, df_GIWS = frameSQL( IssMovement, CancelMovement,db_bronze,tableName1,tableName2, bPrintInfo=True)


if (not ok):
  msg = '{} Error Preparing SQL STMT {}'.format(datetime.datetime.now(), tableName1)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName1))
  
dfok, df_GIW = prepareDF(df_GIWS, tableName1, bAddPrefix, tablePrefix, bPrintInfo=True)  
  
print("Total no. of records from {} table -> {}".format(tableName1, df_GIW.count()))  
df_GIW.show()


ok, df_GIWS_NAF = frameSQL_NOTAFKO( IssMovement, CancelMovement,db_bronze,tableName1, tableName2,tableName3,bPrintInfo=True)


if (not ok):
  msg = '{} Error Preparing SQL STMT {}'.format(datetime.datetime.now(), tableName3)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName3))
  
dfok, df_GIWS_NAF = prepareDF(df_GIWS_NAF, tableName1, bAddPrefix, tablePrefix, bPrintInfo=True)  
  
print("Total no. of records from df_GIWS_NAF -> {}".format( df_GIWS_NAF.count()))    
df_GIWS_NAF.show()

# COMMAND ----------

# DBTITLE 1,Prep - MSEG Movement 717,718 - GI InvDiff.:  blocked
tableName1 = "mseg"
tableName2 = "makt"
db_bronze = "c_ocs_ecc_pr"
IssMovement = "717"
CancelMovement = "718"
bAddPrefix=False
tablePrefix = ""

ok, df_GIVS = frameSQL( IssMovement, CancelMovement,db_bronze,tableName1,tableName2, bPrintInfo=True)


if (not ok):
  msg = '{} Error Preparing SQL STMT {}'.format(datetime.datetime.now(), tableName1)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName1))
  
dfok, df_GIV = prepareDF(df_GIVS, tableName1, bAddPrefix, tablePrefix, bPrintInfo=True)  
  
print("Total no. of records from {} table -> {}".format(tableName1, df_GIV.count()))  
df_GIV.show()

# COMMAND ----------

# DBTITLE 1,Prep  - MSEG join all Movement Types excl. 261,262
df_MSEG_ALL = df_CC.union(df_GR).union(df_GI).union(df_GIQ).union(df_GIS).union(df_GIW).union(df_GIV).union(df_GIWS_NAF).union(df_GI_NAF).union(df_CS_PO_NAF).union(df_CS_NAF)

# COMMAND ----------

#df_MSEG_ALL.filter(col("MSEG_Material") == "979735").display()

df_MSEG_ALL.filter(col("MSEG_PO")=="9005209402").display()
#display(df_MSEG_ALL)

# COMMAND ----------

# DBTITLE 1,Prep - MSEG - Create RTS data (movement type 101 , 102)

tableName = "mseg"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""

sqlStmt = "with cancelmovement as  (select  mandt as Client,trim(leading '0' from aufnr)  as MSEG_PO, werks as MSEG_Plant,trim(leading '0' from matnr) as MSEG_Material,meins as MSEG_UOM,nvl(date_format(budat_mkpf,'y-MM'),'') as MSEG_Postingdate,sum(menge) as MSEG_Quantity from {0}.{1} where bwart = 102 and kzbew = 'F'  group by mandt,trim(leading '0' from aufnr) , werks,trim(leading '0' from matnr),meins, nvl(date_format(budat_mkpf,'y-MM'),'')),confirm_mvmt as (select  mandt as Client,trim(leading '0' from aufnr) as MSEG_PO, werks as MSEG_Plant,trim(leading '0' from matnr) as MSEG_Material,meins as MSEG_UOM,nvl(date_format(budat_mkpf,'y-MM'),'') as MSEG_Postingdate, sum(menge) as MSEG_Quantity from {0}.{1} where bwart = 101 and kzbew = 'F' group by mandt,trim(leading '0' from aufnr), werks,trim(leading '0' from matnr),meins, nvl(date_format(budat_mkpf,'y-MM'),'')) select a.Client, a.MSEG_PO,a.MSEG_Plant,a.MSEG_Material,a.MSEG_UOM , a.MSEG_Postingdate, cast(a.MSEG_Quantity - nvl(b.MSEG_Quantity,'0') as decimal(23,2)) as MSEG_Quantity from confirm_mvmt a left join cancelmovement b on ( a.Client = b.Client and a.MSEG_PO = b.MSEG_PO  and a.MSEG_Plant = b.MSEG_Plant and a.MSEG_Material = b.MSEG_Material and a.MSEG_UOM = b.MSEG_UOM and a.mseg_postingdate = b.mseg_postingdate) ".format(db_bronze, tableName)

ok, df_RTS = prepareDF(sqlStmt, tableName, bAddPrefix, tablePrefix, bPrintInfo=True)


if (not ok):
  msg = '{} Error generating dataframe {}'.format(datetime.datetime.now(), tableName)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName))
  
print("Total no. of records from {} table -> {}".format(tableName, df_RTS.count()))    

# COMMAND ----------

# DBTITLE 1,Prep - RESB - Prod Order Components Table

tableName = "resb"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""
#where b.bwart in (261,262) -- Added Date Filter  to bring all Movement Types

sqlStmt = "select distinct b.mandt as resb_client, b.rsnum as resb_NoOfreserv,b.rspos as ItemNoOfReserv,b.bdart as reqType, trim(leading '0' from b.matnr) resb_matnr_trim, b.matnr as materialID,b.MATKL as materialgrpcd,  b.EKGRP as Purchasinggrp, b.rssta as reservstatus, b.werks as resb_plant, b.lgort as storagelocation,b.charg as batchnum, b.bdter as requirementdate ,b.bdmng as reqqty, b.meins as baseuom, b.shkzg as creditdebitInd,b.waers as currency,b.enmng as qtywithdrawn, b.enwrt as valuewithdrawn,b.plnum as plannedorder, b.aufnr as resb_poordernumber, trim(leading '0' from b.aufnr) resb_poordernumber_trim, b.kdauf as salesordernum,b.kdpos as salesitemordnum, b.bwart as resb_movementkey, b.postp as itemcat_bom,b.posnr as itembom, b.stlty as resb_BOMcat, b.stlkn as bom_item_nodenum,  b.STLAL as altBOM,  b.PEINH as priceunit,  b.GPREIS_2 as totpriceinlocal,  b.FPREIS_2 as fixpriceinlocal,  b.DBSKZ as dir_procurement_ind, b.EBELN as purchasingdocnum,b.AUSCH as RESB_CScrap,b.AVOAU as RESB_OPScrap, b.STLNR as RESB_BOM,b.GPREIS as RESB_price,b.DUMPS as RESB_phantomitem,b.RGEKZ as RESB_backflush from {}.{} b where date_format( b.bdter, 'yyyyMMdd') >= '20210101'  ".format(db_bronze, tableName)


ok, df_RESB = prepareDF(sqlStmt, tableName, bAddPrefix, tablePrefix, bPrintInfo=True)


if (not ok):
  msg = '{} Error generating dataframe {}'.format(datetime.datetime.now(), tableName)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName))
  
print("Total no. of records from {} table -> {}".format(tableName, df_RESB.count()))    #106335651

#df_RESB.show()


# COMMAND ----------

df_RESB.filter(col("resb_NoOfreserv") == "0401980657" ).display()
#display(df_RESB)

# COMMAND ----------

# DBTITLE 1,Prep - AFPO - Sales Details  (Not Required)
tableName = "afpo"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""

try:
  df_AFPO =spark.sql('''select distinct mandt, aufnr,kdauf as saledocno,kdpos as salesitemno  from {0}.{1} '''.format(db_bronze, tableName)) 

except Exception as e:
    df_AFPO = "null"
    msg = "Error message ->\t {}".format(str(e)[0:250])  
    print (e)

 
print("Total no. of records from {} -> {}".format(tableName, df_AFPO.count())) #4882081

# COMMAND ----------

# DBTITLE 1,Prep - AFKO - Order Header Table
tableName = "afko"
tableName2 = "afpo"
tableName3 = "vbap"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""

#added date filter actual start date > 2021
try:
  #df_AFKO_1 =spark.sql('''Select distinct a.mandt as afko_client,a.aufnr as afko_poorderno,a.rsnum as afko_NoOfreserv, a.GSTRP as basicstart, GLTRP as basicend,a.GSTRS as schedulestart,a.GLTRS as scheduleend,a.GSTRI as actualstart,a.GLTRI as actualend, a.GETRI as confirmedactualend, a.PLNTY as tasklisttype, trim(leading '0' from a.PLNBEZ)  as afko_matnr,trim(leading '0' from a.STLBEZ) as afko_material_no, a.STLTY as afko_bom_category, a.BMENGE as AFKO_baseqty, a.STLAN as bomusage, a.GAMNG as AFKO_totalordqty, a.DISPO as AFKO_mrpcontroller,a.PLNNR as AFKO_Group,a.AUFPL as AFKO_RoutingNo,a.GASMG as AFKO_TotScrapQty,a.IASMG as AFKO_ConfirmedScrap ,a.STLNR as AFKO_BOM , a.GMEIN as AFKO_baseUOM ,a.IGMNG as AFKO_confirmedqty, c.kdauf as saledocno,c.kdpos as salesitemno,zzcatnum as catalog_number , b.PRODH as prod_hier, b.NETWR as NetValue, b.UMVKZ as Numerator,b.UMVKN as Denominator, b.NETPR as Netprice,b.VRKME as salesunit,  b.KWMENG as orderqty_salesunit from {0}.{1} a , {0}.{2} c , {0}.{3} b where c.mandt  = b.mandt and a.mandt = c.mandt and a.aufnr = c.aufnr and nvl(c.kdauf,'-') = nvl(b.vbeln,'-') and c.kdpos = b.posnr  and date_format( a.GSTRI, 'yyyyMMdd') >= '20210101'  '''.format(db_bronze, tableName,tableName2,tableName3)) 

    #df_AFKO_1 =spark.sql('''Select distinct a.mandt as afko_client,a.aufnr as afko_poorderno,a.rsnum as afko_NoOfreserv,  cast(a.GSTRP as date) as basicstart, cast(GLTRP as date) as basicend,cast(a.GSTRS as date) as schedulestart,cast(a.GLTRS as date) as scheduleend,cast(a.GSTRI as date) as actualstart,cast(a.GLTRI as date) as actualend, cast(a.GETRI as date) as confirmedactualend, a.PLNTY as tasklisttype, trim(leading '0' from a.PLNBEZ)  as afko_matnr,trim(leading '0' from a.STLBEZ) as afko_material_no, a.STLTY as afko_bom_category, cast(a.BMENGE as decimal(14,2)) as AFKO_baseqty, a.STLAN as bomusage, cast(a.GAMNG as decimal(14,2)) as AFKO_totalordqty, a.DISPO as AFKO_mrpcontroller,a.PLNNR as AFKO_Group,a.AUFPL as AFKO_RoutingNo,cast(a.GASMG as int) as AFKO_TotScrapQty,a.IASMG as AFKO_ConfirmedScrap ,a.STLNR as AFKO_BOM , a.GMEIN as AFKO_baseUOM ,cast(a.IGMNG as decimal(14,2))  as AFKO_confirmedqty, cast(zzcatnum as int) as catalog_number , b.PRODH as prod_hier, cast(b.NETWR as decimal(14,2)) as NetValue, b.UMVKZ as Numerator,b.UMVKN as Denominator, cast(b.NETPR as decimal(14,2)) as Netprice,cast(b.VRKME as decimal(14,2)) as salesunit,  cast(b.KWMENG as decimal(14,2)) as orderqty_salesunit, b.vbeln as saledocno,b.posnr as salesitemno  from {0}.{1} a left join  {0}.{2} b  on ( a.mandt  = b.mandt and a.aufnr = b.aufnr and date_format( a.GSTRI, 'yyyyMMdd') >= '20210101' ) '''.format(db_bronze, tableName,tableName3)) 

    
  df_AFKO_1 =spark.sql('''select distinct a.kdauf as saledocno,a.kdpos as salesitemno ,c.aufnr as afkp_aufnr,zzcatnum  as catalog_number, b.PRODH as prod_hier, cast(b.NETWR as decimal(14,2)) as NetValue, b.UMVKZ as Numerator,b.UMVKN as Denominator, cast(b.NETPR as decimal(14,2)) as Netprice,cast(b.VRKME as decimal(14,2)) as salesunit,  cast(b.KWMENG as decimal(14,2)) as orderqty_salesunit,c.mandt as afko_client,c.aufnr as afko_poorderno,c.rsnum as afko_NoOfreserv,  cast(c.GSTRP as date) as basicstart, cast(GLTRP as date) as basicend,cast(c.GSTRS as date) as schedulestart,cast(c.GLTRS as date) as scheduleend,cast(c.GSTRI as date) as actualstart,cast(c.GLTRI as date) as actualend, cast(c.GETRI as date) as confirmedactualend, c.PLNTY as tasklisttype, trim(leading '0' from c.PLNBEZ)  as afko_matnr,trim(leading '0' from c.STLBEZ) as afko_material_no, c.STLTY as afko_bom_category, cast(c.BMENGE as decimal(14,2)) as AFKO_baseqty, c.STLAN as bomusage, cast(c.GAMNG as decimal(14,2)) as AFKO_totalordqty, c.DISPO as AFKO_mrpcontroller,c.PLNNR as AFKO_Group,c.AUFPL as AFKO_RoutingNo,cast(c.GASMG as int) as AFKO_TotScrapQty,c.IASMG as AFKO_ConfirmedScrap ,c.STLNR as AFKO_BOM ,c.GMEIN as AFKO_baseUOM ,cast(c.IGMNG as decimal(14,2))  as AFKO_confirmedqty from {0}.{3} b inner join  {0}.{2} a on (  a.mandt = b.mandt and nvl(a.kdauf,'-') = nvl(b.vbeln,'-') and a.kdpos = b.posnr  ) right join {0}.{1} c on (c.mandt = a.mandt  and c.aufnr = a.aufnr  and date_format( c.GSTRI, 'yyyyMMdd') >= '20210101') '''.format(db_bronze, tableName,tableName2,tableName3)) 

except Exception as e:
    df_AFKO_1 = "null"
    msg = "Error message ->\t {}".format(str(e)[0:250])  
    print (e)

 
print("Total no. of records from {} -> {}".format(tableName, df_AFKO_1.count())) #6805152

#df_AFKO.show()

# COMMAND ----------

df_AFKO_1.filter(col("afko_poorderno") == "000008038136" ).display()
#display(df_AFKO_1)

# COMMAND ----------

# DBTITLE 1,Prep - PLPO table
tableName = "plpo"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""

try:
  df_PLPO =spark.sql(''' select mandt, plnnr ,plnty,(case when sum(cast(aufak as double) ) is null then '0.0' else sum(cast(aufak as double)) end)  as afko_plpo_scrap from {0}.{1} group by mandt, plnnr,plnty '''.format(db_bronze, tableName)) 

except Exception as e:
    df_PLPO = "null"
    msg = "Error message ->\t {}".format(str(e)[0:250])  
    print (e)

 
print("Total no. of records from {} -> {}".format(tableName, df_PLPO.count()))

# COMMAND ----------

df_AFKO = (df_AFKO_1.join(df_PLPO, (df_AFKO_1.afko_client == df_PLPO.mandt) 
                                        & (df_AFKO_1.AFKO_Group == df_PLPO.plnnr)
                                        & (df_AFKO_1.tasklisttype == df_PLPO.plnty), "left")
                   # .join(df_AFPO, (df_AFKO_1.afko_client == df_AFPO.mandt) 
                               #         & (df_AFKO_1.afko_poorderno == df_AFPO.aufnr)
                               #         & (df_AFKO_1.saledocno == df_AFPO.saledocno)                          
                                #        & (df_AFKO_1.salesitemno == df_AFPO.salesitemno), "left")           
                 .drop(df_PLPO.mandt) 
                 .drop(df_PLPO.plnnr) 
                 .drop(df_PLPO.plnty) 
                # .drop(df_AFPO.mandt) 
                # .drop(df_AFPO.aufnr) 
                 #.drop(df_AFPO.saledocno)             
                # .drop(df_AFPO.salesitemno)             
                )


# COMMAND ----------

df_AFKO.filter(col("afko_poorderno") == "000008038136" ).display()
#display(df_AFKO)

# COMMAND ----------

# DBTITLE 1,Prep - MSEG - Material Doc Segment Table (Movement 261,262) with PO & Reservation
tableName1 = "mseg"
tableName2 = "makt"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""
#sqlStmt = "select distinct  c.mandt as mseg_client, c.matnr as mseg_matnumber,trim(leading '0' from c.matnr) mseg_matnr_trim, b.MAKTG as mseg_desc,c.werks as mseg_plant,sum(cast( c.MENGE as double)) as mseg_qty, c.MEINS as mseg_uom,c.AUFNR as mseg_prodorder, c.bwart as mseg_movement_type,c.rsnum as mseg_NoOfDepReq,c.rspos as mseg_ItemNoOfDepReq, sum(cast( c.DMBTR as double)) AS mseg_val_abs_local, c.LGNUM||' - '||c.LGTYP as Warehouse_Bin,max(nvl(date_format(budat_mkpf,'yyyy-MM-dd'),'')) as Max_Posting_date, min(nvl(date_format(budat_mkpf,'yyyy-MM-dd'),'')) as Min_Posting_Date from {0}.{1} c, {0}.{2} b where c.mandt = b.mandt and c.matnr = b.matnr and b.spras = 'E' group by c.mandt ,c.matnr,trim(leading '0' from c.matnr) ,b.MAKTG,c.werks,c.MEINS ,c.AUFNR ,  c.bwart ,c.rsnum, c.rspos , (c.LGNUM||' - '||c.LGTYP ) ". format(db_bronze, tableName1,tableName2)

sqlStmt = "with cancelmovement as (select  mandt as Client,trim(leading '0' from aufnr)  as MSEG_PO, werks as MSEG_Plant,trim(leading '0' from matnr) as MSEG_Material,rspos as mseg_ItemNoOfDepReq,rsnum as mseg_NoOfDepReq,meins as MSEG_UOM,max(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Max_ReversalDate,min(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Min_ReversalDate,sum(menge) as MSEG_Quantity , (LGNUM||' - '||LGTYP ) as Warehouse_Bin, sum(cast( DMBTR as double)) AS mseg_val_abs_local from  {0}.{1} where bwart = 262  group by mandt,trim(leading '0' from aufnr) , werks,trim(leading '0' from matnr),meins,rspos,rsnum, (LGNUM||' - '||LGTYP ) ) ,confirm_mvmt as (select  bwart as mseg_movement_type,mandt as Client,trim(leading '0' from aufnr) as MSEG_PO, werks as MSEG_Plant,trim(leading '0' from matnr) as MSEG_Material,rspos as mseg_ItemNoOfDepReq,rsnum as mseg_NoOfDepReq,meins as MSEG_UOM,max(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Max_ConsumedDate,min(nvl(date_format(budat_mkpf,'y-MM-dd'),'')) as MSEG_Min_ConsumedDate, sum(menge) as MSEG_Quantity,  sum(cast( DMBTR as double)) AS mseg_val_abs_local ,(LGNUM||' - '||LGTYP ) as Warehouse_Bin from {0}.{1} where bwart = 261  group by mandt,bwart,trim(leading '0' from aufnr), werks,trim(leading '0' from matnr),meins,rspos,rsnum, (LGNUM||' - '||LGTYP )) select a.Client, a.MSEG_PO,a.MSEG_Plant,a.MSEG_Material,a.mseg_movement_type,c.MAKTG as mseg_desc,a.Warehouse_Bin,a.MSEG_UOM ,a.mseg_NoOfDepReq,a.mseg_ItemNoOfDepReq, a.MSEG_Max_ConsumedDate as MSEG_Max_PostingDate,a.MSEG_Min_ConsumedDate as MSEG_Min_PostingDate,b.MSEG_Max_ReversalDate, b.MSEG_Min_ReversalDate,cast(a.mseg_val_abs_local - nvl(b.mseg_val_abs_local,'0') as decimal(23,2)) as mseg_val_abs_local, cast(a.MSEG_Quantity - nvl(b.MSEG_Quantity,'0') as decimal(23,2)) as MSEG_Quantity from confirm_mvmt a left join cancelmovement b on ( a.Client = b.Client and a.MSEG_PO = b.MSEG_PO  and a.MSEG_Plant = b.MSEG_Plant and a.MSEG_Material = b.MSEG_Material and a.MSEG_UOM = b.MSEG_UOM and a.mseg_NoOfDepReq= b.mseg_NoOfDepReq and a.mseg_ItemNoOfDepReq = b.mseg_ItemNoOfDepReq  and a.Warehouse_Bin = b.Warehouse_Bin) inner join {0}.{2} c on (a.Client = c.mandt and a.MSEG_Material = trim(leading '0' from c.matnr) and c.spras = 'E' )" . format(db_bronze, tableName1,tableName2)

ok, df_MSEG = prepareDF(sqlStmt, tableName1, bAddPrefix, tablePrefix, bPrintInfo=True)

if (not ok):
  msg = '{} Error generating dataframe {}'.format(datetime.datetime.now(), tableName1)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName1))
  
print("Total no. of records from {} -> {}".format(tableName, df_MSEG.count()))  # 421028612

#df_MSEG.show()

# COMMAND ----------

df_MSEG.filter(col("mseg_NoOfDepReq")=="0401980657").display()
#display(df_MSEG)

# COMMAND ----------

# DBTITLE 1,Prep - Factory
tableName = "ocs_warehouse_location"
db_gold = "scm_gold_np"
bAddPrefix=False
tablePrefix = ""
sqlStmt = "Select factoryWarehousebin as Warehouse_Bin_factory, factorybindescription as Factory_Warehouse_Descr, factorylocation as Factory_by_Warehouse from {}.{} ".format(db_gold, tableName)

ok, dfWhseLoc = prepareDF(sqlStmt, tableName, bAddPrefix, tablePrefix, bPrintInfo=True)


if (not ok):
  msg = '{} Error generating dataframe {}'.format(datetime.datetime.now(), tableName)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName))
  
print("Total no. of records from {} -> {}".format(tableName, dfWhseLoc.count())) #183


# COMMAND ----------

# DBTITLE 1,Prep - T024 - Purchasing Group Desc
tableName = "T024"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""
sqlStmt = "Select cast(mandt as STRING) as t024mandt ,cast(ekgrp as String) as t024ekgrp,eknam from {}.{} ".format(db_bronze, tableName)

ok, dfPurgrp = prepareDF(sqlStmt, tableName, bAddPrefix, tablePrefix, bPrintInfo=True)


if (not ok):
  msg = '{} Error generating dataframe {}'.format(datetime.datetime.now(), tableName)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName))
  
#print("Total no. of records from {} -> {}".format(tableName, dfPurgrp.count())) -- 398

# COMMAND ----------

# DBTITLE 1,Prep - T024d - MRP Controller Desc
tableName = "T024d"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""
sqlStmt = "Select mandt,werks,dispo,dsnam from {}.{} ".format(db_bronze, tableName)

ok, dfMrp = prepareDF(sqlStmt, tableName, bAddPrefix, tablePrefix, bPrintInfo=True)


if (not ok):
  msg = '{} Error generating dataframe {}'.format(datetime.datetime.now(), tableName)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName))
  
print("Total no. of records from {} -> {}".format(tableName, dfMrp.count())) -- 2272

# COMMAND ----------

# DBTITLE 1,Prep MVKE - Product Hierarchy
tableName = "mvke"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""
sqlStmt = "select  distinct mandt as Client, trim(leading '0' from matnr) as Material_ID, prodh as Product_Hierarchy from  {}.{} where mandt = '010'  ".format(db_bronze, tableName)

ok, dfMVKE = prepareDF(sqlStmt, tableName, bAddPrefix, tablePrefix, bPrintInfo=True)


if (not ok):
  msg = '{} Error generating dataframe {}'.format(datetime.datetime.now(), tableName)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName))
  
print("Total no. of records from {} -> {}".format(tableName, dfMVKE.count()))  

# COMMAND ----------

# DBTITLE 1,Prep T179T - Product Hierarchy
tableName = "t179t"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""
sqlStmt = "select  a.mandt as Client,  a.prodh as Product_Hierarchy, case when substring(h2.ph2,0,1) == 4 then (case when h2.ph2 in ('4B','4C','4D','4F','4T')then 'OCS' else 'OFC' end)  else 'OCS'end  as Division, nvl(h2.ph2,'-') as ph2, nvl( h2.ph2_descr,'Not Available') as  ph2_descr, nvl( h3.ph3,'-') as  ph3, nvl( h3.ph3_descr,'Not Available') as  ph3_descr from {0}.{1} a LEFT OUTER JOIN (select nvl(prodh, '-') as ph2, nvl(vtext, 'Not Available')  as ph2_descr from {0}.{1}  where spras = 'E' and mandt = '010' and length(prodh) = 2 ) h2 on substr(a.prodh,0,2) = h2.ph2 LEFT OUTER JOIN (select nvl(prodh, '-') as ph3, nvl(vtext, 'Not Available')  as ph3_descr from {0}.{1} where spras = 'E' and mandt = '010' and length(prodh) = 3 ) h3 on substr(a.prodh,0,3) = h3.ph3 where a. spras = 'E' and a.mandt = '010'  ".format(db_bronze, tableName)

ok, dft179t = prepareDF(sqlStmt, tableName, bAddPrefix, tablePrefix, bPrintInfo=True)


if (not ok):
  msg = '{} Error generating dataframe {}'.format(datetime.datetime.now(), tableName)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName))
  
print("Total no. of records from {} -> {}".format(tableName, dft179t.count())) 

# COMMAND ----------

# DBTITLE 1,Prep - T179t & MVKE
dfPH = (dfMVKE.join(dft179t, ['Client', 'Product_Hierarchy' ], how = 'left_outer'))
print("Total no. of records from {} -> {}".format(tableName, dfPH.count())) 

# COMMAND ----------

# DBTITLE 1,Prep - Mseg & factory
df_mseg_factory = (df_MSEG.join(dfWhseLoc, (df_MSEG.Warehouse_Bin == dfWhseLoc.Warehouse_Bin_factory) , "left")
                   .drop("Warehouse_Bin_factory")
                   )
print("Total count of records inserted into the table: {}".format( df_mseg_factory.count()))  #  419497343

# COMMAND ----------

df_mseg_factory.filter(col("mseg_NoOfDepReq")=="0401980657").display()

# COMMAND ----------

# DBTITLE 1,Prep - MSEG Factory all Movements Inv Adjustments 
df_mseg_allfactory = (df_MSEG_ALL.join(dfWhseLoc, (df_MSEG_ALL.Warehouse_Bin == dfWhseLoc.Warehouse_Bin_factory) , "left")
                   .drop("Warehouse_Bin_factory")
                   )
print("Total count of records inserted into the table: {}".format( df_mseg_allfactory.count()))  #  419497343

# COMMAND ----------

df_mseg_allfactory.filter(col("MSEG_PO")=="9005209402").display()
#display(df_mseg_allfactory)

# COMMAND ----------

# DBTITLE 1,Prep ProfitCenter Details
tableName = "ocs_profitcenter_plant_mapping"
db_gold = "scm_gold_np"
bAddPrefix=False
tablePrefix = ""
sqlStmt = " select Profit_Center,profit_center_name, pl_local_defn, plm_mgmt_group, product_line_group,telecom_mgmt,attribute1, (case when business = 'NaN' then '' end) as business from {0}.{1}  ".format(db_gold,tableName )

ok, dfpft = prepareDF(sqlStmt, tableName, bAddPrefix, tablePrefix, bPrintInfo=True)


if (not ok):
  msg = '{} Error generating dataframe {}'.format(datetime.datetime.now(), tableName)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName))
  
print("Total no. of records from {} -> {}".format(tableName, dfpft.count())) 

# COMMAND ----------

# DBTITLE 1,Prep SAP Factory Mapping
tableName = "ocs_sap_factory_mapping"
db_gold = "scm_gold_np"
bAddPrefix=False
tablePrefix = ""
sqlStmt = " select plant,sap_factory,region  from {0}.{1}  ".format(db_gold,tableName )
 
ok, dfsapfact = prepareDF(sqlStmt, tableName, bAddPrefix, tablePrefix, bPrintInfo=True)
 
 
if (not ok):
  msg = '{} Error generating dataframe {}'.format(datetime.datetime.now(), tableName)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName))
  
print("Total no. of records from {} -> {}".format(tableName, dfsapfact.count())) 

# COMMAND ----------

# DBTITLE 1,Prep RY Factory Mapping
tableName = "ocs_ry_factory_mapping"
db_gold = "scm_gold_np"
bAddPrefix=False
tablePrefix = ""
sqlStmt = " select profit_center,ry_factory from {0}.{1}  ".format(db_gold,tableName )

ok, dfryfact = prepareDF(sqlStmt, tableName, bAddPrefix, tablePrefix, bPrintInfo=True)


if (not ok):
  msg = '{} Error generating dataframe {}'.format(datetime.datetime.now(), tableName)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName))
  
print("Total no. of records from {} -> {}".format(tableName, dfryfact.count())) 

# COMMAND ----------

# DBTITLE 1,Prep - MSEGAll , MARC, MARA WITH NO PO & NO Reserv
tableName1 = "marc"
tableName4 = "mara"
tableName5 = "makt"
tableName6 = "mseg"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""

# Added Date Filter and removed movement key
try:
  df_mseg_marc_mara_po = spark.sql (''' select distinct cast(a.mandt as string) as resb_marc_mandt,  nvl(date_format(f.budat_mkpf,'y-MM-dd'),'') as mseg_postingdate,trim(leading '0' from a.matnr) as resb_marc_matnr ,f.rsnum as resb_marc_NoOfReserv,f.rspos as resb_marc_ItemNoOfReserv,trim(leading '0' from f.aufnr) as resb_marc_poorder_trim, f.aufnr as resb_marc_poorder,a.werks as resb_marc_plant,cast(a.ekgrp as STRING) as resb_marc_pgr, a.dispo as resb_marc_MRPCn, a.ausss as resb_marc_ascrap,a.kausf as resb_marc_cscrap,a.prctr as resb_marc_profitcenter,d.PRDHA as resb_mara_ProductHierarchy,d.ZZSNR as resb_mara_catlogno, d.ZZSSN as resb_mara_condensedcatlogno, e.MAKTG as resb_mara_MaterialDesc,d.ZZFAS as resb_mara_FiberorLead,substr(case when length(d.EXTWG) <= 2 then '-' else d.EXTWG end,0,2) as resb_CommodityCode,case when length(d.EXTWG) <= 2 then '-' else d.EXTWG end as resb_Commodity , f.bwart as resb_movementkey,f.grund as resb_movement_reason from {0}.{1} a, {0}.{2} d, {0}.{3} e, {0}.{4} f where a.mandt = d.mandt  and f.mandt = e.mandt and e.mandt = a.mandt and a.matnr = d.matnr and a.matnr = e.matnr and f.matnr = a.matnr and a.werks = f.werks  and e.spras = 'E' and  f.bwart in (201, 202, 221, 222, 551, 552, 553, 554, 555, 556, 711, 712, 717, 718) and date_format( f.budat_mkpf, 'yyyyMMdd') >= '20210101'  and trim(f.rsnum) ='0000000000' and trim(f.rspos) = '0000'and trim(f.aufnr) == ''  '''.format(db_bronze, tableName1,tableName4,tableName5,tableName6))   
except Exception as e:
  df_mseg_marc_mara_po = "null"
  msg = "Error message ->\t {}".format(str(e)[0:250])

 
print("Total no. of records from  {}".format(df_mseg_marc_mara_po.count()))  # 76686
df_mseg_marc_mara_po.show()


# COMMAND ----------

df_mseg_marc_mara_po.filter(col("resb_marc_poorder")=="9005209402").display()

# COMMAND ----------

# DBTITLE 1,Prep - MSEGAll_W/WoAFKO , MARC, MARA
tableName1 = "marc"
tableName4 = "mara"
tableName5 = "makt"
tableName6 = "mseg"
tableName7 = "afko"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""

#Removed AFKO condition so that both In AFKO/NOT IN AFKO all picked
try:
  #df_mseg_marc_mara_all_naf = spark.sql (''' select distinct cast(a.mandt as string) as resb_marc_mandt,nvl(date_format(f.budat_mkpf,'y-MM-dd'),'') as mseg_postingdate,trim(leading '0' from a.matnr) as resb_marc_matnr ,f.rsnum as resb_marc_NoOfReserv,f.rspos as resb_marc_ItemNoOfReserv,f.aufnr as resb_marc_poorder,a.werks as resb_marc_plant,cast(a.ekgrp as STRING) as resb_marc_pgr, a.dispo as resb_marc_MRPCn, a.ausss as resb_marc_ascrap,a.kausf as resb_marc_cscrap,a.prctr as resb_marc_profitcenter,d.PRDHA as resb_mara_ProductHierarchy,d.ZZSNR as resb_mara_catlogno, d.ZZSSN as resb_mara_condensedcatlogno, e.MAKTG as resb_mara_MaterialDesc,d.ZZFAS as resb_mara_FiberorLead,substr(case when length(d.EXTWG) <= 2 then '-' else d.EXTWG end,0,2) as resb_CommodityCode,case when length(d.EXTWG) <= 2 then '-' else d.EXTWG end as resb_Commodity , f.bwart as resb_movementkey,f.grund as resb_movement_reason from {0}.{1} a, {0}.{2} d, {0}.{3} e, {0}.{4} f where a.mandt = d.mandt  and f.mandt = e.mandt and e.mandt = a.mandt and a.matnr = d.matnr and a.matnr = e.matnr and f.matnr = a.matnr and a.werks = f.werks  and e.spras = 'E' and  f.bwart in (201, 202, 261, 262, 551, 552, 711, 712) and date_format( f.budat_mkpf, 'yyyyMMdd') >= '20210101'  and trim(f.rsnum) ='0000000000' and trim(f.rspos) = '0000'and trim(f.aufnr) != '' and not exists (select 1 from  {0}.{5} b where b.mandt = f.mandt and b.aufnr = f.aufnr) '''.format(db_bronze, tableName1,tableName4,tableName5,tableName6,tableName7)) 
  df_mseg_marc_mara_all_naf = spark.sql (''' select distinct cast(a.mandt as string) as resb_marc_mandt,nvl(date_format(f.budat_mkpf,'y-MM-dd'),'') as mseg_postingdate,trim(leading '0' from a.matnr) as resb_marc_matnr ,f.rsnum as resb_marc_NoOfReserv,f.rspos as resb_marc_ItemNoOfReserv,trim(leading '0' from f.aufnr) as resb_marc_poorder_trim, f.aufnr as resb_marc_poorder,a.werks as resb_marc_plant,cast(a.ekgrp as STRING) as resb_marc_pgr, a.dispo as resb_marc_MRPCn, a.ausss as resb_marc_ascrap,a.kausf as resb_marc_cscrap,a.prctr as resb_marc_profitcenter,d.PRDHA as resb_mara_ProductHierarchy,d.ZZSNR as resb_mara_catlogno, d.ZZSSN as resb_mara_condensedcatlogno, e.MAKTG as resb_mara_MaterialDesc,d.ZZFAS as resb_mara_FiberorLead,substr(case when length(d.EXTWG) <= 2 then '-' else d.EXTWG end,0,2) as resb_CommodityCode,case when length(d.EXTWG) <= 2 then '-' else d.EXTWG end as resb_Commodity , f.bwart as resb_movementkey,f.grund as resb_movement_reason from {0}.{1} a, {0}.{2} d, {0}.{3} e, {0}.{4} f where a.mandt = d.mandt  and f.mandt = e.mandt and e.mandt = a.mandt and a.matnr = d.matnr and a.matnr = e.matnr and f.matnr = a.matnr and a.werks = f.werks  and e.spras = 'E' and  f.bwart in (201, 202, 261, 262, 551, 552, 711, 712) and date_format( f.budat_mkpf, 'yyyyMMdd') >= '20210101'  and trim(f.rsnum) ='0000000000' and trim(f.rspos) = '0000' and trim(f.aufnr) != '' '''.format(db_bronze, tableName1,tableName4,tableName5,tableName6))  
  
except Exception as e:
  df_mseg_marc_mara_all_naf = "null"
  msg = "Error message ->\t {}".format(str(e)[0:250])

 
print("Total no. of records from  {}".format(df_mseg_marc_mara_all_naf.count())) 
df_mseg_marc_mara_all_naf.show()

# COMMAND ----------

#df_mseg_marc_mara_all_naf.filter(col("resb_marc_matnr")=="TBF5027").display() #979735 NOT in AFKO ##TBF5027  in AFKO
df_mseg_marc_mara_all_naf.filter(col("resb_marc_poorder_trim")=="9005209402").display()

# COMMAND ----------

# DBTITLE 1,Merge DF MSEG_MARA_WITHPO and MSEG_MARA_WITHOUTPO
df_mseg_marc_mara_all = df_mseg_marc_mara_po.union(df_mseg_marc_mara_all_naf)

# COMMAND ----------

df_mseg_marc_mara_all.filter(col("resb_marc_poorder")=="009005209402").display() #979735 NOT in AFKO ##TBF5027  in AFKO

# COMMAND ----------

# DBTITLE 1,Join DF -MSEGALL , MARC,MARA
df_mseg_marc_mara_fn1 = (df_MSEG_ALL.join(df_mseg_marc_mara_all,(df_MSEG_ALL.Client == df_mseg_marc_mara_all.resb_marc_mandt )
                                & (df_MSEG_ALL.MSEG_Plant == df_mseg_marc_mara_all.resb_marc_plant )
                                & (df_MSEG_ALL.MSEG_PO == df_mseg_marc_mara_all.resb_marc_poorder_trim  )
                                & (df_MSEG_ALL.MSEG_Material == df_mseg_marc_mara_all.resb_marc_matnr ) 
                                & (df_MSEG_ALL.mseg_movement_type == df_mseg_marc_mara_all.resb_movementkey ) 
                                & (df_MSEG_ALL.mseg_movement_reason == df_mseg_marc_mara_all.resb_movement_reason )
                                & (df_MSEG_ALL.MSEG_Max_PostingDate == df_mseg_marc_mara_all.mseg_postingdate ),"inner" )     
                        .join(df_mseg_allfactory,(df_MSEG_ALL.Client== df_mseg_allfactory.Client  ) 
                                         & (df_MSEG_ALL.MSEG_Plant == df_mseg_allfactory.MSEG_Plant  ) 
                                         & (df_MSEG_ALL.MSEG_PO == df_mseg_allfactory.MSEG_PO  )
                                         & (df_MSEG_ALL.MSEG_Material == df_mseg_allfactory.MSEG_Material )
                                         & (df_MSEG_ALL.mseg_NoOfDepReq == df_mseg_allfactory.mseg_NoOfDepReq  )
                                         & (df_MSEG_ALL.mseg_ItemNoOfDepReq == df_mseg_allfactory.mseg_ItemNoOfDepReq  )
                                         & (df_MSEG_ALL.mseg_movement_type == df_mseg_allfactory.mseg_movement_type  )
                                         & (df_MSEG_ALL.mseg_movement_reason == df_mseg_allfactory.mseg_movement_reason )
                                         & (df_MSEG_ALL.MSEG_Max_PostingDate == df_mseg_allfactory.MSEG_Max_PostingDate ),"left") 
                    .drop(df_mseg_allfactory.Client)                        
                    .drop(df_mseg_allfactory.MSEG_Max_PostingDate)   
                    .drop(df_mseg_allfactory.MSEG_Min_PostingDate) 
                    .drop(df_mseg_allfactory.mseg_val_abs_local)                        
                    .drop(df_mseg_allfactory.MSEG_Quantity)                           
                    .drop(df_mseg_allfactory.Warehouse_Bin)                        
                    .drop(df_mseg_allfactory.MSEG_UOM)                          
                    .drop(df_mseg_allfactory.MSEG_Plant)                          
                    .drop(df_mseg_allfactory.MSEG_Material)  
                    .drop(df_mseg_allfactory.MSEG_PO )         
                    .drop(df_mseg_allfactory.mseg_movement_type)                          
                    .drop(df_mseg_allfactory.mseg_movement_reason)
 
                    ).distinct()
df_mseg_marc_mara_fn1.show()

# COMMAND ----------

df_mseg_marc_mara_fn1.filter(col("MSEG_PO")=="9005209402").display()

# COMMAND ----------

df_mseg_marc_mara_fn = (df_mseg_marc_mara_fn1.join(df_AFKO,(df_mseg_marc_mara_fn1.resb_marc_mandt ==df_AFKO.afko_client) 
                                               & (df_mseg_marc_mara_fn1.resb_marc_poorder == df_AFKO.afko_poorderno),"left") 
                        )

# COMMAND ----------

df_mseg_marc_mara_fn.filter(col("MSEG_PO")=="9005209402").display()

# COMMAND ----------

# DBTITLE 1,joining all DFs MSEGALL, Purchasingorg,MRP
df_mseg_marc_mara = (df_mseg_marc_mara_fn.join(dfPurgrp,(df_mseg_marc_mara_fn.resb_marc_mandt ==dfPurgrp.t024mandt) 
                                           & (df_mseg_marc_mara_fn.resb_marc_pgr == dfPurgrp.t024ekgrp),"left") 
                                       .join(dfMrp, (df_mseg_marc_mara_fn.resb_marc_mandt == dfMrp.mandt) 
                                           & (df_mseg_marc_mara_fn.resb_marc_plant == dfMrp.werks) 
                                           & (df_mseg_marc_mara_fn.resb_marc_MRPCn == dfMrp.dispo),"left")
                                  .join(dfPH,( df_mseg_marc_mara_fn.resb_marc_mandt  == dfPH.Client )
                                        & (df_mseg_marc_mara_fn.resb_marc_matnr == dfPH.Material_ID ),"left") 
                                  .join(dfsapfact,( df_mseg_marc_mara_fn.resb_marc_plant  == dfsapfact.plant ),"left")
                                  .join(dfryfact,( df_mseg_marc_mara_fn.resb_marc_profitcenter == dfryfact.profit_center ),"left")                           
                                  .withColumn("resb_marc_MRPName",dfMrp.dsnam)
                                  .withColumn("resb_marc_PGrDesc",dfPurgrp.eknam)
                                  .withColumn("resb_marc_Product_Hierarchy",dfPH.Product_Hierarchy)                         
                                  .withColumn("resb_marc_Division",dfPH.Division)                                                  
                                  .withColumn("resb_marc_PH2",dfPH.ph2)                                                                           
                                  .withColumn("resb_marc_PH2_Descr",dfPH.ph2_descr)  
                                  .withColumn("resb_marc_PH3",dfPH.ph3)                                                                           
                                  .withColumn("resb_marc_PH3_Descr",dfPH.ph3_descr)
                                  .withColumn("resb_marc_SAP_factory",dfsapfact.sap_factory)                         
                                  .withColumn("resb_marc_region",dfsapfact.region)        
                                  .withColumn("resb_marc_RY_factory",dfryfact.ry_factory)                           
                                  .withColumn("resb_marc_factory",when(col("resb_marc_SAP_factory").isin('RY'),col("resb_marc_RY_factory") ).otherwise(col("resb_marc_SAP_factory")))  
                                  .withColumn("plant_component_key", xxhash64(lit(df_mseg_marc_mara_fn.resb_marc_plant),lit(df_mseg_marc_mara_fn.resb_marc_matnr)) )  
                                  .withColumn("plant_material_key", xxhash64(lit(df_mseg_marc_mara_fn.resb_marc_plant),lit(df_mseg_marc_mara_fn.resb_marc_matnr)) )     
                                  .withColumn("load_date", current_timestamp())  
                                  .drop(df_mseg_marc_mara_fn.resb_mara_ProductHierarchy)
                                  .drop(df_MSEG_ALL.mseg_movement_type)
                                  .drop(dfPH.Client)
                                  .drop(dfPH.Material_ID)
                                  .drop(dfPH.Product_Hierarchy)
                                  .drop(dfPH.Division)
                                  .drop(dfPH.ph2)                         
                                  .drop(dfPH.ph2_descr)                             
                                  .drop(dfPH.ph3_descr)  
                                  .drop(dfPH.ph3)                          
                                  .drop(dfPurgrp.t024mandt)
                                  .drop(dfPurgrp.t024ekgrp)
                                  .drop(dfMrp.mandt)
                                  .drop(dfMrp.werks)
                                  .drop(dfMrp.dispo)    
                                  .drop(dfMrp.dsnam) 
                                  .drop(dfPurgrp.eknam)     
                                  .drop(dfsapfact.plant)
                                  .drop(dfryfact.profit_center)                           
                                       
                )
print("Total count of records inserted into the table: {}".format( df_mseg_marc_mara.count()))  #  77408113

# COMMAND ----------

df_mseg_marc_mara.filter(col("MSEG_Material") =="TBF5027").display()

# COMMAND ----------

# DBTITLE 1,Prep - RESB,Marc and Mara
tableName1 = "marc"
tableName4 = "mara"
tableName5 = "makt"
tableName6 = "resb"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""

# Added Date Filter and removed movement key
try:
  #df_resb_marc_mara = spark.sql (''' select distinct a.mandt as resb_marc_mandt,trim(leading '0' from a.matnr) as resb_marc_matnr ,f.rsnum as resb_marc_NoOfReserv,f.aufnr as resb_marc_poorder,a.werks as resb_marc_plant,a.ekgrp as resb_marc_pgr, a.dispo as resb_marc_MRPCn, a.ausss as resb_marc_ascrap,a.kausf as resb_marc_cscrap,a.prctr as resb_marc_profitcenter,d.PRDHA as resb_mara_ProductHierarchy,d.ZZSNR as resb_mara_catlogno, d.ZZSSN as resb_mara_condensedcatlogno, e.MAKTG as resb_mara_MaterialDesc,d.ZZFAS as resb_mara_FiberorLead,substr(case when length(d.EXTWG) <= 2 then '-' else d.EXTWG end,0,2) as resb_CommodityCode from {0}.{1} a, {0}.{4} d, {0}.{5} e, {0}.{6} f where a.mandt = d.mandt and f.mandt = e.mandt and e.mandt = a.mandt and a.matnr = d.matnr and a.matnr = e.matnr and f.matnr = a.matnr and a.werks = f.werks  and e.spras = 'E' and f.bwart in (261,262) '''.format(db_bronze, tableName1,tableName4,tableName5,tableName6)) 
  df_resb_marc_mara = spark.sql (''' select distinct cast(a.mandt as string) as resb_marc_mandt,trim(leading '0' from a.matnr) as resb_marc_matnr ,f.rsnum as resb_marc_NoOfReserv,f.rspos as resb_marc_ItemNoOfReserv,f.aufnr as resb_marc_poorder,a.werks as resb_marc_plant,cast(a.ekgrp as STRING) as resb_marc_pgr, a.dispo as resb_marc_MRPCn, a.ausss as resb_marc_ascrap,a.kausf as resb_marc_cscrap,a.prctr as resb_marc_profitcenter,d.PRDHA as resb_mara_ProductHierarchy,d.ZZSNR as resb_mara_catlogno, d.ZZSSN as resb_mara_condensedcatlogno, e.MAKTG as resb_mara_MaterialDesc,d.ZZFAS as resb_mara_FiberorLead,substr(case when length(d.EXTWG) <= 2 then '-' else d.EXTWG end,0,2) as resb_CommodityCode,case when length(d.EXTWG) <= 2 then '-' else d.EXTWG end as resb_Commodity, '' as resb_movement_reason from {0}.{1} a, {0}.{2} d, {0}.{3} e, {0}.{4} f where a.mandt = d.mandt  and f.mandt = e.mandt and e.mandt = a.mandt and a.matnr = d.matnr and a.matnr = e.matnr and f.matnr = a.matnr and a.werks = f.werks  and e.spras = 'E' and date_format( f.bdter, 'yyyyMMdd') >= '20210101' '''.format(db_bronze, tableName1,tableName4,tableName5,tableName6))   
except Exception as e:
  df_resb_marc_mara = "null"
  msg = "Error message ->\t {}".format(str(e)[0:250])

 
print("Total no. of records from  {}".format(df_resb_marc_mara.count()))  # 77408113


# COMMAND ----------

df_resb_marc_mara.filter(col("resb_marc_NoOfReserv")=="0401980657").display()
#display(df_resb_marc_mara)

# COMMAND ----------

# DBTITLE 1,Prep - RESB-Purchasing Grp Desc
df_resb_marc_mara_pur = (df_resb_marc_mara.join(dfPurgrp,(df_resb_marc_mara.resb_marc_mandt ==dfPurgrp.t024mandt) 
                                           & (df_resb_marc_mara.resb_marc_pgr == dfPurgrp.t024ekgrp),"left") 
                                       .join(dfMrp, (df_resb_marc_mara.resb_marc_mandt == dfMrp.mandt) 
                                           & (df_resb_marc_mara.resb_marc_plant == dfMrp.werks) 
                                           & (df_resb_marc_mara.resb_marc_MRPCn == dfMrp.dispo),"left")
                                  .join(dfPH,( df_resb_marc_mara.resb_marc_mandt  == dfPH.Client )
                                        & (df_resb_marc_mara.resb_marc_matnr == dfPH.Material_ID ),"left") 
                                  .join(dfsapfact,( df_resb_marc_mara.resb_marc_plant  == dfsapfact.plant ),"left")
                                  .join(dfryfact,( df_resb_marc_mara.resb_marc_profitcenter == dfryfact.profit_center ),"left")                           
                                  .withColumn("resb_marc_MRPName",dfMrp.dsnam)
                                  .withColumn("resb_marc_PGrDesc",dfPurgrp.eknam)
                                  .withColumn("resb_marc_Product_Hierarchy",dfPH.Product_Hierarchy)                         
                                  .withColumn("resb_marc_Division",dfPH.Division)                                                  
                                  .withColumn("resb_marc_PH2",dfPH.ph2)                                                                           
                                  .withColumn("resb_marc_PH2_Descr",dfPH.ph2_descr)  
                                  .withColumn("resb_marc_PH3",dfPH.ph3)                                                                           
                                  .withColumn("resb_marc_PH3_Descr",dfPH.ph3_descr)
                                  .withColumn("resb_marc_SAP_factory",dfsapfact.sap_factory)                         
                                  .withColumn("resb_marc_region",dfsapfact.region)        
                                  .withColumn("resb_marc_RY_factory",dfryfact.ry_factory)                           
                                  .withColumn("resb_marc_factory",when(col("resb_marc_SAP_factory").isin('RY'),col("resb_marc_RY_factory") ).otherwise(col("resb_marc_SAP_factory")))                           
                                  .drop(df_resb_marc_mara.resb_mara_ProductHierarchy)
                                  .drop(dfPH.Client)
                                  .drop(dfPH.Material_ID)
                                  .drop(dfPH.Product_Hierarchy)
                                  .drop(dfPH.Division)
                                  .drop(dfPH.ph2)                         
                                  .drop(dfPH.ph2_descr)                             
                                  .drop(dfPH.ph3_descr)  
                                  .drop(dfPH.ph3)                          
                                  .drop(dfPurgrp.t024mandt)
                                  .drop(dfPurgrp.t024ekgrp)
                                  .drop(dfMrp.mandt)
                                  .drop(dfMrp.werks)
                                  .drop(dfMrp.dispo)    
                                  .drop(dfMrp.dsnam) 
                                  .drop(dfPurgrp.eknam)     
                                  .drop(dfsapfact.plant)
                                  .drop(dfryfact.profit_center)                           
                                       
                )
print("Total count of records inserted into the table: {}".format( df_resb_marc_mara_pur.count()))  #  77408113

# COMMAND ----------

df_resb_marc_mara_pur.filter(col("resb_marc_NoOfReserv")=="0401980657").display()

# COMMAND ----------

# DBTITLE 1,Prep - AFKO, MARA and MARC
tableName1 = "marc"
tableName4 = "mara"
tableName5 = "makt"
tableName6 = "afko"
db_bronze = "c_ocs_ecc_pr"
bAddPrefix=False
tablePrefix = ""
#  df_afko_marc_mara = spark.sql(''' select distinct a.mandt as afko_marc_mandt,trim(leading '0' from a.matnr) as afko_marc_matnr ,a.werks as afko_marc_plant,f.aufnr as afko_marc_poorder,f.rsnum as afko_marc_NoOfReserv,a.ekgrp as afko_marc_pgr, a.dispo as afko_marc_MRPCn, c.Dsnam as afko_marc_PGrDesc,b.eknam as afko_marc_MRPName,a.ausss as afko_marc_ascrap,a.kausf as afko_marc_cscrap,a.prctr as afko_marc_profitcenter,d.PRDHA as afko_mara_ProductHierarchy,d.ZZSNR as afko_mara_catlogno, d.ZZSSN as afko_mara_condensedcatlogno, e.MAKTG as afko_mara_MaterialDesc,d.ZZFAS as afko_mara_FiberorLead, substr(case when length(d.EXTWG) <= 2 then '-' else d.EXTWG end,0,2) as afko_CommodityCode from {0}.{1} a, {0}.{2} b, {0}.{3} c,{0}.{4} d, {0}.{5} e, {0}.{6} f where a.mandt = b.mandt and b.mandt = c.mandt and a.dispo = c.dispo and a.werks = c.werks and a.ekgrp = b.ekgrp  and e.mandt = d.mandt and f.mandt =e.mandt  and e.matnr = d.matnr and a.matnr = d.matnr and f.STLBEZ = e.matnr and e.spras = 'E'  '''.format(db_bronze, tableName1,tableName2,tableName3,tableName4,tableName5,tableName6))
try:
  df_afko_marc_mara = spark.sql(''' select distinct a.mandt as afko_marc_mandt,trim(leading '0' from a.matnr) as afko_marc_matnr ,a.werks as afko_marc_plant,f.aufnr as afko_marc_poorder,f.rsnum as afko_marc_NoOfReserv,a.ekgrp as afko_marc_pgr, a.dispo as afko_marc_MRPCn, a.ausss as afko_marc_ascrap,a.kausf as afko_marc_cscrap,a.prctr as afko_marc_profitcenter,d.PRDHA as afko_mara_ProductHierarchy,d.ZZSNR as afko_mara_catlogno, d.ZZSSN as afko_mara_condensedcatlogno, e.MAKTG as afko_mara_MaterialDesc,d.ZZFAS as afko_mara_FiberorLead, substr(case when length(d.EXTWG) <= 2 then '-' else d.EXTWG end,0,2) as afko_CommodityCode,case when length(d.EXTWG) <= 2 then '-' else d.EXTWG end as afko_Commodity  from {0}.{1} a,{0}.{2} d, {0}.{3} e, {0}.{4} f where a.mandt = d.mandt  and e.mandt = d.mandt and f.mandt =e.mandt  and e.matnr = d.matnr and a.matnr = d.matnr and f.STLBEZ = e.matnr and e.spras = 'E' and date_format( f.GSTRI, 'yyyyMMdd') >= '20210101'   '''.format(db_bronze, tableName1,tableName4,tableName5,tableName6))
 
except Exception as e:
  df_afko_marc_mara = "null"
  msg = "Error message ->\t {}".format(str(e)[0:250])

print("Total no. of records from  {}".format(df_afko_marc_mara.count()))  #14964916


# COMMAND ----------

df_afko_marc_mara.filter(col("afko_marc_poorder")=="007000468357").display()
#display(df_afko_marc_mara)

# COMMAND ----------

# DBTITLE 1,Prep - AFKO - Purchasing grp desc
df_afko_marc_mara_pur = (df_afko_marc_mara.join(dfPurgrp, (df_afko_marc_mara.afko_marc_mandt == dfPurgrp.t024mandt) 
                                        & (df_afko_marc_mara.afko_marc_pgr == dfPurgrp.t024ekgrp),"left")
                                       .join(dfMrp, (df_afko_marc_mara.afko_marc_mandt == dfMrp.mandt) 
                                        & (df_afko_marc_mara.afko_marc_plant == dfMrp.werks) 
                                        & (df_afko_marc_mara.afko_marc_MRPCn == dfMrp.dispo),"left")
                            .join(dfPH,( df_afko_marc_mara.afko_marc_mandt  == dfPH.Client )
                                        & (df_afko_marc_mara.afko_marc_matnr == dfPH.Material_ID ),"left")   
                            .join(dfpft,( df_afko_marc_mara.afko_marc_profitcenter  == dfpft.Profit_Center ),"left")  
                                  .join(dfsapfact,( df_afko_marc_mara.afko_marc_plant  == dfsapfact.plant ),"left")
                                  .join(dfryfact,( df_afko_marc_mara.afko_marc_profitcenter == dfryfact.profit_center ),"left")                         
                                  .withColumn("afko_marc_MRPName",dfMrp.dsnam)
                                  .withColumn("afko_marc_PGrDesc",dfPurgrp.eknam)
                                  .withColumn("afko_marc_Product_Hierarchy",dfPH.Product_Hierarchy)                         
                                  .withColumn("afko_marc_Division",dfPH.Division)                                                  
                                  .withColumn("afko_marc_PH2",dfPH.ph2)                                                                           
                                  .withColumn("afko_marc_PH2_Descr",dfPH.ph2_descr)  
                                  .withColumn("afko_marc_PH3",dfPH.ph3)                                                                           
                                  .withColumn("afko_marc_PH3_Descr",dfPH.ph3_descr)   
                                  .withColumn("afko_marc_profit_center_name",dfpft.profit_center_name)                            
                                  .withColumn("afko_marc_pl_local_defn",dfpft.pl_local_defn)                                                     
                                  .withColumn("afko_marc_plm_mgmt_group",dfpft.plm_mgmt_group)                                                                              
                                  .withColumn("afko_marc_product_line_group",dfpft.product_line_group)                                                                         
                                  .withColumn("afko_marc_telecom_mgmt",dfpft.telecom_mgmt)   
                                  .withColumn("afko_marc_attribute1",dfpft.attribute1)                            
                                  .withColumn("afko_marc_business",dfpft.business)  
                                  .withColumn("afko_marc_SAP_factory",dfsapfact.sap_factory)                         
                                  .withColumn("afko_marc_region",dfsapfact.region)        
                                  .withColumn("afko_marc_RY_factory",dfryfact.ry_factory)                           
                                  .withColumn("afko_marc_factory",when(col("afko_marc_SAP_factory").isin('RY'),col("afko_marc_RY_factory") ).otherwise(col("afko_marc_SAP_factory")))                                           .drop (dfpft.Profit_Center)
                                  .drop(df_afko_marc_mara.afko_mara_ProductHierarchy)
                                  .drop(dfPH.Client)
                                  .drop(dfPH.Material_ID)
                                  .drop(dfPH.Product_Hierarchy)
                                  .drop(dfPH.Division)
                                  .drop(dfPH.ph2)                         
                                  .drop(dfPH.ph2_descr)                             
                                  .drop(dfPH.ph3_descr)  
                                  .drop(dfPH.ph3)                                                   
                                  .drop(dfPurgrp.t024mandt)
                                  .drop(dfPurgrp.t024ekgrp)
                                  .drop(dfMrp.mandt)
                                  .drop(dfMrp.werks)
                                  .drop(dfMrp.dispo) 
                                  .drop(dfMrp.dsnam) 
                                  .drop(dfsapfact.plant)                          
                                  .drop(dfPurgrp.eknam)                                                   
                                  .drop(dfryfact.profit_center)                          
                         
               )
print("Total count of records inserted into the table: {}".format( df_afko_marc_mara_pur.count()))  #  14964916

# COMMAND ----------

df_afko_marc_mara_pur.filter(col("afko_marc_poorder")=="000008038136").display()
#display(df_afko_marc_mara_pur)

# COMMAND ----------

 ## commenting BOM category , once Michal confirm uncomment if required
df_resb_afko = (df_RESB.join(df_AFKO, (df_AFKO.afko_poorderno == df_RESB.resb_poordernumber) 
                                        & (df_AFKO.afko_client == df_RESB.resb_client)
                                        & (df_AFKO.afko_NoOfreserv == df_RESB.resb_NoOfreserv), "inner")
                                       # & (df_AFKO.afko_bom_category == df_RESB.resb_BOMcat), "inner")
                        .join(df_mseg_factory,(df_RESB.resb_client== df_mseg_factory.Client  ) 
                                         & (df_RESB.resb_plant == df_mseg_factory.MSEG_Plant  ) 
                                         & (df_RESB.resb_poordernumber_trim == df_mseg_factory.MSEG_PO )
                                         & (df_RESB.resb_NoOfreserv == df_mseg_factory.mseg_NoOfDepReq  )
                                         & (df_RESB.ItemNoOfReserv == df_mseg_factory.mseg_ItemNoOfDepReq  )
                                         & (df_RESB.resb_movementkey == df_mseg_factory.mseg_movement_type  ),"left")                
                                       
                )
print("Total count of records inserted into the table: {}".format( df_resb_afko.count()))  #   93470815(include BOM)  102164436(WITHOUT BOM)

# COMMAND ----------

df_resb_afko.filter(col("resb_NoOfreserv")=="0401980657").display()
#display(df_resb_afko)

# COMMAND ----------

df_poorder_merge = (df_resb_afko.join(df_resb_marc_mara_pur ,(df_resb_marc_mara_pur.resb_marc_mandt == df_resb_afko.resb_client) 
                                        & (df_resb_marc_mara_pur.resb_marc_poorder == df_resb_afko.resb_poordernumber)
                                        & (df_resb_marc_mara_pur.resb_marc_matnr == df_resb_afko.resb_matnr_trim)
                                        & (df_resb_marc_mara_pur.resb_marc_plant == df_resb_afko.resb_plant)
                                        & (df_resb_marc_mara_pur.resb_marc_NoOfReserv == df_resb_afko.resb_NoOfreserv), "inner")   
                            .join(df_afko_marc_mara_pur, (df_afko_marc_mara_pur.afko_marc_mandt == df_resb_afko.resb_client) 
                                        & (df_afko_marc_mara_pur.afko_marc_poorder == df_resb_afko.resb_poordernumber)
                                        & (df_afko_marc_mara_pur.afko_marc_matnr == df_resb_afko.afko_material_no)
                                        & (df_afko_marc_mara_pur.afko_marc_NoOfReserv == df_resb_afko.resb_NoOfreserv) 
                                        & (df_afko_marc_mara_pur.afko_marc_plant == df_resb_afko.resb_plant),"inner")                       
                            .withColumn("ComponentValue", (df_resb_afko.RESB_price/ df_resb_afko.priceunit) * df_resb_afko.reqqty.cast('double') )
                            .withColumn("CScrap%", (df_resb_afko.RESB_CScrap/ 100 ) .cast('double'))     
                            .withColumn("Scrap%", df_resb_afko.afko_plpo_scrap/ 100 )                                         
                            .withColumn("plant_component_key", xxhash64(lit(df_resb_afko.resb_plant),lit(df_resb_afko.resb_matnr_trim)) )  
                            .withColumn("plant_material_key", xxhash64(lit(df_resb_afko.resb_plant),lit(df_resb_afko.afko_material_no)) )  
                            .withColumn("CScrapValue",((df_resb_afko.RESB_price/ df_resb_afko.priceunit) * df_resb_afko.reqqty) * (df_resb_afko.RESB_CScrap/ 100).cast('double'))
                            .withColumn("Theoretical_Consumption_Qty", df_resb_afko.AFKO_confirmedqty * (df_resb_afko.reqqty / df_resb_afko.AFKO_baseqty).cast('double')) 
                            .withColumn("Theoretical_Consumption_value", (df_resb_afko.AFKO_confirmedqty * (df_resb_afko.reqqty / df_resb_afko.AFKO_baseqty)) * (df_resb_afko.RESB_price / df_resb_afko.priceunit  ).cast('double'))                                 
                            .withColumn("load_date", current_timestamp())                    
                            .drop("afko_poorderno","afko_client","afko_NoOfreserv","afko_client","mseg_client","mseg_plant", "mseg_prodorder","mseg_NoOfDepReq","mseg_ItemNoOfDepReq","mseg_movement_type","afko_marc_mandt","resb_marc_mandt","resb_marc_plant","marc_pgr")               
                )

print("Total count of records inserted into the table: {}".format( df_poorder_merge.count())) #141194798

# COMMAND ----------

df_poorder_merge.filter(col("resb_poordernumber_trim")=="9004816296").display()
#df_poorder_merge.filter(col("resb_matnr_trim")=="323152").display()
#display(df_poorder_merge)

# COMMAND ----------

df_scrap_resb = df_poorder_merge.select(col("plant_component_key"),col("plant_material_key"),col("resb_client").alias("Client"),
                         col("resb_poordernumber_trim").alias("RESB_PO_OrderNo"),
                         col("resb_NoOfreserv").alias("RESB_Reserv_No"),
                         col("ItemNoOfReserv").alias("RESB_Item_Reserv_No"),
                         col("reqType").alias("ReqType"),
                         col("ComponentValue").cast('double'),
                         col("CScrap%").cast('double'),
                         col("CScrapValue").cast('double'), col("Scrap%").cast('double').alias("PLPO_Scrap%")  ,                                      
                         col("afko_material_no").alias("AFKO_MaterialID"),
                         col("resb_matnr_trim").alias("RESB_ComponentID") ,                
                         col("mseg_desc").alias("MSEG_Desc"),
                         col("MSEG_Max_PostingDate").cast('date'),col("MSEG_Min_PostingDate").cast('date'),     
                         col("basicstart").alias("AFKO_BasicStartDate"),
                         col("basicend").alias("AFKO_BasicEndDate"),
                         col("schedulestart").alias("AFKO_ScheduleStartDate"),                                          
                         col("scheduleend").alias("AFKO_ScheduleEndDate"),                                          
                         col("actualstart").alias("AFKO_ActualStartDate"),                                          
                         col("actualend").alias("AFKO_ActualEndDate"),                                          
                         col("confirmedactualend").alias("AFKO_ConfirmedActualEndDate"),    
                         col("tasklisttype").alias("AFKO_PLPO_TaskListType"),                                              
                         col("AFKO_baseqty").cast('double').alias("AFKO_BaseQty"), col("Theoretical_Consumption_Qty").cast('double') , 
                         (when(isnull(col('Theoretical_Consumption_value')),'0.00').otherwise(col('Theoretical_Consumption_value'))).cast('double').alias('Theoretical_Consumption_value'),  
                         col("AFKO_totalordqty").cast('double').alias("AFKO_TotalOrdQty"),col("AFKO_confirmedqty").cast('double'),
                         col("AFKO_Group").alias("AFKO_PLPO_Group"),col("AFKO_RoutingNo"),col("AFKO_TotScrapQty").cast('double'),
                         col("AFKO_ConfirmedScrap"),col("afko_bom_category"),col("AFKO_BOM"), col("AFKO_mrpcontroller"),col("AFKO_baseUOM"),
                         col("saledocno").alias("AFPO_Salesdocno"),col("salesitemno").alias("AFPO_Salesitemno"),col("catalog_number").alias("VBAP_Catalogno"),
                         col("prod_hier").alias("VBAP_ProdHierarchy"),col("NetValue").cast('double').alias("VBAP_Netvalue"),col("Numerator").alias("VBAP_Numerator"),
                         col("Denominator").alias("VBAP_Denominator"),
                         col("Netprice").cast('double').alias("VBAP_Netprice"),col("salesunit").cast('double').alias("VBAP_Salesunit"),
                         col("orderqty_salesunit").cast('double').alias("VBAP_Orderqty_Salesunit"),
                         col("RESB_CScrap").cast('double'), col("RESB_OPScrap").cast('double'), col("RESB_BOM"), col("RESB_price").cast('double'),col("RESB_phantomitem"), 
                         col("RESB_backflush"),                
                         col("materialgrpcd").alias("RESB_Material_GrpCd"),
                         col("Purchasinggrp").alias("RESB_Purchasinggrp"),
                         col("reservstatus").alias("RESB_Reserv_Status"),
                         col("resb_plant"),
                         col("storagelocation").alias("RESB_StorageLocation"),
                         col("batchnum").alias("RESB_Batch_Num"),
                         col("requirementdate").alias("RESB_Req_Date"),                              
                         col("reqqty").cast('double').alias("RESB_Req_Qty"), 
                         col("baseuom").alias("RESB_Base_UOM"),
                         col("creditdebitInd").alias("RESB_Credit_Debit_Ind"),
                         col("currency").alias("RESB_Currency"), 
                         col("qtywithdrawn").cast('double').alias("RESB_QtyWithdrawn"),
                         col("valuewithdrawn").cast('double').alias("RESB_ValueWithdrawn"),                            
                         col("plannedorder").alias("RESB_PlannedOrder"),    
                         col("resb_movementkey").alias("RESB_MovementKey"), col("resb_movement_reason"),
                         col("resb_BOMcat").alias("RESB_BOM_Category"), 
                         col("bomusage").alias("AFKO_BOMUsage"),                                                                                                                               
                         col("altBOM").alias("RESB_AltBOM"),
                         col("priceunit").cast('double').alias("RESB_PriceUnit"),                 
                         col("totpriceinlocal").cast('double').alias("RESB_TotPriceInLocal"), 
                         col("fixpriceinlocal").cast('double').alias("RESB_FixPriceInLocal"),                                             
                         col("dir_procurement_ind").alias("RESB_Dir_Procurement_Ind"), 
                         col("purchasingdocnum").alias("RESB_PurchasingDocNum"),   
                         (when(isnull(col('Mseg_Material')),'').otherwise(col('Mseg_Material'))).alias('MSEG_Matnr'),                
                         (when(isnull(col('MSEG_UOM')),'').otherwise(col('MSEG_UOM'))).alias("MSEG_UOM"),                                             
                         (when(isnull(col('MSEG_Quantity')),'0.00').otherwise(col('MSEG_Quantity'))).cast('double').alias("MSEG_Qty"),
                         (when(isnull(col('mseg_val_abs_local')),'0.00').otherwise(col('mseg_val_abs_local'))).cast('double').alias("MSEG_Val_Abs_local"),
                         col("resb_marc_matnr"),
                         col("resb_marc_MRPCn"),col("resb_marc_PGrDesc"),col("resb_marc_MRPName"),col("resb_marc_ascrap"),
                         col("resb_marc_cscrap"),col("resb_marc_profitcenter"),
col("resb_mara_catlogno"),col("resb_mara_condensedcatlogno"),col("resb_mara_MaterialDesc"),col("resb_mara_FiberorLead"),col("resb_CommodityCode"),col("resb_Commodity"),
col("resb_marc_Product_Hierarchy"),col("resb_marc_Division"),col("resb_marc_PH2"),col("resb_marc_PH2_Descr"),col("resb_marc_PH3"),col("resb_marc_PH3_Descr"),
                        col("resb_marc_SAP_factory"),col("resb_marc_region"),col("resb_marc_RY_factory"),col("resb_marc_factory"),                                           
                         col("afko_marc_matnr"),                                          
                         col("afko_marc_MRPCn"),col("afko_marc_PGrDesc"),col("afko_marc_MRPName"),col("afko_marc_ascrap"),
                         col("afko_marc_cscrap"),col("afko_marc_profitcenter"),col("afko_marc_Product_Hierarchy"),col("afko_marc_Division"),
                         col("afko_marc_PH2"),col("afko_marc_PH2_Descr"),col("afko_marc_PH3"),col("afko_marc_PH3_Descr"),
col("afko_mara_catlogno"),col("afko_mara_condensedcatlogno"),col("afko_mara_MaterialDesc"),col("afko_mara_FiberorLead"),col("afko_CommodityCode"),col("afko_Commodity"),                                                     col("Warehouse_Bin"),col("Factory_Warehouse_Descr") ,col("Factory_by_Warehouse")  ,
col("afko_marc_pl_local_defn"),col("afko_marc_plm_mgmt_group"),col("afko_marc_product_line_group"),col("afko_marc_telecom_mgmt"),   
                         col("afko_marc_attribute1") ,col("afko_marc_business"),col("afko_marc_SAP_factory"),col("afko_marc_region"),col("afko_marc_RY_factory"),col("afko_marc_factory"),                              col("load_date")).distinct()





# COMMAND ----------

df_scrap_resb.filter(col("RESB_PO_OrderNo")=="9004816296").display()

# COMMAND ----------

df_scrap_mseg = df_mseg_marc_mara.select(col("plant_component_key"),col("plant_material_key"),col("client").alias("Client"),
                          (when(isnull(col('MSEG_PO')),'').otherwise(col('MSEG_PO'))).alias("RESB_PO_OrderNo"),
                         col("resb_marc_NoOfReserv").alias("RESB_Reserv_No"),
                         col("resb_marc_ItemNoOfReserv").alias("RESB_Item_Reserv_No"),
                         lit('').alias("ReqType"),
                         lit('0.00').cast('double').alias("ComponentValue"),
                         lit('0.00').cast('double').alias("CScrap%"),
                         lit('0.00').cast('double').alias("CScrapValue"), lit('0.00').cast('double').alias("PLPO_Scrap%")  ,                                      
                         lit('').alias("AFKO_MaterialID"),
                         (when(isnull(col('Mseg_Material')),'').otherwise(col('Mseg_Material'))).alias("RESB_ComponentID") ,                
                         col("resb_mara_MaterialDesc").alias("MSEG_Desc"),
                         col("MSEG_Max_PostingDate").cast('date'),col("MSEG_Min_PostingDate").cast('date'),
                         (when(isnull(col('basicstart')),col("MSEG_Min_PostingDate")).otherwise(col('basicstart'))).alias("AFKO_BasicStartDate"), 
                         #(when((col('MSEG_PO') == ''),col("MSEG_Min_PostingDate")).otherwise(col('basicstart'))).cast('date').alias("AFKO_BasicStartDate"),
                         (when(isnull(col('basicend')),col("MSEG_Max_PostingDate")).otherwise(col('basicend'))).alias("AFKO_BasicEndDate"),                                          
                         #(when((col('MSEG_PO') == ''),col("MSEG_Max_PostingDate")).otherwise(col('basicend'))).cast('date').alias("AFKO_BasicEndDate"),
                         (when(isnull(col('schedulestart')),col("MSEG_Min_PostingDate")).otherwise(col('schedulestart'))).cast('date').alias("AFKO_ScheduleStartDate"), 
                         (when(isnull(col('actualstart')),col("MSEG_Max_PostingDate")).otherwise(col('scheduleend'))).cast('date').alias("AFKO_ScheduleEndDate"),                           
                         (when(isnull(col('schedulestart')),col("MSEG_Min_PostingDate")).otherwise(col('actualstart'))).cast('date').alias("AFKO_ActualStartDate"),                                                    (when(isnull(col('actualend')),col("MSEG_Max_PostingDate")).otherwise(col('actualend'))).cast('date').alias("AFKO_ActualEndDate"),     
                         (when(isnull(col('confirmedactualend')),col("MSEG_Max_PostingDate")).otherwise(col('confirmedactualend'))).cast('date').alias("AFKO_ConfirmedActualEndDate"),    
                         col("tasklisttype").alias("AFKO_PLPO_TaskListType"),                                              
                         col("AFKO_baseqty").cast('double').alias("AFKO_BaseQty"), lit('0.00').cast('double').alias("Theoretical_Consumption_Qty") , 
                         lit('0.00').cast('double').alias('Theoretical_Consumption_value'),  
                         col("AFKO_totalordqty").cast('double').alias("AFKO_TotalOrdQty"),col("AFKO_confirmedqty").cast('double').alias("AFKO_confirmedqty"),
                         col("AFKO_Group").alias("AFKO_PLPO_Group"),col("AFKO_RoutingNo").alias("AFKO_RoutingNo"),col("AFKO_TotScrapQty").cast('double').alias("AFKO_TotScrapQty"),
                         col("AFKO_ConfirmedScrap").cast('double').alias("AFKO_ConfirmedScrap"),col("afko_bom_category"),
                         col("AFKO_BOM"), col("AFKO_mrpcontroller"),col("AFKO_baseUOM"),
                         col("saledocno").alias("AFPO_Salesdocno"),col("salesitemno").alias("AFPO_Salesitemno"),col("catalog_number").alias("VBAP_Catalogno"),
                         col("prod_hier").alias("VBAP_ProdHierarchy"),col("NetValue").cast('double').alias("VBAP_Netvalue"),col("Numerator").alias("VBAP_Numerator"),
                         col("Denominator").alias("VBAP_Denominator"),
                         col("Netprice").cast('double').alias("VBAP_Netprice"),col("salesunit").cast('double').alias("VBAP_Salesunit"),
                         col("orderqty_salesunit").cast('double').alias("VBAP_Orderqty_Salesunit"),
                         lit('0').cast('double').alias("RESB_CScrap"), lit('0').cast('double').alias("RESB_OPScrap"), 
                         lit('').alias("RESB_BOM"), lit('0').cast('double').alias("RESB_price"),lit('').alias("RESB_phantomitem"), 
                         lit('').alias("RESB_backflush"),                
                         lit('').alias("RESB_Material_GrpCd"),
                         lit('').alias("RESB_Purchasinggrp"),
                         lit('').alias("RESB_Reserv_Status"),
                         col("MSEG_Plant").alias("resb_plant"),
                         lit('').alias("RESB_StorageLocation"),
                         lit('').alias("RESB_Batch_Num"),
                         lit('').alias("RESB_Req_Date"),                              
                         lit('0').cast('double').alias("RESB_Req_Qty"), 
                         lit('').alias("RESB_Base_UOM"),
                         lit('').alias("RESB_Credit_Debit_Ind"),
                         lit('').alias("RESB_Currency"), 
                         lit('0').cast('double').alias("RESB_QtyWithdrawn"),
                         lit('0').cast('double').alias("RESB_ValueWithdrawn"),                            
                         lit('').alias("RESB_PlannedOrder"),    
                         col("resb_movementkey").alias("RESB_MovementKey"), col("resb_movement_reason"),
                         lit('').alias("RESB_BOM_Category"), lit('').alias("AFKO_BOMUsage"),       
                         lit('').alias("RESB_AltBOM"),
                         lit('0').cast('double').alias("RESB_PriceUnit"),                 
                         lit('0').cast('double').alias("RESB_TotPriceInLocal"), 
                         lit('0').cast('double').alias("RESB_FixPriceInLocal"),                                             
                         lit('').alias("RESB_Dir_Procurement_Ind"), 
                         lit('').alias("RESB_PurchasingDocNum"),   
                         (when(isnull(col('Mseg_Material')),'').otherwise(col('Mseg_Material'))).alias('MSEG_Matnr'),                
                         col("MSEG_UOM"),                                             
                         (when(isnull(col('MSEG_Quantity')),'0.00').otherwise(col('MSEG_Quantity'))).cast('double').alias("MSEG_Qty"),
                         (when(isnull(col('mseg_val_abs_local')),'0.00').otherwise(col('mseg_val_abs_local'))).cast('double').alias("MSEG_Val_Abs_local"),
                         col("resb_marc_matnr"),
                         col("resb_marc_MRPCn"),col("resb_marc_PGrDesc"),col("resb_marc_MRPName"),lit('').alias("resb_marc_ascrap"),
                         lit('').alias("resb_marc_cscrap"),col("resb_marc_profitcenter"),
                         col("resb_mara_catlogno"),col("resb_mara_condensedcatlogno"),col("resb_mara_MaterialDesc"),
                         col("resb_mara_FiberorLead"),col("resb_CommodityCode"),col("resb_Commodity"),
                         col("resb_marc_Product_Hierarchy"),col("resb_marc_Division"),col("resb_marc_PH2"),col("resb_marc_PH2_Descr"),col("resb_marc_PH3"),col("resb_marc_PH3_Descr"),
                         col("resb_marc_SAP_factory"),col("resb_marc_region"),col("resb_marc_RY_factory"),col("resb_marc_factory"),
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_marc_matnr'))).alias("afko_marc_matnr"),                                         
                                                                 
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_marc_MRPCn'))).alias("afko_marc_MRPCn"),
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_marc_PGrDesc'))).alias("afko_marc_PGrDesc"),
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_marc_MRPName'))).alias("afko_marc_MRPName"),
                         lit('').alias("afko_marc_ascrap"),
                         lit('').alias("afko_marc_cscrap"),
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_marc_profitcenter'))).alias("afko_marc_profitcenter"),
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_marc_Product_Hierarchy'))).alias("afko_marc_Product_Hierarchy"),
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_marc_Division'))).alias("afko_marc_Division"),
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_marc_PH2'))).alias("afko_marc_PH2"),
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_marc_PH2_Descr'))).alias("afko_marc_PH2_Descr"),
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_marc_PH3'))).alias("afko_marc_PH3"),
						 (when(isnull(col('basicstart')),'').otherwise(col('resb_marc_PH3_Descr'))).alias("afko_marc_PH3_Descr"),
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_mara_catlogno'))).alias("afko_mara_catlogno"),
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_mara_condensedcatlogno'))).alias("afko_mara_condensedcatlogno"),
						 (when(isnull(col('basicstart')),'').otherwise(col('resb_mara_MaterialDesc'))).alias("afko_mara_MaterialDesc"),
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_mara_FiberorLead'))).alias("afko_mara_FiberorLead"),
						 (when(isnull(col('basicstart')),'').otherwise(col('resb_CommodityCode'))).alias("afko_CommodityCode"),
                         (when(isnull(col('basicstart')),'').otherwise(col('resb_Commodity'))).alias("afko_Commodity"),  
                         col("Warehouse_Bin"),col("Factory_Warehouse_Descr") ,col("Factory_by_Warehouse")  ,
                         lit('').alias("afko_marc_pl_local_defn"),lit('').alias("afko_marc_plm_mgmt_group"),lit('').alias("afko_marc_product_line_group"),
                         lit('').alias("afko_marc_telecom_mgmt"), lit('').alias("afko_marc_attribute1"),
                         (when(isnull(col('basicstart')),'').otherwise(col("resb_marc_Division"))).alias("afko_marc_business"),
                         (when(isnull(col('basicstart')),'').otherwise(col("resb_marc_SAP_factory"))).alias("afko_marc_SAP_factory"),
                         (when(isnull(col('basicstart')),'').otherwise(col("resb_marc_region"))).alias("afko_marc_region"),
						 (when(isnull(col('basicstart')),'').otherwise(col("resb_marc_RY_factory"))).alias("afko_marc_RY_factory"),
                         (when(isnull(col('basicstart')),'').otherwise(col("Factory_by_Warehouse"))).alias("afko_marc_factory"), 
						 col("load_date")).distinct()

# COMMAND ----------

df_scrap_mseg.filter(col("MSEG_PO")=="9005209402").display()

# COMMAND ----------

df_scrap_result =df_scrap_resb.union(df_scrap_mseg)

# COMMAND ----------

df_scrap_result.filter(col("RESB_PO_OrderNo")=="9004816296").display()

# COMMAND ----------


#display(df_scrap_result)

# COMMAND ----------

#dropDBDeltaTable("scm_gold_np", "r_ocs_scrap2", True, spark)
#dropDBDeltaTable("scm_gold_np", "r_ocs_scrap_test", True, spark)
#dropDBDeltaTable("scm_gold_np", "r_ocs_scrap_final", True, spark)
#dropDBDeltaTable("scm_gold_np", "r_ocs_scrap_rts_final", True, spark)
#dropDBDeltaTable("scm_gold_np", "r_ocs_scrap_rts_final_ordtotals", True, spark)


# COMMAND ----------


# write / overwrite table

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.formatCheck.enabled",False)
ok = createDBDeltaTable(db_gold, "r_ocs_scrap_final", databasePath, df_scrap_result, spark)

print('\nRecords loaded to the {} table ->  {}'.format("r_ocs_scrap_final", df_scrap_result.count()))

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.formatCheck.enabled",False)
ok = createDBDeltaTable(db_gold, "r_ocs_scrap_rts_final", databasePath2, df_RTS, spark)

print('\nRecords loaded to the {} table ->  {}'.format("r_ocs_scrap_rts_final", df_RTS.count()))

# COMMAND ----------

# DBTITLE 1,Prep - Create RTS Order Totals

tableName = "r_ocs_scrap_rts_final"
db_gold = "scm_gold_np"
bAddPrefix=False
tablePrefix = ""

sqlStmt = "with wipid as (select  client,mseg_po,mseg_plant,mseg_material,mseg_uom, count(1) as WIP_ID from {0}.{1} group by client,mseg_po,mseg_plant,mseg_material,mseg_uom) select distinct a.Client,a.MSEG_PO,a.MSEG_Plant,a.MSEG_Material,a.MSEG_UOM, sum(a.MSEG_Quantity) as MSEG_Quantity, WIP_ID, (case when WIP_ID > 1 then 'WIP' else '' end) as WIP_Status  from {0}.{1}  a , wipid b where a.client = b.client and a.mseg_po = b.mseg_po and a.mseg_plant = b.mseg_plant and a.mseg_material = b.mseg_material and a.mseg_uom = b.mseg_uom  group by a.Client,a.MSEG_PO,a.MSEG_Plant,a.MSEG_Material,a.MSEG_UOM,WIP_ID, (case when WIP_ID > 1 then 'WIP' else '' end) ".format(db_gold, tableName)

ok, df_RTS_OrderTot = prepareDF(sqlStmt, tableName, bAddPrefix, tablePrefix, bPrintInfo=True)


if (not ok):
  msg = '{} Error generating dataframe {}'.format(datetime.datetime.now(), tableName)
  print(msg)
  dbutils.notebook.exit('Error generating dataframe {}'.format(tableName))
  
print("Total no. of records from {} table -> {}".format(tableName, df_RTS_OrderTot.count()))  

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.formatCheck.enabled",False)
ok = createDBDeltaTable(db_gold, "r_ocs_scrap_rts_final_ordtotals", databasePath3, df_RTS_OrderTot, spark)
 
print('\nRecords loaded to the {} table ->  {}'.format("r_ocs_scrap_rts_final_ordtotals", df_RTS_OrderTot.count()))

# COMMAND ----------

# remove dataframe from memory
df_scrap_result.unpersist()
