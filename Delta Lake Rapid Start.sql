-- Databricks notebook source
-- MAGIC %py
-- MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
-- MAGIC            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
-- MAGIC            "fs.azure.account.oauth2.client.id": "<application-id>",
-- MAGIC            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
-- MAGIC            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}
-- MAGIC 
-- MAGIC # Optionally, you can add <directory-name> to the source URI of your mount point.
-- MAGIC dbutils.fs.mount(
-- MAGIC   source = "abfss://ncsqldb@ncdatalake.dfs.core.windows.net",
-- MAGIC   mount_point = "/mnt/ncsqldb",
-- MAGIC   extra_configs = configs)

-- COMMAND ----------

create database if not exists dbacademy;

use dbacademy;

-- COMMAND ----------

set spark.sql.shuffle.partitions = 8

-- COMMAND ----------

Select * from health_tracker_data_2020_01 limit 1000

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC rm -r /dbacademy/DLRS/healthtracker/silver

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_silver;  -- ensures that if we run this again, it won't fail
                                                          
CREATE TABLE health_tracker_silver                        
USING PARQUET                                             
PARTITIONED BY (p_device_id)   -- column used to partition the data
LOCATION "/dbacademy/DLRS/healthtracker/silver"   -- location where the parquet files will be saved
AS (                                                      
  SELECT name,    -- query used to transform the raw data
         heartrate,                                       
         CAST(FROM_UNIXTIME(time) AS TIMESTAMP) AS time,  
         CAST(FROM_UNIXTIME(time) AS DATE) AS dte,        
         device_id AS p_device_id                         
  FROM health_tracker_data_2020_01   
)

-- COMMAND ----------

select count(*) from health_tracker_silver

-- COMMAND ----------

Describe Detail health_tracker_silver


-- COMMAND ----------

CONVERT TO DELTA 
  parquet.`/dbacademy/DLRS/healthtracker/silver` 
  PARTITIONED BY (p_device_id double)

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_silver;

CREATE TABLE health_tracker_silver
USING DELTA
LOCATION "/dbacademy/DLRS/healthtracker/silver"

-- COMMAND ----------

Describe Detail health_tracker_silver


-- COMMAND ----------

select count(*) from health_tracker_silver

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC 
-- MAGIC rm -r /dbacademy/DLRS/healthtracker/gold/health_tracker_user_analytics

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_user_analytics;

CREATE TABLE health_tracker_user_analytics
USING DELTA
LOCATION '/dbacademy/DLRS/healthtracker/gold/health_tracker_user_analytics'
AS (
  SELECT p_device_id, 
         AVG(heartrate) AS avg_heartrate,
         STD(heartrate) AS std_heartrate,
         MAX(heartrate) AS max_heartrate 
  FROM health_tracker_silver 
  GROUP BY p_device_id
)

-- COMMAND ----------

SELECT * FROM health_tracker_user_analytics

-- COMMAND ----------

INSERT INTO health_tracker_silver
SELECT name,
       heartrate,
       CAST(FROM_UNIXTIME(time) AS TIMESTAMP) AS time,
       CAST(FROM_UNIXTIME(time) AS DATE) AS dte,
       device_id as p_device_id
FROM health_tracker_data_2020_02

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 0

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

SELECT 
p_device_id, 
COUNT(*) 
FROM health_tracker_silver 

GROUP BY p_device_id

-- COMMAND ----------

SELECT * 
FROM health_tracker_silver 

WHERE p_device_id IN (3, 4)

-- COMMAND ----------

--Create Temporary View for Broken Readings

--First, we create a temporary view for the Broken Readings in the 
--health_tracker_silver table.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW broken_readings
AS (
  SELECT 
    COUNT(*) as broken_readings_count, 
    dte 
  FROM health_tracker_silver
  
  WHERE heartrate < 0 --> broken entry
  GROUP BY dte
  ORDER BY dte
)

-- COMMAND ----------

SELECT * 
FROM broken_readings

-- COMMAND ----------

SELECT SUM(broken_readings_count) 
FROM broken_readings

-- COMMAND ----------

/*
In the previous lesson, we identified two issues with the health_tracker_silver table:

There were 95 missing records

There were 67 records with broken readings

In this lesson, we will repair the table by modifying the health_tracker_silver table.

There are two patterns for modifying existing Delta tables. 

appending files to an existing directory of Delta files

merging a set of updates and insertions 
*/

-- COMMAND ----------

/*
Prepare Updates View

In order to repair the broken sensor readings (less than zero), 
we'll interpolate using the value recorded before and after for each device. 

The Spark SQL functions LAG and LEAD will make this a trivial calculation. 

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW updates 
AS (
  SELECT 
    name, 
    (prev_amt+next_amt)/2 AS heartrate, 
    time, 
    dte, 
    p_device_id
  FROM (
    SELECT *, 
    LAG(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS prev_amt, 
    LEAD(heartrate) OVER (PARTITION BY p_device_id, dte ORDER BY p_device_id, dte) AS next_amt 
    FROM health_tracker_silver
  ) 
  WHERE heartrate < 0
)

-- COMMAND ----------

DESCRIBE health_tracker_silver

-- COMMAND ----------

DESCRIBE updates

-- COMMAND ----------

SELECT COUNT(*) FROM updates

-- COMMAND ----------

/*
Prepare insert View

It turns out that our expectation of receiving the missing records late was correct. These records have subsequently been made available to us as the table health_tracker_data_2020_02_01.

In addition to updating the broken records, we wish to add this late-arriving data. We begin by preparing another temporary view with the appropriate transformations:

Use the FROM_UNIXTIME Spark SQL function and then cast the time column to type TIMESTAMP to replace the column time

Use the FROM_UNIXTIME Spark SQL function and then cast the time column to type DATE to create the column dte

We will write these values to a temporary view called inserts. This will be used later to upsert values into our health_tracker_silver Delta table.

These records will be the inserts of the upsert we will be performing. 
*/




-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW inserts 
AS (
    SELECT 
      name, 
      heartrate,
      CAST(FROM_UNIXTIME(time) AS timestamp) AS time,
      CAST(FROM_UNIXTIME(time) AS date) AS dte,
      device_id
      
    FROM health_tracker_data_2020_02_01
   )

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW upserts
AS (
    SELECT * FROM updates 
    UNION ALL 
    SELECT * FROM inserts
    )

-- COMMAND ----------

MERGE INTO health_tracker_silver                            -- the MERGE instruction is used to perform the upsert
USING upserts

ON health_tracker_silver.time = upserts.time AND        
   health_tracker_silver.p_device_id = upserts.p_device_id  -- ON is used to describe the MERGE condition
   
WHEN MATCHED THEN                                           -- WHEN MATCHED describes the update behavior
  UPDATE SET
  health_tracker_silver.heartrate = upserts.heartrate   
WHEN NOT MATCHED THEN                                       -- WHEN NOT MATCHED describes the insert behavior
  INSERT (name, heartrate, time, dte, p_device_id)              
  VALUES (name, heartrate, time, dte, p_device_id)

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 1

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

DESCRIBE HISTORY health_tracker_silver

-- COMMAND ----------

SELECT SUM(broken_readings_count) FROM broken_readings

-- COMMAND ----------

SELECT sum(broken_readings_count) FROM broken_readings WHERE dte < '2020-02-25'

-- COMMAND ----------

/*
Prepare New Upsert View

We will repair the new broken sensor readings (less than zero), again by interpolating using the value recorded before and after for each device. 

We will write these values directly to the temporary view upserts as there are no inserts to be performed at this time.
*/



-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW upserts
AS (
  SELECT * FROM updates
)

-- COMMAND ----------

SELECT COUNT(*) FROM upserts

-- COMMAND ----------

MERGE INTO health_tracker_silver                            -- the MERGE instruction is used to perform the upsert
USING upserts

ON health_tracker_silver.time = upserts.time AND        
   health_tracker_silver.p_device_id = upserts.p_device_id  -- ON is used to describe the MERGE condition
   
WHEN MATCHED THEN                                           -- WHEN MATCHED describes the update behavior
  UPDATE SET
  health_tracker_silver.heartrate = upserts.heartrate   
WHEN NOT MATCHED THEN                                       -- WHEN NOT MATCHED describes the insert behavior
  INSERT (name, heartrate, time, dte, p_device_id)              
  VALUES (name, heartrate, time, dte, p_device_id)

-- COMMAND ----------

SELECT SUM(broken_readings_count)FROM broken_readings

-- COMMAND ----------

DELETE FROM health_tracker_silver where p_device_id = 4

-- COMMAND ----------

DESCRIBE HISTORY health_tracker_silver

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 0

-- COMMAND ----------

/*
Time Travel, Version 2

When we time travel to Version 2, we see two fulls months of data, five device measurements, 24 hours a day for (31 + 29) days, or 7200 records. 

This operation was the first upsert. The operation is a MERGE with operation

*/

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 0

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 2

-- COMMAND ----------

/*
Time Travel, Version 3

Version 3 shows the same number of records as Version 2. This is because this operation was simply a repair of broken records. No new records were appended.

This operation was the second upsert. The operation is a MERGE with operation

*/

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 3

-- COMMAND ----------

/*
Time Travel Version 4

Version 4 is the most recent and reflects the fact that we just deleted all records associated with the IoT device with device_id, 4.

The operation is a DELETE with operation parameters:
*/


-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 4

-- COMMAND ----------

/*
View the files in the Transaction Log

The transaction record containing the history of this table is in the _delta_log directory within the /dbacademy/DLRS/healthtracker/silver directory containing all of the Delta files defining the health_tracker_silver table. We can view these files with an ls command issued to the Databricks File System. 

*7

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls /dbacademy/DLRS/healthtracker/silver/_delta_log

-- COMMAND ----------

/*
Prepare New upserts View

We prepare a view for upserting using Time Travel to recover the missing records.

Note that we have replaced the entire name column with the value NULL.

*/

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW upserts
AS (
  SELECT NULL AS name, heartrate, time, dte, p_device_id 
  FROM health_tracker_silver VERSION AS OF 3
  WHERE p_device_id = 4
)

-- COMMAND ----------

MERGE INTO health_tracker_silver                            -- the MERGE instruction is used to perform the upsert
USING upserts

ON health_tracker_silver.time = upserts.time AND        
   health_tracker_silver.p_device_id = upserts.p_device_id  -- ON is used to describe the MERGE condition
   
WHEN MATCHED THEN                                           -- WHEN MATCHED describes the update behavior
  UPDATE SET
  health_tracker_silver.heartrate = upserts.heartrate   
WHEN NOT MATCHED THEN                                       -- WHEN NOT MATCHED describes the insert behavior
  INSERT (name, heartrate, time, dte, p_device_id)              
  VALUES (name, heartrate, time, dte, p_device_id)

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

DESCRIBE HISTORY health_tracker_silver

-- COMMAND ----------

SELECT * FROM health_tracker_silver WHERE p_device_id = 4

-- COMMAND ----------

/*
Query an Earlier Table Version 

We query the health_tracker_silver table against an earlier version to demonstrate that it is still possible to retrieve the name associated with device 4.

*/


-- COMMAND ----------

SELECT * FROM health_tracker_silver VERSION AS OF 2 WHERE p_device_id = 4

-- COMMAND ----------

/*
Vacuum Table to Remove Old Files

The VACUUM Spark SQL command can be used to solve this problem. The VACUUM command recursively vacuums directories associated with the Delta table and removes files that are no longer in the latest state of the transaction log for that table and that are older than a retention threshold. The default threshold is 7 days. 

*/

-- COMMAND ----------

VACUUM health_tracker_silver RETAIN 0 Hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

SELECT * FROM health_tracker_silver VERSION AS OF 3 WHERE p_device_id = 4
