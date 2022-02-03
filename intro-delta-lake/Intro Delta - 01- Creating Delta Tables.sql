-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC There's not much code you need to write to create a table with Delta! 
-- MAGIC 
-- MAGIC We need: 
-- MAGIC - A `CREATE` statement
-- MAGIC - A table name (below we use `test`)
-- MAGIC - A schema (below we use the name `firstcol` and give it a datatype of `int`)
-- MAGIC - The statement USING DELTA

-- COMMAND ----------

CREATE TABLE test (firstcol int) USING DELTA;

-- In Databricks Runtime 8.0 and above, USING DELTA is optional. See the note below.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Note 🧐: In Databricks Runtime 8.0 and above the USING clause is optional. If you don’t specify the USING clause, DELTA is the default format. In Databricks Runtime 7.x, when you don’t specify the USING clause, the SQL parser uses the CREATE TABLE with Hive format syntax to parse it. See the Databricks Runtime 8.0 migration guide for details.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now go back and run that cell again. Something interesting will happen - it will error out. This is expected - because the table exists already, we receive an error.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC We can add in an additional argument, `IF NOT EXISTS` which checks if the table exists. This will overcome our error!

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS test (firstcol int) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The `IF NOT EXISTS` checks for the existence of our `test` table. 
-- MAGIC 
-- MAGIC Now that we've seen some basics, let's try reading a Delta table from a source. Many times, we will want to ingest data from a filepath. 
-- MAGIC 
-- MAGIC We're going to ingest some data from a CSV file.

-- COMMAND ----------

CREATE TABLE student (id INT, name STRING, age INT) USING CSV;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC How about another ingest source. What if we have a table already created in our database? We can read the table into Delta.

-- COMMAND ----------

CREATE TABLE "airbnb_demo_int_delta_db.preds_staging" (
id STRING
batch_date STRING
price DOUBLE
predicted DOUBLE) 
USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Recall that there are two ways to create tables: from the metastore and not. Assuming a table is in the metastore, you can specify a location and Delta inherits the schema.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS events
  USING DELTA
  LOCATION '/mnt/delta/events';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Once that table is created, we can view its contents

-- COMMAND ----------

SELECT * FROM events; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC No results?! Looks like we'll need to add some records into the table. Open up the inserts notebook to learn more.

-- COMMAND ----------

-- MAGIC %md What if the table wasn't registered in the metastore already? Let's take a directory of Parquet files and read into Delta.
-- MAGIC 
-- MAGIC Note: we're simplifying here for demostration purposes. We'd first want to mount a data store like S3 or ADLS to a directory called `intro-data`.

-- COMMAND ----------

CREATE TABLE customers USING PARQUET OPTIONS (path '/intro-data/')
CONVERT TO DELTA customers


-- COMMAND ----------

-- MAGIC %md What about another common use case: create table as (CTAS) statements? I'll create a table and insert some values first:

-- COMMAND ----------

CREATE TABLE students (name VARCHAR(64), street_address VARCHAR(64), student_id INT)
  USING DELTA 

-- COMMAND ----------

INSERT INTO students VALUES
    ('Issac Newton', 'Main Ave', 3145),
    ('Ada Lovelace', 'Not Main Ave', 2718);

-- COMMAND ----------

-- MAGIC %md Now, we could create a new table:

-- COMMAND ----------

CREATE TABLE main_street AS 
SELECT * FROM students
WHERE street_address = 'Main Ave'

-- COMMAND ----------

SELECT * 
FROM main_street

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Bonus! 
-- MAGIC Check out what's inside of a Delta Transaction log. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os, pprint
-- MAGIC path = "/dbfs/user/kevin.coyle@databricks.com/gym/mac_logs/_delta_log/00000000000000000002.json" 
-- MAGIC with open(path, "r") as file:
-- MAGIC   text = file.read()
-- MAGIC pprint.pprint(text)b

-- COMMAND ----------

-- Let's clean up the tables we created!
DROP TABLE test;

-- COMMAND ----------

DROP TABLE students;

-- COMMAND ----------

DROP TABLE main_street
