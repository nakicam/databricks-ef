-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Time Travel

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Time travel gives us the ability to roll back to previous versions of our table. Let's start our by creating our table. 

-- COMMAND ----------

CREATE TABLE students (name VARCHAR(64), address VARCHAR(64), student_id INT)
    USING DELTA PARTITIONED BY (student_id);

-- COMMAND ----------

INSERT INTO students VALUES
    ('Issac Newton', '123 Main Ave, Woolsthorpe', 3145),
    ('Ada Lovelace', '321 Main Ave, London', 2718);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can use a query an old snapshot of a table using time travel. Time travel is a specialized read on our dataset which allows us to read previous versions of our data. There are several ways to go about this. 
-- MAGIC 
-- MAGIC The `@` symbol can be used with a version number, aliased to `v#`, like the syntax below.
-- MAGIC 
-- MAGIC The `@` symbol can also be used with a version number or a datestamp in the format: `yyyyMMddHHmmssSSS`

-- COMMAND ----------

SELECT * FROM students@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can can also use `VERSION AS OF` to select a previous version:

-- COMMAND ----------

SELECT * FROM students VERSION AS OF 1

-- COMMAND ----------

-- Clean up
DROP TABLE students

-- COMMAND ----------


