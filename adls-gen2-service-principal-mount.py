# Databricks notebook source
# DBTITLE 1,Configure authentication for mounting
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "<application-id>",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="nckeyvault",key="<service-credential-key-name>"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# COMMAND ----------

# DBTITLE 1,Mount filesystem
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Read Databricks Dataset IoT Devices JSON
df = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")

# COMMAND ----------

# DBTITLE 1,Write IoT Devices JSON
df.write.json("/mnt/<mount-name>/iot_devices.json")

# COMMAND ----------

# DBTITLE 1,List filesystem
dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/")

# COMMAND ----------

# DBTITLE 1,Read IoT Devices JSON from ADLS Gen2 filesystem
df2 = spark.read.json("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/iot_devices.json")
display(df2)

# COMMAND ----------

# DBTITLE 1,List mount
dbutils.fs.ls("/mnt/<mount-name>")

# COMMAND ----------

# DBTITLE 1,Read IoT Devices JSON from mount
df2 = spark.read.json("/mnt/<mount-name>/iot_devices.json")
display(df2)

# COMMAND ----------

# DBTITLE 1,Unmount filesystem
dbutils.fs.unmount("/mnt/<mount-name>") 
