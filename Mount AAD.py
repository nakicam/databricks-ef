# Databricks notebook source
# MAGIC %python
# MAGIC environment = "ncdatalake"
# MAGIC new_container = "ncsqldb"

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://ncsqldb@ncdatalake.dfs.core.windows.net",
  mount_point = "/mnt/ncsqldb",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('/mnt/ncsqldb')
