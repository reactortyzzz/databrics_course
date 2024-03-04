# Databricks notebook source
client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demo@databricsformula1dl.dfs.core.windows.net/",
  mount_point = "/mnt/databricsformula1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(
    dbutils.fs.ls("/mnt/databricsformula1dl/demo")
)

# COMMAND ----------

display(
    spark.read.csv("/mnt/databricsformula1dl/demo/circuits.csv")
)

# COMMAND ----------

display(
    dbutils.fs.mounts()
)

# COMMAND ----------

## dbutils.fs.unmount("/mnt/databricsformula1dl/demo")
