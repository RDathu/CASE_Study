# Databricks notebook source
blobkey=dbutils.secrets.get(scope = "csbb6", key = "blobkey") 

# COMMAND ----------

import json
spark.conf.set("fs.azure.account.key.csb6.blob.core.windows.net",blobkey)
dbutils.fs.mkdirs("/FileStore/Processing/")
dbutils.fs.cp("wasbs://csb@csb6.blob.core.windows.net/raw_files/result.json","/FileStore/Processing/")
#dbutils.fs.ls("/FileStore/Processing/")
with open("/dbfs/FileStore/Processing/result.json") as f:
    sample=json.load( f)

# COMMAND ----------

import json
spark.conf.set("fs.azure.account.key.csb6.blob.core.windows.net","AkUeIntcfErY+8MTosal8yM+y52HCFpl3SCg2a3DD2Hc//Whh6Rjk0OAr+Af9DQyf2Sq2PY/wslw+AStAsjf2Q==")
dbutils.fs.mkdirs("/FileStore/Processing/")
dbutils.fs.cp("wasbs://csb@csb6.blob.core.windows.net/raw_files/result.json","/FileStore/Processing/")
#dbutils.fs.ls("/FileStore/Processing/")
with open("/dbfs/FileStore/Processing/result.json") as f:
    sample=json.load( f)

# COMMAND ----------

raw_data=spark.createDataFrame(sample['records'])

# COMMAND ----------

URI=dbutils.secrets.get(scope="csbb6",key="cosmosuri")
PrimaryKey=dbutils.secrets.get(scope="csbb6",key="cosmosprimarykey")

# COMMAND ----------

cosmosEndpoint = dbutils.secrets.get(scope="csbb6",key="cosmosuri")
cosmosMasterKey =dbutils.secrets.get(scope="csbb6",key="cosmosprimarykey")
cosmosDatabaseName = "case_study"
cosmosContainerName = "raw_data"

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
}

# COMMAND ----------

raw_data.write\
   .format("cosmos.oltp")\
   .options(**cfg)\
   .mode("APPEND")\
   .save()

# COMMAND ----------


