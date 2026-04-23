# Databricks notebook source
# MAGIC %md 
# MAGIC ###data reading and writing and creating delta tables  
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ##data access

# COMMAND ----------

# MAGIC %md
# MAGIC **database creation**

# COMMAND ----------

# Step 1: Set authentication type
spark.conf.set(
  "fs.azure.account.auth.type.storagedatapractice.dfs.core.windows.net",
  "OAuth"
)

# Step 2: Set OAuth provider
spark.conf.set(
  "fs.azure.account.oauth.provider.type.storagedatapractice.dfs.core.windows.net",
  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

# Step 3: Client ID
spark.conf.set(
  "fs.azure.account.oauth2.client.id.storagedatapractice.dfs.core.windows.net",
  "f6bd8b68-65f8-4a00-9d39-b38202b8c162"
)

# Step 4: Client Secret
spark.conf.set(
  "fs.azure.account.oauth2.client.secret.storagedatapractice.dfs.core.windows.net",
  "ZC28Q~j2TWNacs6.D79fNdRpnL2LrleWELl.Yc82"
)

# Step 5: Tenant endpoint (VERY IMPORTANT - Global Azure)
spark.conf.set(
  "fs.azure.account.oauth2.client.endpoint.storagedatapractice.dfs.core.windows.net",
  "https://login.microsoftonline.com/7a0d33ee-43e9-4d9b-82c7-68e74f439418/oauth2/token"
)



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold

# COMMAND ----------

# MAGIC %md 
# MAGIC ###to import libraries

# COMMAND ----------

from pyspark.sql.functions import*
from pyspark.sql.functions import*

# COMMAND ----------

# MAGIC %md
# MAGIC **storage variables**

# COMMAND ----------

silver = 'abfss://silver@storagedatapractice.dfs.core.windows.net'
gold = 'abfss://gold@storagedatapractice.dfs.core.windows.net'

# COMMAND ----------

# MAGIC %md 
# MAGIC data zone

# COMMAND ----------

df_zone_name = spark.read.format('parquet')\
                     .option('inferschema' , True)\
                     .option('header' , True)\
                     .load(f'{silver}/trip_zone')

# COMMAND ----------

df_zone_name.display()
        

# COMMAND ----------

df_zone_name.write.format('delta') \
    .mode('overwrite') \
    .saveAsTable('trip_zone')


# COMMAND ----------

# MAGIC %sql
# MAGIC select *from gold.trip_zone
# MAGIC where zone_name = 'Midtown'

# COMMAND ----------

# MAGIC %md
# MAGIC trip type

# COMMAND ----------

df_type = spark.read.format('parquet')\
                     .option('inferschema' , True)\
                     .option('header' , True)\
                     .load(f'{silver}/trip_type')


# COMMAND ----------

dbutils.fs.rm(
    f"{silver}/trip_type/part-00000-tid-5743727273250091185-ae8b1916-8bc9-4f4d-9d93-37f756714794-17-1-c000.csv",
    True
)

# COMMAND ----------

display(dbutils.fs.ls(f'{silver}/trip_type'))

# COMMAND ----------

df_type.write.format('delta') \
    .mode('overwrite') \
    .saveAsTable('gold.trip_type')

# COMMAND ----------

# MAGIC %md 
# MAGIC trips data

# COMMAND ----------

df_trip = spark.read.format('parquet')\
                     .option('inferschema' , True)\
                     .option('header' , True)\
                     .load(f'{silver}/trip')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.write.format('delta') \
    .mode('append') \
    .saveAsTable('gold.trip')

# COMMAND ----------

# MAGIC %md 
# MAGIC # Learning Delta Lake

# COMMAND ----------

# MAGIC %md 
# MAGIC **versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE gold.trip_zone 
# MAGIC SET zone_name = 'SoHo' where zone_id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM trip_zone 
# MAGIC WHERE zone_id = 1

# COMMAND ----------

# MAGIC %md 
# MAGIC **versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.trip_zone

# COMMAND ----------

# MAGIC %md 
# MAGIC time **travel**

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE  gold.trip_zone to VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %md 
# MAGIC # delta tables

# COMMAND ----------

# MAGIC %md 
# MAGIC ** trip type**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from delta.`abfss://gold@storagedatapractice.dfs.core.windows.net/trip_type`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_type
# MAGIC     

# COMMAND ----------

# MAGIC %md 
# MAGIC ### trip data 2023

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from gold.trip

# COMMAND ----------

