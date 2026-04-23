# Databricks notebook source
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

# Step 6: Check storage access
dbutils.fs.ls("abfss://bronze@storagedatapractice.dfs.core.windows.net/")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@storagedatapractice.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %md 
# MAGIC #data reading

# COMMAND ----------

# MAGIC %md 
# MAGIC ##importing libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md 
# MAGIC ###reading csv data
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #trip type data

# COMMAND ----------

df_trip_type = spark.read.format('csv')\
                    .option('inferSchema' , True)\
                    .option('header' , True)\
                    .load('abfss://bronze@storagedatapractice.dfs.core.windows.net/trip_type')

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##trip_zone

# COMMAND ----------

df_trip_zone = spark.read.format('csv')\
                    .option('inferSchema' , True)\
                    .option('header' , True)\
                    .load('abfss://bronze@storagedatapractice.dfs.core.windows.net/trip_zone')

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip = spark.read.format('csv')\
              .option('inferschema' , True)\
              .option('header' , True)\
              .option('recursiveFilelookup' , True)\
              .load('abfss://bronze@storagedatapractice.dfs.core.windows.net/tripspracticedata/')

# COMMAND ----------

myschema = '''
account_id:string
account_age_days:string
credit_limit:string
home_country:string
risk_score:string
is_high_risk:string
avg_txn_amount:string
avg_monthly_txns:string
has_2fa:string
account_type:string
total_transactions:string
total_amount:string
avg_amount:double
max_amount:double
fraud_count:double
fraud_amount:double
pct_foreign:double
avg_velocity:double
unique_countries:double
unique_categories:double
avg_ip_risk:double
fraud_rate:double
'''

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###data transformation

# COMMAND ----------

# MAGIC %md 
# MAGIC **Taxi trip type**

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format('parquet')\
            .mode('append')\
            .option('path' , 'abfss://silver@storagedatapractice.dfs.core.windows.net/trip_type')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip_zone**

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip_zone.withColumn("zone1", split(col('zone_name'), "/")[0]).display()

# COMMAND ----------

df_trip_zone = df_trip_zone.withColumn("zone1",split(col("zone_name"), "/")[0])\
                           .withColumn("zone2",split(col("zone_name"), "/")[1])
df_trip_zone.display()

# COMMAND ----------

df_trip_zone.write.format('parquet')\
            .mode('append')\
            .option('path', 'abfss://silver@storagedatapractice.dfs.core.windows.net/trip_zone')\
            .save()

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.select('max_amount' , "avg_ip_risk" , 'unique_countries' , "account_id")
df_trip.display()

# COMMAND ----------

df_trip.write.format('parquet')\
             .mode('append')\
             .option('path', 'abfss://silver@storagedatapractice.dfs.core.windows.net/trip')\
             .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC **analysis**

# COMMAND ----------

display(df_trip)

# COMMAND ----------

