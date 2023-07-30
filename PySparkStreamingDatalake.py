# Databricks notebook source
from pyspark.sql.types import *
import pyspark.sql.functions as F
from datetime import datetime as dt
import json



connectionString = "...."
ehConf = {}
ehConf[
    "eventhubs.connectionString"
] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
ehConf["eventhubs.consumerGroup"] = "..."

json_schema = StructType(
    [
        StructField("deviceID", IntegerType(), True),
        StructField("rpm", IntegerType(), True),
        StructField("angle", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("windspeed", IntegerType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("deviceTimestamp", StringType(), True),
        StructField("deviceDate", StringType(), True),
    ]
)


# COMMAND ----------

# Setup access to storage account 
storage_account = 'apodatalake'
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", '....')

# Setup storage locations for all data
ROOT_PATH = f"abfss://iot@{storage_account}.dfs.core.windows.net/"
BRONZE_PATH = ROOT_PATH + "bronze/"
SILVER_PATH = ROOT_PATH + "silver/"
GOLD_PATH = ROOT_PATH + "gold/"
CHECKPOINT_PATH = ROOT_PATH + "checkpoints/"


# Enable auto compaction and optimized writes in Delta
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")
spark.conf.set("spark.databricks.delta.formatCheck.enabled","false")


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS delta_table1_raw;
# MAGIC DROP TABLE IF EXISTS delta_table2_raw;
# MAGIC DROP TABLE IF EXISTS silver_table_1;
# MAGIC DROP TABLE IF EXISTS silver_table_2;
# MAGIC DROP TABLE IF EXISTS gold_table;

# COMMAND ----------

eventhub_stream = (
  spark.readStream.format("eventhubs")                                              
    .options(**ehConf)                                                               
    .load()                                                                          
    .withColumn('body', F.from_json(F.col('body').cast('string'), json_schema))       
    .select(F.col("body.deviceID"), F.col("body.rpm"), F.col("body.angle"), F.col("body.humidity"),F.col("body.windspeed"),F.col("body.temperature"),F.to_timestamp(F.col("body.deviceTimestamp"),'dd/MM/yyyy HH:mm:ss').alias("deviceTimestamp"),F.to_date(F.col("body.deviceDate"),"dd/MM/yyyy").alias("deviceDate"))
)
display(eventhub_stream)

# COMMAND ----------

#spark.conf.set("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH + "turbine_raw")
delta_table1_raw = (
  eventhub_stream                                       
    .select('deviceID','rpm','angle','deviceTimestamp','deviceDate')                             
    .writeStream.format('delta')                                                     
    .partitionBy('deviceDate')                                                             
    .option("checkpointLocation", CHECKPOINT_PATH + "delta_table1_raw")
    #.trigger(once=True)                 
    .start(BRONZE_PATH + "delta_table1_raw")                                              
)

delta_table2_raw = (
  eventhub_stream                          
    .select('deviceID','humidity','windspeed','temperature','deviceTimestamp','deviceDate') 
    .writeStream.format('delta')                                                     
    .partitionBy('deviceDate')                                                             
    .option("checkpointLocation", CHECKPOINT_PATH + "delta_table2_raw")
    #.trigger(once=True)                
    .start(BRONZE_PATH + "delta_table2_raw")                                              
)


# COMMAND ----------

spark.sql(f'CREATE TABLE IF NOT EXISTS delta_table1_raw USING DELTA LOCATION "{BRONZE_PATH + "delta_table1_raw"}"')
spark.sql(f'CREATE TABLE IF NOT EXISTS delta_table2_raw USING DELTA LOCATION "{BRONZE_PATH + "delta_table2_raw"}"')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_table1_raw

# COMMAND ----------

def merge_delta(incremental, target): 
  incremental.createOrReplaceTempView("incremental")
  
  try:
    # MERGE records into the target table using the specified join key
    incremental._jdf.sparkSession().sql(f"""
      MERGE INTO delta.`{target}` t
      USING incremental i
      ON  i.deviceID = t.deviceID
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
  except:
    # If the †arget table does not exist, create one
    incremental.write.format("delta").partitionBy("deviceDate").save(target)



# COMMAND ----------

silver_table_1 = (spark.readStream.format("delta").table("delta_table1_raw").
                  groupBy("deviceID","deviceDate",F.window("deviceTimestamp","30 minutes"))
                  .agg(F.avg("rpm").alias("avg_rpm"),F.avg("angle").alias("avg_angle"))
                  .writeStream
                  .foreachBatch(lambda i,b: merge_delta(i,SILVER_PATH+"silver_table_1"))
                  .outputMode("update")
                  .option('checkpointLocation',CHECKPOINT_PATH+'silver_table_1')
                 # .trigger(once=True)
                  .start()
)

# COMMAND ----------

silver_table_2 = (spark.readStream.format("delta").table("delta_table2_raw").
                  groupBy("deviceID","deviceDate",F.window("deviceTimestamp","30 minutes"))
                  .agg(F.avg("humidity").alias("avg_humidity"),F.avg("windspeed").alias("avg_windspeed"),F.avg("temperature").alias("avg_temperature"))
                  .writeStream
                  .foreachBatch(lambda i,t: merge_delta(i,SILVER_PATH+"silver_table_2"))
                  .outputMode("update")
                  .option('checkpointLocation',CHECKPOINT_PATH+'silver_table_2')
                  # .trigger(once=True)
                  .start()
)

# COMMAND ----------

spark.sql(f'CREATE TABLE IF NOT EXISTS silver_table_1 USING DELTA LOCATION "{SILVER_PATH + "silver_table_1"}"')
spark.sql(f'CREATE TABLE IF NOT EXISTS silver_table_2 USING DELTA LOCATION "{SILVER_PATH + "silver_table_2"}"')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.deviceID,avg_humidity,avg_temperature FROM silver_table_1 t JOIN silver_table_2 w ON (t.deviceID=w.deviceID) ORDER BY avg_temperature

# COMMAND ----------

silver_table_agg = spark.readStream.format('delta').option('ignoreChanges',True).table('silver_table_1')
silver_table_agg2 = spark.readStream.format('delta').option('ignoreChanges',True).table('silver_table_2')
table_enriched = silver_table_agg.alias("a").join(
    silver_table_agg2.alias("b"), silver_table_agg['deviceID'] == silver_table_agg2['deviceID']
).select("a.deviceID","a.deviceDate", "a.avg_rpm","a.avg_angle","b.avg_humidity","b.avg_windspeed","b.avg_temperature")
#table_enriched = silver_table_agg.join(silver_table_agg2, ['deviceID'])

gold_table = (
  table_enriched
    .selectExpr('deviceID','deviceDate','avg_rpm','avg_angle','avg_humidity','avg_windspeed','avg_temperature')
    .writeStream 
    .foreachBatch(lambda i, b: merge_delta(i, GOLD_PATH + "gold_table"))
    .option("checkpointLocation", CHECKPOINT_PATH + "gold_table")
    .outputMode("append")
    #.trigger(once=True)
    .start()
)

# COMMAND ----------

spark.sql(f'CREATE TABLE IF NOT EXISTS gold_table USING DELTA LOCATION "{GOLD_PATH + "gold_table"}"')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_table
