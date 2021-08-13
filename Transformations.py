# Databricks notebook source
# MAGIC %md 
# MAGIC **Goal:** Read data using Apache Spark and transform the data and write it into output path provided by parameter 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define Widgets/ Parameters
# MAGIC Creating widgets for leveraging parameters to be passed from Azure Data Factory

# COMMAND ----------

dbutils.widgets.text("output", "","") 
dbutils.widgets.get("output")

dbutils.widgets.text("filename", "","") 
dbutils.widgets.get("filename")

dbutils.widgets.text("pipelineRunId", "","") 
dbutils.widgets.get("pipelineRunId")

# COMMAND ----------

#Supply storageName and accessKey values
storageName = "{storageaccountname}"
accessKey = "{storageaccountaccesskey}"

try:
  
  dbutils.fs.unmount('/mnt/adfdata')
  dbutils.fs.mount(
    source = "wasbs://sinkdata@"+storageName+".blob.core.windows.net/",
    mount_point = "/mnt/adfdata",
    extra_configs = {"fs.azure.account.key."+storageName+".blob.core.windows.net":
                     accessKey})
except Exception as e:
  import re
  result = re.findall(r"^\s*Caused by:\s*\S+:\s*(.*)$", e.message, flags=re.MULTILINE)
  if result:
    print(result[-1]) 
  else:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC You can check to see what's in the mount by using `%fs ls <mount name>` - For Interactive debugging  

# COMMAND ----------

# MAGIC %fs ls /mnt/adfdata

# COMMAND ----------

# MAGIC %md
# MAGIC ####![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) Instructions
# MAGIC 0. Start with the file **"Product.csv"**
# MAGIC 0. Apply the necessary transformations and actions to return a DataFrame which satisfies these requirements:
# MAGIC 
# MAGIC   a. Select just the `product_id`, `category`, `brand`, `model`, `size` and `price` columns
# MAGIC   
# MAGIC   b. Rename `product_id` to `prodID`
# MAGIC   
# MAGIC   c. Sort by the `price` column in descending order
# MAGIC    
# MAGIC 0. Write the transformed DataFrame in CSV or parquet format to **"dbfs:/mnt/adfdata/processed_sink/Products__{DataFactoryPipelineRunId}/"** path.

# COMMAND ----------

# Create DataFrame and cleanse the data

from pyspark.sql.functions import desc

inputFile = "dbfs:/mnt/adfdata/"+getArgument("filename")
initialDF = (spark.read           # The DataFrameReader
  .option("header", "true")       # Use first line of all files as header
  .option("inferSchema", "true")  # Automatically infer data types
  .csv(inputFile)                 # Creates a DataFrame from CSV after reading in the file
)

finalDF = (initialDF
  .select("product_id", "category", "brand", "model", "size", "price")
  .withColumnRenamed("product_id", "prodID")
  .sort(desc("price"))
)

display(finalDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the **output** to **CSV** or Parquet

# COMMAND ----------

#Removing Extension from filename
import os
file = os.path.splitext(getArgument("filename"))[0]
print(file)

# write the output into parquet or csv.
finalDF.coalesce(1).write.mode("overwrite").option("header", "true").csv("dbfs:/mnt/adfdata"+getArgument("output")+"/"+file+"_"+getArgument("pipelineRunId")+"/csv") 
