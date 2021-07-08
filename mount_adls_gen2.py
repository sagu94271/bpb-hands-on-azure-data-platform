# Configurations to connect to ADLS Gen2
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "<appId>",
       "fs.azure.account.oauth2.client.secret": "<clientSecret>",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant>/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

# Mount ADLS Gen2
dbutils.fs.mount(
source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/input",
mount_point = "/mnt/transactiondata",
extra_configs = configs)

# List files available in the mount point
dbutils.fs.ls("/mnt/transactiondata")

# create a data frame to read data.
txDF = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/transactiondata/*.csv")

# read the  csv file and write the output to parquet format
txDF.write.mode("append").parquet("/mnt/transactiondata/")
print("Done")

# Check Number of transactions in the database
print("Number of transactions in the database: ", txDF.count())
