# Databricks notebook source
# MAGIC %md Download Airline On-Time data<br/>
# MAGIC Bureau of Transportation Statistics - On-Time : Reporting Carrier On-Time Performance (1987-present)<br/>
# MAGIC The data can be manually downloaded from here: <a href="https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236">TranStats</a>.

# COMMAND ----------

# MAGIC %sh
# MAGIC # This will create a folder to store the data
# MAGIC mkdir /dbfs/bts/data

# COMMAND ----------

import sys
import requests

url="http://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_"
folder="/dbfs/bts/data/"
for year in range(2011,2013):
 for month in range(1,13):
  downloadURL=url+str(year)+"_"+str(month)+".zip"
  downloadFILE=folder+str(year)+"_"+str(month)+".zip"
  print(str(year)+"_"+str(month))
  print(downloadURL)
  print(downloadFILE)
  r = requests.get(downloadURL, stream=True)
  with open(downloadFILE, 'wb') as f:
   for chunk in r.iter_content(chunk_size=1024): 
    if chunk:
     f.write(chunk)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ### This process will download the files and takes about 15 minutes ###
# MAGIC 
# MAGIC url="https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_"
# MAGIC folder="/dbfs/bts/data/"
# MAGIC 
# MAGIC for year in {2011..2012}
# MAGIC do
# MAGIC   for month in {1..12}
# MAGIC   do
# MAGIC     downloadURL=$url$year"_"$month".zip"
# MAGIC     downloadFILE=$folder$year"_"$month".zip"
# MAGIC     echo $year $month $folder $downloadURL $downloadFILE
# MAGIC     
# MAGIC     wget "$downloadURL" --output-document $downloadFILE
# MAGIC     
# MAGIC   done
# MAGIC done

# COMMAND ----------

# MAGIC %sh
# MAGIC # Next we can view the data that was downloaded
# MAGIC ls /dbfs/bts/data

# COMMAND ----------

# MAGIC %md ADB - Azure Storage Account - blob, mnt to dbfs

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://<your-container-name>@<your-storage-account-name>.blob.core.windows.net",
  mount_point = "/mnt/<mount-name>",
  extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------

storage_account_access_key = "1jSZYSx7QCoY3GpHCPBHW5HbThzICFm8iNnrbENYutRbxNfHnVL6eXHjF0b6mT9SV+WLUvvYa8CunYm56A/zPA=="
storage_account_name = "fwk"

spark.conf.set(
  "fs.azure.account.key." + storage_account_name + ".blob.core.windows.net",
  storage_account_access_key
)

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

file_location = "wasbs://traffic@fwk.blob.core.windows.net/DOT_H2_2017.csv"
schema = StructType([
  StructField("dt", TimestampType(),True),
  StructField("link_name", StringType(),True),
  StructField("borough", StringType(),True),
  StructField("owner", StringType(),True),
  StructField("id", IntegerType(),True),
  StructField("travel_time", IntegerType(),True),
  StructField("speed", DoubleType(),True)  
])

schema

df = spark.read.format("csv").schema(schema).option("header","true").load(file_location)


# COMMAND ----------

import zipfile
import io

def zip_extract(x):
    in_memory_data = io.BytesIO(x[1])
    file_obj = zipfile.ZipFile(in_memory_data, "r")
    files = [i for i in file_obj.namelist()]
    return dict(zip(files, [file_obj.open(file).read() for file in files]))


zips = sc.binaryFiles("hdfs:/Testing/*.zip")
files_data = zips.map(zip_extract).collect()