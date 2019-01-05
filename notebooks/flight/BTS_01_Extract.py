# Databricks notebook source
# MAGIC %md
# MAGIC Download Airline On-Time data<br/>
# MAGIC Bureau of Transportation Statistics - On-Time : Reporting Carrier On-Time Performance (1987-present)<br/>
# MAGIC The data can be manually downloaded from here: <a href="https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236">TranStats</a>.

# COMMAND ----------

# MAGIC %sh
# MAGIC # This will create a folder to store the data
# MAGIC mkdir /dbfs/bts/data

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
# MAGIC     wget $downloadURL --output-document $downloadFILE
# MAGIC     
# MAGIC   done
# MAGIC done

# COMMAND ----------

# MAGIC %sh
# MAGIC # Next we can view the data that was downloaded
# MAGIC ls /dbfs/bts/data