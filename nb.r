# Databricks notebook source
# MAGIC %md
# MAGIC # Main

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function

# COMMAND ----------

g <- function(x, y) {
  
  x - y
  
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Application

# COMMAND ----------

g(3, 2)

# COMMAND ----------

# MAGIC %md
# MAGIC # Test

# COMMAND ----------

# MAGIC %md
# MAGIC ## Csv

# COMMAND ----------

library(sparklyr)

# COMMAND ----------

sc <- spark_connect("local", method = "databricks")

# COMMAND ----------

df <- spark_read_csv(sc, path = "dbfs:/FileStore/shared_uploads/HH6011/test.csv")

# COMMAND ----------

df

# COMMAND ----------

spark_disconnect(sc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working directory

# COMMAND ----------

getwd()

# COMMAND ----------

# MAGIC %md
# MAGIC dir.create("/tmp/Rserv/conn3589/profiler-gas")
# MAGIC 
# MAGIC dir.create("/tmp/Rserv/conn3589/profiler-gas/input")
# MAGIC 
# MAGIC dir.create("/tmp/Rserv/conn3589/profiler-gas/model")
# MAGIC 
# MAGIC dir.create("/tmp/Rserv/conn3589/profiler-gas/run")
# MAGIC 
# MAGIC dir.create("/tmp/Rserv/conn3589/profiler-gas/run/input")
# MAGIC 
# MAGIC dir.create("/tmp/Rserv/conn3589/profiler-gas/run/cc")
# MAGIC 
# MAGIC dir.create("/tmp/Rserv/conn3589/profiler-gas/run/cc/bt")
# MAGIC 
# MAGIC dir.create("/tmp/Rserv/conn3589/profiler-gas/run/cc/raw")

# COMMAND ----------

list.files("/tmp/Rserv/conn3589")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run notebook

# COMMAND ----------

# MAGIC %run /Repos/HH6011/test/fct

# COMMAND ----------

fct(2, 3)
