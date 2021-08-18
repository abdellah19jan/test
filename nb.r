# Databricks notebook source
options(tidyverse.quiet = TRUE)
library(tidyverse)

# COMMAND ----------

# MAGIC %md
# MAGIC # Working directory

# COMMAND ----------

getwd()

# COMMAND ----------

setwd("/dbfs/FileStore/shared_uploads/HH6011")

# COMMAND ----------

list.files("./")

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

# MAGIC %md
# MAGIC # CSV

# COMMAND ----------

read_csv("test.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # Read RDS

# COMMAND ----------

read_rds("1970_01_01.rds")

# COMMAND ----------

# MAGIC %md
# MAGIC # Run notebook

# COMMAND ----------

# MAGIC %run /Repos/HH6011/test/fct

# COMMAND ----------

fct(2, 3)

# COMMAND ----------

list.files("/.")
