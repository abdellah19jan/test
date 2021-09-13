# Databricks notebook source
# MAGIC %run /Repos/HH6011/test/api

# COMMAND ----------

load_omega("GI076512", list(from = ymd("2021-01-01"), to = ymd("2021-01-05")))

# COMMAND ----------

load_wattson("GI076512", "GRDF", list(from = ymd("2021-01-01"), to = ymd("2021-01-05")))

# COMMAND ----------

dic <- spark_read_parquet(spark_connect(method = "databricks"),
                          path = "s3://cdh-ovcteemexploratorydev-382109/ovc_teem_exploratory_output/profiler-gas/input/dic_temp_moy.parquet"
) %>% as_tibble()

load_temp_moy("75114001", ymd("2021-01-01"), ymd("2021-01-05"), dic)

# COMMAND ----------

load_cor_clim()
