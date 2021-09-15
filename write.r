# Databricks notebook source
library(jsonlite)
library(sparklyr)

x   <- runif(100)
y   <- x ^ 2 + runif(100)
z   <- x ^ 3 + 2 * runif(100)
mod <- tibble::tibble(zet = c("ZET04", "ZET06"), model = list(mgcv::gam(y ~ s(x)), mgcv::gam(z ~ s(x))))
tbl <- tibble::tibble(line = as.character(serializeJSON(mod)))
sc  <- spark_connect(method = "databricks")

spark_write_text(copy_to(sc, tbl), "s3://cdh-ovcteemexploratorydev-382109/ovc_teem_exploratory_output/raw/mod.txt")

# COMMAND ----------

x <- spark_read_text(sc, path = "s3://cdh-ovcteemexploratorydev-382109/ovc_teem_exploratory_output/raw/mod.txt")

unserializeJSON(sdf_read_column(x, "line"))
