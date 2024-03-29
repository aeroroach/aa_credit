# Databricks notebook source
lag_shift <- as.numeric(dbutils.widgets.get("lag"))

# Production
# dbutils.widgets.remove("lag")
# lag_shift <- 0 

lag_shift

# COMMAND ----------

# DBTITLE 1,Delay Time
delay <- function(x)
{
    p1 <- proc.time()
    Sys.sleep(x)
    proc.time() - p1 # The cpu usage should be negligible
}
delay(240)

# COMMAND ----------

# MAGIC %md # Scoring data for Advanced Airtime project
# MAGIC 
# MAGIC This notebokk illustrate the pipeline that use in model scoring. All the dependencies functions has been kept in `./func/`.
# MAGIC 
# MAGIC This training pipeline consist of steps as below...
# MAGIC 1. Data preparation : Get all the relevance features in multiple data sources
# MAGIC 2. Feature enhancing : Derive features has been made to represent time series
# MAGIC 3. Data cleansing : Impute missing values and regroup the values
# MAGIC 4. Model training : Scoring by latest saved model (H2O)

# COMMAND ----------

# DBTITLE 1,Library loading
library(dplyr)
library(lubridate)
library(sparklyr)
library(readr)

# COMMAND ----------

# DBTITLE 1,Initiate spark connection
sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md ## 1: Datamart generation step

# COMMAND ----------

# DBTITLE 1,Run latest aggregation table
# MAGIC %run ./func/04_Datamart_gen_func

# COMMAND ----------

usage_gen(lag_time=1 + lag_shift)

# COMMAND ----------

# MAGIC %md ## 2: Data preparation step

# COMMAND ----------

# MAGIC %run ./func/02_Preparation_func

# COMMAND ----------

dt_prep <- prep_data(df_input = NULL, lag_time = 1 + lag_shift, training = F)

# COMMAND ----------

sdf_dim(dt_prep)

# COMMAND ----------

# MAGIC %md ## 3: Feature enhancing step

# COMMAND ----------

dt_prep_enh <- feat_enhance(dt_prep, his_time = 3 + lag_shift)

# COMMAND ----------

sdf_dim(dt_prep_enh)

# COMMAND ----------

# MAGIC %md ## 4: Data cleansing step

# COMMAND ----------

dt_cleansed <- dt_cleansing(dt_prep_enh, training = F)

# COMMAND ----------

dt_final <- feat_derive(dt_cleansed)

# COMMAND ----------

sdf_dim(dt_final)

# COMMAND ----------

spark_write_table(dt_final, "mck_aa.te02_pre_ea_scoring", mode = "overwrite")

# COMMAND ----------

dt_final <- tbl(sc, "mck_aa.te02_pre_ea_scoring")

# COMMAND ----------

glimpse(dt_final)

# COMMAND ----------

# MAGIC %md ## 5: Model training

# COMMAND ----------

library(h2o)
library(rsparkling)

# COMMAND ----------

# MAGIC %run ./func/03_Model_func

# COMMAND ----------

h2o_context(sc)

# COMMAND ----------

model_scoring(dt_final)

# COMMAND ----------

# MAGIC %fs ls /mnt/cvm02/cvm_output/MCK/AA/CAMPAIGN/h2o/

# COMMAND ----------

# MAGIC %md ## 6: Export files

# COMMAND ----------

list_export(lag_time = 1 + lag_shift, new = F)