# Databricks notebook source
lag_shift <- as.numeric(dbutils.widgets.get("lag"))
lag_shift

# COMMAND ----------

# MAGIC %md # Training data for Advanced Airtime project
# MAGIC 
# MAGIC This notebokk illustrate the pipeline that use in model training. All the dependencies functions has been kept in `./func/`.
# MAGIC 
# MAGIC This training pipeline consist of steps as below...
# MAGIC 1. Data labeling : Get the label for supervised learning
# MAGIC 2. Data preparation : Get all the relevance features in multiple data sources
# MAGIC 3. Feature enhancing : Derive features has been made to represent time series
# MAGIC 4. Data cleansing : Impute missing values and regroup the values
# MAGIC 5. Model training : Training model by H20 lib

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

# MAGIC %md ## 1: Data labeling step

# COMMAND ----------

# DBTITLE 1,Loading labeling function
# MAGIC %run ./func/01_labeling_func

# COMMAND ----------

response <- labeling_data(month_lag=1 + lag_shift)

# COMMAND ----------

sdf_dim(response)

# COMMAND ----------

glimpse(response)

# COMMAND ----------

# MAGIC %md ## 2: Data preparation step

# COMMAND ----------

# MAGIC %run ./func/02_Preparation_func

# COMMAND ----------

dt_prep <- prep_data(response, lag_time = 4 + lag_shift, training = T)

# COMMAND ----------

sdf_dim(dt_prep)

# COMMAND ----------

glimpse(dt_prep)

# COMMAND ----------

# MAGIC %md ## 3: Feature enhancing step

# COMMAND ----------

dt_prep_enh <- feat_enhance(dt_prep, his_time = 6 + lag_shift)

# COMMAND ----------

sdf_dim(dt_prep_enh)

# COMMAND ----------

# MAGIC %md ## 4: Data cleansing step

# COMMAND ----------

# DBTITLE 1,Imputing and regroup
dt_cleansed <- dt_cleansing(dt_prep_enh, training = T)

# COMMAND ----------

# DBTITLE 1,Derive features
dt_final <- feat_derive(dt_cleansed)

# COMMAND ----------

sdf_dim(dt_final)

# COMMAND ----------

# DBTITLE 1,Writing final table
spark_write_table(dt_final, "mck_aa.te01_pre_ea_training", mode = "overwrite")

# COMMAND ----------

dt_final <- tbl(sc, "mck_aa.te01_pre_ea_training")

# COMMAND ----------

glimpse(dt_final)

# COMMAND ----------

# MAGIC %md ## 5: Model training

# COMMAND ----------

library(h2o)
library(rsparkling)

# COMMAND ----------

library(mlflow)
install_mlflow()

# COMMAND ----------

# MAGIC %run ./func/03_Model_func

# COMMAND ----------

# DBTITLE 1,Export model path
model_path <- "/dbfs/mnt/cvm02/user/pitchaym/h2o_model_aa/"

# COMMAND ----------

# ML flow start log
mlflow_start_run()

best_xg <- model_training(dt_final, lag_time = 4 + lag_shift)

# ML flow end log
mlflow_end_run()

# COMMAND ----------

display(h2o.varimp(best_xg))

# COMMAND ----------

# MAGIC %sh rm /dbfs/mnt/cvm02/user/pitchaym/h2o_model_aa/*

# COMMAND ----------

h2o.saveModel(best_xg, model_path, force = T)