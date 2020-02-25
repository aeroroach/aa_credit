# Databricks notebook source
# MAGIC %md
# MAGIC ### Model function
# MAGIC 
# MAGIC Model function utilized H2O library. It's mandatory to attached the cluster that prepared for H2O.
# MAGIC This notebook consist with list of functions below.
# MAGIC 1. `model_training()` : Train the model by H2O XGboost
# MAGIC 2. `model_scoring()` : Scoring the model from the model object
# MAGIC 3. `list_export()` : Export the list for campaign

# COMMAND ----------

model_training <- function (dt_input, sample_frac = 0.1) {
  
  dt_input %>%
  sdf_sample(fraction = sample_frac, replacement = F) -> dt_down
  
  dt_hex <- as_h2o_frame(sc, dt_down)
  
  # Casting ---------------------
  # Label
  dt_hex$bad_flag <- as.factor(dt_hex$bad_flag)

  # Predictor
  dt_hex$mnp_flag <- as.factor(dt_hex$mnp_flag)
  dt_hex$gender <- as.factor(dt_hex$gender)
  dt_hex$handset_os <- as.factor(dt_hex$handset_os)
  dt_hex$serenade <- as.factor(dt_hex$serenade)
  dt_hex$top30_province <- as.factor(dt_hex$top30_province)
  
  # Define relevance variables
  var_list <- h2o.names(dt_hex)
  ommit_var <- c(-1:-4)
  var_model <- var_list[ommit_var]
  
  # Splitting data frame
  dt.split <- h2o.splitFrame(dt_hex, ratios = 0.75, seed = 567)
  
  dt.train <- dt.split[[1]]
  dt.test <- dt.split[[2]]
  
  best_xg <- h2o.xgboost(training_frame = dt.train , 
                          x = var_model , 
                          y = "bad_flag" , 
                          validation_frame = dt.test , 
                          ntrees = 400 ,
                          learn_rate = 0.02 ,
                          max_depth = 6 ,
                          sample_rate = 0.56 ,
                          col_sample_rate = 0.34 , 
                          seed = 567
                         )
  
  xg_perf <- h2o.performance(best_xg, newdata = dt.test)
  print(paste("AUC is :", h2o.auc(xg_perf)))
  return(best_xg)
  
}

# COMMAND ----------

model_scoring <- function(dt_input, 
                         model_path = "/dbfs/mnt/cvm02/user/pitchaym/h2o_model_aa/") {
  
  gen_dir <- "/dbfs/mnt/cvm02/cvm_output/MCK/AA/CAMPAIGN/h2o"
  
  target_model <- list.files(model_path, pattern = "model")
  target_model <- paste0(model_path, target_model)
  
  classifier <- h2o.loadModel(target_model)
  
  dt_input %>%
  mutate(register_date = as.character(register_date)) -> dt_score
  
  # Transform to h2o
  dt_score_hex <- as_h2o_frame(sc, dt_score)
  
  # Casting variables
  dt_score_hex$mnp_flag <- as.factor(dt_score_hex$mnp_flag)
  dt_score_hex$gender <- as.factor(dt_score_hex$gender)
  dt_score_hex$handset_os <- as.factor(dt_score_hex$handset_os)
  dt_score_hex$serenade <- as.factor(dt_score_hex$serenade)
  dt_score_hex$top30_province <- as.factor(dt_score_hex$top30_province)
  
  pred <- h2o.predict(object = classifier, newdata = dt_score_hex)
  pred$p0 <- round(pred$p0, digit = 4)
  pred <- h2o.cbind(dt_score_hex[,c("crm_sub_id", "analytic_id", "register_date")],pred[,"p0"])
  
  unlink(gen_dir, recursive= T)
  
  h2o.exportFile(pred, gen_dir, parts = -1)
  
}

# COMMAND ----------

list_export <- function(lag_time = 1, new = F,
                        path = "/mnt/cvm02/cvm_output/MCK/AA/CAMPAIGN/h2o/", 
                       gen_dir = "/mnt/cvm02/cvm_output/MCK/AA/CAMPAIGN/") {
  
  out_table <- "mck_aa.tb08_pre_scoring_output"
  end_month <- ceiling_date(today() %m-% months(lag_time), unit="month") %m-% days(1)
  
  dt <- spark_read_csv(sc, "score", path, delimiter = ",")
  
  dt %>%
  rename(aa_score = p0) %>%
  mutate(register_date = to_date(register_date), 
        aa_decile = ntile(aa_score, 10),
        aa_percentile = ntile(aa_score, 100),
        aa_decile = 11-aa_decile, 
        aa_percentile = 101-aa_percentile,
        charge_type = "Post-paid") -> dt
  
  dt %>%
  mutate(prof_date = as.Date(end_month)) -> dt

  # Export to Databricks Table
  if (new == T) {
    spark_write_table(dt, out_table, partition_by = "prof_date")
    print(paste(out_table, "has been created"))
  } else {
    spark_write_table(dt, out_table, mode = "append")
    print(paste(out_table, end_month, "has been appended"))
  }
  
  # Export file output
  
#   dt %>%
#   sdf_coalesce(1) -> dt_export
  
#   spark_write_csv(dt_export, paste0(gen_dir,"/post"), delimiter="|")
#   target_file <- list.files(paste0("/dbfs", gen_dir, "/post/"), pattern = "csv$")
  
#   file_name <- format(Sys.Date(), "%Y%m")
#   file_name <- paste0("FMC_post_" ,file_name, "_SCORE.txt")
  
#   file.rename(from = file.path(paste0("/dbfs", gen_dir, "/post/", target_file)), 
#             to = file.path(paste0("/dbfs", gen_dir, file_name)))
  
#   unlink(paste0("/dbfs", gen_dir, "/post/"), recursive = T)
  unlink(paste0("/dbfs", gen_dir, "/h2o/"), recursive = T)
  
#   print(paste(file_name, "has been written"))
#   glimpse(dt)
  
}