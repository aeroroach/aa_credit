# Databricks notebook source
# MAGIC %md
# MAGIC ### Datamart generation function
# MAGIC 
# MAGIC These notebook consist of function that generate the data that will be utilized in model features below
# MAGIC 1. `usage_gen()` : Generating summary usage function

# COMMAND ----------

usage_gen <- function(lag_time = 1) {
  
  begin_month <- floor_date(today() %m-% months(lag_time), unit="month")
  end_month <- ceiling_date(today() %m-% months(lag_time), unit="month") %m-% days(1)
  output_tbl <- "mck_aa.usage_agg_monthly"
  
  # Loading data
  usage_sum <- tbl(sc, output_tbl)
  topup_daily <- tbl(sc, "prod_raw.dm01_fin_top_up")
  usage_daily <- tbl(sc, "prod_raw.dm09_mobile_day_split")
  
  # Checkine latest data
  usage_sum %>%
  distinct(ddate) %>%
  arrange(desc(ddate)) %>%
  top_n(1) %>%
  collect() -> latest_ddate
  latest_ddate <- latest_ddate$ddate[1]
  
  if (end_month != latest_ddate) {
    
    # Topup -------------------------------------------
    topup_daily %>%
    mutate(register_date = to_date(register_date)) %>%
    filter(ddate >= begin_month, ddate <= end_month) -> topup_daily
    
    topup_daily %>%
    group_by(analytic_id, register_date) %>% 
    summarise(sum_monthly_tran_topup = sum(top_up_tran),  
         mean_monthly_topup_tran = mean(top_up_tran), 
         sd_monthly_topup_tran = sd(top_up_tran), 
         min_monthly_topup_tran = min(top_up_tran), 
         max_monthly_topup_tran = max(top_up_tran), 
         sum_monthly_topup_value = sum(top_up_value), 
         mean_monthly_topup_value = mean(top_up_value), 
         sd_monthly_topup_value = sd(top_up_value), 
         min_monthly_topup_value = min(top_up_value), 
         max_monthly_topup_value = max(top_up_value)) -> topup_monthly
    
    sdf_register(topup_monthly, "topup_monthly")
    tbl_cache(sc, "topup_monthly")
    
    # Usage -------------------------------------------
    usage_daily %>%
    filter(ddate >= begin_month, ddate <= end_month) %>%
    group_by(analytic_id) %>%
    summarise(sum_monthly_data_kb = sum(data_vol_kb), 
          sum_monthly_voice_out_minute = sum(voice_out_minute), 
          sum_monthly_voice_out_cnt = sum(voice_out_cnt), 
          sum_monthly_voice_in_minute = sum(voice_in_minute), 
          sum_monthly_voice_in_cnt = sum(voice_in_cnt), 
          sum_monthly_sms_in_cnt = sum(sms_in_cnt)) -> usage_monthly
    
    sdf_register(usage_monthly, "usage_monthly")
    tbl_cache(sc, "usage_monthly")
    
    # Joining table -------------------------------------
    topup_monthly %>%
    full_join(usage_monthly, by = "analytic_id") %>%
    mutate(ddate = as.Date(end_month)) -> final_table
    
    spark_write_table(final_table, output_tbl, mode = "append")
    print(paste(output_tbl,"new month(ddate) has been appended :", end_month))
    
  } else {
    print(paste(output_tbl, "on", end_month, "already existed"))
  }
  
}