# Databricks notebook source
# MAGIC %md
# MAGIC ### Data preparation functions
# MAGIC 
# MAGIC This notebook consist with list of functions below
# MAGIC 1. `prep_data()` : Gathering data from relevance sources and join into single table
# MAGIC 2. `feat_enhance()` : Enriching features by derive the meaningful time series
# MAGIC 3. `dt_cleansing()` : Imputing and regroup categories variables
# MAGIC 4. `feat_derive()` : Calculating derive time series features

# COMMAND ----------

# DBTITLE 1,Data prep function
prep_data <- function(df_input = NULL, 
                     lag_time = 4, 
                     training = T) {

  # Target month
  end_month <- ceiling_date(today() %m-% months(lag_time), unit="month") %m-% days(1)
  print(paste("Target month is :", end_month))
  
  # List of original source tables
  prof_table <- "prod_delta.dm07_sub_clnt_info"
  usage_table <- "mck_aa.usage_agg_monthly"
  surv_table <- "mck.topup_svv"
  valid_table <- "prod_delta.dm23_prepaid_remain_balance_validity"
  
  # Loading source data
  prof <- tbl(sc, prof_table)
  usage <- tbl(sc, usage_table)
  surv <- tbl(sc, surv_table)
  valid <- tbl(sc, valid_table)
  
  # Filter selected month
  prof %>% filter(ddate == end_month) -> prof
  usage %>% filter(ddate == end_month) -> usage
  surv %>% filter(month_id == end_month) -> surv
  valid %>% filter(ddate == end_month) -> valid
  
  # Select relevance features
  prof %>%
  filter(charge_type == "Pre-paid", mobile_status == "SA") %>%
  mutate(register_date = to_date(activation_date)) %>%
  select(analytic_id, national_id, crm_sub_id, register_date, ddate,
               age, credit_limit, days_active, service_month, foreigner_flag, gender, charge_type, mobile_status, 
               mnp_flag, master_segment_id, 
               handset_launchprice, handset_os, 
               norms_net_revenue_avg_3mth_p0_p2, norms_net_revenue_gprs, norms_net_revenue_vas, norms_net_revenue_voice) -> prof

  surv %>%
  mutate(register_date = to_date(register_date)) %>%
  select(-mobile_status) -> surv

  valid %>% 
  mutate(register_date = to_date(register_date)) %>%
  select(-date_id, -crm_subscription_id) -> valid
  
  # Joining data
  
  if (training == T) {
  df_input %>%
  select(analytic_id, register_date, bad_flag, prof_month) -> df_input
  
  df_input %>%
  left_join(prof, by = c("analytic_id", "register_date","prof_month"="ddate")) %>%
  left_join(usage, by = c("analytic_id", "register_date", "prof_month"="ddate")) %>%
  left_join(surv, by = c("analytic_id", "register_date","prof_month"="month_id")) %>%
  left_join(valid, by = c("analytic_id", "register_date", "prof_month"="ddate")) -> feature_table
  } else {
    
    prof %>%
    left_join(usage, by = c("analytic_id", "register_date", "ddate")) %>%
    left_join(surv, by = c("analytic_id", "register_date", "ddate"="month_id")) %>%
    left_join(valid, by = c("analytic_id", "register_date", "ddate")) %>%
    rename(prof_month = ddate) -> feature_table
  }
    
  sdf_register(feature_table, "feature_tbl")
  tbl_cache(sc, "feature_tbl")
  
  return(feature_table)
  
}

# COMMAND ----------

# DBTITLE 1,Feature enhancing function
feat_enhance <- function(data_input, 
                    his_time = 6) {
  
  # Table sources
  prof_table <- "prod_delta.dm07_sub_clnt_info"
  usage_table <- "mck_aa.usage_agg_monthly"
  
  # Target month
  oldest_month <- ceiling_date(today() %m-% months(his_time), unit="month") %m-% days(1)
  newest_month <- ceiling_date(today() %m-% months(his_time - 1), unit="month") %m-% days(1)
  
  print(paste("Oldest history month is :", oldest_month))
  print(paste("Newest history month is :", newest_month))
  
  data_input %>%
  mutate(p1_map = add_months(prof_month, -1), 
      p2_map = add_months(prof_month, -2)) -> dt
  
  # Loading history data
  prof <- tbl(sc, prof_table)
  usage <- tbl(sc, usage_table)
  
  prof %>% filter(ddate >= oldest_month, ddate <= newest_month) %>%
  semi_join(dt, by = "analytic_id", "register_date") %>%
  mutate(register_date = to_date(activation_date)) %>%
  select(analytic_id, register_date, ddate, 
         norms_net_revenue_avg_3mth_p0_p2, norms_net_revenue_gprs, norms_net_revenue_vas, 
         norms_net_revenue_voice) -> prof
  
  usage %>% filter(ddate >= oldest_month, ddate <= newest_month) %>%
  semi_join(dt, by = "analytic_id", "register_date") %>%
  select(analytic_id, register_date, ddate, 
        sum_monthly_tran_topup, sum_monthly_topup_value, 
        sum_monthly_data_kb : sum_monthly_sms_in_cnt) -> usage
  
  prof %>%
  full_join(usage, by = c("analytic_id", "register_date", "ddate")) -> his_dt
  
  sdf_register(his_dt, "his_dt")
  tbl_cache(sc, "his_dt")
  
  # Joining with main table
  dt %>%
  left_join(his_dt, by = c("analytic_id", "register_date", "p1_map"="ddate"), suffix= c("","_p1")) -> dt_p1
  
  sdf_register(dt_p1, "dt_p1")
  tbl_cache(sc, "dt_p1")
  
  dt_p1 %>%
  left_join(his_dt, by = c("analytic_id", "register_date", "p2_map"="ddate"), suffix= c("","_p2")) -> dt_p2
  
  sdf_register(dt_p2, "dt_p2")
  tbl_cache(sc, "dt_p2")
  
  dt_p2 %>%
  select(-p1_map, -p2_map) -> dt_out
  
  return(dt_out)
  
}

# COMMAND ----------

# DBTITLE 1,Data cleansing function
dt_cleansing <- function (dt_input, training = T) {
  
  # Export directory
  top_province_export <- "/dbfs/mnt/cvm02/user/pitchaym/share/aa_top_province.csv"
  
  # initial criteria
  dt_input %>%
  mutate(age = as.numeric(age)) %>%
  filter(mobile_status == "SA", !is.na(analytic_id), service_month >= 4, 
         norms_net_revenue_avg_3mth_p0_p2 >= 40) -> dt_trans
  
  # Mean imputing
  im_mean <- c("age", "handset_launchprice", "credit_limit")
  
  if (training == T) {
    dt_trans %>%
    ft_imputer(input_cols=im_mean, output_cols=im_mean, strategy="mean") %>%
    select(analytic_id, crm_sub_id, bad_flag, national_id, register_date, age, handset_launchprice, credit_limit, 
          mnp_flag, master_segment_id) -> mean_im
  } else {
    dt_trans %>%
    ft_imputer(input_cols=im_mean, output_cols=im_mean, strategy="mean") %>%
    select(analytic_id, crm_sub_id, national_id, register_date, age, handset_launchprice, credit_limit, 
          mnp_flag, master_segment_id) -> mean_im
  }
  
  # Unknown imputing
  dt_trans %>%
  select(analytic_id, register_date, gender, handset_os) %>%
  na.replace("unknown") -> unknown_im
 
  # Zero imputing
  dt_trans %>%
  select(analytic_id, register_date, days_active, service_month, 
         norms_net_revenue_avg_3mth_p0_p2:norms_net_revenue_voice, 
         sum_monthly_tran_topup:max_monthly_topup_value, sum_monthly_data_kb:sum_monthly_sms_in_cnt, 
         vald_res_10:sum_monthly_sms_in_cnt_p2
        ) %>%
  na.replace(0) -> zero_im
  
  # Joining back
  mean_im %>%
  left_join(unknown_im, by = c("analytic_id", "register_date")) %>%
  left_join(zero_im, by = c("analytic_id", "register_date")) -> base_clean
  
  # Regroup ----------------------------
  
  # Serenade class
  base_clean %>%
  mutate(serenade = case_when(
    master_segment_id %in% c("NA", "Prospect Emerald") | is.na(master_segment_id) ~ "Classic",
    master_segment_id %in% c("Platinum Plus", "Prospect Plat Plus") ~ "Platinum",
    master_segment_id == "Prospect Platinum" ~ "Gold",
    master_segment_id == "Prospect Gold" ~ "Emerald",
    master_segment_id == "Standard" ~ "Classic",
    TRUE ~ master_segment_id)) %>%
  select(-master_segment_id) -> base_clean
  
  # Handset os
  base_clean %>%
  mutate(handset_os = case_when(
  handset_os %in% c("Android", "iOS", "Proprietary", "Unknown") ~ handset_os, 
  TRUE ~ "others")) -> base_clean
  
  # MNP flag
  base_clean %>%
  mutate(mnp_flag = ifelse(is.na(mnp_flag),"N" , mnp_flag)) -> base_clean
  
  sdf_register(base_clean, "base_clean")
  tbl_cache(sc, "base_clean")
  
  return(base_clean)
}

# COMMAND ----------

# DBTITLE 1,Feature deriving
feat_derive <- function (dt_input) {
  
  # Average
  dt_input %>%
  mutate(
  avg_norms_net_revenue_avg_3mth_p0_p2 =  (norms_net_revenue_avg_3mth_p0_p2 + norms_net_revenue_avg_3mth_p0_p2_p1 + norms_net_revenue_avg_3mth_p0_p2_p2)/3,
  avg_norms_net_revenue_gprs =  (norms_net_revenue_gprs + norms_net_revenue_gprs_p1 + norms_net_revenue_gprs_p2)/3,
  avg_norms_net_revenue_vas =  (norms_net_revenue_vas + norms_net_revenue_vas_p1 + norms_net_revenue_vas_p2)/3,
  avg_norms_net_revenue_voice =  (norms_net_revenue_voice + norms_net_revenue_voice_p1 + norms_net_revenue_voice_p2)/3,
  avg_sum_monthly_tran_topup =  (sum_monthly_tran_topup + sum_monthly_tran_topup_p1 + sum_monthly_tran_topup_p2)/3,
  avg_sum_monthly_topup_value =  (sum_monthly_topup_value + sum_monthly_topup_value_p1 + sum_monthly_topup_value_p2)/3,
  avg_sum_monthly_data_kb =  (sum_monthly_data_kb + sum_monthly_data_kb_p1 + sum_monthly_data_kb_p2)/3,
  avg_sum_monthly_voice_out_minute =  (sum_monthly_voice_out_minute + sum_monthly_voice_out_minute_p1 + sum_monthly_voice_out_minute_p2)/3,
  avg_sum_monthly_voice_out_cnt =  (sum_monthly_voice_out_cnt + sum_monthly_voice_out_cnt_p1 + sum_monthly_voice_out_cnt_p2)/3,
  avg_sum_monthly_voice_in_minute =  (sum_monthly_voice_in_minute + sum_monthly_voice_in_minute_p1 + sum_monthly_voice_in_minute_p2)/3,
  avg_sum_monthly_voice_in_cnt =  (sum_monthly_voice_in_cnt + sum_monthly_voice_in_cnt_p1 + sum_monthly_voice_in_cnt_p2)/3,
  avg_sum_monthly_sms_in_cnt =  (sum_monthly_sms_in_cnt + sum_monthly_sms_in_cnt_p1 + sum_monthly_sms_in_cnt_p2)/3
  ) -> base_feature
  
  # SD
  base_feature %>%
  mutate(
  sd_norms_net_revenue_avg_3mth_p0_p2 =  sqrt(((norms_net_revenue_avg_3mth_p0_p2-avg_norms_net_revenue_avg_3mth_p0_p2)^2 + (norms_net_revenue_avg_3mth_p0_p2_p1-avg_norms_net_revenue_avg_3mth_p0_p2)^2 + (norms_net_revenue_avg_3mth_p0_p2_p2-avg_norms_net_revenue_avg_3mth_p0_p2)^2)/3),
  sd_norms_net_revenue_gprs =  sqrt(((norms_net_revenue_gprs-avg_norms_net_revenue_gprs)^2 + (norms_net_revenue_gprs_p1-avg_norms_net_revenue_gprs)^2 + (norms_net_revenue_gprs_p2-avg_norms_net_revenue_gprs)^2)/3),
  sd_norms_net_revenue_vas =  sqrt(((norms_net_revenue_vas-avg_norms_net_revenue_vas)^2 + (norms_net_revenue_vas_p1-avg_norms_net_revenue_vas)^2 + (norms_net_revenue_vas_p2-avg_norms_net_revenue_vas)^2)/3),
  sd_norms_net_revenue_voice =  sqrt(((norms_net_revenue_voice-avg_norms_net_revenue_voice)^2 + (norms_net_revenue_voice_p1-avg_norms_net_revenue_voice)^2 + (norms_net_revenue_voice_p2-avg_norms_net_revenue_voice)^2)/3),
  sd_sum_monthly_tran_topup =  sqrt(((sum_monthly_tran_topup-avg_sum_monthly_tran_topup)^2 + (sum_monthly_tran_topup_p1-avg_sum_monthly_tran_topup)^2 + (sum_monthly_tran_topup_p2-avg_sum_monthly_tran_topup)^2)/3),
  sd_sum_monthly_topup_value =  sqrt(((sum_monthly_topup_value-avg_sum_monthly_topup_value)^2 + (sum_monthly_topup_value_p1-avg_sum_monthly_topup_value)^2 + (sum_monthly_topup_value_p2-avg_sum_monthly_topup_value)^2)/3),
  sd_sum_monthly_data_kb =  sqrt(((sum_monthly_data_kb-avg_sum_monthly_data_kb)^2 + (sum_monthly_data_kb_p1-avg_sum_monthly_data_kb)^2 + (sum_monthly_data_kb_p2-avg_sum_monthly_data_kb)^2)/3),
  sd_sum_monthly_voice_out_minute =  sqrt(((sum_monthly_voice_out_minute-avg_sum_monthly_voice_out_minute)^2 + (sum_monthly_voice_out_minute_p1-avg_sum_monthly_voice_out_minute)^2 + (sum_monthly_voice_out_minute_p2-avg_sum_monthly_voice_out_minute)^2)/3),
  sd_sum_monthly_voice_out_cnt =  sqrt(((sum_monthly_voice_out_cnt-avg_sum_monthly_voice_out_cnt)^2 + (sum_monthly_voice_out_cnt_p1-avg_sum_monthly_voice_out_cnt)^2 + (sum_monthly_voice_out_cnt_p2-avg_sum_monthly_voice_out_cnt)^2)/3),
  sd_sum_monthly_voice_in_minute =  sqrt(((sum_monthly_voice_in_minute-avg_sum_monthly_voice_in_minute)^2 + (sum_monthly_voice_in_minute_p1-avg_sum_monthly_voice_in_minute)^2 + (sum_monthly_voice_in_minute_p2-avg_sum_monthly_voice_in_minute)^2)/3),
  sd_sum_monthly_voice_in_cnt =  sqrt(((sum_monthly_voice_in_cnt-avg_sum_monthly_voice_in_cnt)^2 + (sum_monthly_voice_in_cnt_p1-avg_sum_monthly_voice_in_cnt)^2 + (sum_monthly_voice_in_cnt_p2-avg_sum_monthly_voice_in_cnt)^2)/3),
  sd_sum_monthly_sms_in_cnt =  sqrt(((sum_monthly_sms_in_cnt-avg_sum_monthly_sms_in_cnt)^2 + (sum_monthly_sms_in_cnt_p1-avg_sum_monthly_sms_in_cnt)^2 + (sum_monthly_sms_in_cnt_p2-avg_sum_monthly_sms_in_cnt)^2)/3)
  ) -> base_feature
  
  # Slope
  base_feature %>%
  mutate(
  slope_norms_net_revenue_avg_3mth_p0_p2_p1 = norms_net_revenue_avg_3mth_p0_p2/((norms_net_revenue_avg_3mth_p0_p2_p1 + norms_net_revenue_avg_3mth_p0_p2_p2)/2) - 1,
  slope_norms_net_revenue_gprs_p1 = norms_net_revenue_gprs/((norms_net_revenue_gprs_p1 + norms_net_revenue_gprs_p2)/2) - 1,
  slope_norms_net_revenue_vas_p1 = norms_net_revenue_vas/((norms_net_revenue_vas_p1 + norms_net_revenue_vas_p2)/2) - 1,
  slope_norms_net_revenue_voice_p1 = norms_net_revenue_voice/((norms_net_revenue_voice_p1 + norms_net_revenue_voice_p2)/2) - 1,
  slope_sum_monthly_tran_topup_p1 = sum_monthly_tran_topup/((sum_monthly_tran_topup_p1 + sum_monthly_tran_topup_p2)/2) - 1,
  slope_sum_monthly_topup_value_p1 = sum_monthly_topup_value/((sum_monthly_topup_value_p1 + sum_monthly_topup_value_p2)/2) - 1,
  slope_sum_monthly_data_kb_p1 = sum_monthly_data_kb/((sum_monthly_data_kb_p1 + sum_monthly_data_kb_p2)/2) - 1,
  slope_sum_monthly_voice_out_minute_p1 = sum_monthly_voice_out_minute/((sum_monthly_voice_out_minute_p1 + sum_monthly_voice_out_minute_p2)/2) - 1,
  slope_sum_monthly_voice_out_cnt_p1 = sum_monthly_voice_out_cnt/((sum_monthly_voice_out_cnt_p1 + sum_monthly_voice_out_cnt_p2)/2) - 1,
  slope_sum_monthly_voice_in_minute_p1 = sum_monthly_voice_in_minute/((sum_monthly_voice_in_minute_p1 + sum_monthly_voice_in_minute_p2)/2) - 1,
  slope_sum_monthly_voice_in_cnt_p1 = sum_monthly_voice_in_cnt/((sum_monthly_voice_in_cnt_p1 + sum_monthly_voice_in_cnt_p2)/2) - 1,
  slope_sum_monthly_sms_in_cnt_p1 = sum_monthly_sms_in_cnt/((sum_monthly_sms_in_cnt_p1 + sum_monthly_sms_in_cnt_p2)/2) - 1
  ) -> base_feature
  
  # Imputing
  base_feature %>%
  select(-national_id) %>%
  select(-norms_net_revenue_avg_3mth_p0_p2_p1:-sum_monthly_sms_in_cnt_p2) %>%
  na.replace(0) -> base_feature

  sdf_register(base_feature, "base_feature")
  tbl_cache(sc, "base_feature")
  
  return(base_feature)
  
} 