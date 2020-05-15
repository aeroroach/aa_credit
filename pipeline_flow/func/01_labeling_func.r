# Databricks notebook source
# MAGIC %md 
# MAGIC ### Data labeling function
# MAGIC 
# MAGIC `labeling_data()`
# MAGIC 
# MAGIC Table sources
# MAGIC + `prod_raw.dm1211_aat_borrowing_money` - Campaign label and gathering latest months of campaign data
# MAGIC 
# MAGIC There's some criteria that need to be filtered
# MAGIC + M is the month that use AA feature
# MAGIC + To find subs that use AA on M, the profile is M-2, label file use M+1
# MAGIC + `remain_balance_p1` and `recharge_count` = 0 : To find bad debtor

# COMMAND ----------

labeling_data <- function (in_table = "prod_delta.dm1211_aat_borrowing_money", 
                           month_lag = 1) {
  
  # Target month
  label_end <- ceiling_date(today() %m-% months(month_lag), unit="month") %m-% days(1)
  print(paste("Target month is :", label_end))
  
  # Loading table
  response <- tbl(sc, in_table)
  
  response %>% 
  filter(ddate == label_end) %>%
  mutate(bad_flag = ifelse(remain_balance_p1 < 0 & recharge_count == 0, 1, 0),
        prof_month = add_months(ddate, -3)) -> response
  
  # Filtering duplication sub
  response %>%
  count(analytic_id) %>%
  filter(n > 1) -> dup_sub
  
  response %>%
  anti_join(dup_sub, by = "analytic_id") -> response
  
  sdf_register(response, "response_tbl")
  tbl_cache(sc, "response_tbl")
  
  return(response)
}