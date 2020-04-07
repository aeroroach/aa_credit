library(shiny)
library(shinydashboard)
library(tidyverse)
library(lubridate)
library(DT)

dt <- read_csv("total_lift_tbl.csv")

dt %>% 
  mutate(prof_date = as.character(prof_date)) -> dt
