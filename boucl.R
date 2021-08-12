# Databricks notebook source
options(tidyverse.quiet = TRUE)

library(RPostgres)
library(tidyverse)

DATE_MIN_BOUCLAGE <- "2019-01-01"

load_bouclage <- function() {
  
  con <- dbConnect(Postgres(),
                   dbname   = "c3_gaz", 
                   host     = "dtcoprdpgs004l.pld.infrasys16.com", 
                   port     = 5435, 
                   user     = "c3gaz_reader",
                   password = "97TBqjAZz64KkLFYq4PKhphfi2V8RFtRQVgunX9xw63H"
  )
  
  query <- str_c("select * ",
                 "from estimate_data.bouclage_zet ",
                 "where date_bouclage >= '", DATE_MIN_BOUCLAGE, "'",
                 "and zet in ('ZET04', 'ZET06')"
  )
  
  df <- dbGetQuery(con, query) %>% as_tibble()
  
  dbDisconnect(con)
  
  df %>%
    group_by(zet, date_bouclage) %>%
    arrange(desc(integration_date)) %>%
    filter(row_number() == 1) %>%
    ungroup() %>%
    select(zet, date = date_bouclage, bouclage = k2_zet)
  
}
