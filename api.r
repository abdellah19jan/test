# Databricks notebook source
options(tidyverse.quiet = TRUE)

library(comprehenr)
library(httr)
library(glue)
library(lubridate, warn.conflicts = FALSE)
library(tidyverse)

#==============================#
# Charger les données de OMEGA #
#==============================#

load_omega <- function(pce, rng) {
  
  r <- POST(url    = "https://gem.okta-emea.com/oauth2/aus2jp8vmz3yYrDnc0i7/v1/token",
            body   = list(grant_type    = "client_credentials",
                          client_id     = "3-s-downstream-forecasting-connector-7KPFVGH0H6GYMU7",
                          client_secret = "7e03uKQWLdCnsQqqIfdrpg16kYa8sOLIMtPSDk9O",
                          scope         = "api.wattson.supplydata.omega.read"
            ),
            encode = "form"
  )
  
  access_token <- content(r)$access_token
  
  payload <- list(
    points = list(
      list(
        autorisationConso                  = "F",
        autorisationConsoEtendue           = TRUE,
        autorisationSituationContractuelle = TRUE,
        consultationConsoPCELibre          = FALSE,
        mandat                             = "true",
        numeroPce                          = pce,
        plageConsoDebut                    = rng$from,
        plageConsoFin                      = rng$to + months(1),
        typeOccupant                       = "Actuel"
      )
    )
  )
  
  r <- POST(url    = "https://external-requests.downstream-prd.ncd.infrasys16.com/Omega/consumption",
            config = add_headers("Content-Type" = "application/json",
                                 accept         = "*/*",
                                 Authorization  = str_c("Bearer ", access_token)
            ),
            body   = payload,
            encode = "json"
  )
  
  if (r$status_code == 200) {
    
    consump <- content(r)[[1]]$consumptions
    
    if (length(consump) > 0) {
      
      l <- transpose(consump) %>% map(unlist)
      
      df <- l[names(l) %in% c("startDate", "endDate", "quantity")] %>%
        as_tibble() %>%
        transmute(from  = ymd_hms(startDate) %>% as_date(),
                  to    = ymd_hms(endDate) %>% as_date(),
                  to    = if_else(from == to, to + days(), to),
                  conso = as.double(quantity)
        ) %>%
        filter(from < rng$to, to > rng$from, from < to, !is.na(conso))
      
      if (nrow(df) == 0) {
        
        NULL
        
      } else {
        
        df %>% arrange(from)
        
      }
      
    }
    
  } else {
    
    NULL
    
  }
  
}

#=======================================#
# Paramètres de connexion à API WATTSON #
#=======================================#

URL_WATTSON <- "https://api-core.downstream-prd.ncd.infrasys16.com"

HEADERS <- c(Accept          = "application/json, text/javascript1",
             "Content-Type"  = "application/x-www-form-urlencoded",
             "x-api-key"     = "3279807a-fb8c-4d40-8e16-40bbdc1657b7",
             pragma          = "no-cache",
             "cache-control" = "no-cache, no-store",
             "x-user-id"     = "c3_gas_profilage_sur_mesure@engie.com"
)

#===========================================#
# Charger les consommations via API WATTSON #
#===========================================#

load_wattson <- function(pce, operator, rng) {
  
  pce     <- str_remove(pce, "^PIC")
  url     <- str_c(URL_WATTSON, "volume", "gas", operator, pce, sep = "/")
  url_idx <- str_c(url, "consumption-index", sep = "/")
  start   <- format(rng$from, "%Y-%m-%dT00:00:00Z")
  end     <- format(rng$to  , "%Y-%m-%dT00:00:00Z")
  key_val <- glue("start_date={start}",
                  "end_date={end}",
                  "date_format=iso",
                  .sep = "&"
  )
  
  r <- GET(url = str_c(url_idx, key_val, sep = "?"), add_headers(HEADERS))
  
  if (r$status_code == 200) {
    
    l <- content(r, as = "text", encoding = "UTF-8") %>% fromJSON()
    
    if (l$length == 0) {
      
      r <- GET(url = str_c(url, key_val, sep = "?"), add_headers(HEADERS))
      
      if (r$status_code == 200) {
        
        l <- content(r, as = "text", encoding = "UTF-8") %>% fromJSON()
        
        if (l$length == 0) {
          
          NULL
          
        } else {
          
          mat           <- l$items
          colnames(mat) <- l$columns
          
          df <- as_tibble(mat) %>%
            transmute(from  = ymd(str_sub(timestamp, end = 10L)),
                      to    = from + days(),
                      conso = as.double(value)
            ) %>%
            filter(!is.na(conso))
          
          if (nrow(df) == 0) {
            
            NULL
            
          } else {
            
            df %>% arrange(from)
            
          }
          
        }
        
      } else {
        
        NULL
        
      }
      
    } else {
      
      df <- as_tibble(l$consumption_indexes) %>%
        mutate(from  = ymd(str_sub(series_start_date, end = 10L)),
               to    = ymd(str_sub(series_end_date  , end = 10L)),
               conso = as.double(value)
        ) %>%
        arrange(desc(series_integration_date)) %>%
        group_by(from) %>%
        filter(row_number() == 1) %>%
        ungroup() %>%
        group_by(to) %>%
        filter(row_number() == 1) %>%
        ungroup() %>%
        select(from, to, conso) %>%
        arrange(from)
      
      nb        <- nrow(df)
      bool      <- vector(length = nb)
      bool[[1]] <- TRUE
      
      if (nb > 1) {
        
        tp <- df$to[[1]]
        
        for (i in 2:nb) {
          
          if (df$from[[i]] >= tp) {
            
            bool[[i]] <- TRUE
            tp        <- df$to[[i]]
            
          }
          
        }
        
      }
      
      df <- df %>% filter(bool, from < to, !is.na(conso))
      
      if (nrow(df) == 0) {
        
        NULL
        
      } else {
        
        df %>% arrange(from)
        
      }
      
    }
    
  } else {
    
    NULL
    
  }
  
}

#==================================#
# Mettre en forme les températures #
#==================================#

shape <- function(l) {
  
  transpose(l) %>%
    as_tibble() %>%
    unnest(c(Value, Date)) %>%
    transmute(date = ymd(str_sub(Date, end = 10L)), temp = Value)
  
}

#==================================================#
# Charger les températures moyennes depuis Mercure #
#==================================================#

load_temp_moy <- function(station, start, end, dic) {
  
  r <- POST(url    = "https://gem.okta-emea.com/oauth2/aus2jp8vmz3yYrDnc0i7/v1/token",
            body   = list(grant_type    = "client_credentials",
                          client_id     = "3-s-downstream-forecasting-connector-7KPFVGH0H6GYMU7",
                          client_secret = "7e03uKQWLdCnsQqqIfdrpg16kYa8sOLIMtPSDk9O",
                          scope         = "api.mercure"
            ),
            encode = "form"
  )
  
  access_token <- content(r)$access_token
  
  id_ref <- tibble(station) %>% left_join(dic, by = "station") %>% pull()
  
  payload <- list(
    IncludeOffset = TRUE,
    ApplicationDate = list(
      From = start + days(),
      To = end + days()
    ),
    Items = to_list(for(i in seq_along(id_ref))
      list(
        IdRef = id_ref[[i]],
        MaturityType = "D",
        Sliding = list(
          From = -1,
          To = -1
        ),
        GroupingMode = 0,
        FormulaVarName = str_c("x", as.character(i))
      )
    )
  )
  
  temp_moy <- POST(url    = "https://api.gem.myengie.com/internal/mercure/Curves",
                   config = add_headers("Content-Type" = "application/json",
                                        Accept         = "application/json",
                                        Authorization  = str_c("Bearer ", access_token)
                   ),
                   body   = payload,
                   encode = "json"
  ) %>%
    content() %>%
    transpose() %>%
    as_tibble() %>%
    transmute(id_ref = IdRef, data = map(Points, shape)) %>%
    unnest(c(id_ref, data)) %>%
    mutate(id_ref = as.character(id_ref), date = date - days())
  
  expand_grid(station, date = seq(from = start, to = end, by = "day")) %>%
    left_join(dic, by = "station") %>%
    left_join(temp_moy, by = c("id_ref", "date")) %>%
    select(-id_ref)
  
}

#========================================#
# Paramètres de connexion à API OPENDATA #
#========================================#

URL_OPENDATA <- str_c("https://opendata.grdf.fr/api/records/1.0/search/",
                      "?dataset=correction_climatique_grdf&rows=",
                      (ymd("2017-01-01") %--% today()) %/% months(1) + 1
)

#===================================================#
# Charger les coefficients de correction climatique #
#===================================================#

load_cor_clim <- function() {
  
  pattern <- "ratio_consommation_corrigee_consommation_brute_profil_|_grdf"
  
  GET(URL_OPENDATA) %>%
    content() %>%
    pluck("records") %>%
    transpose() %>%
    as_tibble() %>%
    mutate(fields = map(fields, as_tibble)) %>%
    unnest(datasetid:record_timestamp) %>%
    select(mois_annee, starts_with("ratio_con")) %>%
    pivot_longer(-mois_annee, names_to = "profil", values_to = "cor_clim") %>%
    transmute(profil = str_remove_all(profil, pattern) %>% str_to_upper(),
              dtmth  = ymd(mois_annee),
              cor_clim
    ) %>%
    arrange(profil, dtmth)
  
}

#======#
# Test #
#======#

load_omega("GI076512", list(from = ymd("2021-01-01"), to = ymd("2021-01-05")))
load_wattson("GI076512", "GRDF", list(from = ymd("2021-01-01"), to = ymd("2021-01-05")))
load_temp_moy("75114001", ymd("2021-01-01"), ymd("2021-01-05"), read_csv("dic.csv", col_types = cols(id_ref = col_character())))
load_cor_clim()
