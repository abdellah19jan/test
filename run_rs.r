source("api.r")

load_omega("GI076512", list(from = ymd("2021-01-01"), to = ymd("2021-01-05")))

load_wattson("GI076512", "GRDF", list(from = ymd("2021-01-01"), to = ymd("2021-01-05")))

dic <- read_csv("dic.csv", col_types = cols(id_ref = col_character()))
load_temp_moy("75114001", ymd("2021-01-01"), ymd("2021-01-05"), dic)

load_cor_clim()
