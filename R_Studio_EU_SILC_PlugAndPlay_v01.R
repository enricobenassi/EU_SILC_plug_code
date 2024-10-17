# Read-in EU SILC cross sectional data -----
# 0. Provide key inputs -------

## key directories -----
setwd("C:/RData_DPhil/DECIPHE Project/")
base_folder <- "C:/RData_DPhil/EUROSTAT/EU-SILC/Cross_23_09/Cross/"

## countries and years of interest -----
countries <- c("AT", "BE", "BG", "CH", "CY", "CZ", "DE", "DK", "EE", "EL",
               "ES", "FI", "FR", "HR", "HU", "IE", "IS", "IT", "LT", "LU", 
               "LV", "MT", "NL", "NO", "PL", "PT", "RO", "RS", "SE", "SI", "SK", "UK")
years <- c(2004:2022)
dir_vector <- c()

## variables of inetrest for each database -----

### R VARS ------
varR <- c("RB010", "RB020", "RB030" # year country pid
          )


### P VARS ------
varP <- c("PB010", "PB020", "PB030", # year country id
          "PX030", # household id
          "PB140" # year of birth
)

### D VARS -----
varD <- c("DB010", "DB020", "DB030" # year country hid
          )


### H VARS  ------
varH <- c("HB010", "HB020", "HB030", # year country id
          "HH021", # housing tenure
          "HH020" # pre 2010 housing tenure
)



# 1. Library and working directory set-up ----
library(dplyr)
library(readr)
library(data.table)
rm(list=ls())

# 2. Set the directory containing the CSV files and read them in -----

for (country in  countries) {
  for (year in years) {
    dir <- paste0(base_folder, 
                  country,"/", year,"/")
    dir_vector <- c(dir_vector, dir)
  }
}

# 3. R - dataset read and set up

list_R <- c()
for (directory in dir_vector ) {
  curr_file_list <- list.files(path = directory, pattern = "*R.csv", full.names = 
                                 TRUE) 
  list_R <- c(list_R, curr_file_list)
}

# Read only selected variables for R 
read_selected_variables <- function(file, variables) {
  fread(file, select = variables)
}
R_SILC <- lapply(list_R, read_selected_variables, variables = varR)

# Merge all R db
R_SILC_db <- bind_rows(R_SILC)


# 4. P - dataset read and set up ------
list_P <- c()
for (directory in dir_vector ) {
  curr_file_list <- list.files(path = directory, pattern = "*P.csv", full.names = 
                                 TRUE) 
  list_P <- c(list_P, curr_file_list)
}

# Read only selected variables for P 
read_selected_variables <- function(file, variables) {
  fread(file, select = variables)
}
P_SILC <- lapply(list_P, read_selected_variables, variables = varP)

# Merge all R db
P_SILC_db <- bind_rows(P_SILC)


# 5. D - dataset read and set up ------
list_D <- c()
for (directory in dir_vector ) {
  curr_file_list <- list.files(path = directory, pattern = "*D.csv", full.names = 
                                 TRUE) 
  list_D <- c(list_D, curr_file_list)
}

# Read only selected variables for D 
read_selected_variables <- function(file, variables) {
  fread(file, select = variables)
}
D_SILC <- lapply(list_D, read_selected_variables, variables = varD)

# Merge all R db
D_SILC_db <- bind_rows(D_SILC)


# 6. H - dataset read and set up ------
list_H <- c()
for (directory in dir_vector ) {
  curr_file_list <- list.files(path = directory, pattern = "*H.csv", full.names = 
                                 TRUE) 
  list_H <- c(list_H, curr_file_list)
}

# Read only selected variables for H
read_selected_variables <- function(file, variables) {
  fread(file, select = variables)
}
H_SILC <- lapply(list_H, read_selected_variables, variables = varH)

# Merge all R db
H_SILC_db <- bind_rows(H_SILC)


# 7. Merge indidividual datasets R and P -----
# clean lists
rm(R_SILC, P_SILC, D_SILC, H_SILC)

RP_SILC <- merge(R_SILC_db, P_SILC_db, 
              by.x = c("RB010", "RB020", "RB030"), 
              by.y = c("PB010", "PB020", "PB030"), 
              all.x = T, all.y = T)

# 8. Merge household datsets D and H -----

DH_SILC <- merge(D_SILC_db, H_SILC_db, 
                 by.x = c("DB010", "DB020", "DB030"), 
                 by.y = c("HB010", "HB020", "HB030"), 
                 all.x = T, all.y = T)


# 9. Merge indidivual and household datasets RP and DH -----

RPDH_SILC <- merge(RP_SILC, DH_SILC, 
              by.x = c("RB010", "RB020", "PX030"), 
              by.y = c("DB010", "DB020", "DB030"), 
              all.x = T, all.y = T)

# 10. Save SILC DB in easily format readable by r -----

saveRDS(RPDH_SILC, file = "RPDH_SILC.rds")
