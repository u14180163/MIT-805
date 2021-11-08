getwd()
install.packages("janitor")
install.packages("dplyr")
library(janitor)
library(dplyr)


# -import the datasets into R 
accepted <- read.csv("accepted_2007_to_2018Q4.csv")

rejected <- read.csv("rejected_2007_to_2018Q4.csv")

# clean column names(add underscore if variable names have spaces)
accepted_clean <- clean_names(accepted)
colnames(accepted_clean)
rejected_clean <- clean_names(rejected)
colnames(rejected_clean)

# remove empty row or column
accepted_ln <- accepted_clean %>% remove_empty(whic=c("rows"))
rejected_ln <- rejected_clean %>% remove_empty(whic=c("rows"))

accepted_omit <- na.omit(accepted_loans)

#remove unwanted columns from datasets
accepted  <-  select(accepted_ln, -c( member_id, url,desc, title, zip_code,24:27,30:32,38:50, next_pymnt_d, 54:56, 58:61,
  67:69,92:104, 130:143, 146:150))

rejected <- select(rejected_ln, -c(zip_code, policy_code))
str(accepted)

