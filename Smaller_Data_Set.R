#install.packages('dplyr')
#install.packages("data.table")
#install.packages('lubridate')
library(dplyr)
library(data.table)
library(lubridate)

#setwd('\\\\icnas3.cc.ic.ac.uk/am718/R_Scripts/Econometrics')

#loads the initial data
load('TEST_data_subset.RData')

#turns the dataframe into a r table object
df = tbl_df(data_mining)
remove(data_mining)

df = df %>%
  dplyr::mutate(year = lubridate::year(date), 
                month = lubridate::month(date), 
                day = lubridate::day(date))

numYears = tail(df$year, n=1) - head(df$year, n=1)
numMonths = numYears*12
df = arrange(df, date, permno)
















#loads in the SIC descriptions and changes it to a table
sic_dict = read.csv('Data/SIC07_CH_condensed_list_en.csv')
sic_dict = tbl_df(sic_dict)

#adjusts column names
sic_dict = sic_dict %>% 
  rename(
    sic = SIC.Code,
    des = Description
  )


#used to join the SIC description to each SIC
#left_join(df, sic_dict, by = "sic")

#generates the months to perform holding and return selection on
monthSelect = seq(3, 12)
holdingSelect = seq(3, 12)


#generates the simple and LN returns of daily prices
#Double checked this function to make sire resutls are correct even between permno's
df = mutate(df, lagprice = lag(price), simplereturns = ((price-lagprice)/lagprice)*100,
            logreturns = (log(price/lagprice)*100))
#removes all NaN values
df = na.omit(df)



#groups the permno's and sums the log returns for the whole period
df %>%
  group_by(permno) %>%
  summarize(sum_logreturns = sum(logreturns))
