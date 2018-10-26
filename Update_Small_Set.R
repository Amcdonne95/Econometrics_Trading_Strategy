library(dplyr)
library(data.table)
library(lubridate)


sicCodeRange = c(0100, 0999, 1000, 1499, 1500, 1799, 1800, 1999, 2000, 3999,
                 4000, 4999, 5000, 5199, 5200, 5999, 6000, 6799, 7000, 8999,
                 91000, 9729, 9900, 9999)
sicDivision = c('Agriculture, Forestry and Fishing', 'Mining', 'Construction',
                'not used','Manufacturing', 'Transportation, Communications, Electric, Gas and Sanitary service',
                'Wholesale Trade', 'Retail Trade', 'Finance, Insurance and Real Estate',
                'Services', 'Public Administration', 'Nonclassifiable')

getSicTable = function(rawData){
  for i in length(sicDivision):
    divsion = sicDivision[i]
    lower = sicCodeRange[i*2-1]
    higher = sicCodeRange[i*2]
}




load('TEST_data_subset.RData')
df = tbl_df(data_mining)
remove(data_mining)
df = select(df, -cap)

df = df %>%
  dplyr::mutate(year = lubridate::year(date), 
                month = lubridate::month(date), 
                day = lubridate::day(date))
dfReduced = data.table(
  permno = integer(),
  sic = double(),
  year = double(),
  month = double(),
  day = double(),
  return = double()
)
for i in range(1990, 2014){
  for j in range(1, 12){
    chopt = filter(df, year == i, )
  }
}

