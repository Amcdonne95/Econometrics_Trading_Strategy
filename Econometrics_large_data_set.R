setwd('\\\\icnas3.cc.ic.ac.uk/am718/R_Scripts/Econometrics')
#install.packages('dplyr')

load('Data/assignment_data18.RData')
colnames(data)
sapply(data, class)


firstPermno = data[data$permno==1000]





