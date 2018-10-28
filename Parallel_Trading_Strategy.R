library(dplyr)
library(data.table)
library(lubridate)
library(parallel)
setwd("C:\\Users\\austi\\Desktop\\Imperial\\Modules\\Econometrics\\R_CourseWork\\Econometrics_Trading_Strategy")


load('TEST_data_subset.RData')
data = tbl_df(data_mining)
remove(data_mining)

sicCodeRange = c(0099, 0999, 1499, 1799, 1999, 3999,
                 4999, 5199, 5999, 6799, 8999, 9099,
                 9729, 9899, 9999)
sicDivsionCode = c(1:10, 0, 11, 0, 12)

data$industry = cut(data$sic, sicCodeRange)
levels(data$industry) = sicDivsionCode
data$industry=as.numeric(data$industry)


numcores = detectCores()
cl = makeCluster(numcores)

clusterEvalQ(cl, {
  library(dplyr)
  library(data.table)
  library(lubridate)
})
uniquePermno = unique(data$permno)
permnoQuarters = splitIndices(length(uniquePermno), 4)

seperatePermno = function(data, permnoList, quartile){
  data %>%
    filter(permno %in% permnoList[quartile[1]:tail(quartile, n=1)])
  return(data)
}


clusterExport(cl, c("data", "uniquePermno", "seperatePermno", "permnoQuarters"))

clusterEvalQ(cl, seperatePermno(data, uniquePermno, permnoQuarters))


stopCluster(cl)




permnos = unique(data$permno)
numPermno = length(permnos)

quarterSplit = as.integer(floor(numPermno/4))

permno25 = permnos[1:quarterSplit]
permno50 = permnos[(quarterSplit+1):(2*quarterSplit)]
permno75 = permnos[(2*quarterSplit+1):(3*quarterSplit)]
permno100 = permnos[(3*quarterSplit+1):(4*quarterSplit+(numPermno%%4))]

remove(permnos)

data1 = data %>%
  filter(permno %in% permno25)
data2 = data %>%
  filter(permno %in% permno50)
data3 = data %>%
  filter(permno %in% permno75)
data4 = data %>%
  filter(permno %in% permno100)

remove(data)
#data = c(data1, data2, data3, data4)

library(foreach)
library(doParallel)

numcores = detectCores()

registerDoParallel(numcores)  # use multicore, set to the number of our cores
foreach (i=1:4) %dopar% {
  data = paste("data", toString(i), sep="")
  
  dates=unique(data$date)
  months=month(dates)
  indic=c(1, diff(months))
  datesInd=data.frame(dates, months, indic)
  firstDays=datesInd$date[datesInd$indic!=0]
  #Now we have the first days of each month
  for(t in 1:(length(firstDays)-17)){
    formation=data %>% filter(date>=firstDays[t] & date<firstDays[12+t])
    
    
    ##### FILTERING THE DATA #####
    formation=group_by(formation, permno)
    formation=filter(formation, mean(industry)==industry)
    #If a stock changes industries, we filter it out, up for debate
    formation=mutate(formation, check=(is.na(price) | price==0)*1)
    formation=formation %>% filter(sum(check)==0)
    
    #This should filter any stocks with prices NA or 0 for at least one day during formation period
    
    
    ##### SELECTING WINNERS AND LOSERS #####
    formation=mutate(formation, return=price/lag(price), log_ret=log(return))
    #This creates 3 columns: returns, log returns and industry ID
    for(i in 1:12){
      temp=filter(formation, industry==i)
      rank = temp %>% summarise(cum_ret=sum(log_ret, na.rm=T), avg_ME=mean(cap), month=t) %>% arrange(cum_ret)
      #rank contains all stocks, their SIC, cumulative returns and average market cap over formation period
      #Note that we ignore any NAs when computing cumulative returns, this should only happen for the first day in the formation period
      n=ceiling(nrow(rank)/10) 
      #I choose to round up, but it could be debated
      if(t==1){
        losers[[i]]=rank[1:n,]
        winners[[i]]=rank[(nrow(rank)-n+1):nrow(rank),]
        
        #This if/else is not crucial, it just makes winners not have an NA row if there are no stocks
      }
      else{
        losers[[i]]=bind_rows(losers[[i]], rank[1:n,])
        winners[[i]]=bind_rows(winners[[i]], rank[(nrow(rank)-n+1):nrow(rank),])
      }
      #This if is again not crucial, it just makes winners not have an NA row if there are no stocks
    }
    #This selects winners and losers for each industry
    print(t)
  }
  
}








numcores = detectCores()
cl = makeCluster(numcores)

clusterEvalQ(cl, {
  library(dplyr)
  library(data.table)
  library(lubridate)
})





stopCluster(cl)


##



















load('Data/industry_1-4.RData')

data_1_4 = data1
remove(data1)

data1 = data_1_4 %>%
  filter(industry==1)
data2 = data_1_4 %>%
  filter(industry==2)
data3 = data_1_4 %>%
  filter(industry==3)
data4 = data_1_4 %>%
  filter(industry==4)

data = tbl_df(data_mining)
remove(data_mining)

sicCodeRange = c(0099, 0999, 1499, 1799, 1999, 3999,
                 4999, 5199, 5999, 6799, 8999, 9099,
                 9729, 9899, 9999)
sicDivsionCode = c(1:10, 0, 11, 0, 12)

data$industry = cut(data$sic, sicCodeRange)
levels(data$industry) = sicDivsionCode
data$industry=as.numeric(data$industry)



dates=unique(data$date)
months=month(dates)
indic=c(1, diff(months))
datesInd=data.frame(dates, months, indic)
firstDays=datesInd$date[datesInd$indic!=0]
remove(datesInd)
for(t in 1:(length(firstDays)-17)){
  #can condense this filtering and eliminate excess columns
  formation = data %>%
    filter(date >= firstDays[t] & date < firstDays[t+12]) %>%
    group_by(permno) %>%
    filter(mean(industry)==industry) %>%
    mutate(check=(is.na(price) | price==0)*1) %>% #Can eliminate check and condense into 1 line
    filter(sum(check)==0) %>%
    mutate(return=price/lag(price), log_ret=log(return))
    
  print(formation)
  
}




#remove(data)


winners=list()
losers=list()

sicCodeRange = c(0099, 0999, 1499, 1799, 1999, 3999,
                 4999, 5199, 5999, 6799, 8999, 9099,
                 9729, 9899, 9999)

sicDivsionCode = c(1:10, 0, 11, 0, 12)


data$industry = cut(data$sic, sicCodeRange)










numcores = detectCores()
cl = makeCluster(numcores)

clusterEvalQ(cl, {
  library(dplyr)
  library(data.table)
  library(lubridate)
})


stopCluster(cl)
