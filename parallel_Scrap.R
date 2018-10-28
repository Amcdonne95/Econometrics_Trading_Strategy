library(parallel)
library(dplyr)
library(data.table)
library(lubridate)
library(multidplyr)
library(foreach)
#library(nycflights13)


setwd("C:\\Users\\austi\\Desktop\\Imperial\\Modules\\Econometrics\\R_CourseWork\\Econometrics_Trading_Strategy")
winners=list()
losers=list()

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

single = data %>%
  filter(permno == 93422)

#
dates=unique(data$date)
months=month(dates)
indic=c(1, diff(months))
datesInd=data.frame(dates, months, indic)
firstDays=datesInd$date[datesInd$indic!=0]

numcores = detectCores(logical = FALSE)
cl = makeCluster(numcores)

clusterEvalQ(cl, {
  library(dplyr)
  library(data.table)
  library(lubridate)
})
clusterExport(cl, c("winners", "losers", "data", "firstDays"))


foreach(t = 1:(length(firstDays)-17), .combine = 'rbind') %dopar% {
  formation= data %>%
    filter(date>=firstDays[t] & date<firstDays[12+t]) %>%
    group_by(permno) %>%
    filter(mean(industry)==industry) %>%
    mutate(check=(is.na(price) | price==0)*1) %>%
    filter(sum(check)==0) %>%
    mutate(return=price/lag(price), log_ret=log(return))
  
  foreach(i = 1:12, .combine = 'rbind') %dopar% {
    temp=filter(formation, industry==i)
    rank = temp %>%
      summarise(cum_ret=sum(log_ret, na.rm=T), avg_ME=mean(cap), month=t) %>%
      arrange(cum_ret)
    n=ceiling(nrow(rank)/10)
    #print(cat("Industry----------- ", i))
    #print(i)
    
    if(t==1){
      losers[[i]] = rank[1:n,]
      winners[[i]]=rank[(nrow(rank)-n+1):nrow(rank),]
      
      #This if/else is not crucial, it just makes winners not have an NA row if there are no stocks
    }
    else{
      losers[[i]]=bind_rows(losers[[i]], rank[1:n,])
      winners[[i]]=bind_rows(winners[[i]], rank[(nrow(rank)-n+1):nrow(rank),])
    }
  }
}

stopCluster(cl)

winners_report=winners[[1]] %>% summarise(nStocks=n(), avg_ME=mean(avg_ME))
losers_report=losers[[1]] %>% summarise(nStocks=n(), avg_ME=mean(avg_ME))
for(i in 2:12){
  winners_report=bind_rows(winners_report, winners[[i]] %>% summarise(nStocks=n(), avg_ME=mean(avg_ME)))
  losers_report=bind_rows(losers_report, losers[[i]] %>% summarise(nStocks=n(), avg_ME=mean(avg_ME)))
}









sicCodeRange = c(0099, 0999, 1499, 1799, 1999, 3999,
                 4999, 5199, 5999, 6799, 8999, 9099,
                 9729, 9899, 9999)
sicDivsionCode = c(1:10, 0, 11, 0, 12)

data$industry = cut(data$sic, sicCodeRange)
levels(data$industry) = sicDivsionCode
data$industry=as.numeric(data$industry)

uniquePermno = unique(data$permno)

dates=unique(data$date)
months=month(dates)
indic=c(1, diff(months))
datesInd=data.frame(dates, months, indic)
firstDays=datesInd$date[datesInd$indic!=0]

tradingStrat = function(group, days){
  for (t in 1:length(days)-17){
    formation = group %>% 
      filter(group>=days[t] & date<days[12+t]) %>%
      group_by(permno) %>%
      filter(industry == mean(industry)) %>%
      mutate(check=(is.na(price) | price==0)*1) %>%
      filter(sum(check)==0) %>%
      mutate(return=price/lag(price), log_ret=log(return))
    
    
    for(i in 1:12){
      temp= formation %>%
        filter(industry==i)
      rank = temp %>%
        summarise(cum_ret=sum(log_ret, na.rm=T), avg_ME=mean(cap), month=t) %>%
        arrange(cum_ret)
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
  }
  
}





cl = detectCores(logical = FALSE)
x = split(uniquePermno, sort(uniquePermno%%cl))

cluster = create_cluster(cores = cl)

by_permno = data %>%
  partition(permno, cluster = cluster)


by_permno %>%
  # Assign libraries
  cluster_library("dplyr") %>%
  cluster_library("lubridate") %>%
  # Assign values (use this to load functions or data to each core)
  cluster_assign_value("firstDays", firstDays) %>%
  cluster_assign_value("tradingStrat", tradingStrat)

dataParallel = by_permno %>%
  


