##### LOAD DATA #####

library(dplyr)
library(lubridate)
setwd('\\\\icnas3.cc.ic.ac.uk/am718/R_Scripts/Econometrics')
load('Data/assignment_data18.RData')
#data=data_mining # This name is just because that's how the test file called the data


##### INITIALIZING VARIABLES #####
winners=list()
losers=list()

sicCodeRange = c(0099, 0999, 1499, 1799, 1999, 3999,
                 4999, 5199, 5999, 6799, 8999, 9099,
                 9729, 9899, 9999)
sicDivision = c('Agriculture, Forestry and Fishing', 'Mining', 'Construction',
                'not used','Manufacturing', 'Transportation, Communications, Electric, Gas and Sanitary service',
                'Wholesale Trade', 'Retail Trade', 'Finance, Insurance and Real Estate',
                'Services', 'NAAAA',  'Public Administration', 'AAAAN', 'Nonclassifiable')
sicDivsionCode = c(1:10, 0, 11, 0, 12)


data$industry = cut(data$sic, sicCodeRange)

data = tbl_df(data)

levels(data$industry) = sicDivsionCode
data$industry=as.numeric(data$industry)



#data=mutate(data, industry=if(sic<=999){1}else if(sic<=1499){2}else if(sic<=1799){3}else if (sic<=1999){4}else if (sic<=3999){5}else if(sic<=4999){6}else if(sic<=5199){7}else if(sic<=5999){8}else if(sic<=6799){9}else if(sic<=8999){10}else if(sic<=9729){11}else{12})
#This adds an industry column,matching SIC codes into industries



###### SETTING UP FORMATION PERIOD #####
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
#This closes the for loop

##### REPORTING RESULTS #####
winners_report=winners[[1]] %>% summarise(nStocks=n(), avg_ME=mean(avg_ME))
losers_report=losers[[1]] %>% summarise(nStocks=n(), avg_ME=mean(avg_ME))
for(i in 2:12){
  winners_report=bind_rows(winners_report, winners[[i]] %>% summarise(nStocks=n(), avg_ME=mean(avg_ME)))
  losers_report=bind_rows(losers_report, losers[[i]] %>% summarise(nStocks=n(), avg_ME=mean(avg_ME)))
}
#Note winnersSIC and losersSIC are lists fo 12 elements (tibbles)



#I save this piece of code just in  case I need to reuse it
"
nStocks=rep(0,length(firstDays)-17)
winnersME=rep(0,length(firstDays)-17)
losersME=rep(0,length(firstDays)-17)
#We initialize these arrays to keep track of how many stocks we added to the portfolio each month and their market cap

#####From here on it should go inside the for loop
nStocks[t]=n
winnersME[t]=mean(rank[(nrow(rank)-n+1):nrow(rank),]$avg_ME)
losersME[t]=mean(rank[1:n,]$avg_ME)

if(t==1){
  losers=rank[1:n,]
winners=rank[(nrow(rank)-n+1):nrow(rank),]
}
#For the first period we just create the portfolios
if(t>=2 & t<=6){
losers=bind_rows(losers, rank[1:n,])
winners=bind_rows(winners, rank[(nrow(rank)-n+1):nrow(rank),])
}
#For periods 2 to 6 we add stocks to existing portfolios
if(t>6){
losers=losers[-(1:nStocks[t-6]),]
losers=bind_rows(losers, rank[1:n,])
winners=winners[-(1:nStocks[t-6]),]
winners=bind_rows(winners, rank[(nrow(rank)-n+1):nrow(rank),])
}
#for subsequent periods, we eliminate the stocks we bought 6 months ago and add the new ones
"
