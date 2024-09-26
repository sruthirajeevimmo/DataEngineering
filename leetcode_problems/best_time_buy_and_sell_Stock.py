#Input: prices = [7,1,5,3,6,4]
#Output: 5
def maxProfit(prices):
    profit=0
    days=prices
    i=0
    j=0
    for i in range(i,len(days)):
        
            for j in range(j,len(days)):
                if days[j]==days[0]:
                    continue
                else:
                    if days[j]>prices[i]:
                        if days[j]-prices[i]>profit:
                            profit=days[j]-prices[i]
                            print(profit,prices[i],days[j])
            i=i+1                   
    return profit

profit=maxProfit([7,1,5,3,6,4])
print(profit)