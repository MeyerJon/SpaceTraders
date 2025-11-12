with sales as (
    select 
        shipSymbol,
        tradeSymbol,
        type,
        sum(units) as totalVolume,
        sum(totalPrice) as totalPrice
    from TRADES
    group by shipSymbol, tradeSymbol, type
    having type = "SELL"
),

purchases as (
    select 
        shipSymbol,
        tradeSymbol,
        type,
        sum(units) as totalVolume,
        sum(totalPrice) as totalPrice
    from TRADES
    group by shipSymbol, tradeSymbol, type
    having type = "PURCHASE"
)


select
    shipSymbol,
    tradeSymbol,
    p.totalPrice as totalCost,
    s.totalPrice as totalRevenue,
    s.totalPrice - p.totalPrice as totalProfit
from sales s
join purchases p using (shipSymbol, tradeSymbol)
