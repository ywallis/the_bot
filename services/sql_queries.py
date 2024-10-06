unmatched = """
SELECT 
    orders.clientorderid,
    SUM(CASE WHEN trades.side = 'sell' THEN trades.asset_net_q ELSE 0 END) AS total_sell_amount,
    SUM(CASE WHEN trades.side = 'buy' THEN trades.asset_net_q ELSE 0 END) AS total_buy_amount,
    SUM(CASE WHEN trades.side = 'sell' THEN trades.asset_net_q ELSE 0 END) - 
    SUM(CASE WHEN trades.side = 'buy' THEN trades.asset_net_q ELSE 0 END) AS sell_minus_buy
FROM 
    trades
LEFT JOIN 
    orders ON trades.order_id = orders.id
WHERE
		trades.datetime >= '2024-09-01 00:00:00'
GROUP BY 
    orders.clientorderid
HAVING 
    SUM(CASE WHEN trades.side = 'sell' THEN trades.asset_net_q ELSE 0 END) - 
    SUM(CASE WHEN trades.side = 'buy' THEN trades.asset_net_q ELSE 0 END) > 2
    OR 
    SUM(CASE WHEN trades.side = 'sell' THEN trades.asset_net_q ELSE 0 END) - 
    SUM(CASE WHEN trades.side = 'buy' THEN trades.asset_net_q ELSE 0 END) < -2;


"""

daily_inventory = """
SELECT
    DATE(datetime) AS date,
    COUNT(*) AS item_count,
    SUM(CASE WHEN trades.side = 'sell' THEN trades.asset_net_q ELSE 0 END) AS q_sells,
    SUM(CASE WHEN trades.side = 'buy' THEN trades.asset_net_q ELSE 0 END) AS q_buys,
    SUM(CASE WHEN trades.side = 'buy' THEN trades.asset_net_q ELSE 0 END) -
    SUM(CASE WHEN trades.side = 'sell' THEN trades.asset_net_q ELSE 0 END) AS delta
    
FROM
    trades
GROUP BY
    DATE(datetime)
ORDER BY
    date;
"""

orphans = """
SELECT 
    trades.*,
    orders.clientorderid
FROM 
    trades
LEFT JOIN 
    orders ON trades.order_id = orders.id
WHERE
		orders.clientorderid is Null;
"""