fetch_imbalances = """
WITH trade_data AS (
    SELECT 
        trades.symbol,
  			orders.clientorderid,
        SUM(CASE WHEN trades.side = 'sell' THEN trades.asset_net_q ELSE 0 END) AS total_sell_amount,
        SUM(CASE WHEN trades.side = 'buy' THEN trades.asset_net_q ELSE 0 END) AS total_buy_amount,
        SUM(CASE WHEN trades.side = 'buy' THEN trades.asset_net_q ELSE 0 END) - 
        SUM(CASE WHEN trades.side = 'sell' THEN trades.asset_net_q ELSE 0 END) AS delta,
        CASE 
            WHEN SUM(CASE WHEN trades.side = 'sell' THEN trades.asset_net_q ELSE 0 END) = 0 THEN NULL
            ELSE SUM(CASE WHEN trades.side = 'sell' THEN trades.price * trades.asset_net_q ELSE 0 END) /
                 SUM(CASE WHEN trades.side = 'sell' THEN trades.asset_net_q ELSE 0 END)
        END AS weighted_avg_sell_price,
        CASE 
            WHEN SUM(CASE WHEN trades.side = 'buy' THEN trades.asset_net_q ELSE 0 END) = 0 THEN NULL
            ELSE SUM(CASE WHEN trades.side = 'buy' THEN trades.price * trades.asset_net_q ELSE 0 END) /
                 SUM(CASE WHEN trades.side = 'buy' THEN trades.asset_net_q ELSE 0 END)
        END AS weighted_avg_buy_price
    FROM 
        trades
    LEFT JOIN 
        orders ON trades.order_id = orders.id
    WHERE
        trades.datetime >= '2024-09-01 00:00:00'
    GROUP BY
  			trades.symbol,
        orders.clientorderid
)

SELECT
    symbol,
    clientorderid,
    total_sell_amount,
    total_buy_amount,
    delta,
    CASE 
        WHEN delta < 0 THEN weighted_avg_sell_price
        ELSE weighted_avg_buy_price
    END AS applicable_price,
    delta * CASE 
        WHEN delta < 0 THEN weighted_avg_sell_price
        ELSE weighted_avg_buy_price
    END AS usdt_delta
FROM
    trade_data
WHERE
		ABS(delta * CASE 
        WHEN delta < 0 THEN weighted_avg_sell_price
        ELSE weighted_avg_buy_price
    END) > 3.1;
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
    date desc
LIMIT 10;
"""

orphans = """
SELECT DISTINCT
    trades.*,
    orders.clientorderid
FROM 
    trades
LEFT JOIN 
    orders ON trades.order_id = orders.id
WHERE
		orders.clientorderid is Null;
"""


daily_usdt = """
SELECT
    DATE(datetime) AS date,
    COUNT(*) AS item_count,
    SUM(CASE WHEN trades.side = 'sell' THEN trades.usdt_value ELSE 0 END) AS q_sells,
    SUM(CASE WHEN trades.side = 'buy' THEN trades.usdt_value ELSE 0 END) AS q_buys,
    SUM(CASE WHEN trades.side = 'sell' THEN trades.usdt_value ELSE 0 END) -
    SUM(CASE WHEN trades.side = 'buy' THEN trades.usdt_value ELSE 0 END) AS delta
    
FROM
    trades
GROUP BY
    DATE(datetime)
ORDER BY
    date desc
LIMIT 10;
"""