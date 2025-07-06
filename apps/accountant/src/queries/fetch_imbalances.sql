-- Fetch imbalances over a value of 3.1 USDT

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
    END) > 3.1 and symbol = %(symbol)s;
