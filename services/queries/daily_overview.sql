-- Generates an overview of the day-by-day performance over the past 7 days
SELECT
    trades.symbol,
    DATE(datetime) AS date,
    COUNT(*) AS item_count,

    SUM(CASE WHEN trades.side = 'sell' THEN trades.usdt_value ELSE 0 END) -
    SUM(CASE WHEN trades.side = 'buy' THEN trades.usdt_value ELSE 0 END) AS usdt_gain,
    SUM(CASE WHEN trades.side = 'buy' THEN trades.asset_net_q ELSE 0 END) -
    SUM(CASE WHEN trades.side = 'sell' THEN trades.asset_net_q ELSE 0 END) AS inventory_delta

FROM
    trades
WHERE
    trades.symbol = %(symbol)s
GROUP BY
    trades.symbol,
    DATE(datetime)
ORDER BY
    date DESC,
    trades.symbol

LIMIT
%(range)s