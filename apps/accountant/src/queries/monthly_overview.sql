-- Generates a month-by-month overview of bot performance

SELECT
    overview.symbol,
    TO_CHAR(DATE_TRUNC('month', overview.date), 'YYYY-MM') AS month,
    SUM(overview.usdt_gain) AS total_usdt_gain,
    SUM(overview.inventory_delta) AS total_inventory_delta
FROM (
    SELECT
        trades.symbol,
        DATE(trades.datetime) AS date,
        COUNT(*) AS item_count,
        SUM(CASE WHEN trades.side = 'sell' THEN trades.usdt_value ELSE 0 END) -
        SUM(CASE WHEN trades.side = 'buy' THEN trades.usdt_value ELSE 0 END) AS usdt_gain,
        SUM(CASE WHEN trades.side = 'buy' THEN trades.asset_net_q ELSE 0 END) -
        SUM(CASE WHEN trades.side = 'sell' THEN trades.asset_net_q ELSE 0 END) AS inventory_delta
    FROM
        trades
    WHERE
	trades.symbol = {symbol}
    GROUP BY
        trades.symbol,
        DATE(trades.datetime)
) AS overview
GROUP BY
    overview.symbol,
    DATE_TRUNC('month', overview.date)
ORDER BY
    overview.symbol,
    month DESC
LIMIT {range};
