-- Find trades which do not have a parent order

SELECT DISTINCT
    trades.*,
    orders.clientorderid
FROM
    trades
LEFT JOIN
    orders ON trades.order_id = orders.id
WHERE
		orders.clientorderid is Null;
