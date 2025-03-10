from apps.maker.src.enums import OrderSide


def maker_order_sizer(
    maker_level: float,
    taker_book: list[list],
    side: OrderSide,
    min_spread: float,
    max_maker_size: float,
    min_maker_size: float = 10,
) -> float:
    """NEEDS FLESHING OUT!
    Goal of function is to watch how much liquidity is available on the taker client within the defined spread.
    """

    cumulative: float = 0
    if side == OrderSide.SELL:
        for level in taker_book:
            if maker_level >= level[0] * min_spread:
                cumulative += level[1]
            else:
                break

    if side == OrderSide.BUY:
        for level in taker_book:
            if level[0] >= maker_level * min_spread:
                cumulative += level[1]
            else:
                break

    if cumulative < min_maker_size:
        return min_maker_size
    elif cumulative > max_maker_size:
        return max_maker_size
    else:
        return cumulative
