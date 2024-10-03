def min_max_usd_converter(price, instance_settings):

    instance_settings['maker_size'] = round(instance_settings['maker_size'] / price, 2)
    instance_settings['taker_max_order_size'] = round(instance_settings['taker_max_order_size'] / price, 2)

    print(f'Last USD quote is {price}.')
    print(f'Max maker size is {instance_settings['maker_size']}.')
    print(f'Max taker size is {instance_settings['taker_max_order_size']}.')

    return instance_settings