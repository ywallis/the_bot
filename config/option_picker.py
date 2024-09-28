from config.strategies import deployed_strategies


def strategy_picker():
    choosing_strategy = True

    while choosing_strategy:
        keys = list(deployed_strategies.keys())
        try:
            choice = int(input(f'The deployed strategies are {keys}. 0 Indexing.'))
            selected_strategy = keys[choice]
        except IndexError:
            print('Invalid option, try again.')
        else:
            return deployed_strategies[selected_strategy]


def status_client_picker(clients):

    choosing_client = True

    while choosing_client:
        try:
            choice = input(f'The active clients are {clients}. 0 Indexing. Leave empty for all')

            if choice == '':
                return clients
            else:
                selected_client = [clients[int(choice)]]

        except IndexError:
            print('Invalid option, try again.')
        else:
            return selected_client

def maker_client_picker(strategy):
    """Takes in a strategy and lets the user pick a maker client
    that will be passed to be started. It returns the exchange ID, and the index for the appropriate values."""

    choosing_client = True
    exchange_list = [exchange['id'] for exchange in strategy['maker_exchanges']]
    print(f'')

    while choosing_client:
        try:
            choice = int(input(f'The available clients are {exchange_list}. 0 Indexing.'))
            selected_client = exchange_list[choice]
        except IndexError:
            print('Invalid option, try again.')
        else:
            return selected_client, choice
