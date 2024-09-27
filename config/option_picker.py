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
            choice = int(input(f'The active clients are {clients}. 0 Indexing. Leave empty for all'))

            if choice == '':
                return clients
            else:
                selected_client = [clients[choice]]

        except IndexError:
            print('Invalid option, try again.')
        else:
            return selected_client