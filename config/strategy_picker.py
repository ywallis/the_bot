from config.strategies import deployed_strategies

def strategy_picker():
    choosing_strategy = True

    while choosing_strategy:
        keys = list(deployed_strategies.keys())
        try:
            choice = int(input(f'The deployed strategies are {keys}. 0 Indexing'))
        except IndexError:
            print('Invalid option, try again.')
        else:
            selected_strategy = keys[choice]
            return deployed_strategies[selected_strategy]

