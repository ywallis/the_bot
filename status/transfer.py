# from config.config import gate_client, mexc_client, bitget_client


def initiate_mexc_sub_account_transfer(client, destination_account, currency, amount):
    """This function takes in a MEXC ccxt client, destination account, currency code and amount and initiates a cross account transfer."""

    # print(mexc_client_master.spotPrivateGetSubAccountList())
    # print(mexc_client_master.spot_private_get_capital_sub_account_universaltransfer(params={'toAccount': 'alpharb1', 'fromAccountType': 'SPOT', 'toAccountType': 'SPOT'}))
    # print(mexc_client_master.spot_private_post_capital_sub_account_universaltransfer(params={'fromAccount': 'alpharb1', 'fromAccountType': 'SPOT', 'toAccountType': 'SPOT', 'asset': 'USDT', 'amount': 3999}))
    # # mexc_client_master.spot_private_sub
    # # print(mexc_client.fetch_accounts())
    # # print(mexc_client_master.spot_private_get_sub_account_asset(params={'fromAccountType': 'SPOT'}))
    #

    pass

def initiate_exchange_transfer(sender_client, receiver_client, currency, network, amount):
    """This function takes two ccxt client, a currency code, currency network and an amount, and initiates a transfer between exchanges."""

    # MEXC CAN'T TRANSFER DIRECTLY IN OR OUT OF SUBACCOUNTS
    # BITGET CAN'T TRANSFER OUT OF SUBACCOUNTS

    # Check if deposits are available for currency / network

    if sender_client.name == 'MEXC Global' or receiver_client.name == 'MEXC Global':
        print("MEXC can't send directly out of subaccounts.")
        return

    if sender_client.name == 'Bitget':
        print("Bitget can't send directly out of subaccounts.")
        return

    receiver_client.load_markets()
    sender_client.load_markets()

    try:
        if not receiver_client.currencies[f'{currency}']['networks'][f'{network}']['deposit']:
            print('Deposits are disabled for the selected network')
            return
    except KeyError:
        print('Deposits not available for the selected network, check for typos.')
        return

    try:
        if not receiver_client.currencies[f'{currency}']['networks'][f'{network}']['withdraw']:
            print('Withdrawals are disabled for the selected network')
            return
    except KeyError:
        print('Withdrawals not available for the selected network, check for typos.')
        return

    # Check if amount is within min max allowed

    # Get address from recipient

    recipient_address = receiver_client.fetch_deposit_address(code=f'{currency}', params={'network': f'{network}'})['address']

    print(f'Preparing a {amount} {currency} transfer to {recipient_address} on {network}.')

    # Initiate transfer and log details

    # MEXC ONLY: loop and wait for transfer to have arrived, then transfer to subaccount
    pass

# initiate_exchange_transfer(mexc_client, bitget_client, 'USDT', 'BEP20', 1)