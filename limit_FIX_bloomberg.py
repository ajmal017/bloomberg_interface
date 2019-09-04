from time import sleep
import numpy as np
import pandas as pd
import numba
import pickle
import itertools as it
import datetime
from mc_functions import functions
import zmq
import sys
import filelock


instruments_pairs_dictionary = {}
if len(sys.argv) > 1:
    port =  sys.argv[1]
    int(port)
if len(sys.argv) > 2:
    port1 =  sys.argv[2]
    int(port1)

POSITIONS_PICKLE_PATH = 'positions.pickle'

#We set a instrument ID in order to create contract and get the appropriate results in Historical and Real Bars Prices
#The txt file is InstrumentsTickersPairs


def limit_strategy(position):
    exit_signal = False

    if position != 1:
        buy_signal = True
        sell_signal = False
    else:
        buy_signal = False
        sell_signal = True

    return buy_signal, sell_signal, exit_signal


def main(args=None):
    global duration_string, bar_size_setting, what_to_show, initialRequest
    import time
    import json
    file = "D:/Data/"
    import os
    import gc
    port = args.port
    SYMBOL = args.symbol
    CURRENCY = args.currency
    LIVE_TRADING = args.livetrading
    TEST_TRADING = args.testtrading
    LOT = args.lot
    BAR_SIZE = args.barsize
    HOUR_MIN = args.minhour
    LOSS_PIPS = args.losspips
    PROFIT_PIPS =args.profitpips
    LIMIT_PIPS = args.limitpips
    CUSTOM_TIMEDELTA = args.customtimedelta
    market_params = [{'LOT': LOT, 'profit_pips': PROFIT_PIPS, 'loss_pips': LOSS_PIPS}]

    print(f'SYMBOL:{SYMBOL}')
    print(f'CURRENCY:{CURRENCY}')
    print(f'LIMIT PIPS:{LIMIT_PIPS}')
    print(market_params)
    # os.chdir(os.path.dirname(file))
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect('tcp://10.10.1.33:%s' % port)
    if len(sys.argv) > 2:
        socket.connect('tcp://10.10.1.33:%s' % port1)

    def stop_quotes_message(symbol):
        dict = {'action': 'cancel_quotes', 'symbol': symbol, 'lot': 0, 'currency': None,
                'price': 0, 'stop_loss': 0, 'take_profit': 0}
        print(f'SEND CANCEL QUOTES MESSAGE')
        socket.send_json(json.dumps(dict))
        # Get the reply.
        message = socket.recv()
        message = message.decode("utf-8")
        print("Received reply " + "[", message + "]")

    def get_action_limit_sl_tp(buy_signal, sell_signal, market_parameters, decimals):

        # get action and limit price
        if buy_signal:
            action = 'buy'
            limit_price = ask_price - LIMIT_PIPS / float(decimals)
        if sell_signal:
            action = 'sell'
            limit_price = ask_price + LIMIT_PIPS / float(decimals)
        # if time_run_criterion and last_position != 0:
        #     action = 'update_limit'

        # create stop loss and take profit
        market_parameters = market_params[0]
        if action == 'buy':
            stop_loss = ask_price - market_parameters['loss_pips'] / float(decimals)
            take_profit = ask_price + market_parameters['profit_pips'] / float(decimals)
        elif action == 'sell':
            stop_loss = ask_price + market_parameters['loss_pips'] / float(decimals)
            take_profit = ask_price - market_parameters['profit_pips'] / float(decimals)
        else:
            stop_loss = 0
            take_profit = 0
        return action, limit_price, stop_loss, take_profit

    buy_signal = False
    sell_signal = False
    exit_signal = True
    executed_period = None
    last_period = None

    while True:
        gc.collect()
        intrabar_run_criterion = False
        time_run_criterion = False
        sleep(10)
        now = datetime.datetime.now()
        print(now)
        if TEST_TRADING == 1:
            test_scenario_dict = {1:test_scenario_1, 2:test_scenario_2, 3:test_scenario_3, 4:test_scenario_4, 5:test_scenario_5,
                                  6:test_scenario_6, 7:test_scenario_7, 8:test_scenario_8, 9:test_scenario_9,
                                10:test_scenario_10, 11:test_scenario_11, 12:test_scenario_12,
                                13:test_scenario_13, 14:test_scenario_14,
                                15:test_scenario_15, 16:test_scenario_16, 17:test_scenario_17
                                 }

            test_scenario = int(input(f'Choose test scenario: '))
            test_order = test_scenario_dict[test_scenario]()
            socket.send_json(json.dumps(test_order))

            # Get the reply.
            try:
                message = socket.recv()
                message = message.decode("utf-8")
                print("Received reply " + "[", message + "]")
            except:
                print('no reply received')
                continue

        else:

            hdf_path = 'D:/Data/minute_data' + SYMBOL + '.h5'
            lock = filelock.FileLock(hdf_path+".lock")

            with lock.acquire(poll_intervall=0.005):
                store = pd.HDFStore(hdf_path,
                                    mode='r')
                df = store[SYMBOL]

                store.close()

            ask = df.copy()
            ask.index = ask.index + pd.Timedelta(hours=CUSTOM_TIMEDELTA)
            del df

            decimals = np.where("JPY" in SYMBOL, 100, 10000)

            print('Last Period : '+str(last_period) + " while mext period : " +str((ask.index[-1]+pd.to_timedelta('1min')).ceil(str(BAR_SIZE)+HOUR_MIN))+
                  " - ASK INDEX"+str(ask.index[-1])+ '- ASK INDEX PREVIOUS : '+str(ask.index[-2]))
            new_period = (ask.index[-1]+pd.to_timedelta('1min')).ceil(str(BAR_SIZE)+HOUR_MIN)

            #run criterios
            #intrabar criterion
            # if datetime.datetime.fromtimestamp(os.path.getmtime(POSITIONS_PICKLE_PATH)) + datetime.timedelta(seconds=15) >= now:
            #     time.sleep(5)

            #timeframe criterion
            time_run_criterion = (last_period != new_period) & (new_period != executed_period)

            if time_run_criterion:

                print(f'RUNNING DUE TO TIMEFRAME')
                # send dummy message with no_trade = 1 to stop quotes in order to avoid conflict with positions.pickle
                stop_quotes_message(SYMBOL)

                executed_period = new_period
                ask = ask.resample(str(BAR_SIZE) + HOUR_MIN, label='right', closed='right', base=0).agg(
                    {'open': 'first', 'low': 'min', 'high': 'max', 'close': 'last'})

                ask = ask.dropna()
                ask_price = ask.iloc[-1].close
                print('Strategy run')
                print("Close Price : " + str(ask.iloc[-1].close))

                # sleep to catch order handler the cancel quotes
                time.sleep(5)

                # read last orders
                with open(POSITIONS_PICKLE_PATH, 'rb')as f:
                    orders = pickle.load(f)
                last_position = orders[f'{SYMBOL[:3]}/{SYMBOL[3:]}']['position']
                quantity = LOT
                buy_signal = True
                sell_signal = False
                if last_position < 0:
                    quantity = 2 * LOT
                elif last_position > 0:
                    quantity = 2 * LOT
                    buy_signal = False
                    sell_signal = True

                action, limit_price, stop_loss, take_profit = get_action_limit_sl_tp(buy_signal=buy_signal,
                                                                                     sell_signal=sell_signal,
                                                                                     market_parameters=market_params,
                                                                                     decimals=decimals)
                #create order dictionary
                dict = {'action': action, 'symbol': SYMBOL, 'lot': quantity, 'currency':CURRENCY,
                            'price':limit_price,'stop_loss': stop_loss, 'take_profit': take_profit}

                if LIVE_TRADING == 1:
                    print(dict)
                    socket.send_json(json.dumps(dict))

                    # Get the reply.
                    message = socket.recv()
                    message = message.decode("utf-8")
                    print ("Received reply "+"[", message+"]")
                    print('UPDATING LIMIT PRICE - SLEEPING')
                    time.sleep(10)
                else:
                    1

            else:
                with open(f'{POSITIONS_PICKLE_PATH}', "rb") as f:
                    orders = pickle.load(f)
                if len(orders[f'{SYMBOL[:3]}/{SYMBOL[-3:]}']['trades']) > 0:
                    last_order = orders[f'{SYMBOL[:3]}/{SYMBOL[-3:]}']['trades'][-1]
                    # print(pd.to_datetime(last_order['time']) + datetime.timedelta(hours=3) , datetime.timedelta(seconds=40))
                    if pd.to_datetime(last_order['time']) + datetime.timedelta(hours=3) + datetime.timedelta(seconds=20) > datetime.datetime.now():
                        # send dummy message with no_trade = 1 to stop quotes in order to avoid conflict with positions.pickle
                        # sleep to catch order handler the cancel quotes
                        stop_quotes_message(symbol=SYMBOL)
                        time.sleep(5)
                        print(f'RUNNING INTRABAR')

                        intrabar_run_criterion = True
                        last_position = orders[f'{SYMBOL[:3]}/{SYMBOL[3:]}']['position']
                        quantity = abs(last_position) + LOT
                        last_trade_price = orders[f'{SYMBOL[:3]}/{SYMBOL[3:]}']['trades'][-1]['price']
                        ask_price = float(last_trade_price)
                        last_position = np.where(last_position <= 0, 2, 1)
                        buy_signal, sell_signal, exit_signal = limit_strategy(position=int(last_position))
                        action, limit_price, stop_loss, take_profit = get_action_limit_sl_tp(buy_signal=buy_signal,
                                                                                             sell_signal=sell_signal,
                                                                                             market_parameters=market_params,
                                                                                                 decimals=decimals)
                        # create order dictionary
                        dict = {'action': action, 'symbol': SYMBOL, 'lot': quantity, 'currency': CURRENCY,
                                'price': limit_price, 'stop_loss': stop_loss, 'take_profit': take_profit}

                        if LIVE_TRADING == 1:
                            print(dict)
                            socket.send_json(json.dumps(dict))

                            # Get the reply.
                            message = socket.recv()
                            message = message.decode("utf-8")
                            print("Received reply " + "[", message + "]")
                            print('SETTING LIMIT PRICE INTRABAR - SLEEPING')
                            time.sleep(10) #to avoid running again
                        else:
                            1
                    else:
                        pass
                else:
                    pass


                            # if time_run_criterion or intrabar_run_criterion:

                #send dummy message with no_trade = 1 to stop quotes in order to avoid conflict with positions.pickle
                # stop_quotes_message(SYMBOL)

                # executed_period = new_period
                # ask = ask.resample(str(BAR_SIZE) + HOUR_MIN, label='right', closed='right', base=0).agg(
                #     {'open': 'first', 'low': 'min', 'high': 'max', 'close': 'last'})
                #
                # ask = ask.dropna()
                # print('Strategy run')
                # print("Close Price : " + str(ask.iloc[-1].close))

                #sleep to catch order handler the cancel quotes
                # time.sleep(5)
                #script runs because trade is executed intrabar
                # if intrabar_run_criterion:
                    # time.sleep(1)#1
                    # read last orders
                    # with open(POSITIONS_PICKLE_PATH, 'rb')as f:
                    #     orders = pickle.load(f)
                    # last_position = orders[f'{SYMBOL[:3]}/{SYMBOL[3:]}']['position']
                    # quantity = abs(last_position) + LOT
                    # last_trade_price = orders[f'{SYMBOL[:3]}/{SYMBOL[3:]}']['trades'][-1]['price']
                    # ask_price = float(last_trade_price)
                    # print(f'RUNNING INTRABAR')
                    # last_position = np.where(last_position <= 0 ,2,1)
                    # buy_signal, sell_signal, exit_signal = limit_strategy(position=int(last_position))
                #script runs because of timeframe
                # elif time_run_criterion and (not intrabar_run_criterion):
                    # print(f'RUNNING DUE TO TIMEFRAME')
                    #time sleep to catch any possible changes close to timeframe bar
                    # time.sleep(5)

                    # read last orders
                    # with open(POSITIONS_PICKLE_PATH, 'rb')as f:
                    #     orders = pickle.load(f)
                    # last_position = orders[f'{SYMBOL[:3]}/{SYMBOL[3:]}']['position']
                    # ask_price = ask.iloc[-1].close
                    # quantity = 2 * LOT
                    # if last_position < 0:
                    #     buy_signal = False
                    #     sell_signal = True
                    # elif last_position > 0:
                    #     buy_signal = True
                    #     sell_signal = False
                    # else:
                    #     quantity = LOT
                    #     buy_signal = True
                    #     sell_signal = False


                # #create order dictionary
                # dict = {'action': action, 'symbol': SYMBOL, 'lot': quantity, 'currency':CURRENCY,
                #             'price':limit_price,'stop_loss': stop_loss, 'take_profit': take_profit}
                #
                # if LIVE_TRADING == 1:
                #     print(dict)
                #     socket.send_json(json.dumps(dict))
                #
                #     # Get the reply.
                #     message = socket.recv()
                #     message = message.decode("utf-8")
                #     print ("Received reply "+"[", message+"]")
                # else:
                #     1
            last_period = (ask.index[-1] + pd.to_timedelta('1min')).ceil(str(BAR_SIZE) + HOUR_MIN)


def test_scenario_1():
    '''
    Client requests quotes from two LPs,
    client sends trade request to one, LP fills
    '''
    print(f'TESTING SCENARIO 1...')
    action = 'buy'
    SYMBOL = 'EURCAD'
    CURRENCY = 'EUR'
    LOT = 1000
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 999
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

def test_scenario_2():
    '''
    Client requests quotes from two LPs,
    after quotes arrive,
    client sends quote response pass
    '''
    #resend scenario_1 --> trade will pass because it is in the same direction
    print(f'TESTING SCENARIO 3')
    dict = test_scenario_1()
    return dict

def test_scenario_3():
    '''
    Client requests quotes two way quotes from two LPs,
    both dealers fail to respond to quote request and do not quote,
    client cancels after 30sec
    '''
    print(f'TESTING SCENARIO 3')
    action = 'buy'
    SYMBOL = 'EURUSD'
    CURRENCY = 'EUR'
    LOT = 1137
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 999
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

def test_scenario_4():
    '''
    Client requests quotes from two LPs,
    all LPs reject
    '''
    print(f'TESTING SCENARIO 4...')
    action = 'buy'
    SYMBOL = 'EURAUD'
    CURRENCY = 'EUR'
    LOT = 1779
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 999
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

def test_scenario_5():
    '''
    Client requests quotes from two LPs,
    client fails to send trade request to one LP,
    no trade is done
    '''
    print(f'TESTING SCENARIO 5...')
    action = 'buy'
    SYMBOL = 'EURCAD'
    CURRENCY = 'EUR'
    LOT = 1000000
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 999
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

def test_scenario_6():
    '''
    Client requests quotes from two LPs,
    one LP rejects,
    '''
    print(f'TESTING SCENARIO 6...')
    action = 'buy'
    SYMBOL = 'EURCAD'
    CURRENCY = 'EUR'
    LOT = 1779
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 999
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

def test_scenario_7():
    '''
    Client requests quotes from two LPs,
    client sends trade request to one,
    dealer rejects last look,
    '''
    print(f'TESTING ORDER 7...LP REJECT LAST LOOK')
    action = 'buy'
    SYMBOL = 'EURCHF'
    CURRENCY = 'EUR'
    LOT = 5241
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 999
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

def test_scenario_8():
    '''
    Client requests quotes from two LPs,
    client sends trade request to one,
    LP times out,
    '''
    print(f'TESTING ORDER 8...LP TIME OUT')
    action = 'buy'
    SYMBOL = 'GBPUSD'
    CURRENCY = 'GBP'
    LOT = 6531
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 999
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

def test_scenario_9():
    '''
    '''
    print(f'TESTING ORDER 9...')
    pass

def test_scenario_10():
    '''
    Client requests two way quotes from two LPs,
    after 10s LPs cancel all quotes,
    no trade is done
    '''
    print(f'TESTING ORDER 10...TWO WAY')
    action = 'none'
    SYMBOL = 'EURCAD'
    CURRENCY = 'EUR'
    LOT = 1885
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 999
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

def test_scenario_11():
    '''
    '''
    print(f'TESTING ORDER 11...LP TIME OUT')
    pass

def test_scenario_12():
    '''
    Client requests quotes from two LPs,
    client sends trade request with the wrong side to on,
    LP rejects, client cancels
    '''
    print(f'TESTING ORDER 12...WRONG SIDE')
    action = 'buy'
    SYMBOL = 'GBPCAD'
    CURRENCY = 'GBP'
    LOT = 1000
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 999
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

def test_scenario_13():
    '''
    Client requests two way quotes from two LPs,
    client sends trade request to one,
    LP fills
    '''
    print(f'TESTING ORDER 13...TWO WAY QUOTES')
    action = 'none'
    SYMBOL = 'GBPCAD'
    CURRENCY = 'CAD'
    LOT = 1000
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 999
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

def test_scenario_14():
    '''
    Send BUY order in base CURRENCY
    '''
    print(f'TESTING ORDER 14...BUY order in BASE CURRENCY')
    action = 'buy'
    SYMBOL = 'EURUSD'
    CURRENCY = 'EUR'
    LOT = 1000000
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 999
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

def test_scenario_15():
    '''
    Send SELL order in base CURRENCY
    '''
    print(f'TESTING ORDER 14...SELL order in BASE CURRENCY')
    action = 'sell'
    SYMBOL = 'EURUSD'
    CURRENCY = 'EUR'
    LOT = 1000000
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 0
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

def test_scenario_16():
    '''
    Send BUY order in secondary CURRENCY
    '''
    print(f'TESTING ORDER 16...BUY order in SECONDARY CURRENCY')
    action = 'buy'
    SYMBOL = 'EURUSD'
    CURRENCY = 'USD'
    LOT = 1000000
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 0
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

def test_scenario_17():
    '''
    Send SELL order in secondary CURRENCY
    '''
    print(f'TESTING ORDER 14...BUY order in SECONDARY CURRENCY')
    action = 'sell'
    SYMBOL = 'EURUSD'
    CURRENCY = 'USD'
    LOT = 1000000
    STOP_LOSS = 10000
    TAKE_PROFIT = 10000
    LIMIT_PRICE = 999
    dict = {'action': action, 'symbol': SYMBOL, 'lot': LOT,
            'currency':CURRENCY,'stop_loss': STOP_LOSS, 'take_profit': TAKE_PROFIT,
            'price':LIMIT_PRICE}
    print(dict)
    return dict

if __name__ == "__main__":
    import argparse

    port = "9998"
    SYMBOL = 'AUDCAD'
    CURRENCY = 'AUD'
    LIVE_TRADING = 1
    TEST_TRADING = 0
    LOT = 10000
    BAR_SIZE = 5
    HOUR_MIN = 'T'
    LOSS_PIPS = 10000
    PROFIT_PIPS = 10000
    LIMIT_PIPS = 2
    CUSTOM_TIMEDELTA = 7
    parser = argparse.ArgumentParser(description='Arguements for running limit strategy')
    parser.add_argument('--port', type=str, default="9998")
    parser.add_argument('--symbol', type=str, default="AUDCAD")
    parser.add_argument('--currency', type=str, default="AUD")
    parser.add_argument('--minhour', type=str, default="T")
    parser.add_argument('--livetrading', type=int, default=1)
    parser.add_argument('--testtrading', type=int, default=0)
    parser.add_argument('--lot', type=int, default=10000)
    parser.add_argument('--barsize', type=int, default=5)
    parser.add_argument('--losspips', type=int, default=10000)
    parser.add_argument('--profitpips', type=int, default=10000)
    parser.add_argument('--limitpips', type=int, default=2)
    parser.add_argument('--customtimedelta', type=int, default=7)

    args = parser.parse_args()
    print(args)
    main(args=args)