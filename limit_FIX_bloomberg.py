from time import sleep
import numpy as np
import pandas as pd
import numba

import itertools as it
import datetime
from mc_functions import functions
import zmq
import sys
import filelock

port = "9998"
instruments_pairs_dictionary = {}
if len(sys.argv) > 1:
    port =  sys.argv[1]
    int(port)
if len(sys.argv) > 2:
    port1 =  sys.argv[2]
    int(port1)
symbol = 'EUR'
currency = 'USD'
print(symbol+currency)
live_trading = 0
lot = 10000
bar_size = 5
hour_min = 'T'
stop_loss = 10000
take_profit = 10000
limit_pips = 0

functions_to_test = {'limit_reverse':functions.limit_reverse

                     }

# experiments = [{'limit_reverse': {}}]

#We set a instrument ID in order to create contract and get the appropriate results in Historical and Real Bars Prices
#The txt file is InstrumentsTickersPairs
market_params = [{'lot': 10000, 'profit_pips': take_profit, 'loss_pips': stop_loss}]


def get_datetime_index(now,bar_time,hour_or_min):
    if "min" in hour_or_min:
        if now.day-2 <0:
            start = datetime.datetime(now.year, now.month-1, now.day  + 28, now.hour, now.minute)
        else:
            start = datetime.datetime(now.year,now.month,now.day-2,now.hour,now.minute)
        end = datetime.datetime(now.year,now.month,now.day,now.hour,now.minute)
    else:
        if now.day -20<0:
            start = datetime.datetime(now.year, now.month - 1, now.day +10, now.hour)
        else:
            start = datetime.datetime(now.year, now.month, now.day-20, now.hour)
        end = datetime.datetime(now.year, now.month, now.day, now.hour)
    return start,end

def calculate_sleep_time(hour_min, bar_size):
    if hour_min == 'h':
        now = datetime.datetime.now()
        hours_to_wait = bar_size -  np.mod(now.hour,bar_size) - 1
        minutes_to_wait = 60 - now.minute
        seconds_to_wait = 60- now.second
        seconds_to_wait = seconds_to_wait + (minutes_to_wait*60) + (hours_to_wait*3600) + 5
        # if bar_size == 24:
        #     seconds_to_wait = seconds_to_wait + 15*60
    else:
        now = datetime.datetime.now()

        minutes_to_wait = bar_size - np.mod(now.minute,bar_size)-1
        seconds_to_wait = 60 - now.second
        seconds_to_wait = seconds_to_wait + (60*minutes_to_wait) + 5
        # if bar_size == 5:
        #     seconds_to_wait = seconds_to_wait + 2*60
    print(seconds_to_wait)
    return seconds_to_wait

def return_experiment_combinations(dictionary):
    dict_list = []
    for key in dictionary:
        dict_list.append(dictionary[key])
    variants = {}

    for item in dict_list:
        variants = variants.copy()
        variants.update(item)
    varNames = sorted(variants)
    combinations = [dict(zip(varNames, prod)) for prod in it.product(*(variants[varName] for varName in varNames))]
    return combinations

def create_signal(df,dictionary,market,decimals):
    parameter_set = return_experiment_combinations(dictionary)
    buy_condition = True
    sell_condition = True
    exit_condition = True

    for key in dictionary:
        buy, sell,exit_pos = functions_to_test[key](df, **parameter_set[0])
        buy_condition = np.logical_and(buy_condition, buy)
        sell_condition = np.logical_and(sell_condition, sell)
        exit_condition = np.logical_and(exit_condition,exit_pos)
        signals = np.zeros(len(df))
        exit_signals = np.zeros(len(df))

        signals[buy_condition] = 1
        signals[sell_condition] = -1
        exit_signals[exit_condition] = 1
        pip_value = 0.0001
        if "JPY" in currency:
            pip_value = 0.01
        lot = np.ones(shape=len(df)) * 10000
        filter_cumsum = pd.read_csv('C:/clusters/cumscum.csv')
        filter_cumsum.set_index(pd.to_datetime(filter_cumsum['time']))
        y = np.zeros(shape=len(df))
        y[-len(filter_cumsum):] = filter_cumsum['cumsum'].values

        df['cumsum'] = y


        df['bars'], df['trades'], df['market_trades'], df['trades_sum'], df['take_profit'], df['stop_loss'], df['equity'], df['exit_signals'] = fast_characterize_bars(
            signals, 1, df.open.values, df.high.values, df.low.values, df.close.values, 10000, 10000, pip_value,
            lot, 0, 5, exit_signals)


        df.to_csv('C:/clusters/cluster_' + symbol + '.' + currency + '_nofilter.csv')
        signals, exit_signals = apply_equity_filter(signals, exit_signals, df['cumsum'],
                                                    df['cumsum'].ewm(span=100).mean().bfill())
        df['bars'], df['trades'], df['market_trades'], df['trades_sum'], df['take_profit'], df['stop_loss'], df[
            'equity'], df['exit_signals'] = fast_characterize_bars(
            signals, 1, df.open.values, df.high.values, df.low.values, df.close.values, take_profit, 10000, 10000,
            lot, 0, 5, exit_signals)

        df.to_csv('C:/clusters/cluster_' + symbol + '.' + currency + '.csv')
        np.savetxt('C:/clusters/live_' + symbol + '.' + currency + '.txt', df.bars.values, newline='\r\n', delimiter='\t',
                   fmt='%d')
        if df.bars.iloc[-1] == 0:
            return False,False,True
        elif df.bars.iloc[-1]>0:
            return True,False,False
        elif df.bars.iloc[-1]<0:
            return False, True, False



@numba.jit(nopython=True, nogil=True)
def fast_characterize_bars(signals, bars_to_ignore, open, high, low, close, profit_pips, loss_pips, pip_value, lot,
                           is_combined,commision_amount,exit_signal):
    exit_signal = exit_signal.copy()
    sign = 0
    first_occurence = 0
    bars = np.zeros(signals.shape[0])
    trades_sum = np.zeros(signals.shape[0])
    trades = np.zeros(signals.shape[0])
    take_profit = np.zeros(signals.shape[0])
    stop_loss = np.zeros(signals.shape[0])
    market_trades = np.zeros(signals.shape[0])
    profit_value = 0
    loss_value = 0
    exit_signal[:bars_to_ignore] = 0
    for i in range(bars_to_ignore, len(signals), 1):

        if exit_signal[i] ==1:
            if sign ==1:
                sign = 0
                first_occurence = 0
                exit_signal[i] = -1
            elif sign ==-1:
                sign =0
                first_occurence = 0
                exit_signal[i] = 1
            elif sign == 0:
                exit_signal[i] = 0

        elif sign == 1 and high[i] > take_profit[i - 1]:
            market_trades[i] = -1
            sign = 0
            first_occurence = 0
        elif sign == 1 and low[i] < stop_loss[i - 1]:
            market_trades[i] = -1
            sign = 0
            first_occurence = 0
        elif sign == -1 and low[i] < take_profit[i - 1]:
            market_trades[i] = 1
            sign = 0
            first_occurence = 0
        elif sign == -1 and high[i] > stop_loss[i - 1]:
            market_trades[i] = 1
            sign = 0
            first_occurence = 0
        if signals[i - 1] > 0 and market_trades[i] == 0 and exit_signal[i] == 0:
            bars[i] = 1
            sign = 1
            if first_occurence > 0:
                trades[i] = 0
                take_profit[i] = profit_value
                stop_loss[i] = loss_value
            else:
                trades[i] = 1

                profit_value = open[i] + (profit_pips * pip_value)
                loss_value = open[i] - (loss_pips * pip_value)
                take_profit[i] = profit_value
                stop_loss[i] = loss_value
            first_occurence = 1
        elif signals[i - 1] < 0 and market_trades[i] == 0 and exit_signal[i] == 0:
            bars[i] = -1
            sign = -1
            if first_occurence < 0:
                trades[i] = 0
                take_profit[i] = profit_value
                stop_loss[i] = loss_value
            else:
                trades[i] = -1
                profit_value = open[i] - (profit_pips * pip_value)
                loss_value = open[i] + (loss_pips * pip_value)
                take_profit[i] = profit_value
                stop_loss[i] = loss_value
            first_occurence = -1
        else:
            bars[i] = sign
            trades[i] = 0
            take_profit[i] = profit_value
            stop_loss[i] = loss_value
    bars = bars * lot
    trade_sign = 0
    trade_open_price = 0
    trade_close_price = 0

    for i in range(bars_to_ignore, len(signals), 1):
        if trades[i] == 1:
            if trade_sign == -1:
                trades_sum[i] = trade_sign * (open[i] - trade_open_price) * (1/pip_value)
                trade_open_price = open[i]
                trade_sign = 1
            else:
                trade_sign = 1
                trade_open_price = open[i]
        elif trades[i] == -1:
            if trade_sign == 1:
                trades_sum[i] = trade_sign * (open[i] - trade_open_price) * (1/pip_value)
                trade_open_price = open[i]
                trade_sign = -1
            else:
                trade_sign = -1
                trade_open_price = open[i]
        elif exit_signal[i] != 0:
            if trade_sign == -1:
                trades_sum[i] = trade_sign*(open[i+1] - trade_open_price)*(1/pip_value)
                trade_sign = 0
                trade_open_price = 0
            elif trade_sign == 1:
                trades_sum[i] = trade_sign * (open[i + 1] - trade_open_price) * (1/pip_value)
                trade_sign = 0
                trade_open_price = 0

        elif market_trades[i] == 1:
            if trade_sign == -1:
                if take_profit[i] > low[i]:
                    if is_combined:
                        trades_sum[i] = trade_sign * (open[i + 1] - trade_open_price) * (1/pip_value)
                    else:
                        trades_sum[i] = trade_sign * (take_profit[i] - trade_open_price) * (1/pip_value)
                elif stop_loss[i] < high[i]:
                    if is_combined:
                        trades_sum[i] = trade_sign * (open[i + 1] - trade_open_price) * (1/pip_value)
                    else:
                        trades_sum[i] = trade_sign * (stop_loss[i] - trade_open_price) * (1/pip_value)
                else:
                    1

                trade_sign = 0
                trade_open_price = 0
        elif market_trades[i] == -1:
            if trade_sign == 1:
                if take_profit[i] < high[i]:
                    if is_combined:
                        trades_sum[i] = trade_sign * (open[i + 1] - trade_open_price) * (1/pip_value)
                    else:
                        trades_sum[i] = trade_sign * (take_profit[i] - trade_open_price) * (1/pip_value)

                elif stop_loss[i] > low[i]:
                    if is_combined:
                        trades_sum[i] = trade_sign * (open[i + 1] - trade_open_price) * (1/pip_value)
                    else:
                        trades_sum[i] = trade_sign * (stop_loss[i] - trade_open_price) * (1/pip_value)
                else:
                    1
                trade_sign = 0
                trade_open_price = 0

    trades_sum[-1] = trade_sign * (open[-1] - trade_open_price) * (1/pip_value)
    trades[-1] = trade_sign
    difference = np.zeros(shape=len(close))
    equity = np.zeros(shape=len(close))
    difference[:-1] = np.diff(open)

    equity = ((difference * bars/(lot*pip_value)) - (np.abs(trades) * commision_amount))
    trades_sum = trades_sum - (commision_amount)
    for i in range(0, len(market_trades), 1):
        if exit_signal[i] != 0:
            if signals[i-1] !=0:
                equity[i] = ((open[i + 1] - open[i]) * ((1 / pip_value)) * bars[i - 1] / abs(
                    bars[i - 1])) - (commision_amount)
            else:
                equity[i] = (open[i+1] - open[i])*((1/pip_value))*bars[i-1]/abs(bars[i-1]) - commision_amount
                trades_sum[i] = trades_sum[i] - commision_amount
        elif market_trades[i] < 0:
            if high[i] > take_profit[i]:
                equity[i] = (take_profit[i] - open[i])*(1/pip_value) - commision_amount
                trades_sum[i] = trades_sum[i] - commision_amount
            elif low[i] < stop_loss[i]:
                equity[i] = (stop_loss[i] - open[i])*(1/pip_value) - commision_amount
                trades_sum[i] = trades_sum[i] - commision_amount
        elif market_trades[i] > 0:
            if high[i]>stop_loss[i]:
                equity[i] = (open[i]- stop_loss[i])*(1/pip_value) - commision_amount
                trades_sum[i] = trades_sum[i] - commision_amount
            elif low[i] < take_profit[i]:
                equity[i] = ( open[i]- take_profit[i])*(1/pip_value) - commision_amount
                trades_sum[i] = trades_sum[i] - commision_amount

    return bars, trades, market_trades, trades_sum,take_profit,stop_loss,equity,exit_signal
def main():
    global duration_string, bar_size_setting, what_to_show, initialRequest
    import time
    import json
    file = "C:/Data/"
    import os
    import gc
    os.chdir(os.path.dirname(file))
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect('tcp://10.10.1.12:%s' % port)
    if len(sys.argv) > 2:
        socket.connect('tcp://10.10.1.12:%s' % port1)

    total_conts = 0
    current_conts = 0
    executed_period = None
    last_period = None
    while True:
        gc.collect()
        # sleep(calculate_sleep_time(hour_min,bar_size))
        sleep(10)
        now = datetime.datetime.now()
        print(now)
        hdf_path = 'minute_data' + symbol + currency+ '.h5'
        lock = filelock.FileLock(hdf_path+".lock")

        with lock.acquire(poll_intervall=0.005):
            store = pd.HDFStore(hdf_path,
                                mode='r')
            df = store[symbol+currency]

            store.close()

        #start,end = get_datetime_index(datetime.datetime.now(),bar_size,hour_min)
        #ask = df[np.logical_and(df.index>start,df.index<end)]
        ask = df.copy()
        ask.index = ask.index + pd.Timedelta(hours=7)
        del df


        now_minute = now.minute
        now_hour = now.hour
        decimals = np.where("JPY" in currency, 100, 10000)
        position = 0

        print('Last Period : '+str(last_period) + " while mext period : " +str((ask.index[-1]+pd.to_timedelta('1min')).ceil(str(bar_size)+hour_min))+ " - ASK INDEX"+str(ask.index[-1])+ '- ASK INDDEX PREVIOUS : '+str(ask.index[-2]))
        new_period = (ask.index[-1]+pd.to_timedelta('1min')).ceil(str(bar_size)+hour_min)
        if (last_period != new_period) & (new_period != executed_period):
            executed_period = new_period
            ask = ask.resample(str(bar_size) + hour_min, label='right', closed='right', base=0).agg(
                {'open': 'first', 'low': 'min', 'high': 'max', 'close': 'last'})

            ask = ask.dropna()
            ask = ask.iloc[-(cluster_slow + 200):]
            print('Strategy run')
            print("Close Price : " + str(ask.iloc[-1].close))
            ask_price = ask.iloc[-1].close

            # if len(experiments) == 1:
            if position == 0 | position < 0:
                buy = True
                action = 'buy'
                limit_price = ask_price - limit_pips

            elif position > 0:
                sell = True
                action = 'sell'
                limit_price = ask_price + limit_pips

                market_parameters = market_params[0]

            ###SEND QUOTE REQUEST FROM bloomberg_api.py
            #IN bloomberg_api.py GET QUOTES



            if action == 'buy':
                stop_loss = ask_price - market_parameters['loss_pips']/decimals
                take_profit = ask_price + market_parameters['profit_pips']/decimals
            elif action == 'sell':
                stop_loss = ask_price + market_parameters['loss_pips']/decimals
                take_profit = ask_price - market_parameters['profit_pips']/decimals
            else:
                stop_loss = 0
                take_profit = 0

            lot = market_parameters['lot']
            dict = {'action': action, 'symbol': symbol + currency, 'lot': lot,
                    'stop_loss': stop_loss, 'take_profit': take_profit}

            if live_trading==1:
                print(dict)
                socket.send_json(json.dumps(dict))

                # Get the reply.
                message = socket.recv()
                message = message.decode("utf-8")
                print ("Received reply "+"[", message+"]")
            else:
                1
        last_period = (ask.index[-1] + pd.to_timedelta('1min')).ceil(str(bar_size) + hour_min)
if __name__ == "__main__":
    main()