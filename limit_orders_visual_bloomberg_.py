import pandas as pd
import filelock
import dash
from dash.dependencies import Input, Output, Event, State
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import zmq
import sys
import json
from datetime import datetime
import pickle


port = "9998"
host_connect = "127.0.0.1"
instruments_pairs_dictionary = {}
if len(sys.argv) > 1:
    port =  sys.argv[1]
    int(port)
counter = 0
if len(sys.argv) > 2:
    port1 =  sys.argv[2]
    int(port1)
action_switch = 0
POSITIONS_PICKLE_PATH = r'positions.pickle'
WARNINGS_PICKLE_PATH = r'warnings.pickle'

symbols = {"EURUSD" :("EUR",'USD'), 'EURGBP': ('EUR','GBP'), 'EURJPY': ('EUR','JPY'),'EURCHF':('EUR','CHF'), "EURCAD": ("EUR","CAD"), 'EURAUD': ('EUR','AUD'),'EURNZD':('EUR','NZD'),
                 "GBPUSD": ("GBP",'USD'),'USDJPY' :('USD','JPY'),'USDCHF':('USD','CHF'),'USDCAD':('USD','CAD'),'AUDUSD':('AUD','USD'),'NZDUSD':('NZD','USD'),
                 'GBPJPY' : ('GBP','JPY'),'GBPCHF':('GBP','CHF'),'GBPCAD':('GBP','CAD'),'GBPAUD':('GBP','AUD'),'GBPNZD':('GBP','NZD'),
                 "CHFJPY": ("CHF",'JPY'),'CADJPY':('CAD','JPY'),'AUDJPY':('AUD','JPY'), 'NZDJPY':('NZD','JPY'),
                 'CADCHF':('CAD','CHF'), 'AUDCHF':( 'AUD','CHF'),'NZDCHF':('NZD','CHF'),
                 'AUDCAD':('AUD','CAD'), 'AUDNZD':('AUD','NZD'),
                 'NZDCAD':('NZD','CAD'),

                 }
accounts = {'U1369584' : 'Phantom S',
            'U1379767': 'Master',
            'U1419799': 'Phantom C',
            'U1430991': 'Phantom HL',
            'U1431012': 'Phantom ZZ',
            'U14310022': 'Phantom ST'}
pip_values = {f'{s[:3]}/{s[-3:]}':(1. if 'JPY' not in s else 0.01) for s in symbols.keys()}

def close_position(symbol):
    with open(POSITIONS_PICKLE_PATH, 'rb') as file:
        positions = pickle.load(file)
        file.close()
        symbol_state = positions[symbol]
    current_position = symbol_state['position']
    if current_position > 0:
        action = 'sell'
        price = 999
    else:
        action = 'buy'
        price = 0
    dict = {'action': action, 'symbol': symbol, 'lot': current_position, 'currency': symbol[:3],
            'price': price, 'stop_loss': 0, 'take_profit': 0, }
    print(f'CLOSE POSITION MESSAGE')
    return dict


app = dash.Dash(csrf_protect=False)
app.layout = html.Div(children=[
    html.H2(children='FIX Limit Orders Monitoring', style = {'color':'grey'}),

    html.Div(id='row', children=[
    html.Div(id='time', children=[]),
    html.Div(id= 'symbols',children = [

    html.Div(id = 'open',children = [], style={'display': 'table-cell','width':'600px','border-spacing':'27px'}),
    html.Div(id = 'trades',children = [], style={'display': 'table-cell','width':'600px'}),
    html.Button('close position', id='close_position_button',n_clicks=0),
    html.Div(id='button_output', children=''),
    html.Div(dcc.Input(id='input_for_close', type='text',placeholder='Write symbol to close position')),

    dcc.Interval(id='interval-component', interval=100*100),
    html.Br()
    # ,dcc.RadioItems(
    #     id='dropdown-b',
    #     options=[{'label': i, 'value': i} for i in symbols],
    #     value='EURUSD'
    # )
    ]),

]),

html.H3(children='NoQuotesWarning', style={'color':'grey'}),
    html.Div(id='no_quotes',children=[]),

])

@app.callback(
        Output('button_output', 'children'),
        [Input(component_id='close_position_button', component_property='n_clicks')],
        [State('input_for_close', 'value')]
)
def send_close_order(n_clicks, value):
    if n_clicks > 0:
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect('tcp://127.0.0.1:%s' % port)
        order = close_position(symbol=value)
        order['symbol'] = order['symbol'].replace('/','').upper()
        # order = {'dash':999}
        socket.send_json(json.dumps(order))
        message = socket.recv()
        message = message.decode("utf-8")
        print("Received reply from order handler for close order " + "[", message + "]")


@app.callback(Output(component_id = 'open',component_property='children'),events = [Event('interval-component','interval')])
def change_symbol():
    global action_switch

    # context = zmq.Context()
    # socket = context.socket(zmq.REQ)
    # socket.connect('tcp://'+host_connect+':%s' % port)
    # if len(sys.argv) > 2:
    #     socket.connect('tcp://'+host_connect+':%s' % port1)
    # dict = {'get':'positions'}
    #
    #
    # socket.send_json(json.dumps(dict))
    ##  Get the reply.
    # message = socket.recv_json()
    with open(POSITIONS_PICKLE_PATH, 'rb')as f:
        orders = pickle.load(f)

    current_prices = {symbol:0 for symbol in orders.keys()}
    last_open_price = {symbol:0. for symbol in orders.keys()}
    for symbol in orders.keys():
        if len(orders[symbol]['trades']) == 0:
            last_open_price[symbol] = 0
        else:
            last_open_price[symbol] = float(orders[symbol]['trades'][-1]['price'])
    for symbol in orders.keys():
        hdf_path = f'C:/Data/minute_data' + symbol.replace('/','') + '.h5'
        lock = filelock.FileLock(hdf_path + ".lock")
        with lock.acquire(poll_intervall=0.005):
            store = pd.HDFStore(hdf_path,
                                mode='r')
            df = store[symbol.replace('/','')]
            store.close()
        current_prices[symbol] = df.iloc[-1].close
        del df


    action_switch = 1
    print("CURRENT ORDERS ", "[", orders, "]")
    # print("Received reply ", "[", message, "]")

    return html.Table(
        # Header

        [html.Tr([html.Th('Symbol')]+[html.Th('Position')] +[html.Th('Trades')] + [html.Th('Open Equity')])] +

        # Body
        [html.Tr([html.Td(pair)]+[html.Td(orders[pair]['position'])] + [html.Td(len(orders[pair]['trades']))] + [html.Td(round(pip_values[pair] * orders[pair]['position'] * (current_prices[pair] - last_open_price[pair]),2))])
            for pair in orders.keys()]
                 )

@app.callback(Output(component_id = 'trades',component_property='children'),events = [Event('interval-component','interval')])
def change_symbol():

    # global counter
    # context = zmq.Context()
    # socket = context.socket(zmq.REQ)
    # socket.connect('tcp://'+host_connect+':%s' % port)
    # if len(sys.argv) > 2:
    #     socket.connect('tcp://'+host_connect+':%s' % port1)
    # dict = {'get':'orders'}
    #
    # socket.send_json(json.dumps(dict))
    ##  Get the reply.
    # message = socket.recv_json()

    with open(POSITIONS_PICKLE_PATH, 'rb')as f:
        orders = pickle.load(f)

    if datetime.now().hour >= 20:

        x = pd.DataFrame.from_dict(orders)
        with open('//10.10.1.13/Development_Production/Performance_live/speedlab_trades/stergios_trades.csv', 'a') as f:
            x.to_csv(f, header=False)
        counter = 1
    if datetime.now().hour >= 20:
        x = pd.DataFrame.from_dict(orders)
        with open('//10.10.1.13/Development_Production/Performance_live/speedlab_trades/stergios_trades_backup.csv', 'a') as f:
            x.to_csv(f, header=False)
        counter = 1


    print("Received reply ", "[", orders, "]")


    return html.Table(
        # Header

        [html.Tr([html.Th('Symbol')]+[html.Th('Time')]+[html.Th('Price')]+[html.Th('Lot')] + [html.Th('Side')] + [html.Th('PnL')])] +

        # Body
        [html.Tr([html.Td(pair)]+[html.Td(each_order['time'])]+[html.Td(each_order['price'])]+
                 [html.Td(each_order['quantity'])]+[html.Td(each_order['side'])] + [html.Td(each_order['pnl'])])
            for pair in orders.keys() for each_order in orders[pair]['trades']]
    )

@app.callback(Output(component_id = 'no_quotes',component_property='children'),events = [Event('interval-component','interval')])
def update_warnings():

    with open(WARNINGS_PICKLE_PATH, 'rb')as f:
        warnings = pickle.load(f)
        f.close()

    return html.Table(
        # Header

        [html.Tr([html.Th('QuoteReqID')]+[html.Th('Reason')])] +

        # Body
        [html.Tr([html.Td(req_id)]+[html.Td(warnings[req_id])])
            for req_id in warnings.keys()]
                 )

@app.callback(Output(component_id = 'time',component_property='children'),events = [Event('interval-component','interval')])
def change_time():

    return 'Current Time : '+str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
# @app.callback(Output(component_id = 'pair-b',component_property='figure'),[Input('dropdown-b','value')])
# def change_symbol(symbol):
#     close = symbol_pnl[symbol]
#     return  {'data':[go.Scatter(x = close.index,y = close.cumsum().values)],'layout':go.Layout( hovermode='closest')}
#
# @app.callback(Output(component_id = 'pair-e',component_property='figure'),[Input('dropdown-b','value')])
# def change_symbol(symbol):
#     close = pnl_cumsum
#     return  {'data':[go.Scatter(x = close.index,y = close.sum(axis=1).values)],'layout':go.Layout( hovermode='closest')}
#
#
# @app.callback(Output(component_id = 'pair-d',component_property='figure'),[Input('dropdown-a','value'),Input('dropdown-b','value')])
# def change_symbol(symbol1,symbol2):
#     returns1 = symbol_returns[symbol1]
#     returns2 = symbol_returns[symbol2]
#     return  {'data':[go.Scatter(x = returns1.index,y = returns1.values),go.Scatter(x = returns2.index,y = returns2 .values)],'layout':go.Layout( hovermode='closest')}
# @app.callback(Output(component_id = 'corr',component_property='children'),[Input('dropdown-a','value'),Input('dropdown-b','value')])
# def calculate_corr(pair1, pair2):
#     return "Price Correlation : " +str(bars[pair1+str(0)].corr(bars[pair2+str(0)],method='spearman')) + "  -  Returns Correlation : "+str(bars[pair1+'returns'].corr(bars[pair2+'returns'],method='spearman'))


if __name__ == "__main__":
    # main()
    app.run_server(host='127.0.0.1', port=9111)
