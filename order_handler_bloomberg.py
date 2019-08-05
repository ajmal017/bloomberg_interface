import pandas as pd
import numpy as np
import pickle
from bloomberg_api import Application
import zmq
import sys
import time
from time import sleep
from datetime import datetime
import json
from ib_interface.ib import IBInterface
from ibapi.execution import ExecutionFilter
import sys
import time
import _thread
import argparse
import datetime
import quickfix as fix
import quickfix50sp2 as fix50sp2
import pandas as pd
import numpy as np
import os
import json
import filelock
from pandas.tseries.offsets import BDay
import gc
FLOAT_FORMAT = '%.5f'
DATE_FORMAT = '%Y%m%d%H%M'
prices = []
dealers_no = 2
import threading
port = "9998"
host = '10.10.1.32'
# account = 'DU517095'
account = 'DU194566'
contract_dict = {"EURUSD" :("EUR",'USD'), 'EURGBP': ('EUR','GBP'), 'EURJPY': ('EUR','JPY'),'EURCHF':('EUR','CHF'), "EURCAD": ("EUR","CAD"), 'EURAUD': ('EUR','AUD'),'EURNZD':('EUR','NZD'),
                 "GBPUSD": ("GBP",'USD'),'USDJPY' :('USD','JPY'),'USDCHF':('USD','CHF'),'USDCAD':('USD','CAD'),'AUDUSD':('AUD','USD'),'NZDUSD':('NZD','USD'),
                 'GBPJPY' : ('GBP','JPY'),'GBPCHF':('GBP','CHF'),'GBPCAD':('GBP','CAD'),'GBPAUD':('GBP','AUD'),'GBPNZD':('GBP','NZD'),
                 "CHFJPY": ("CHF",'JPY'),'CADJPY':('CAD','JPY'),'AUDJPY':('AUD','JPY'), 'NZDJPY':('NZD','JPY'),
                 'CADCHF':('CAD','CHF'), 'AUDCHF':( 'AUD','CHF'),'NZDCHF':('NZD','CHF'),
                 'AUDCAD':('AUD','CAD'), 'AUDNZD':('AUD','NZD'),
                 'NZDCAD':('NZD','CAD'),}
if len(sys.argv) >1:
    port = sys.argv[1]
    int(port)
if len(sys.argv) >2:
    port1 = sys.argv[2]
    int(port)

class Application_Order_Handler(Application):
    offer_prices = []
    bid_prices = []
    no_trade = 0
    symbol_positions={}
    #### Uncomment to initialize the positions pickle
    # for contract in contract_dict:
    #     symbol_positions[f'{contract[:3]}/{contract[3:]}']=dict(position=0,trades=[])
    # with open('positions.pickle', 'wb') as f:
    #     pickle.dump(symbol_positions, f, pickle.HIGHEST_PROTOCOL)
    with open('positions.pickle','rb' )as f:
        symbol_positions = pickle.load(f)

    for contract in contract_dict:
        print(contract, symbol_positions[f'{contract[:3]}/{contract[3:]}']['position'])
    def fromApp(self, message, sessionID):

        print(f'<-APP: {message.toString()}')

        msg_type = message.getHeader().getField(35)
        global offer_prices
        global bid_prices
        if msg_type == 'S': #QUOTES
            dealer_no = 0
            hit = False
            parties_no = int(message.getField(453))
            quote_req_id = message.getField(131)
            side = quote_req_id[-1]
            print(f'REQUESTING LIMIT PRICE :{self.limit_price} - {side} ')
            group_453 = fix50sp2.QuoteRequest().NoRelatedSym().NoPartyIDs()
            quote_id = message.getField(117)
            symbol = message.getField(55)
            product = message.getField(460)
            quantity = int(message.getField(38))
            currency = message.getField(15)

            for party_id in range(1, parties_no + 1):
                message.getGroup(party_id, group_453)
                if group_453.getField(452) == '1':  # dealer
                    dealer_no += 1
                    dealer_id = group_453.getField(448)
                    print(f'Dealer ID : {dealer_id} and Party ID {party_id}')
                    try:
                        bid_px = float(message.getField(132))
                        self.bid_prices.append(bid_px)
                    except:
                        bid_px = 999
                    try:
                        offer_px = float(message.getField(133))
                        self.offer_prices.append(offer_px)
                    except:
                        offer_px = 999

                    # if hit:
                    #     self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                    #                         symbol=symbol, product=product, side=side, dealer_id=dealer_id)
                    #     print(f'PRICE FILLED FROM PREVIOUS DEALER - PASS QUOTES FROM {dealer_id}')
                    #     continue
                    print(f'CHECKING QUOTES FROM DEALER: {dealer_id}')
                    print(f'BID PX:{bid_px} - OFFER PX:{offer_px}')

                print(f'Offer Prices:{self.offer_prices}, Bid Prices:{self.bid_prices}')
                if (len(self.offer_prices)==dealers_no or len(self.bid_prices)==dealers_no):
                    print(f'Position for {symbol} is {self.symbol_positions[symbol]["position"]}')

                    if side == '1':  # long
                       offer_px = self.offer_prices[0]
                       self.offer_prices = []

                       # if offer_px <= self.limit_price:
                       with open('positions.pickle','wb') as f:
                           pickle.dump(self.symbol_positions,f,pickle.HIGHEST_PROTOCOL)
                       if self.no_trade:
                            self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                                                symbol=symbol, product=product, side=side, dealer_id=dealer_id)
                            print(f'PASSED QUOTES FROM {dealer_id} ')
                            time.sleep(1)
                            continue
                       else:
                        self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=1,
                                           symbol=symbol, product=product, side=side, dealer_id=dealer_id,
                                           price=offer_px,currency=currency,quantity=quantity)
                        print(f'BUY at {offer_px} from {dealer_id} - PASS QUOTES FROM OTHER DEALERS')


                       # time.sleep(10)
                        continue
                       # else:
                       #     self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                       #                         symbol=symbol, product=product, side=side, dealer_id=dealer_id)
                       #     print(f'PASSED QUOTES FROM {dealer_id} ')
                       #     time.sleep(1)
                       #     continue
                           # if party_id == parties_no:
                           #     self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                           #                         symbol=symbol, product=product, side=side, dealer_id=dealer_id) #check other dealers
                           #     print(f'CURRENT QuoteRequestPassed - REQUEST NEW QUOTES')
                           #     self.quote_request(symbol=symbol,currency=currency,quantity=quantity,side=side,order_type=1)
                           #
                           # else: #pass current request - send new
                           #     continue
                           #     # self.quote_request(symbol=symbol,currency=currency,quantity=quantity,side=side,order_type=1,)

                    elif side == '2':  # short
                        bid_px = self.bid_prices[0]
                        self.bid_prices = []
                        # if self.symbol_positions[symbol] == 0:
                        #     no_trade = 0
                        #     self.symbol_positions[symbol] = -quantity
                        #     quantity = quantity
                        # elif self.symbol_positions[symbol] < 0:
                        #     no_trade = 1
                        #     quantity = 0
                        # else:
                        #     no_trade = 0
                        #     self.symbol_positions[symbol] = -quantity
                        #     quantity = 2 * quantity
                        # if bid_px >= self.limit_price:
                        with open('positions.pickle', 'wb') as f:
                            pickle.dump(self.symbol_positions, f, pickle.HIGHEST_PROTOCOL)
                        if self.no_trade:
                            self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                                                symbol=symbol, product=product, side=side, dealer_id=dealer_id)
                            print(f'PASSED QUOTES FROM {dealer_id} ')
                            # time.sleep(10)
                            continue
                        else:
                            self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=1,
                                               symbol=symbol, product=product, side=side, dealer_id=dealer_id,
                                               price=bid_px,currency=currency,quantity=quantity)
                            print(f'SELL at {bid_px} from {dealer_id} - PASS QUOTES FROM OTHER DEALERS')

                           # hit = True
                            continue
                       # else:
                       #     self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                       #                         symbol=symbol, product=product, side=side, dealer_id=dealer_id)
                       #     print(f'PASSED QUOTES FROM {dealer_id} ')
                       #     # time.sleep(10)
                       #     continue

                        # if party_id == parties_no:
                        #     self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                        #                         symbol=symbol, product=product, side=side, dealer_id=dealer_id) #check other dealers
                        #     print(f'CURRENT QuoteRequestPassed - REQUEST NEW QUOTES')
                        #     self.quote_request(symbol=symbol,currency=currency,quantity=quantity,side=side,order_type=1)
                        #
                        # else: #pass current request - send new
                        #     continue
                        #     # self.quote_request(symbol=symbol,currency=currency,quantity=quantity,side=side,order_type=1,)

                    # if (not hit) and party_id == parties_no: # checked all and did not get the price - send again request
                    #     self.quote_request(symbol=symbol, currency=currency, quantity=quantity, side=side, order_type=1)
                    #     print(f'COULD NOT GET PRICE FROM {dealer_no} DEALERS - SEND NEW QUOTE REQUEST ')

                    # bid_spot_rate = message.getField(188)
                    # offer_spot_rate = message.getField(190)

                    # self.quote_response(quote_req_id=quote_req_id,quote_id=quote_id,quote_resp_type=1,symbol=symbol,
                    #                     product=product,dealer_id=dealer_id,side=side)
                    time.sleep(1)

                else:
                    continue
        # print(f'PRICE : {prices}')
        if msg_type == 'AI':
            print(f'RECEIVED AI message')
        if msg_type == '8':
            print(f'RECEIVED EXECUTION REPORT')
            ord_status = message.getField(39) #0->NEW,1->PARTIALLY FILLED,2->FILLED,8->REJECTED

            quantity = message.getField(14)
            avg_px = message.getField(6)
            order_id =message.getField(17)
            side = message.getField(54)
            symbol = message.getField(55)
            if ord_status == fix.OrdStatus_FILLED:
                order_dict = dict(price=avg_px,quantity=quantity,side= side,order_id=order_id)
                self.symbol_positions[symbol]['trades'].append(order_dict)
                with open('positions.pickle', 'wb') as f:
                    pickle.dump(self.symbol_positions, f, pickle.HIGHEST_PROTOCOL)


        return

    def quote_response(self,quote_req_id,quote_id,quote_resp_type,symbol,product,side,dealer_id,price=None,currency = None,quantity = None):

        #HEADER
        quote_resp = fix50sp2.QuoteRequest()
        quote_resp.getHeader().setField(fix.StringField(8,'FIXT.1.1')) #BeginMessage
        quote_resp.getHeader().setField(fix.StringField(35,'AJ'))#MessageType
        quote_resp.getHeader().setField(fix.StringField(1128,'9'))  #ApplVerID - FIX50SP2
        quote_resp.getHeader().setField(fix.StringField(49,'ORP_RESZ_B'))
        quote_resp.getHeader().setField(fix.StringField(56,'BLPORPBETA'))
        quote_resp.getHeader().setField(fix.StringField(128,'DOR'))
        quote_resp.getHeader().setField(fix.SendingTime(1))#52
        quote_resp.getHeader().setField(fix.StringField(1156,'208'))#ApplExtID
        quote_resp.getHeader().setField(fix.StringField(1129,'1.5'))#CstmApplVerID
        quote_resp.getHeader().setField(fix.SendingTime(1))
        #BODY
        quote_resp.setField(fix.QuoteRespID(quote_req_id[:-4] + 'RESP' + self.genOrderID()))#693
        quote_resp.setField(fix.QuoteReqID(quote_req_id))  #131
        quote_resp.setField(fix.QuoteRespType(quote_resp_type))#694
        quote_resp.setField(fix.Symbol(symbol))#55
        quote_resp.setField(fix.Product(int(product)))#460
        quote_resp.setField(fix.Side(side))#54

        quote_resp.setField(fix.StringField(167,'FXSPOT'))
        print(datetime.datetime.utcnow().strftime('%Y%m%d-%H:%M:%S'))
        quote_resp.setField(fix.StringField(60,datetime.datetime.utcnow().strftime('%Y%m%d-%H:%M:%S')))#60
        # quote_resp.setField(fix.TransactTime(1))#60
        quote_resp.setField(fix.SettlType('0'))#63
        settl_date = datetime.datetime.utcnow()+BDay(n=2)
        quote_resp.setField(fix.SettlDate(settl_date.strftime('%Y%m%d')))#64

        if quote_resp_type == 1:
            print(f'Side - {side}')
            if side =='1':
            # quote_resp.setField(fix.Price(float(price)))
                quote_resp.setField(fix.OfferPx(float(price)))
                quote_resp.setField(fix.OfferSpotRate(float(price)))
            elif side == '2':
                quote_resp.setField(fix.BidPx(float(price)))
                quote_resp.setField(fix.BidSpotRate(float(price)))
            quote_resp.setField(fix.StringField(15, str(currency)))  # Currency
            # quote_resp.setField(fix.BidPx(float(price)))
            quote_resp.setField(fix.QuoteID(quote_id))  # 117
            quote_resp.setField(fix.OrderQty(quantity)) #38
            quote_resp.setField(fix.ClOrdID(quote_resp.getField(693)+self.genOrderID()))  # 11

        group_453 = fix50sp2.QuoteRequest().NoRelatedSym().NoPartyIDs()

        group_453.setField(fix.StringField(448,'7613723'))#PartyID
        group_453.setField(fix.StringField(447,'D'))#PartyIDSource
        group_453.setField(fix.PartyRole(11))#452 - PartyRole
        quote_resp.addGroup(group_453)

        group_453.setField(fix.StringField(448,dealer_id))#PartyID
        group_453.setField(fix.StringField(447,'D'))#PartyIDSource
        group_453.setField(fix.PartyRole(1))#452 - PartyRole.
        quote_resp.addGroup(group_453)

        quote_resp.setField(fix.StringField(1300,'XOFF'))#market segment id

        print(f'SENDING QUOTE RESPONSE MESSAGE:\n ')
        # print(f'SENDING QUOTE RESPONSE MESSAGE:\n {quote_resp.toString()}')
        fix.Session.sendToTarget(quote_resp, self.sessionID)
        ####################

    def quote_request(self,symbol,currency,quantity,side,order_type,price=None):

        #HEADER
        trade = fix50sp2.QuoteRequest()
        trade.getHeader().setField(fix.StringField(8,'FIXT.1.1')) #BeginMessage
        trade.getHeader().setField(fix.StringField(35,'R'))#MessageType
        trade.getHeader().setField(fix.StringField(1128,'9'))  #ApplVerID - FIX50SP2
        trade.getHeader().setField(fix.StringField(49,'ORP_RESZ_B'))
        trade.getHeader().setField(fix.StringField(56,'BLPORPBETA'))
        trade.getHeader().setField(fix.StringField(128,'DOR'))
        trade.getHeader().setField(fix.SendingTime(1))#52
        trade.getHeader().setField(fix.StringField(1156,'208'))#ApplExtID
        trade.getHeader().setField(fix.StringField(1129,'1.5'))#CstmApplVerID

        #BODY
        trade.setField(fix.QuoteReqID(datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S') + symbol + 'RFQ' + str(side)))  # 131

        group_146 = fix50sp2.QuoteRequest().NoRelatedSym()

        # group_146 = fix.Group(146,1)
        if '/' not in symbol:
            group_146.setField(fix.StringField(55,str(symbol[:3]+'/'+symbol[3:])))
        else:
            group_146.setField(fix.StringField(55, str(symbol)))
        group_146.setField(fix.StringField(167,'FXSPOT'))
        group_146.setField(fix.IntField(460,4))
        group_146.setField(fix.SettlType('0'))#SettlType
        # group_146.setField(fix.StringField(63,'0'))#SettlType
        settl_date = datetime.datetime.utcnow()+BDay(n=2)

        group_146.setField(fix.SettlDate(settl_date.strftime('%Y%m%d')))#SettlDate

        group_146.setField(fix.OrderQty(quantity))#38 - Qty
        group_146.setField(fix.StringField(15,str(currency)))#Currency
        group_146.setField(fix.StringField(54,str(side)))


        # trade.setField(fix.NoPartyIDs(4))#453 - NumberOfParties

        group_453 = fix50sp2.QuoteRequest().NoRelatedSym().NoPartyIDs()
        # group_453 = fix.Group(453,1)
        group_453.setField(fix.StringField(448,'7613723'))#PartyID
        group_453.setField(fix.StringField(447,'D'))#PartyIDSource
        group_453.setField(fix.PartyRole(11))#452 - PartyRole
        group_146.addGroup(group_453)

        group_453.setField(fix.StringField(448,'BGD1'))#PartyID
        group_453.setField(fix.StringField(447,'D'))#PartyIDSource
        group_453.setField(fix.PartyRole(1))#452 - PartyRole.
        group_146.addGroup(group_453)

        group_453.setField(fix.StringField(448,'DOR1'))#PartyID
        group_453.setField(fix.StringField(447,'D'))#PartyIDSource
        group_453.setField(fix.PartyRole(1))#452 - PartyRole.
        group_146.addGroup(group_453)

        group_453.setField(fix.StringField(448,'DOR2'))#PartyID
        group_453.setField(fix.StringField(447,'D'))#PartyIDSource
        group_453.setField(fix.PartyRole(1))#452 - PartyRole.
        group_146.addGroup(group_453)

        trade.addGroup(group_146)
        trade.setField(fix.StringField(1300,'XOFF'))#market segment id
        # trade.setField(fix.StringField(21,'z'))#HandlInst

        if order_type == '2':  # limit
            trade.setField(fix.Price(float(price)))  # if market, this tag  should be absent
        else:
            price = None

        print(f'CREATING THE FOLLOWING ORDER:\n ')
        # print(f'CREATING THE FOLLOWING ORDER:\n {trade.toString()}')
        fix.Session.sendToTarget(trade, self.sessionID)
        ####################


def main():

    print('Running on IP :' +str(host))


    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://*:%s' % port)
    print('Collecting Orders from clients')
    #socket.connect('tcp://0.0.0.0:%s' %port)
   # if len(sys.argv) >2:
   #     socket.connect('tcp://0.0.0.0:%s' % port1)

    config_file = './client.cfg'
    try:
        sleep_seconds = 3
        #start session
        settings = fix.SessionSettings(config_file)
        application = Application_Order_Handler()

        storeFactory = fix.FileStoreFactory(settings)
        logFactory = fix.FileLogFactory(settings)
        initiator = fix.SocketInitiator(application, storeFactory, settings, logFactory)
        initiator.start()
        print(f'INITIATOR STARTED...\nSLEEPING {sleep_seconds} SECONDS...')
        time.sleep(sleep_seconds)
        timeframe = 5
        previous_bar = datetime.datetime.now().minute

        run = True
        auto = False

        if not auto:
            while True:
                if run:
                    application.timeframe = timeframe


                    print(f'RUNNING TIME : {datetime.datetime.now()}')

                    message = socket.recv_json()
                    print(message)
                    socket.send_string("great")


                    # input_ = input('enter 1 for order, 2 for exit, 3 for order status update, 4 for order cancel request test for test request :\n ')
                    message = json.loads(message)
                    action = message['action']
                    symbol = message['symbol']
                    currency = symbol[:3]
                    quantity = message['lot']
                    bloomberg_symbol = f'{symbol[:3]}/{symbol[3:]}'
                    if action == 'buy':
                        if application.symbol_positions[bloomberg_symbol]['position'] == 0:
                            no_trade = 0
                            application.symbol_positions[bloomberg_symbol]['position'] = quantity
                            quantity = quantity
                        elif application.symbol_positions[bloomberg_symbol]['position'] > 0:
                            no_trade = 1
                            quantity = quantity
                        else:
                            no_trade = 0
                            application.symbol_positions[bloomberg_symbol]['position'] = quantity
                            quantity = 2 * quantity
                    elif action =='sell':
                        if application.symbol_positions[bloomberg_symbol]['position'] == 0:
                            no_trade = 0
                            application.symbol_positions[bloomberg_symbol]['position'] = -quantity
                            quantity = quantity
                        elif application.symbol_positions[bloomberg_symbol]['position'] < 0:
                            no_trade = 1
                            quantity = quantity
                        else:
                            no_trade = 0
                            application.symbol_positions[bloomberg_symbol]['position'] = -quantity
                            quantity = 2 * quantity
                    application.no_trade = no_trade
                    order_type = 1
                    limit_price = message['price']
                    if action=="buy":
                        side=1
                    elif action =='sell':
                        side = 2
                    else:
                        side = 0

                    print ("Putin Order")
                    # limit_price = np.float(input('Limit Price From Strategy =')) #from strategy output, last_close +- limit
                    application.limit_price = limit_price
                    # order_type = str(input('Order Type = ')) #from strategy output
                    # if order_type != '1':
                    #     price = np.float(input('Limit Price='))
                    # else:
                    #     price=None
                    # symbol = str(input('Symbol='))
                    # currency = str(input('Currency='))
                    # side = str(input('Side='))
                    # time_id = str(input('time_id='))
                    # quantity = int(input('quantity='))
                    application.quote_request(symbol=symbol, currency=currency, quantity=quantity, side=side, order_type=order_type,
                                  price=limit_price)
        #             #     # initiator.stop()
        #             time.sleep(10)

        else:
            while True:
                symbol = str(input('Symbol = ' ))
                currency = str(input('Currency = '))
                lot = int(input('Lot = '))
                limit_pips = int(input('Limit Pips = '))


                gc.collect()
                time.sleep(10)
                now = datetime.datetime.now()
                print(now)
                hdf_path = f'd:/Data/minute_data{symbol}.h5'
                # hdf_path = 'minute_data' + symbol + currency + '.h5'
                lock = filelock.FileLock(hdf_path + ".lock")

                executed_period = None
                last_period = None
                decimals = np.where("JPY" in currency, 100, 10000)

                position = 0

                with lock.acquire(poll_intervall=0.005):
                    store = pd.HDFStore(hdf_path,
                                        mode='r')
                    df = store[symbol]
                    # df = store[symbol + currency]

                    store.close()

                ask = df.copy()
                ask.index = ask.index + pd.Timedelta(hours=7)
                del df

                print('Last Period : ' + str(last_period) + " while mext period : " + str(
                    (ask.index[-1] + pd.to_timedelta('1min')).ceil(
                        str(timeframe) + 'T')) + " - ASK INDEX" + str(
                    ask.index[-1]) + '- ASK INDDEX PREVIOUS : ' + str(ask.index[-2]))

                new_period = (ask.index[-1] + pd.to_timedelta('1min')).ceil(str(timeframe) + 'T')
                if (last_period != new_period) & (new_period != executed_period):
                    executed_period = new_period

                    ask = ask.resample(str(timeframe) + 'T', label='right', closed='right', base=0).agg(
                        {'open': 'first', 'low': 'min', 'high': 'max', 'close': 'last'})

                    ask = ask.dropna()
                    print('Strategy run')
                    print("Close Price : " + str(ask.iloc[-1].close))
                    ask_price = ask.iloc[-1].close

                    if np.logical_or(position == 0, position < 0):
                        limit_price = ask_price - limit_pips/decimals
                        position = 1
                    elif position > 0:
                        limit_price = ask_price + limit_pips/decimals
                        position = -1
                    application.limit_price = limit_price
                    application.quote_request(symbol=symbol, currency=currency, quantity=lot, side=position,
                                              order_type=1,
                                              price=None)




    except (fix.ConfigError or fix.RuntimeError) as error:
        print(f'ERROR\n{error}')







    sleep(0.1)





if __name__ == '__main__':
    main()
