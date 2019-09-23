import pickle
from bloomberg_api import Application
import zmq
from time import sleep
from datetime import datetime
import sys
import time
import datetime
import quickfix as fix
import quickfix50sp2 as fix50sp2
import pandas as pd
import numpy as np
import json
import filelock
from pandas.tseries.offsets import BDay
import gc
import filelock
WRITE_PATH = "C:/live/fix_bloomberg"
WARNINGS_PICKLE_PATH = r'warnings.pickle'
#DEALERS_DICT: DEALER_ID --> PARTY ROLE
DEALERS_DICT = {'7613723':11,
                'DOR1':1,
                'DOR2':1,
                # 'BGD1':1
                }
# HOLIDAYS_DICT = {'CAD':['20190902','20191014','20191110','20191224','20191225','20191230','20191231'],
#                       'USD':['20190902','20191014','20191018','20191025','20191127','20191128','20191223',
#                              '20191224','20191225','20191230','20191231','20200101'],
#                       'CHF':['20190902','20190905','20190909','20190915','20190916','20190925','20191031','20191207',
#                              '20191224','20191225','20191230','20191231','20200101'],
#                       'EUR':['20190902','20190909','20190911','20190915','20190917','20190919','20190920','20191003','20191004',
#                              '20191009','20191012','20191030','20191031','20191101','20191108','20191110',
#                              '20191119','20191205','20191206','20191207','20191208','20191224','20191225','20191231'],
#                       'JPY':['20190916','20190923','20191014','20191022','20191102','20191103','20191122','20191231'],
#                       'NZD':['20190923','20191025','20191027','20191103','20191114','20191201','20191224',
#                              '20191225','20191231','20200101'],
#                       'AUD':['20190902','20190927','20190930','20191007','20191104','20191223','20191224',
#                              '20191225','20191230','20191231'],
#                       'GBP':['20190902','20191201','20191224','20191225','20191231','20200101']
#                       }

HOLIDAYS_DICT = {'CAD':['20191014','20191015','20191111','20191112'],
                      'USD':['20191014','20191015','20191111','20191112','20191128'],
                      'CHF':[''],
                      'EUR':[''],
                      'JPY':['20190916','20190923','20190924','20191014','20191015','20191022','20191104','20191105','20191123'],
                      'NZD':['20191028'],
                      'AUD':['20191007', '20191008'],
                      'GBP':['']
                      }
DEALERS_NO = len([dealer for dealer, role in DEALERS_DICT.items() if role == 1])
FLOAT_FORMAT = '%.5f'
DATE_FORMAT = '%Y%m%d%H%M'
prices = []
port = "9998"
host = '127.0.0.1'
# account = 'DU517095'
account = 'DU194566'
contract_dict = {"EURUSD" :("EUR",'USD'), 'EURGBP': ('EUR','GBP'), 'EURJPY': ('EUR','JPY'),'EURCHF':('EUR','CHF'), "EURCAD": ("EUR","CAD"), 'EURAUD': ('EUR','AUD'),'EURNZD':('EUR','NZD'),
                 "GBPUSD": ("GBP",'USD'),'USDJPY' :('USD','JPY'),'USDCHF':('USD','CHF'),'USDCAD':('USD','CAD'),'AUDUSD':('AUD','USD'),'NZDUSD':('NZD','USD'),
                 'GBPJPY' : ('GBP','JPY'),'GBPCHF':('GBP','CHF'),'GBPCAD':('GBP','CAD'),'GBPAUD':('GBP','AUD'),'GBPNZD':('GBP','NZD'),
                 "CHFJPY": ("CHF",'JPY'),'CADJPY':('CAD','JPY'),'AUDJPY':('AUD','JPY'), 'NZDJPY':('NZD','JPY'),
                 'CADCHF':('CAD','CHF'), 'AUDCHF':( 'AUD','CHF'),'NZDCHF':('NZD','CHF'),
                 'AUDCAD':('AUD','CAD'), 'AUDNZD':('AUD','NZD'),
                 'NZDCAD':('NZD','CAD'),}
if len(sys.argv) > 1:
    port = sys.argv[1]
    int(port)
if len(sys.argv) > 2:
    port1 = sys.argv[2]
    int(port)

class Application_Order_Handler(Application):

    def __init__(self):
        super().__init__()
        # self.offer_prices = dict()
        # self.bid_prices = dict()
        self.symbol_positions = dict()
        self.no_trade = dict()
        self.limit_price = dict()
        self.quotes = dict()
        self.warnings = dict()

        #### Uncomment to initialize the positions pickle
        for contract in contract_dict:
            self.quotes[f'{contract[:3]}/{contract[3:]}'] = dict(offer_prices={}, bid_prices={})
            self.no_trade[f'{contract[:3]}/{contract[3:]}'] = 1
            self.limit_price[f'{contract[:3]}/{contract[3:]}'] = 0
        #     self.symbol_positions[f'{contract[:3]}/{contract[3:]}'] = dict(position=0, trades=[])
        # with open('positions.pickle', 'wb') as f:
        #     pickle.dump(self.symbol_positions, f, pickle.HIGHEST_PROTOCOL)

        with open('positions.pickle','rb' )as f:
            self.symbol_positions = pickle.load(f)
        for contract in contract_dict:
            print(contract, self.symbol_positions[f'{contract[:3]}/{contract[3:]}']['position'])

    def fromApp(self, message, sessionID):

        print(f'<-APP: {message.toString()}')

        msg_type = message.getHeader().getField(35)
        # global offer_prices
        # global bid_prices

        #Quotes message
        if msg_type == 'S': #QUOTES

            #decompose Quotes message
            dealer_no = 0
            parties_no = int(message.getField(453))
            quote_req_id = message.getField(131)
            side = quote_req_id[-1]
            group_453 = fix50sp2.QuoteRequest().NoRelatedSym().NoPartyIDs()
            quote_id = message.getField(117)
            symbol = message.getField(55)
            product = message.getField(460)
            quantity = int(message.getField(38))
            currency = message.getField(15)

            #get prices from providers
            #PartyRole (452) == 1
            for party_id in range(1, parties_no + 1):
                message.getGroup(party_id, group_453)
                if group_453.getField(452) == '1':  # dealer
                    dealer_no += 1
                    dealer_id = group_453.getField(448)
                    # print(f'Dealer ID : {dealer_id} and Party ID {party_id}')
                    if message.isSetField(188):
                        self.quotes[symbol]['bid_prices'][dealer_id] = float(message.getField(188))
                        # bid_px = float(message.getField(188))
                    elif message.isSetField(132):
                        self.quotes[symbol]['bid_prices'][dealer_id] = float(message.getField(132))
                        # bid_px = float(message.getField(132))
                    else:
                        self.quotes[symbol]['bid_prices'][dealer_id] = 999
                        # bid_px = 999
                    # self.bid_prices[dealer_id] = bid_px

                    # self.quotes[symbol]['bid_prices'][dealer_id] = bid_px

                    if message.isSetField(190):
                        self.quotes[symbol]['offer_prices'][dealer_id] = float(message.getField(190))
                        # offer_px = float(message.getField(190))
                    elif message.isSetField(133):
                        self.quotes[symbol]['offer_prices'][dealer_id] = float(message.getField(133))
                        # offer_px = float(message.getField(133))
                    else:
                        self.quotes[symbol]['offer_prices'][dealer_id] = 999
                        # offer_px = 999
                    # self.offer_prices[dealer_id] = offer_px

                    # self.quotes[symbol]['offer_prices'][dealer_id] = offer_px

                    print(f'QUOTES FROM DEALER: {dealer_id}')
                    print(f'BID:{self.quotes[symbol]["bid_prices"][dealer_id]} - OFFER:{self.quotes[symbol]["offer_prices"][dealer_id]}')

                if (len(self.quotes[symbol]['offer_prices']) == DEALERS_NO or len(self.quotes[symbol]['offer_prices']) == DEALERS_NO):
                # if (len(self.offer_prices) == DEALERS_NO or len(self.bid_prices) == DEALERS_NO):

                    # #sort offer and bid quotes by best price
                    # self.offer_prices = sorted(self.offer_prices.items(), key=lambda kv: kv[1], reverse=False)
                    # self.bid_prices = sorted(self.bid_prices.items(), key=lambda kv: kv[1], reverse=True)
                    # print(f'Offer Prices:{self.offer_prices}, Bid Prices:{self.bid_prices}')
                    # print(f'Requesting Limit Price :{self.limit_price}')
                    # print(f'Position for {symbol} is {self.symbol_positions[symbol]["position"]}')

                    self.quotes[symbol]['offer_prices'] = sorted(self.quotes[symbol]['offer_prices'].items(), key=lambda kv: kv[1], reverse=False)
                    self.quotes[symbol]['bid_prices'] = sorted(self.quotes[symbol]['bid_prices'].items(), key=lambda kv: kv[1], reverse=True)
                    print(f'{symbol} - Offer Prices:{self.quotes[symbol]["offer_prices"]}, Bid Prices:{self.quotes[symbol]["bid_prices"]}')
                    print(f'{symbol} - Requesting Limit Price :{self.limit_price[symbol]}')
                    print(f'Position for {symbol} is {self.symbol_positions[symbol]["position"]}')

                    #case where trade in base currency
                    if symbol[:3] == currency:

                        #choose action -hit/pass
                        if side == '1':  # long - sort offer prices ascending
                            # offer_px = self.offer_prices[0][1]
                            # dealer_id = self.offer_prices[0][0]
                            # self.offer_prices = {}
                            # self.bid_prices = {}

                            if self.no_trade[symbol]:
                                self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                                                    symbol=symbol, product=product, side=side, dealer_id=self.quotes[symbol]['offer_prices'][0][0])
                                print(f'PASSED QUOTES BECAUSE "NO TRADE" ')
                                # time.sleep(1)
                                # continue
                            else:
                                if np.logical_or(self.quotes[symbol]['offer_prices'][0][1] <= self.limit_price[symbol], self.limit_price[symbol] is None):
                                # if np.logical_or(offer_px <= self.limit_price, self.limit_price is None):

                                    self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=1,
                                               symbol=symbol, product=product, side=side, dealer_id=self.quotes[symbol]['offer_prices'][0][0],
                                               price=self.quotes[symbol]['offer_prices'][0][1],currency=currency,quantity=quantity)
                                    print(f'BUY at {self.quotes[symbol]["offer_prices"][0][1]} from {self.quotes[symbol]["offer_prices"][0][0]}')
                                    # continue
                            self.quotes[symbol]['offer_prices'] = {}
                            self.quotes[symbol]['bid_prices'] = {}

                        elif side == '2':  # short
                            # bid_px = self.bid_prices[0][1]
                            # dealer_id = self.bid_prices[0][0]
                            # self.offer_prices = {}
                            # self.bid_prices = {}

                            if self.no_trade[symbol]:
                                self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                                                    symbol=symbol, product=product, side=side, dealer_id=self.quotes[symbol]['bid_prices'][0][0])
                                print(f'PASSED QUOTES BECAUSE "NO TRADE" ')
                                # time.sleep(10)
                                # continue
                            else:
                                if np.logical_or(self.quotes[symbol]['bid_prices'][0][1] >= self.limit_price[symbol], self.limit_price[symbol] is None):
                                    self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=1,
                                                   symbol=symbol, product=product, side=side, dealer_id=self.quotes[symbol]['bid_prices'][0][0],
                                                   price=self.quotes[symbol]['bid_prices'][0][1],currency=currency,quantity=quantity)
                                    print(f'SELL at {self.quotes[symbol]["bid_prices"][0][1]} from {self.quotes[symbol]["bid_prices"][0][0]}')
                                # continue
                            self.quotes[symbol]['offer_prices'] = {}
                            self.quotes[symbol]['bid_prices'] = {}

                        elif side == '0':
                            offer_px = self.offer_prices[0][1]
                            best_offer_dealer_id = self.offer_prices[0][0]
                            self.offer_prices = {}

                            bid_px = self.bid_prices[0][1]
                            best_bid_dealer_id = self.bid_prices[0][0]
                            self.bid_prices = {}
                            self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=1,
                                                symbol=symbol, product=product, side='1', dealer_id=best_offer_dealer_id,
                                                price=offer_px, currency=currency, quantity=quantity)
                            print(f'BUY at {offer_px} from {best_offer_dealer_id} - RANDOM TWO WAY QUOTES')

                            continue

                        # time.sleep(1)

                    #case where trade in secondary currency
                    elif symbol[-3:] == currency:
                        print('TRADING WITH SECONDARY CURRENCY')

                        #change side because Quotes and opposite to RFQ to take decision
                        if side == '1':
                            side = '2'
                        elif side == '2':
                            side = '1'

                        #choose action (hit/pass) - set side according to RFQ
                        if side == '1':  # long - sort offer prices ascending
                            # offer_px = self.offer_prices[0][1]
                            # dealer_id = self.offer_prices[0][0]
                            # self.offer_prices = {}
                            # self.bid_prices = {}

                            if self.no_trade[symbol]:
                                self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                                                    symbol=symbol, product=product, side='2', dealer_id=self.quotes[symbol]['offer_prices'][0][0])
                                print(f'PASSED QUOTES BECAUSE "NO TRADE" ')
                                # time.sleep(1)
                                # continue
                            else:
                                if np.logical_or(self.quotes[symbol]['offer_prices'][0][1] <= self.limit_price[symbol], self.limit_price[symbol] is None):
                                    self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=1,
                                                        symbol=symbol, product=product, side='2', dealer_id=self.quotes[symbol]['offer_prices'][0][0],
                                                        price=self.quotes[symbol]['offer_prices'][0][1], currency=currency, quantity=quantity)

                                    print(f'BUY at {self.quotes[symbol]["offer_prices"][0][1]} from {self.quotes[symbol]["offer_prices"][0][0]}')
                                    # continue
                            self.quotes[symbol]['offer_prices'] = {}
                            self.quotes[symbol]['bid_prices'] = {}

                        elif side == '2':  # short
                            # bid_px = self.bid_prices[0][1]
                            # dealer_id = self.bid_prices[0][0]
                            # self.offer_prices = {}
                            # self.bid_prices = {}

                            if self.no_trade[symbol]:
                                self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                                                    symbol=symbol, product=product, side='1', dealer_id=self.quotes[symbol]['bid_prices'][0][0])
                                print(f'PASSED QUOTES BECAUSE "NO TRADE" ')
                                # time.sleep(10)
                                # continue
                            else:
                                if np.logical_or(self.quotes[symbol]['bid_prices'][0][1] >= self.limit_price[symbol], self.limit_price[symbol] is None):
                                    self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=1,
                                                        symbol=symbol, product=product, side='1', dealer_id=self.quotes[symbol]['bid_prices'][0][0],
                                                        price=self.quotes[symbol]['bid_prices'][0][1], currency=currency, quantity=quantity)
                                    print(f'SELL at {self.quotes[symbol]["bid_prices"][0][1]} from {self.quotes[symbol]["bid_prices"][0][0]}')
                                # continue
                            self.quotes[symbol]['offer_prices'] = {}
                            self.quotes[symbol]['bid_prices'] = {}
                        elif side == '0':
                            offer_px = self.offer_prices[0][1]
                            best_offer_dealer_id = self.offer_prices[0][0]
                            self.offer_prices = {}

                            bid_px = self.bid_prices[0][1]
                            best_bid_dealer_id = self.bid_prices[0][0]
                            self.bid_prices = {}
                            self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=1,
                                                symbol=symbol, product=product, side='1', dealer_id=best_offer_dealer_id,
                                                price=offer_px, currency=currency, quantity=quantity)
                            print(f'BUY at {offer_px} from {best_offer_dealer_id} - RANDOM TWO WAY QUOTES')

                            continue

                            # time.sleep(1)


        #QuoteStatusReport message
        if msg_type == 'AI':
            quote_status = message.getField(297)
            if quote_status == str(101):
                print(f'RECEIVED QuoteStatusReport - CANCELLED BY CLIENT ')
            if quote_status == str(11):
                print(f'RECEIVED QuoteStatusReport - DEALER REJECTS LAST LOOK ')
            if quote_status == str(100):
                print(f'RECEIVED QuoteStatusReport - LP TIMEOUT ')
            if quote_status == str(4):
                print(f'RECEIVED QuoteStatusReport - LP CANCEL ALL QUOTES ')
            if quote_status == str(5):
                print(f'RECEIVED QuoteStatusReport - TRADE REQUEST REJECTION ')
                # print (message.getField(58))
                # contract = message.getField(55)
                # self.no_trade[f'{contract[:3]}/{contract[3:]}'] = 1


        #QuoteRequestReject
        if msg_type == 'AG':
            self.warnings[message.getField(131)] = message.getField(58)
            with open('warnings.pickle','wb') as f:
                pickle.dump(self.warnings, f, pickle.HIGHEST_PROTOCOL)
            print('RECEIVED QuoteRequestReject - LPs REJECTED QUOTE REQUEST')

        #Execution report
        if msg_type == '8':
            print(f'RECEIVED EXECUTION REPORT')

            #decompose message
            ord_status = message.getField(39) #0->NEW,1->PARTIALLY FILLED,2->FILLED,8->REJECTED
            quantity = message.getField(14)
            avg_px = message.getField(6)
            order_id = message.getField(17)
            side = message.getField(54)
            symbol = message.getField(55)
            exec_time = message.getField(60)
            exec_time = exec_time[:17].replace('-','').replace(':','')
            # exec_time = message.getField(11)
            side = message.getField(54)

            #update symbols_position_dict for keeping track of orders
            if ord_status == fix.OrdStatus_FILLED:

                #update current position and save to symbol_positions_dict
                if side == '1':
                    self.symbol_positions[symbol]['position'] = int(quantity) + self.symbol_positions[symbol]['position']
                elif side == '2':
                    self.symbol_positions[symbol]['position'] = -int(quantity) + self.symbol_positions[symbol]['position']
                if len(self.symbol_positions[symbol]['trades']) == 0:
                    pnl = 100000
                else:
                    previous_pnl = self.symbol_positions[symbol]['trades'][-1]['pnl']
                    price_diff = (float(avg_px) - float(self.symbol_positions[symbol]['trades'][-1]['price'])) *\
                                 float(self.symbol_positions[symbol]['trades'][-1]['quantity'])
                    if 'JPY' in symbol:
                        price_diff = (float(avg_px) - float(self.symbol_positions[symbol]['trades'][-1]['price'])) * \
                                     float(self.symbol_positions[symbol]['trades'][-1]['quantity']/100)
                    pnl = round(price_diff + previous_pnl, 2)

                order_dict = dict(price=avg_px,quantity=self.symbol_positions[symbol]['position'],side=side,
                                  order_id=order_id,time=exec_time, pnl=pnl)
                # order_dict = dict(price=avg_px,quantity=self.symbol_positions[symbol]['position'],side=side,
                #                   order_id=order_id,time=exec_time[3:17],pnl=pnl)
                self.symbol_positions[symbol]['trades'].append(order_dict)

                #save updated symbol_positions_dict
                with open('positions.pickle', 'wb') as f:
                    pickle.dump(self.symbol_positions, f, pickle.HIGHEST_PROTOCOL)

                with open('positions.pickle', 'rb') as f:
                    now = datetime.datetime.now().strftime('%Y%m%d')
                    tmp_orders = pickle.load(f)
                    tmp_orders = pd.DataFrame.from_dict(tmp_orders[symbol]['trades'], dtype=str)
                    tmp_orders.to_csv(f'{WRITE_PATH}/FIXtrades{now}{symbol.replace("/","")}.csv')
                # if os.path.exists(f'd:/fix_bloomberg/FIXtrades{now}{symbol.replace("/","")}.csv'):
                #     tmp_csv = pd.read_csv(f'd:/fix_bloomberg/FIXtrades{now}{symbol.replace("/","")}.csv')
                #     tmp_csv = pd.concat([tmp_csv,tmp_orders],axis=0,ignore_index=True)
                #     tmp_csv.to_csv(f'd:/fix_bloomberg/FIXtrades{now}{symbol.replace("/","")}.csv')
                # else:
                #     tmp_orders.to_csv(f'd:/fix_bloomberg/FIXtrades{now}{symbol.replace("/","")}.csv')


            print(f'New position of {symbol} is {self.symbol_positions[symbol]["position"]}')

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
        # settl_date = datetime.datetime.utcnow() + BDay(n=2)
        if ((datetime.datetime.utcnow() - datetime.timedelta(hours = 4)).hour < 17) or ((datetime.datetime.utcnow() - datetime.timedelta(hours = 4)).weekday() == 6):
            settl_date = datetime.datetime.utcnow()+BDay(n=2)
        else:
            settl_date = datetime.datetime.utcnow() + BDay(n=3)
        while (settl_date.strftime('%Y%m%d') in HOLIDAYS_DICT[symbol[:3]]) or (settl_date.strftime('%Y%m%d') in HOLIDAYS_DICT[symbol[-3:]]) :
            settl_date +=  BDay(n=1)
        quote_resp.setField(fix.SettlDate(settl_date.strftime('%Y%m%d')))#64

        if quote_resp_type == 1:
            print(f'Side - {side}')
            if side == '1':
                if symbol[:3] == currency:
                    quote_resp.setField(fix.OfferPx(float(price)))
                    quote_resp.setField(fix.OfferSpotRate(float(price)))
                elif symbol[-3:] == currency:
                    quote_resp.setField(fix.BidPx(float(price)))
                    quote_resp.setField(fix.BidSpotRate(float(price)))
            elif side == '2':
                if symbol[:3] == currency:
                    quote_resp.setField(fix.BidPx(float(price)))
                    quote_resp.setField(fix.BidSpotRate(float(price)))
                elif symbol[-3:] == currency:
                    quote_resp.setField(fix.OfferPx(float(price)))
                    quote_resp.setField(fix.OfferSpotRate(float(price)))

            quote_resp.setField(fix.StringField(15, str(currency)))  # Currency
            quote_resp.setField(fix.OrderQty(quantity)) #38
            quote_resp.setField(fix.ClOrdID(quote_resp.getField(693)+self.genOrderID()))  # 11

        if quote_id is not None:
            quote_resp.setField(fix.QuoteID(quote_id))  # 117


        if dealer_id is not None:
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

    def quote_request(self,symbol,currency,quantity,side,order_type,dealers_dict):

        '''
        send QuoteRequest message to Liquidity Providers
        build the message according to ORP_v1.5_QuickFIX
        '''

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
        #QuoteReqID (131) --> RFQ+datetime+symbol+side
        quote_request_id =  'RFQ' + datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S') + symbol + str(side)
        trade.setField(fix.QuoteReqID(quote_request_id))  # 131

        group_146 = fix50sp2.QuoteRequest().NoRelatedSym()

        if '/' not in symbol:
            group_146.setField(fix.StringField(55,str(symbol[:3]+'/'+symbol[3:6])))
        else:
            group_146.setField(fix.StringField(55, str(symbol)))
        group_146.setField(fix.StringField(167,'FXSPOT'))
        group_146.setField(fix.IntField(460,4))
        group_146.setField(fix.SettlType('0'))#SettlType
        # settl_date = datetime.datetime.utcnow()+BDay(n=2)
        if ((datetime.datetime.utcnow() - datetime.timedelta(hours = 4)).hour < 17) or ((datetime.datetime.utcnow() - datetime.timedelta(hours = 4)).weekday() == 6):
            settl_date = datetime.datetime.utcnow()+BDay(n=2)
        else:
            settl_date = datetime.datetime.utcnow() + BDay(n=3)
        while (settl_date.strftime('%Y%m%d') in HOLIDAYS_DICT[symbol[:3]]) or (settl_date.strftime('%Y%m%d') in HOLIDAYS_DICT[symbol[-3:]]):
            settl_date +=  BDay(n=1)
        group_146.setField(fix.SettlDate(settl_date.strftime('%Y%m%d')))#SettlDate
        group_146.setField(fix.OrderQty(quantity))#38 - Qty
        group_146.setField(fix.StringField(15,str(currency)))#Currency

        #if side == 0, get two way quotes (omit tag 54)
        if side != 0:
            group_146.setField(fix.StringField(54,str(side)))

        group_453 = fix50sp2.QuoteRequest().NoRelatedSym().NoPartyIDs()
        for dealer, party_role in dealers_dict.items():
            group_453.setField(fix.StringField(448,dealer))#PartyID
            group_453.setField(fix.StringField(447,'D'))#PartyIDSource
            group_453.setField(fix.PartyRole(party_role))#452 - PartyRole.
            group_146.addGroup(group_453)

        trade.addGroup(group_146)
        trade.setField(fix.StringField(1300,'XOFF'))#market segment id
        # trade.setField(fix.StringField(21,'z'))#HandlInst

        if order_type == '2':  # limit
            trade.setField(fix.Price(float(self.limit_price)))  # if market, this tag  should be absent

        print(f'CREATING THE FOLLOWING ORDER:\n {trade.toString()}')
        fix.Session.sendToTarget(trade, self.sessionID)
        ####################


def main():

    print('Running on IP :' + str(host))

    #connect with clients
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://*:%s' % port)
    print('Collecting Orders from clients')

    #set path of config file for FIX connection
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

        run = True
        auto = False

        if not auto:
            while True:
                if run:
                    # application.timeframe = timeframe

                    print(f'RUNNING TIME : {datetime.datetime.now()}')

                    #get order from the client
                    message = socket.recv_json()
                    print(f'RECEIVED ORDER\n{message}')
                    print('###########')
                    socket.send_string("great")
                    message = json.loads(message)
                    action = message['action']
                    symbol = message['symbol']
                    bloomberg_symbol = f'{symbol[:3]}/{symbol[3:]}'
                    if action == 'cancel_quotes':
                        # stop if any current quotes request
                        application.no_trade[bloomberg_symbol] = 1
                        # application.no_trade = 1
                        print(f'{bloomberg_symbol} - ANY PREVIOUS RFQ PASSED!')
                        # time.sleep(5)  # 1 - sleep to catch passing previous quotes
                        continue
                    currency = message['currency']
                    quantity = message['lot']
                    limit_price = message['price']

                    #get last position
                    last_position = application.symbol_positions[bloomberg_symbol]['position']
                    if action == 'buy':
                        if last_position == 0:
                            no_trade = 0
                            # application.symbol_positions[bloomberg_symbol]['position'] = quantity
                            # quantity = quantity
                        elif last_position > 0:
                            no_trade = 1
                            # quantity = quantity
                        else:
                            no_trade = 0
                            # quantity = quantity + abs(last_quantity)
                            # application.symbol_positions[bloomberg_symbol]['position'] = quantity
                            # quantity = 2 * quantity
                    elif action == 'sell':
                        if last_position == 0:
                            no_trade = 0
                            # application.symbol_positions[bloomberg_symbol]['position'] = -quantity
                            # quantity = quantity
                        elif last_position < 0:
                            no_trade = 1
                            # quantity = quantity
                        else:
                            # quantity = quantity + abs(last_quantity)
                            no_trade = 0
                            # application.symbol_positions[bloomberg_symbol]['position'] = -quantity
                            # quantity = 2 * quantity
                    elif (action == 'update_limit' or last_position == 0):
                        # application.no_trade = 1 # set no_trade = 1 to pass previous quotes
                        no_trade = 0 # set no_trade = 0 to request new quotes
                        # time.sleep(2)
                        # print(f'SLEEPING 2s FOR CATCHING PASSING PREVIOUS QUOTES DUE TO LIMIT UPDATE')
                        if last_position > 0:
                            action = 'sell'
                        else:
                            action = 'buy'
                    else:
                        no_trade = 0
                        continue

                    quantity = quantity
                    application.no_trade[bloomberg_symbol] = no_trade
                    # application.no_trade = no_trade
                    application.limit_price[bloomberg_symbol] = limit_price
                    # application.limit_price = limit_price
                    order_type = 1

                    if action == "buy":
                        side = 1
                    elif action == 'sell':
                        side = 2
                    else: #get two way quotes
                        side = 0

                    print ("Putin Order")
                    print(f'Current position of {bloomberg_symbol} is {application.symbol_positions[bloomberg_symbol]["position"]}')
                    application.quote_request(symbol=symbol, currency=currency, quantity=quantity,
                                              side=side, order_type=order_type,dealers_dict=DEALERS_DICT)
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
                hdf_path = f'C:/Data/minute_data{symbol}.h5'
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
