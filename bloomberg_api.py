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
class Application(fix.Application):

    orderID = 100
    execID = 100
    orders_dict = dict()
    active_orders = dict()
    limit_price = 0.
    timeframe = 5
    # write_path = './'
    write_path = r'\\10.10.1.13\Omni/Bloomberg'
    ord_status_dict = {'0':'new',
                       '1':'partially_filled',
                       '2':'filled',
                       '4':'canceled',
                       '5':'replaced',
                       '6':'pending_cancel',
                       '8':'rejected'}

    def genOrderID(self):
        self.orderID = self.orderID+1
        return str(self.orderID)

    def genExecID(self):
        self.execID = self.execID+1
        return str(self.execID)

    def gen_ord_id(self):
        global orderID
        orderID += 1
        return orderID

    def onCreate(self, sessionID):
            print(f'New Session created!\n ID: {sessionID.toString()}')
            self.sessionID = sessionID
            return

    def onLogon(self, sessionID):
            self.sessionID = sessionID
            print ("Successful Logon to session '%s'." % sessionID.toString())
            return

    def onLogout(self, sessionID):
        self.sessionID = sessionID
        print("Successful Logout from session '%s'." % sessionID.toString())
        return

    def toAdmin(self,  message, sessionID):

        self.sessionID = sessionID
        self.message = message

        # print(f'-> ADMIN...{self.message}')
        #
        # if os.path.exists(f'{self.write_path}admin_messages.csv'):
        #     admin_df = pd.read_csv(f'{self.write_path}admin_messages.csv',index_col=0)
        #     admin_df = admin_df.append({'message':self.message},ignore_index=True)
        #     admin_df.to_csv(f'{self.write_path}admin_messages.csv')
        # else:
        #     admin_df = pd.DataFrame(columns=['message'])
        #     app_df = pd.DataFrame(columns=['message'])
        #     admin_df = admin_df.append({'message':self.message},ignore_index=True)
        #     admin_df.to_csv(f'{self.write_path}admin_messages.csv')
        #     app_df.to_csv(f'{self.write_path}app_messages.csv')

        if self.message.getHeader().getField(35) == '0':
            pass
            # print('->HrtBt')
        return

    def fromAdmin(self,  message, sessionID):

        self.sessionID = sessionID
        self.message = message

        # print(f'<- ADMIN...{self.message}')
        #
        # if os.path.exists(f'{self.write_path}admin_messages.csv'):
        #     admin_df = pd.read_csv(f'{self.write_path}admin_messages.csv',index_col=0)
        #     admin_df = admin_df.append({'message':self.message},ignore_index=True)
        #     admin_df.to_csv(f'{self.write_path}admin_messages.csv')
        # else:
        #     admin_df = pd.DataFrame(columns=['message'])
        #     app_df = pd.DataFrame(columns=['message'])
        #     admin_df = admin_df.append({'message':self.message},ignore_index=True)
        #     admin_df.to_csv(f'{self.write_path}admin_messages.csv')
        #     app_df.to_csv(f'{self.write_path}app_messages.csv')
        #
        #
        # if self.message.getHeader().getField(35) == '0':
        #     pass
        #     # print('<-HrtBt')
        if self.message.getHeader().getField(35) == 'A':
            print('LOGON!')
        return

    def toApp(self,  message, sessionID):

        print (f'-> APP: {message.toString()}' )

        # if os.path.exists(f'{self.write_path}app_messages.csv'):
        #     app_df = pd.read_csv(f'{self.write_path}app_messages.csv',index_col=0)
        #     app_df.to_csv(f'{self.write_path}app_messages.csv')
        # else:
        #     app_df = pd.DataFrame(columns=['message'])
        #     app_df = app_df.append({'message':self.message},ignore_index=True)
        #     app_df.to_csv(f'{self.write_path}app_messages.csv')

        return

    def fromApp(self, message, sessionID):

        print(f'<-APP: {message.toString()}')

        msg_type = message.getHeader().getField(35)
        global prices
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
                    except:
                        bid_px = 999
                    try:
                        offer_px = float(message.getField(133))
                    except:
                        offer_px = 999

                    # if hit:
                    #     self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                    #                         symbol=symbol, product=product, side=side, dealer_id=dealer_id)
                    #     print(f'PRICE FILLED FROM PREVIOUS DEALER - PASS QUOTES FROM {dealer_id}')
                    #     continue
                    print(f'CHECKING QUOTES FROM DEALER: {dealer_id}')
                    print(f'BID PX:{bid_px} - OFFER PX:{offer_px}')
                    prices.append(offer_px)

                if len(prices)==dealers_no:
                    print(prices)

                    offer_px = prices[0]
                    prices=[]
                    if side == '1':  # long
                       if offer_px <= self.limit_price:
                           self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=1,
                                               symbol=symbol, product=product, side=side, dealer_id=dealer_id,
                                               price=offer_px,currency=currency,quantity=quantity)
                           print(f'BUY at {offer_px} from {dealer_id} - PASS QUOTES FROM OTHER DEALERS')
                           hit = True
                           # time.sleep(10)
                           continue
                       else:
                           self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                                               symbol=symbol, product=product, side=side, dealer_id=dealer_id)
                           print(f'PASSED QUOTES FROM {dealer_id} ')
                           time.sleep(1)
                           continue
                           # if party_id == parties_no:
                           #     self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                           #                         symbol=symbol, product=product, side=side, dealer_id=dealer_id) #check other dealers
                           #     print(f'CURRENT QuoteRequestPassed - REQUEST NEW QUOTES')
                           #     self.quote_request(symbol=symbol,currency=currency,quantity=quantity,side=side,order_type=1)
                           #
                           # else: #pass current request - send new
                           #     continue
                           #     # self.quote_request(symbol=symbol,currency=currency,quantity=quantity,side=side,order_type=1,)

                    if side == '2':  # long
                       if bid_px >= self.limit_price:
                           self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=1,
                                               symbol=symbol, product=product, side=side, dealer_id=dealer_id,
                                               price=self.limit_price,currency=currency,quantity=quantity)
                           print(f'SELL at {bid_px} from {dealer_id} - PASS QUOTES FROM OTHER DEALERS')
                           hit = True
                           continue
                       else:
                           self.quote_response(quote_req_id=quote_req_id, quote_id=quote_id, quote_resp_type=6,
                                               symbol=symbol, product=product, side=side, dealer_id=dealer_id)
                           print(f'PASSED QUOTES FROM {dealer_id} ')
                           # time.sleep(10)
                           continue

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
            # quote_resp.setField(fix.Price(float(price)))
            quote_resp.setField(fix.OfferPx(float(price)))
            quote_resp.setField(fix.OfferSpotRate(float(price)))
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

    def order_status_request(self,cl_ord_id=None,new_id=None):

        if cl_ord_id == '*':
            ord_status_request = fix.Message()
            ord_status_request.getHeader().setField(fix.BeginString(fix.BeginString_FIX42))  #
            ord_status_request.getHeader().setField(fix.MsgType('H'))  # 39=D
            ord_status_request.getHeader().setField(fix.SendingTime(1))
            ord_status_request.setField(fix.ClOrdID(cl_ord_id))  # 11=*
            fix.Session.sendToTarget(ord_status_request, self.sessionID)
            print(f'REQUEST FOR ALL OPENS ORDERS SENT!')


        if cl_ord_id is None:
            fix_orders = pd.read_csv(f'{self.write_path}/fix_orders.csv',index_col=0)
            for idx in fix_orders.index:
                ord_status_request = fix.Message()
                ord_status_request.getHeader().setField(fix.BeginString(fix.BeginString_FIX42)) #
                ord_status_request.getHeader().setField(fix.MsgType('H')) #39=D
                ord_status_request.getHeader().setField(fix.SendingTime(1))
                # ord_status_request.setField(fix.Symbol('EUR')) #55
                # ord_status_request.setField(fix.Account('U01049'))
                # ord_status_request.setField(fix.SecurityReqID('1'))
                ord_status_request.setField(fix.ClOrdID(str(idx))) #11=
                # ord_status_request.setField(fix.ClOrdID(str('*'))) #11=
                # ord_status_request.setField(fix.ClOrdID(datetime.utcnow().strftime('%Y%m%d%H%M%S')+ 'statReq' + self.genExecID())) #11=
                # ord_status_request.setField(fix.OrderID(datetime.utcnow().strftime('%Y%m%d%H%M%S')+ 'statReq' + self.genExecID()))
                # ord_status_request.setField(fix.OrderID('*'))
                # ord_status_request.setField(fix.SecurityType('CASH')) #167
                # ord_status_request.setField(fix.Side(fix.Side_SELL))
                print(f'Order status message \n {ord_status_request}')
                fix.Session.sendToTarget(ord_status_request,self.sessionID)
                print('order status request for open orders sent!')
        else:
            ord_status_request = fix.Message()
            ord_status_request.getHeader().setField(fix.BeginString(fix.BeginString_FIX42))  #
            ord_status_request.getHeader().setField(fix.MsgType('H'))  # 39=D
            ord_status_request.getHeader().setField(fix.SendingTime(1))
            ord_status_request.setField(fix.ClOrdID(str(cl_ord_id)))  # 11=
            print(f'ORDER STATUS MESSAGE \n {ord_status_request}')
            fix.Session.sendToTarget(ord_status_request, self.sessionID)
            print(f'ORDER STATUS REQUEST FOR {cl_ord_id} SENT!')

    def test_req(self):
        print("Creating testing message... ")
        test_message = fix.Message()
        test_message.getHeader().setField(fix.MsgType('1'))
        test_message.setField(fix.TestReqID('test'))
        print('sending Test message...')
        print (f'test message: {test_message.toString()}')
        # print(f'session ID: {self.sessionID}')
        fix.Session.sendToTarget(test_message, self.sessionID)
        print('test message sent!')

    def order_cancel_request(self,account,symbol,side,quantity):
        print("Creating order_cancel_request message... ")
        cancel_request_message = fix.Message()
        cancel_request_message.getHeader().setField(fix.BeginString(fix.BeginString_FIX42)) #
        cancel_request_message.getHeader().setField(fix.MsgType('F')) #39=D
        cancel_request_message.getHeader().setField(fix.SendingTime(1))

        cancel_request_message.setField(fix.Account(str(account))) #1
        cancel_request_message.setField(fix.ClOrdID(str('order_cancel_request'+self.genOrderID()))) #11
        cancel_request_message.setField(fix.OrigClOrdID(str(self.orders[0]))) #41
        cancel_request_message.setField(fix.Symbol(str(symbol))) #55
        cancel_request_message.setField(fix.Side(str(side))) #54
        cancel_request_message.setField(fix.OrderQty(quantity)) #38

        print('sending order_cancel_request message...')
        print(f'order_cancel_request message: {cancel_request_message.toString()}')
        fix.Session.sendToTarget(cancel_request_message, self.sessionID)
        print('order_cancel_request message sent!')

    def order_cancel_replace(self,account,symbol,side,quantity,order_type,price):
        print("Creating order_cancel_replace message... ")
        cancel_replace_message = fix.Message()
        cancel_replace_message.getHeader().setField(fix.BeginString(fix.BeginString_FIX42)) #
        cancel_replace_message.getHeader().setField(fix.MsgType('G')) #39=D
        cancel_replace_message.getHeader().setField(fix.SendingTime(1))

        cancel_replace_message.setField(fix.Account(str(account))) #1
        cancel_replace_message.setField(fix.HandlInst(fix.HandlInst_AUTOMATED_EXECUTION_ORDER_PUBLIC_BROKER_INTERVENTION_OK)) #21=3 (Manual order), 21=2 automated execution only supported value
        cancel_replace_message.setField(fix.ClOrdID(str('order_cancel_replace'+self.genOrderID()))) #11
        cancel_replace_message.setField(fix.OrigClOrdID(str(self.orders[0]))) #41
        cancel_replace_message.setField(fix.Symbol(str(symbol))) #55
        cancel_replace_message.setField(fix.Side(str(side))) #54
        cancel_replace_message.setField(fix.OrderQty(quantity)) #38
        cancel_replace_message.setField(fix.OrdType(str(order_type))) #40
        cancel_replace_message.setField(fix.Price(price)) #44

        print('sending order_cancel_replace message...')
        print(f'order_cancel_replace message: {cancel_replace_message.toString()}')
        fix.Session.sendToTarget(cancel_replace_message, self.sessionID)
        print('order_cancel_replace message sent!')


def main():


    config_file = './client.cfg'
    try:
        sleep_seconds = 3
        #start session
        settings = fix.SessionSettings(config_file)
        application = Application()
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

                    # print(f'')
                    print(f'RUNNING TIME : {datetime.datetime.now()}')
                    previous_bar = datetime.datetime.now().minute

                    input_ = input('enter 1 for order, 2 for exit, 3 for order status update, 4 for order cancel request test for test request :\n ')
                    print('\n')

                    if input_ == '1':
                        print ("Putin Order")
                        limit_price = np.float(input('Limit Price From Strategy =')) #from strategy output, last_close +- limit
                        application.limit_price = limit_price
                        order_type = str(input('Order Type = ')) #from strategy output
                        if order_type != '1':
                            price = np.float(input('Limit Price='))
                        else:
                            price=None
                        symbol = str(input('Symbol='))
                        currency = str(input('Currency='))
                        side = str(input('Side='))
                        # time_id = str(input('time_id='))
                        quantity = int(input('quantity='))
                        application.quote_request(symbol=symbol, currency=currency, quantity=quantity, side=side, order_type=order_type,
                                      price=price)
        #             #     # initiator.stop()
        #             time.sleep(10)
                    if input_ == '2':
                        sys.exit(0)
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

if __name__=='__main__':

    main()
