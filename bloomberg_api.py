import sys
import time
import _thread
import argparse
import datetime
import quickfix as fix
import pandas as pd
import numpy as np
import os
import json
import filelock

FLOAT_FORMAT = '%.5f'
DATE_FORMAT = '%Y%m%d%H%M'

class Application(fix.Application):

    orderID = 100
    execID = 100
    orders_dict = dict()
    active_orders = dict()
    # write_path = './'
    write_path = r'\\10.10.1.13\Omni/Bloomberg'
    ord_status_dict = {'0':'new',
                       '1':'partially_filled',
                       '2':'filled',
                       '4':'canceled',
                       '5':'replaced',
                       '6':'pending_cancel',
                       '8':'rejected'}



    def gen_ord_id(self):
        global orderID
        orderID += 1
        return orderID

    def onCreate(self, sessionID):
            print(f'New Session created!\n ID: {sessionID.toString()}')
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

        print(f'-> ADMIN...{self.message}')

        if os.path.exists(f'{self.write_path}admin_messages.csv'):
            admin_df = pd.read_csv(f'{self.write_path}admin_messages.csv',index_col=0)
            admin_df = admin_df.append({'message':self.message},ignore_index=True)
            admin_df.to_csv(f'{self.write_path}admin_messages.csv')
        else:
            admin_df = pd.DataFrame(columns=['message'])
            app_df = pd.DataFrame(columns=['message'])
            admin_df = admin_df.append({'message':self.message},ignore_index=True)
            admin_df.to_csv(f'{self.write_path}admin_messages.csv')
            app_df.to_csv(f'{self.write_path}app_messages.csv')

        if self.message.getHeader().getField(35) == '0':
            print('->HrtBt')
        return

    def fromAdmin(self,  message, sessionID):

        self.sessionID = sessionID
        self.message = message

        print(f'<- ADMIN...{self.message}')

        if os.path.exists(f'{self.write_path}admin_messages.csv'):
            admin_df = pd.read_csv(f'{self.write_path}admin_messages.csv',index_col=0)
            admin_df = admin_df.append({'message':self.message},ignore_index=True)
            admin_df.to_csv(f'{self.write_path}admin_messages.csv')
        else:
            admin_df = pd.DataFrame(columns=['message'])
            app_df = pd.DataFrame(columns=['message'])
            admin_df = admin_df.append({'message':self.message},ignore_index=True)
            admin_df.to_csv(f'{self.write_path}admin_messages.csv')
            app_df.to_csv(f'{self.write_path}app_messages.csv')


        if self.message.getHeader().getField(35) == '0':
            print('<-HrtBt')
        if self.message.getHeader().getField(35) == 'A':
            print('LOGON!')
        return


    def toApp(self,  message, sessionID):

        print (f'-> APP: {message.toString()}' )

        if os.path.exists(f'{self.write_path}app_messages.csv'):
            app_df = pd.read_csv(f'{self.write_path}app_messages.csv',index_col=0)
            app_df.to_csv(f'{self.write_path}app_messages.csv')
        else:
            app_df = pd.DataFrame(columns=['message'])
            app_df = app_df.append({'message':self.message},ignore_index=True)
            app_df.to_csv(f'{self.write_path}app_messages.csv')

        return

    def fromApp(self, message, sessionID):

        self.message = message
        print(f'<-APP: {message.toString()}')

        if os.path.exists(f'{self.write_path}app_messages.csv'):
            app_df = pd.read_csv(f'{self.write_path}app_messages.csv',index_col=0)
            app_df = app_df.append({'message': self.message}, ignore_index=True)
            app_df.to_csv(f'{self.write_path}app_messages.csv')
        else:
            app_df = pd.DataFrame(columns=['message'])
            app_df = app_df.append({'message':self.message},ignore_index=True)
            app_df.to_csv(f'{self.write_path}app_messages.csv')

        msg_type = self.message.getHeader().getField(35)
        exec_type = self.message.getField(150)

        if msg_type == '8' and self.message.getField(20) != '3': # execution report
            if exec_type in ['0','8'] : # ack or reject
                self.orders_dict[self.message.getField(11)]['status'] = self.ord_status_dict[self.message.getField(39)]
                # self.orders_dict[self.message.getField(11)]['price'] = self.message.getField(31)

                print(f'ORDER_ID:{self.message.getField(11)}')
                if exec_type == '0':
                    print('NEW ORDER ACKNOWLEDGED!')
                else:
                    print(f'NEW ORDER REJECTED!:{self.message.getField(58)}')

                if os.path.exists(f'{self.write_path}fix_orders.csv'):
                    fix_orders = pd.read_csv(f'{self.write_path}fix_orders.csv',index_col=0)
                    fix_orders.loc[self.message.getField(11)] = self.orders_dict[self.message.getField(11)]
                    print(f'fix_orders.csv SAVED!')
                else:
                    fix_orders = pd.DataFrame(data=self.orders_dict).T
                    fix_orders.to_csv(f'{self.write_path}fix_orders.csv')
                    print(f'fix_orders.csv CREATED AND SAVED!')

            if exec_type in ['1','2','4','5','6'] :
                print(f'EXEC REPORT - TYPE{exec_type}')
                self.orders_dict[self.message.getField(11)]['status'] = self.ord_status_dict[self.message.getField(39)]
                self.orders_dict[self.message.getField(11)]['price'] = self.message.getField(31)

                #update status and price in fix_orders.csv
                fix_orders = pd.read_csv(f'{self.write_path}fix_orders.csv',index_col=0)
                fix_orders.loc[self.message.getField(11),:] = pd.Series(self.orders_dict[self.message.getField(11)])
                fix_orders.to_csv(f'{self.write_path}fix_orders.csv')
                if self.message.getField(20) == '0': #if its new order - get its status
                    self.order_status_request(cl_ord_id=self.message.getField(11))
                    print(f'TEST ID REQ FOR ORDER{self.message.getField(11)} SENT')
            # if exec_type in ['1','2'] and self.message.getField(20) :
            # # if exec_type in ['1','2'] and (self.message.getField(20) != '3'):
            #     #get order status request to update fix_report for multicharts
            #
            #     self.order_status_request(cl_ord_id=self.message.getField(11))
            #     print(f'TEST ID REQ FOR ORDER{self.message.getField(11)} SENT')
            # # if self.message.getField(150) == '2': #answer from order req

        if self.message.getField(20) == '3': #answer from order req
            print(self.message)
        else:
            pass

            print(f'ORDER_ID: {self.message.getField(11)} STATUS RECEIVED!')


            current_time = str((pd.to_datetime(message.getHeader().getField(52))+datetime.timedelta(hours=3)).time()).replace(':','')[:-2]+'00'
            # current_time = message.getHeader().getField(52).replace('-','').replace(':','')[8:]
            symbol = str(self.message.getField(55)) + '.' + str(self.message.getField(15))

            if self.message.getField(54) == '2':
                current_position =  (int(self.message.getField(14)) * int(-1))
            elif self.message.getField(54) == '1':
                current_position = (int(self.message.getField(14)) * int(1))

            # report_data = {
            #                #'account':str(self.message.getField(1)),
            #                 # 'datetime': pd.to_datetime(self.message.getField(11)[:12], format=DATE_FORMAT),
            #                 'id': current_time,
            #                 # 'id': self.message.getField(11)[:6],
            #                'symbol':symbol,
            #                # 'dd/MM/yyyy': str(self.message.getField(11)[:4] +'/'+self.message.getField(11)[4:6]+'/'+self.message.getField(11)[6:8]),
            #                # 'hh:mm:ss': str(self.message.getField(11)[8:10] +':'+self.message.getField(11)[10:12]+':'+self.message.getField(11)[12:14]),
            #                'contracts*marketpos': current_position,
            #                'open':str(self.message.getField(6))
            #                }

            # fix_report['account'] = str(self.message.getField(1))
            # fix_report['symbol'] = self.message.getField(55) + '.' + self.message.getField(15)
            # fix_report['dd/MM/yyyy'] = self.message.getField(11)[:8]
            # fix_report['hh:mm:ss'] = self.message.getField(11)[8:14]
            # fix_report['contracts*marketpos'] = self.message.getField(14) * int(self.message.getField(54))
            # fix_report['open'] = float(self.message.getField(44))

            if not os.path.exists(f'{self.write_path}fix_report.csv'):

                fix_report = pd.DataFrame(columns=['id','symbol','contracts*marketpos','open'])

                # fix_report = fix_report.append({'id':report_data['id'],'symbol':report_data['symbol'],
                #                                 'contracts*marketpos':report_data['contracts*marketpos'],'open':report_data['open']},
                #                                ignore_index=True)

                fix_report = fix_report.append({'id':current_time,'symbol':symbol,
                                                'contracts*marketpos':current_position,
                                                'open':self.message.getField(6)},
                                               ignore_index=True)

                # print(f'contracts*marketpos:{current_position} - {type(current_position)} - {type(self.message.getField(14))} ')

                fix_report.to_csv(f'{self.write_path}fix_report.csv',date_format=DATE_FORMAT)
                print('FIX REPORT FOR MULTICHARTS CREATED!')
            else:

                temp_report = pd.read_csv(f'{self.write_path}fix_report.csv')

                if pd.to_datetime(self.message.getField(6571)) > (datetime.datetime.now() + datetime.timedelta(minutes=2)):
                    current_lot = 0
                else:
                    current_lot = temp_report[temp_report.symbol == symbol]['contracts*marketpos']

                updated_lot = int(current_lot) + int(current_position)
                temp_report['id'] = current_time
                temp_report['contracts*marketpos'] = updated_lot
                temp_report.to_csv(f'{self.write_path}fix_report.csv', index=False, date_format=DATE_FORMAT)
                # report_data['contracts*marketpos'] = updated_lot
                # fix_report = pd.DataFrame(data=report_data, index=np.arange(100, 101))
                # fix_report.to_csv(f'{self.write_path}fix_report.csv',index=False,date_format=DATE_FORMAT)

                print('FIX REPORT FOR MULTICHARTS UPDATED!')

            # print('SLEEPING AFTER MULTICHARTS REPORT...')
            # time.sleep(100)

            # if msg_type == 'H':
            #     print('EXECUTION REPORT - ORDER STATUS!')
            #     if self.message.getField(20) == '3':#sanity check (tag 20 = 3 status)
            #         pass
            #         # fix_report = pd.DataFrame(columns=['account','symbol','dd/MM/yyyy','hh:mm:ss','contracts*marketpos','open'])
            #         # fix_report['account'] = self.message.getField(1)
        return

    def genOrderID(self):
        self.orderID = self.orderID+1
        return str(self.orderID)

    def genExecID(self):
        self.execID = self.execID+1
        return str(self.execID)

    def put_order(self,security_type,symbol,currency,quantity,side,order_type,account,time_id,price=None):

        if quantity != 0 :
            trade = fix.Message()
            '''
            STANDARD MESSAGE HEADER
            Required Tags: 8(BeginString) - 9(BodyLength) - 35(MsgType) - 49(SenderCompID) - 56(TargetCompID) - 34(MsgSeqNum) - 52(SendingTime)
            '''
            trade.getHeader().setField(fix.BeginString(fix.BeginString_FIX42)) #
            trade.getHeader().setField(fix.MsgType(fix.MsgType_NewOrderSingle)) #39=D
            trade.getHeader().setField(fix.SendingTime(1))
            trade.getHeader().setField(fix.ApplExtID('228'))#tag 1156
            # trade.getHeader().setField(fix.CstmApplVerID(''))#tag 1129
            trade.getHeader().setField(fix.ApplVerID('9'))#tag 1128 - FIX50SP2
            # unique_order_id = self.genExecID()
            # print(f'Unique Order ID: {unique_order_id}')
            # trade.setField(fix.ClOrdID(unique_order_id)) #11=Unique order

            trade.setField(fix.HandlInst(fix.HandlInst_AUTOMATED_EXECUTION_ORDER_PUBLIC_BROKER_INTERVENTION_OK)) #21=3 (Manual order), 21=2 automated execution only supported value
            trade.setField(fix.Symbol(str(symbol))) #55
            trade.setField(fix.Currency(str(currency))) #15
            trade.setField(fix.SecurityType(str(security_type))) #167
            trade.setField(fix.Side(str(side))) #54=1 Buy
            trade.setField(fix.OrdType(str(order_type))) #40=2 Limit order, 40=1 Market
            trade.setField(fix.OrderQty(quantity)) #38
            if order_type != '1': #not market
                trade.setField(fix.Price(price)) # if market, this tag  should be absent
            else:
                price = None
            trade.setField(fix.Account(str(account)))
            trade.setField(fix.ExDestination('IDEALPRO'))
            trade.setField(fix.CustomerOrFirm(0))
            trade.setField(fix.ClOrdID(time_id+trade.getField(55)+trade.getField(15)+trade.getField(54))) #11=
            # trade.setField(fix.ClOrdID(datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')+trade.getField(55)+trade.getField(15)+trade.getField(54))) #11=

            # dnow = datetime.utcnow().strftime('%Y%m%d-%H:%M:%S')
            # tag = fix.TransactTime() #default = current time, SendingTime does the same thing
            # tag.setString(dnow.strftime('%Y%m%d-%H:%M:%S'))
            # trade.setField(tag)

            print(f'CREATING THE FOLLOWING ORDER:\n {trade.toString()}')
            fix.Session.sendToTarget(trade, self.sessionID)
            self.orders_dict[trade.getField(11)] = {#'id':trade.getField(11),
                                                    #'datetime': trade.getField(11)[:12],
                                                    'account':trade.getField(1),
                                                    'symbol':trade.getField(55)+'.' + trade.getField(15),
                                                    'qty':trade.getField(38),
                                                    'ord_type':trade.getField(40),
                                                    'side':trade.getField(54),
                                                    'price':price,
                                                    'status':'sent'
                                                    }
        else:
            print(f'QUANTITY IS 0!\nCHECK FOR STATUS OF LAST ORDER')
            if os.path.exists(f'{self.write_path}fix_orders.csv'):
                temp_orders = pd.read_csv(f'{self.write_path}fix_orders.csv',index_col=0)
                symbol_entries = temp_orders[temp_orders['symbol'] == symbol+'.'+currency]
                last_symbol_active = symbol_entries[symbol_entries.qty > 0].iloc[-1] #find last entry of the current symbol when position opened
                new_entry = last_symbol_active.copy()
                new_id = time_id + last_symbol_active.name[6:]
                new_entry.name = new_id
                new_entry['qty'] = 0
                temp_orders = temp_orders.append(new_entry,ignore_index=False)
                temp_orders.to_csv(f'{self.write_path}fix_orders.csv',) #update fix_orders.csv with quantity = 0 (nothing happens)
                self.order_status_request(cl_ord_id=last_symbol_active.name)
                # self.order_status_request(cl_ord_id=temp_orders.index[-1])
            else:
                print(f'QUANTITY IS 0!\n FIX ORDERS FIRST RUN')
                time.sleep(100)
            # print('sleeping put order-quantity0...')
            # time.sleep(100)

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


def main(config_file):


    interval_types_dict = {'2':'min'}
    order_type_dict = {'MARKET':1,'LIMIT':2}
    side_dict = {'BUY_OPEN':1,
                 'SELL_OPEN':2,
                 'SELL_CLOSE':2,
    }


    # path = 'C:/Users/ak\Downloads\quickfix_example/'
    # order = pd.read_csv(f'{path}speedlab_orders.csv')
    # timeframe = '1h'
    timeframe = 1
    # symbol = ''.join([a for a in order.iloc[1][0] if a.isalnum()])
    # strategy = ''.join([a for a in order.iloc[1][1] if a.isalnum()])
    # action = ''.join([a for a in order.iloc[1][5] if a.isalnum()])
    # account = ''.join([a for a in order.iloc[1][7] if a.isalnum()])
    # exec_time = ''.join([a for a in order.iloc[1][15] if a.isalnum()])
    # price = None
    # quantity = ''.join([a for a in order.iloc[1][30] if a.isalnum()])
    # tag_55 = symbol[6:9] #Symbol
    # tag_15 = symbol[9:12] #Currency
    # tag_54 = str(np.where('BUY' in action,1,2)) #Side
    # security_type = 'CASH' #tag 167
    # symbol = 'EUR' #tag 55
    # currency = 'AUD' #tag 15
    # quantity = 10000 #tag 38
    # side = 1 #tag 54
    # order_type = 2 #tag 40
    # account = 'U01049' #tag 1
    # if order_type == 1:
    #     price = None
    # else:
    #     price = 1.68 #limit

    try:
        path = "./"
        read_file = 'fix_json.txt'

        #start session
        settings = fix.SessionSettings(config_file)
        application = Application()
        storeFactory = fix.FileStoreFactory(settings)
        logFactory = fix.FileLogFactory(settings)
        initiator = fix.SocketInitiator(application, storeFactory, settings, logFactory)
        initiator.start()
        print(f'INITIATOR STARTED...\nSLEEPING 10 SECONDS...')
        time.sleep(10)
        previous_bar = datetime.datetime.now().minute

        run = True
        while True:
            if run:
            # if (datetime.datetime.now().minute%int(timeframe) == 0) and (datetime.datetime.now().minute != previous_bar):

                print(f'')
                # print(f'RUNNING TIME : {datetime.datetime.now()}')
    #             time.sleep(5) # to fix_json be updated
    #             previous_bar = datetime.datetime.now().minute
    #         #     current_time = datetime.datetime.now()
    #         #     next_bar = current_time + datetime.timedelta(seconds=timeframe*60-10)
    #             # read data from multicharts
    #             # get timeframe
    #             with open(path+read_file,'r') as f:
    #                 lines = f.readlines()
    #                 f.close()
    #                 last_entry = json.loads(lines[-1].rstrip().strip(r"\'"))
    #             # data_from_mc = pd.read_csv(f'{path+read_file}',error_bad_lines=False)
    #             timeframe = last_entry['timeframe']
    #             interval_type = last_entry['intervaltype']
    #
    #             security_type = 'CASH'  # tag 167
    #             account = 'U01049'  # tag 1
    #             price = None
    #             order_type = order_type_dict[last_entry['limit']['type']] # tag 40
    #             if order_type == 2:
    #                 price =  float(last_entry['limit']['Limit price'])
    #             quantity = int(last_entry['volume']['quantity'])  # tag 38
    #             symbol = last_entry['symbol'][:3] # tag 55
    #             currency = last_entry['symbol'][4:] # tag 15
    #             side = side_dict[last_entry['orderAction']]  # tag 54
    #             time_id = last_entry['time'].replace(':','')
    #
    #             #
    #             action = input('Push 1 for place order, 2 for order request')
    #             if action == '1':
    #                 order_type = str(input('Order Type='))
    #                 if order_type != '1':
    #                     price = np.float(input('Limit Price='))
    #                 symbol = str(input('Symbol='))
    #                 currency = str(input('Currency='))
    #                 side = str(input('Side='))
    #                 time_id = str(input('time_id='))
    #                 quantity = int(input('quantity='))
    #
    #
    #                 if True: #check if last entry is up to date
    #                 # if pd.to_datetime(last_entry['time']) + datetime.timedelta(seconds=15) > datetime.datetime.now(): #check if last entry is up to date
    #
    #                 # if quantity != 0:
    #                 #     print("PUTTING ORDER")
    #                     application.put_order(security_type=security_type, symbol=symbol, currency=currency, quantity=quantity, side=side, order_type=order_type,
    #                                   account=account,price=price,time_id = time_id)
    #                     time.sleep(10)
    #                 else:
    #                     print('LAST BAR IN FIX JSON IS NOT CURRENT!')
    #
    #             if action == '2':
    #                 order_id = input('Give order id for status update')
    #                 application.order_status_request(cl_ord_id=order_id)
    #             # run = False
    #             # else:
    #             #     print(f'QUANTITY = 0 - WAIT TILL NEXT BAR')
    #             #     time.sleep(100)
    #             #     continue
    #         # else:
    #         #     continue
    #             # order = pd.read_csv(f'{path}speedlab_orders.csv')
    #             # symbol_ = ''.join([a for a in order.iloc[1][0] if a.isalnum()]).strip('symbol')
    #             # strategy_ = ''.join([a for a in order.iloc[1][1] if a.isalnum()])
    #             # action_ = ''.join([a for a in order.iloc[1][5] if a.isalnum()]).strip('orderAction')
    #             # account_ = ''.join([a for a in order.iloc[1][7] if a.isalnum()]).strip('accountSegmentatityType')
    #             # exec_time = ''.join([a for a in order.iloc[1][15] if a.isalnum()])
    #             # price = None
    #             # quantity_ = ''.join([a for a in order.iloc[1][30] if a.isalnum()]).strip('volumequantity')
    #             #
    #             # security_type = 'CASH'
    #             # symbol = symbol_[:3]
    #             # currency = symbol_[3:6]
    #             # side = np.where('BUY' in action_,1,np.where('SELL' in action_,2,0))
    #             # order_type = 1 #1:MARKET, 2:LIMIT
    #             # quantity = quantity_
    #             # if order_type == 1:
    #             #     price = None
    #             # else:
    #             #     price = 1.89  # limit
    #             # account = 'U01049'
    #
    #
    #             # input_ = input('enter 1 for order, 2 for exit, 3 for order status update, 4 for order cancel request test for test request :\n ')
    #             # print('\n')
    #             # if input_ == '1':
    #             #     print ("Putin Order")
    #             #     application.put_order(security_type=security_type, symbol=symbol, currency=currency, quantity=quantity, side=side, order_type=order_type,
    #             #                   account=account,price=price)
    #             #     # initiator.stop()
    #             # if input_ == '2':
    #             #     sys.exit(0)
    #             # if input_ == 'test':
    #             #     application.test_req()
    #             # if input_ == '3':
    #             #     print('requesting order status...')
    #             #     application.order_status_request()
    #             # if input_ == '4':
    #             #     print('order cancel request...')
    #             #     application.order_cancel_request(account=account,symbol=symbol,side=side,quantity=quantity)
    #             # if input_ == '5':
    #             #     print('order cancel replace...')
    #             #     application.order_cancel_replace(account=account,symbol=symbol,side=side,quantity=quantity,
    #             #                                      order_type=2,price=1.1)
    #             #
    #             # if input_ == 'd':
    #             #     import pdb
    #             #     pdb.set_trace()
    #             # else:
    #             #     print ("Valid input is 1 for order, 2 for exit")
    #             #     continue
    # # except (fix.ConfigError, fix.RuntimeError), e:
    except (fix.ConfigError or fix.RuntimeError) as error:
        print(f'ERROR\n{error}')

if __name__=='__main__':

    config_path = './client.cfg'

    # def get_config_params(path=config_path):
    #     # parser = argparse.ArgumentParser(description='FIX Client')
    #     # parser.add_argument('file_name', type=str, help='Name of configuration file')
    #     # args = parser.parse_args()

    # import configparser
    # config = configparser.ConfigParser()
    # config['DEFAULT'] = {'ConnectionType':'initiator',
    #                      'LogonTimeout' :'30',
    #                     'ReconnectInterval' : '30'
    #                      }
    # config['SESSION'] = {}
    # config['SESSION']['BeginString'] = 'FIX.4.2'
    # config['SESSION']['SenderCompID'] = 'qafix49'
    # config['SESSION']['TargetCompID'] = 'IB'
    # config['SESSION']['StartTime'] = '00:00:00'
    # config['SESSION']['EndTime'] = '00:00:00'
    # config['SESSION']['HeartBtInt'] = '30'
    # config['SESSION']['SocketConnectPort'] = '7496'
    # config['SESSION']['HeartBtInt'] = '30'
    # config['SESSION']['SocketConnectHost'] = '127.0.0.1'
    # config['SESSION']['DataDictionary'] = 'C:/Users/ak\Downloads\quickfix - 1.15.1_python_un\quickfix - 1.15.1\spec/FIX42.xml'
    # with open('config_file.ini','w') as config_file:
    #     config.write(config_file)
    # main(config)
    # import savReaderWriter as s
    #
    # df = pd.DataFrame(list(s.SavReader('./Data_example.sav', returnHeader=True, rawMode=False)))
    # df.columns = [str(col).strip("b''") for col in df.iloc[0, :]]
    # df.drop([0], axis=0, inplace=True)
    # df.set_index(df.iloc[:, 0], inplace=True)
    # df.drop([df.columns[0]], axis=1, inplace=True)
    # df_ = df.astype(np.float32, copy=True)

    main(config_path)
