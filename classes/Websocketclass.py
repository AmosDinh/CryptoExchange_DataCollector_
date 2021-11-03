from classes.Global_vars import Global_vars
import gc
import os
import psutil
import tracemalloc
import time
import datetime
import pytz
import json
import threading
import websocket
from classes.MyAsyncioWebsocket import MyAsyncioWebsocket

websocket._logging._logger.level = -99


class Websocketclass():

    def __init__(self,
                 connection="",
                 columnformat={
                     "dt": None,
                     "exchange": None,
                     "pair": None,

                     "a": None,
                     "av": None,
                     "awlv": None,
                     "alv": None,
                     "totalav": None,

                     "b": None,
                     "bv": None,
                     "bwlv": None,
                     "blv": None,
                     "totalbv": None,

                     "lp": None,
                     "lv": None,

                     "vtd": None,
                     "v24": None,

                     "vwap": None,
                     "wap": None,
                     "vwaptd": None,
                     "vwap24": None,
                     "bav24": None,
                     "qav24": None,
                     "pchange24": None,
                     "numttd": None,
                     "numt24": None,

                     "o": None,
                     "o24": None,
                     "h": None,
                     "h24": None,
                     "l": None,
                     "l24": None,
                     "c": None,
                     "clv": None
                 },
                 generatepairs={
                     "jsonpath": './json/currencypairs.json',
                     "switchplaces": False,
                     "lowercase": False,
                     "jsonloads": False,
                     "before": "",
                     "between": "",
                     "after": "",
                     "remove": [],
                     "replace": [["", ""]],

                 },
                 ws_runforever={
                     "ping_interval": 4,  # cex debug
                     "origin": None,
                     "ping_timeout": 3
                 }, ws_subscription={
                     # "method": "SUBSCRIBE",
                     # "params": "%iterate%", #pairs are iterated over, each has own send(), replace %iterate%
                     # "params": "%list%"", #pairs are iterated over and put into list and appended once[pair+","+pair] replace %list%
                     # "params": "",# if no %% specified nothing will be added
                 }, orderbook_depth=50  # change from 20 to 50 because coinbase error
                 ):

        # Thread.__init__(self)
        self.connection = connection
        self.columnformat = columnformat.copy()
        self.generatepairs = generatepairs.copy()
        self.ws_runforever = ws_runforever.copy()
        self.ws_subscription = ws_subscription.copy()
        self.orderbook_depth = orderbook_depth
        Global_vars.data_queue[self.__class__.__name__] = []
        gc.set_threshold(200, 10, 10)
    
    def init_vars(self):
        self.orderbooks = dict.fromkeys(self.pairs)
        self.last_bestaskbids = dict.fromkeys(self.pairs)
     
        Global_vars.data_queue[self.__class__.__name__].append(
            Global_vars.csv_columns[self.__class__.__name__])

    def init_after(self):
        tcolumns = []
        for k, v in self.columnformat.items():
            if v != None:
                tcolumns.append(k)
        Global_vars.csv_columns[self.__class__.__name__] = tcolumns

        tlist = []

        f = open(self.generatepairs['jsonpath'], 'r')
        pairs = json.load(f)['pairs']
        # self.lastdicts = {}  # avoid duplicates

        for v in pairs:
            v1, v2 = v[0], v[1]
            #self.lastdicts[v1+v2] = None
            if self.generatepairs['switchplaces']:
                v1, v2 = v[1], v[0]
            if self.generatepairs['lowercase']:
                v1 = v1.lower()
                v2 = v2.lower()

            pair = self.generatepairs['before'] + v1 + \
                self.generatepairs['between']+v2 + self.generatepairs['after']
            for old, new in self.generatepairs['replace']:
                pair = pair.replace(old, new)
            for rm in self.generatepairs['remove']:
                pair = pair.replace(rm, '')

            tlist.append(json.loads(pair)
                         if self.generatepairs['jsonloads'] else pair)

        self.pairs = tlist

        # have to be reinitialized on new thread, not if using Websocketclass.__init__()
        self.orderbooks = dict.fromkeys(self.pairs)#in self.init_vars()
        self.last_bestaskbids = dict.fromkeys(self.pairs)


        self.process_keep_running = True  # Bittrex
      
        self.thread = threading.Thread(target=self.ws_thread)
        self.thread.daemon = True
        self.thread.start()
    
    

    def update_book(self, pair, side, data, pricepos, volumepos):

        for x in data:
            price = x[pricepos]
            if float(x[volumepos]) > 0:
                self.orderbooks[pair][side].update(
                    {price: float(x[volumepos])})

            elif price in self.orderbooks[pair][side]:
                del self.orderbooks[pair][side][price]

        self.orderbooks[pair][side] = dict(list(sorted(self.orderbooks[pair][side].items(
        ), key=lambda x: float(x[0]), reverse=(True if side == "bid" else False)))[:int(self.orderbook_depth)])

    def append_data_list(self, myDict):
        l = []
        for k, v in myDict.items():
            if v != None:
                v = str(v)
                if k != "dt" and "." in v:#fix for numbers without "." (Cex)
                    v = v.strip("0")
                    if v[-1]==".":
                        v = v[:-1]
                l.append(v)
        Global_vars.data_queue[self.__class__.__name__].append(l)

    def ws_message(self, ws, j):  # replace with childclass ws_message
        raise Exception('Please specify ws_message for childclass',
                        self.__class__.__name__)

    def get_datetime(self):
        return Global_vars.get_datetime()

    async def ws_open(self, ws):
        self.init_vars()
        j = self.ws_subscription
        j = json.dumps(j)
        if len(j) == 0:
            print(self.__class__.__name__, "no ws subscription specified")
            return

        if "%iterate%" in j:
            for pair in self.pairs:
                subscription = j
                subscription = j.replace('%iterate%', pair)
                await ws.send(json.dumps(json.loads(subscription)))

        elif "%list%" in j:
            subscription = j.replace('"%list%"', json.dumps(self.pairs)).replace(
                "'%list%'", json.dumps(self.pairs))
            await ws.send(json.dumps(json.loads(subscription)))
        else:
            await ws.send(json.dumps(j))

    async def reroute_ws_open(self, ws):
        await self.ws_open(ws)

    # async def reroute_ws_message(self, ws, msg):
    #     await self.ws_message(ws, json.loads(msg))

    def ws_thread(self, reconnecting=False):
        if reconnecting:
            print(self.__class__.__name__, "websocket reconnecting",
                  threading.current_thread().name)
        else:
            print(self.__class__.__name__, "websocket ",
                  threading.current_thread().name)

        # websocket.enableTrace(True)
        # self.ws = websocket.WebSocketApp(
        #     self.connection,
        #     on_open=lambda ws: self.reroute_ws_open(ws),
        #     on_message=lambda ws, msg: self.reroute_ws_message(ws, msg),
        # )

        # self.ws.run_forever(
        #     ping_interval=self.ws_runforever['ping_interval'], origin=self.ws_runforever['origin'], ping_timeout=self.ws_runforever['ping_timeout'])

        self.ws = MyAsyncioWebsocket(
            connection=self.connection, on_open=lambda ws: self.reroute_ws_open(
                ws),
            on_message=lambda ws, msg: self.ws_message(ws, json.loads(msg)),
            exchangename=self.__class__.__name__)
        self.ws.keep_running = True

        self.ws.run_ws()


        if self.ws.keep_running:
            self.ws_thread(True)

    def stop_ws_thread(self):
        if self.__class__.__name__ != "Bittrex":
            self.ws.keep_running = False
        else:
            self.process_keep_running = False  # keep for bittrex
        #self.thread.join()#isDaemon
        print(self.__class__.__name__+" shut down")
