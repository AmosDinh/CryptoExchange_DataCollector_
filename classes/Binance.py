from classes.Websocketclass import Websocketclass
import time


class Binance(Websocketclass):
    def __init__(self,):
        super(Binance, self).__init__()
        self.connection = "wss://stream.binance.com:9443/ws"
        self.columnformat = {
            "dt": 1,
            "exchange": "Binance",
            "pair": 1,

            "a": 1,
            "av": 1,


            "b": 1,
            "bv": 1,

            "lp": 1,
            "lv": 1,

            "wap": 1,
            "bav24": 1,
            "qav24": 1,
            "numt24": 1,

            "o": 1,
            "h": 1,
            "l": 1,
        }
        self.generatepairs["lowercase"] = True
        self.generatepairs['after'] = "@ticker"

        self.ws_subscription = {
            "method": "SUBSCRIBE",
            "params": "%list%",
            "id": 1
        }

        self.init_after()

    async def ws_message(self, ws, data):
        if 'e' in data and data['e'] == "24hrTicker":
            self.lasttime = self.get_datetime()
            myDict = {
                "dt": self.lasttime.strftime("%Y-%m-%d %H:%M:%S.%f %z"),
                "exchange": "Binance",
                "pair": data['s'],

                "a": data["a"],
                "av": data["A"],

                "b": data["b"],
                "bv": data["B"],

                "lp": data["c"],
                "lv": data["Q"],

                "wap": data["w"],

                "bav24": data["v"],
                "qav24": data["q"],

                "numt24": data["n"],

                "o": data["o"],
                "h": data["h"],
                "l": data["l"],

            }
            self.append_data_list(myDict=myDict)
        elif "code" in data:
            print("Binance",data)
        
