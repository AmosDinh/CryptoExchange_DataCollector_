import asyncio
from concurrent import futures
import psutil
import gc
import os
from pympler import asizeof
from classes.Bittrex import Bittrex  # first import or error
from classes.Binance import Binance
from classes.Bitstamp import Bitstamp
from classes.Cex import Cex
from classes.Coinbase import Coinbase
from classes.Gemini import Gemini
from classes.Hitbtc import Hitbtc
from classes.Kraken import Kraken
from classes.Poloniex import Poloniex

from classes.Drive_upload import Drive_upload

import threading
import time
import signal

from classes.Global_vars import Global_vars
from classes.Memory_profile import Memory_profile
Memory_profiler = Memory_profile()


#from memory_profiler import profile
# Please note that it is currently possible that processes in a
# dyno that is being shut down may receive multiple SIGTERMs


def receive_signal(signum, stack):

    print("Received SIGTERM, shutting down")
    Global_vars.Drive_thread.do_run = False
    for c in Global_vars.myClasses.values():  # not important to exit gracefully
        c.stop_ws_thread()

    # for ex in exchange_names:
    #     Global_vars.data_queue[ex].append(Global_vars.csv_columns[ex])

    myExecutor = futures.ThreadPoolExecutor(len(exchange_names))
    myExecutor.map(Global_vars.Drive_uploader.appendSheetsRows, exchange_names)
    myExecutor.shutdown(wait=True)

    Global_vars.Drive_thread.join()
    print("All threads shut down, finally shutting down")


signal.signal(signal.SIGTERM, receive_signal)
classes = []
classes.append(Binance)
classes.append(Bitstamp)
classes.append(Bittrex)
# # websocket._exceptions.WebSocketConnectionClosedException: Connection is already closed.
# # 2020-07-22T14:31:21.712825+00:00 app[worker.1]: 2020-07-22T14:31:21Z <Greenlet at 0x7f0558aefb48: wrapped_listener> failed with WebSocketConnectionClosedException
# # 2020-07-22T14:31:21.712825+00:00 app[worker.1]:

classes.append(Cex)
classes.append(Coinbase)
classes.append(Gemini)  # no eur pairs
classes.append(Hitbtc)  # no eur pairs
classes.append(Kraken)
classes.append(Poloniex)  # no eur pairs
myClasses = {}
exchange_names = []
# def memoryusage():
#     global activethreads
#     while True:
#         lstr = ""
#         for t in activethreads:
#             lstr+= t.name+" "+str(asizeof.asizeof(t))+", "
#         print(lstr)
#         time.sleep(30)


Global_vars.Drive_uploader = Drive_upload()  # in Global_vars


def main():
    gc.set_threshold(200, 10, 10)
    global Memory_profilex
    for c in classes:
        v = c()
        Global_vars.myClasses[c.__name__] = v
        exchange_names.append(c.__name__)
    time.sleep(5)
    Global_vars.Drive_uploader.init_after()
    Global_vars.Drive_thread = threading.Thread(
        target=Global_vars.Drive_uploader.upload_thread, args=("",))
    # keepalive - is only nondaemonic thread, doesnt need to be but need for receiving signal
    Global_vars.Drive_thread.start()

    # time.sleep(2)
    # receive_signal("signum", "stack")
    """   global activethreads
    for x in classes:
        def f(arg):  # ssl and gevent fix class needabe in same thread as start_threads()
            c = x()
            c.main_process(arg)

        t = threading.Thread(target=f, args=("a",))
        t.start()
        activethreads.append(t) """

    # process = psutil.Process(os.getpid())
    # while True:

    #     print("Total "+str(process.memory_info().rss/(1000000))+" mb")
    #     Memory_profiler.display_top("main",limit=25)
    #     time.sleep(600)


# 9h-12h : 23% ~50mb -----

if __name__ == '__main__':
    main()
# "code": 429,
# 2020-08-10T18:42:47.245795+00:00 app[worker.1]: "message": "Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds' of service 'sheets.googleapis.com' for consumer 'project_number:509311696533'

'''
git add . && git commit -m "m" && git push heroku master && heroku ps:scale worker=1 && heroku logs --tail
'''
'''
heroku logs -t
'''
'''
heroku logs -n 1500 

heroku labs:disable log-runtime-metrics
heroku labs:enable log-runtime-metrics


python -m nuitka --mingw64 --standalone --plugin-enable=pylint-warnings --plugin-enable=gevent main.py 
'''
