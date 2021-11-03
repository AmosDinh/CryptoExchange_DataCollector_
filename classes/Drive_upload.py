import json
import requests
import io
# from googleapiclient.http import MediaIoBaseUpload
# from googleapiclient.discovery import build
import datetime
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
from classes.Memory_profile import Memory_profile
import threading
import psutil
import os
import time
import gc
from classes.Global_vars import Global_vars
from pympler import asizeof
import socket
msglock = threading.Semaphore(value=1)
# from memory_profiler import profile
# from pympler import asizeof
# import gc

# import weakref
# import socket
# socket.setdefaulttimeout(5)


class Drive_upload():

    def __init__(self,):
        self.root_folder = "1crwjQA0Zx4tB4h0tPuRfIP4a8YyVj8el"
        self.service_account_email = "crypto-wscollector@crypto-wscollector.iam.gserviceaccount.com"
        self.mimeType = "application/vnd.google-apps.spreadsheet"
        self.mimeType_extention = ".csv"
        self.buildCredentials(refresh=False)
        self.folders = {}
        self.files = {}
        self.date = ""
        self.beforemidnight = self.get_beforemidnight()
        self.newday_exchanges = []

        self.Memory_profiler = Memory_profile()
        gc.set_threshold(200, 10, 10)

    def init_after(self):
        self.filenrs = {}  # tracks current file number for: err cell limit 5000000-> new spreadsheet
        for ex in list(Global_vars.myClasses):
            self.filenrs[ex] = 0

    def buildCredentials(self, refresh):
        #         import google.auth.transport.requests
        # import requests

        # request = google.auth.transport.requests.Request()
        # credentials.refresh(request)
        if refresh:
            self.credentials.refresh(httplib2.Http())
        else:
            self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
                "json/crypto-wscollector-1d1fc4f5c9ad.json",
                scopes=[
                    'https://www.googleapis.com/auth/drive',
                    'https://www.googleapis.com/auth/spreadsheets',

                ])
        if self.credentials is None or self.credentials.invalid:
            self.buildCredentials(refresh=False)
            return
        delegated_credentials = self.credentials.create_delegated(
            [self.service_account_email])
        access_token = delegated_credentials.get_access_token().access_token
        self.headers = {
            "Authorization": "Bearer "+access_token,
        }
        # self.drive = build('drive', 'v3', credentials=self.credentials)
        # self.sheet = build('sheets', 'v4', credentials=self.credentials)

    # def list_files(self, folder_id):
    #     self.drive = build('drive', 'v3', credentials=self.credentials)
    #     query_fields = "title,size,trashed,id,mimeType,parents"
    #     return self.drive.files().list(q='"'+folder_id+'" in parents', ).execute()

    def get_beforemidnight(self):
        return Global_vars.get_datetime().replace(hour=23, minute=59, second=50, microsecond=0)

    def get_rfc3339_date(self):
        return Global_vars.get_datetime().replace(hour=0, minute=0, second=0, microsecond=0).isoformat("T") + "Z"

    def get_folder(self, exchange):
        if exchange not in self.folders:
            # files = self.drive.files().list(q="'"+self.root_folder +
            #                                "' in parents and name='"+exchange+"' and mimeType='application/vnd.google-apps.folder'").execute()['files']
            files = self.driveList({'q': "'"+self.root_folder + "' in parents and name='" +
                                    exchange+"' and mimeType='application/vnd.google-apps.folder'"})['files']
            if len(files) == 0:
                file_metadata = {
                    'name': exchange,
                    'mimeType': "application/vnd.google-apps.folder",
                    'parents': [self.root_folder],
                }
                self.folders[exchange] = self.driveCreate(
                    file_metadata=file_metadata)['id']
#                self.folders[exchange] = self.drive.files().create(body=file_metadata, fields='id').execute()['id']

            else:
                self.folders[exchange] = files[-1]['id']

        return self.folders[exchange]

    def driveCreate(self, file_metadata, secondcycle=False):
        url = "https://www.googleapis.com/drive/v3/files"
        data = json.dumps(file_metadata)
        headers = self.headers
        headers['Content-Type'] = 'application/json'
        r = requests.post(url, headers=headers, data=data)
        x = r.status_code
        if x > 399:
            print("driveCreate", r)  # ,r.headers,r.text),r.headers,r.text)
            if x == 401 and not secondcycle:  # invalid creds
                self.buildCredentials(refresh=True)
                return self.driveCreate(file_metadata, secondcycle=True)
        return json.loads(r.content)

    def driveList(self, params, secondcycle=False):
        url = "https://www.googleapis.com/drive/v3/files"
        r = requests.get(url, headers=self.headers, params=params)
        x = r.status_code
        if x > 399:
            print("driveList", r)  # ,r.headers,r.text),r.headers,r.text)
            if x == 401 and not secondcycle:  # invalid creds
                self.buildCredentials(refresh=True)
                return self.driveList(params, secondcycle=True)
        return json.loads(r.content)

    def spreadsheetBatchUpdate(self, spreadsheetId, exchange, secondcycle=False):
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheetId}:batchUpdate"
        req = [{
            "deleteDimension": {
                "range": {
                    "sheetId": "0",  # first sheet is "0"
                    "dimension": "COLUMNS",
                    "startIndex": len(Global_vars.csv_columns[exchange]),
                    "endIndex": 26
                }
            }
        }]
        data = json.dumps({
            "requests": req,
            "includeSpreadsheetInResponse": False,
            "responseIncludeGridData": False
        })
        r = requests.post(url, headers=self.headers, data=data)
        x = r.status_code
        if x > 399:
            # ,r.headers,r.text),r.headers,r.text)
            print("spreadsheetBatchUpdate", r)
            if x == 401 and not secondcycle:  # invalid creds
                self.buildCredentials(refresh=True)
                return self.spreadsheetGetId(spreadsheetId, secondcycle=True)

    def spreadsheetGetId(self, spreadsheetId, secondcycle=False):
        # is mostly 0
        return 0
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheetId}"
        r = requests.get(url, headers=self.headers, params={
                         "fields": "sheets.properties"})
        x = r.status_code
        if x > 399:
            # ,r.headers,r.text),r.headers,r.text)
            print("spreadsheetGetId", r)
            if x == 401 and not secondcycle:  # invalid creds
                self.buildCredentials(refresh=True)
                return self.spreadsheetGetId(spreadsheetId, secondcycle=True)

        return json.loads(r.text)["sheets"][0]['properties']['sheetId']

    def spreadsheetAppend(self, spreadsheetId, list2d, exchange, secondcycle=False):
        valueInputOption = "RAW"
        range = '!A:A'
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheetId}/values/{range}:append?valueInputOption={valueInputOption}"
        data = json.dumps({
            "range": range,
            "majorDimension": "ROWS",
            "values": list2d
        })

        # except ConnectionError as e:
        #     return "ERROR:" + "HTTP连接错误"
        # except ConnectTimeout as e:
        #     return "ERROR:" + "HTTP连接超时错误"
        # except Exception as e:
        #     return 'ERROR:' + str(e)
        # try:
        r = requests.post(url, headers=self.headers, data=data)
        # except requests.exceptions.ChunkedEncodingError as e:
        #     return '\r\n'.join(html).strip()

        #  It could be that you are affected by the following: The URL that you get from the file's metadata is short lived. If you are saving that URL to use later it won't work because it could be that the URL gets invalidated.

        # To do this you have to fetch the image metadata every time to get the new downloadURL.

        # We are working on providing non expirable URLs in the future.
        x = r.status_code
        if x > 399:
            print("spreadsheetAppend", r)  # ,r.headers,r.text)
            if x == 401 and not secondcycle:  # invalid creds
                self.buildCredentials(refresh=True)
                self.spreadsheetGetId(spreadsheetId, secondcycle=True)

            elif x == 400 and not secondcycle:  # workbook limit -> create new
                self.filenrs[exchange] = self.filenrs[exchange]+1
                existed, new_spreadsheetId = self.get_filetoday(
                    exchange, filenr=self.filenrs[exchange])
                return self.spreadsheetAppend(new_spreadsheetId, [Global_vars.csv_columns[exchange]]+list2d, exchange, secondcycle=True)

            elif x == 503:  # the service is currently unavailable
                return list2d
        return []

    def get_filetoday(self, exchange, newday=False, filenr=0):
        existed = True
        if exchange not in self.files or newday or filenr:
            files = self.driveList({'q': "'"+self.get_folder(exchange) + "' in parents and createdTime>='" +
                                    self.get_rfc3339_date()+"' and trashed = false", 'orderBy': "createdTime desc"})['files']
            if len(files) == 0 or filenr and int(files[0]['name'].split('_')[2].split('.')[0]) != filenr:
                if filenr and not len(files) == 0:
                    othernr = int(files[0]['name'].split('_')[2].split('.')[0])
                    if othernr > filenr:
                        self.filenrs[exchange] = othernr
                        filenr = othernr
                file_metadata = {
                    'name': exchange.lower()+"_" + str(Global_vars.get_datetime().date())+"_"+str(filenr)+self.mimeType_extention,
                    'mimeType': self.mimeType,
                    'parents': [self.get_folder(exchange)],
                    "convert": "true"  # to google spreadsheet xlsx -> unlimited drive capacity
                }
                spreadsheetId = self.driveCreate(
                    file_metadata=file_metadata)['id']

                self.spreadsheetBatchUpdate(spreadsheetId, exchange)

                existed = False
                self.files[exchange] = spreadsheetId

            else:
                self.files[exchange] = files[0]['id']

        return existed, self.files[exchange]

    # connection reset by peer-> redoappendsheetsrows
    def appendSheetsRows(self, exchange, data=None, lastwasbeforemidnight=False):
        list2d = []
        if data:
            list2d = data
        else:
            list2d = Global_vars.data_queue[exchange]
            Global_vars.data_queue[exchange] = []

        if self.credentials.access_token_expired:
            self.buildCredentials(refresh=True)

        if not self.date == "":
            if not self.date == Global_vars.get_datetime().date():
                self.date = Global_vars.get_datetime().date()
                self.newday_exchanges = list(Global_vars.myClasses)
        else:
            self.date = Global_vars.get_datetime().date()

        newday = False
        if exchange in self.newday_exchanges:
            newday = True
            self.newday_exchanges.remove(exchange)
            self.filenrs[exchange] = 0

        try:
            existed, spreadsheetId = self.get_filetoday(
                exchange, newday=newday, filenr=self.filenrs[exchange])
        except socket.timeout:
            existed, spreadsheetId = self.get_filetoday(
                exchange, newday=newday, filenr=self.filenrs[exchange])

        if not existed:
            list2d = [Global_vars.csv_columns[exchange]]+list2d
        try:
            list_503 = self.spreadsheetAppend(
                spreadsheetId, list2d, exchange)  # 503 service not available
            Global_vars.data_queue[exchange] = list_503 + \
                Global_vars.data_queue[exchange]

        # It could be that you are affected by the following: The URL that you get from the file's metadata is short lived
        except (ConnectionResetError, requests.exceptions.ChunkedEncodingError) as e:
            print(e)
            self.files = {}
            if not existed:
                list2d = list2d[1:]
            self.appendSheetsRows(exchange, data=list2d)

    def monitorMemory(self):

        sleepone = 2
        sleeptotal = 600  # seconds 10min
        limit = int(sleeptotal/sleepone)
        x = limit-20
        while self.monitorMemory_run:
            if x >= limit:
                x = 0
                msglock.acquire()
                try:
                    """ print(self.__class__.__name__)
                    self.tracker.print_diff() """
                    process = psutil.Process(os.getpid())
                    print(self.__class__.__name__+" threashhold "+str(gc.get_threshold()) +
                          " total "+str(process.memory_info().rss/(1025*1024))+" mb")  # in bytes

                    self.Memory_profiler.display_top(
                        exchange=self.__class__.__name__, limit=25)

                except BaseException as err:
                    print(self.__class__.__name__+" error "+str(err))
                msglock.release()
            x += 1
            time.sleep(sleepone)

    def upload_thread(self, arg):  # uploader
        # self.monitorMemory_run = True
        # t = threading.Thread(target=self.monitorMemory)
        # t.start() --------------
        x = 0
        sleepduration = 2
        # delta = datetime.timedelta(minutes=8)  # websocket inactive? restart
        # Heroku sigterm: 30 seconds for shutdown
        exchanges = list(Global_vars.data_queue)
        while getattr(threading.currentThread(), "do_run", True):
            time.sleep(sleepduration)
            # 8-10sec before midnight, change sleepduration so no more uploads beforemidnight
            if sleepduration == 11:
                sleepduration = 2

                self.beforemidnight = self.get_beforemidnight()

            elif Global_vars.get_datetime() > self.beforemidnight and sleepduration != 11:
                sleepduration = 11
                for ex in exchanges:
                    if not getattr(threading.currentThread(), "do_run", True):
                        break
                    self.appendSheetsRows(ex)
                print("The bell is ringing utc midnight")

            elif x >= 60:  # 120sec
                x = 0
                for ex in exchanges:
                    if not getattr(threading.currentThread(), "do_run", True):
                        break
                    self.appendSheetsRows(ex)

            x += 1
            continue

        self.monitorMemory_run = False

        print("drive shut down")
        # Global_vars.myClasses[ex].stop_ws_thread()#too slow -> now in main.py

# 2020-07-19T15:47:39.373Z
# query_fields = "modifiedByMeTime,createdTime,name,size,trashed,id,mimeType,parents"
