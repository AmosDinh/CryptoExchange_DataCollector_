import asyncio
import websockets


class MyAsyncioWebsocket():
    def __init__(self, connection, on_open, on_message, exchangename=""):
        self.connection = connection
        self.keep_running = True
        self.on_open = on_open
        self.on_message = on_message
        self.exchangename = exchangename

    async def mySocket(self):
        try:
            async with websockets.connect(self.connection, ssl=True) as websocket:

                await self.on_open(websocket)
                print(self.connection)
                while self.keep_running:
                    await self.on_message(websocket, await websocket.recv())

                await websocket.close()
                return
        except BaseException as err:
            print(self.exchangename, "error - restarting", err)
            await self.mySocket()

    def run_ws(self):
        asyncio.new_event_loop().run_until_complete(self.mySocket())
