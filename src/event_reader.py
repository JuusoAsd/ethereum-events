from util import get_util
from pprint import pprint
import json
import sys
from websockets import connect
import asyncio


class EventReader:
    def __init__(self, w3):
        self.w3 = w3
        self.loop = asyncio.get_event_loop()


    async def get_events(self, contract, topic, callback):
        async with connect(get_util.ws_url()) as ws:
            await ws.send(
                json.dumps(
                    {
                        "id": 1,
                        "method": "eth_subscribe",
                        "params": [
                            "logs",
                            {
                                "address": contract.address,
                                "topics":[topic,]
                            }
                            
                        ],
                    }
                )
            )
            subscription_response = await ws.recv()
            print(subscription_response)

            while True:
                message = await asyncio.wait_for(ws.recv(), timeout=600)
                callback(message, contract, self.w3)
 
    def track_events(self, contract, topic, callback):
        while True:
            self.loop.run_until_complete(self.get_events(contract, topic, callback))