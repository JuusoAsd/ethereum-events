from old2.utils import get_util
from pprint import pprint
import json
import sys
from websockets import connect
import asyncio

from web3._utils.filters import construct_event_filter_params
from web3._utils.events import get_event_data

class EventReader:
    def __init__(self, w3):
        self.w3 = w3
        self.loop = asyncio.get_event_loop()

    def update_filter(self, filter, from_block=None, to_block=None):
        """Helper function to change interval"""
        filter["fromBlock"] = int(from_block)
        filter["toBlock"] = int(to_block)
        return filter

    async def get_events_async(
        self, 
        contract, 
        topic, 
        callback
    ):
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
                                "topics": [
                                    topic,
                                ],
                            },
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
            self.loop.run_until_complete(self.get_events_ws(contract, topic, callback))

    def fetch_historical_events(
        self,
        event,
        argument_filters=None,
        from_block=None,
        to_block=None,
        address=None,
        topics=None,
        interval=None,
    ):
        """Get events using eth_getLogs API.

        This is a stateless method, as opposite to createFilter and works with
        stateless nodes like QuikNode and Infura.

        :param event: Event instance from your contract.events
        :param argument_filters:
        :param from_block: Start block. Use 0 for all history/
        :param to_block: Fetch events until this contract
        :param address:
        :param topics:
        :param interval: how many blocks per request, should contain less than 10k events
        :return:
        """

        if from_block is None:
            raise TypeError("Missing mandatory keyword argument to getLogs: from_Block")

        if to_block is None:
            raise TypeError("Missing mandatory keyword argument to getLogs: to_Block")

        abi = event._get_event_abi()
        abi_codec = event.web3.codec

        # Set up any indexed event filters if needed
        argument_filters = dict()
        _filters = dict(**argument_filters)

        data_filter_set, event_filter_params = construct_event_filter_params(
            abi,
            abi_codec,
            contract_address=event.address,
            argument_filters=_filters,
            fromBlock=from_block,
            toBlock=to_block,
            address=address,
            topics=topics,
        )

        logs = []

        current_block = from_block
        adjustment = 1
        while current_block < to_block:
            try:
                event_filter_params = self.update_filter(
                    event_filter_params, 
                    from_block=current_block, 
                    to_block=current_block + interval / adjustment
                )
                interval_logs = event.web3.eth.getLogs(event_filter_params)
                logs += interval_logs
                current_block += interval / adjustment
                adjustment = 1
                print(current_block)
            except ValueError:
                adjustment *= 2
                print('Decreasing interval')
            
        # Convert raw binary event data to easily manipulable Python objects
        event_data = []
        for entry in logs:
            data = get_event_data(abi_codec, abi, entry)
            event_data.append(data)
        return event_data

