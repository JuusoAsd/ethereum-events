from web3 import Web3
import json
import websocket

from multiprocessing import Pool
import threading

from web3._utils.filters import construct_event_filter_params
from web3._utils.events import get_event_data

import os
import json

from web3 import Web3
import requests
from dotenv import dotenv_values
import time

ETHERSCAN = "https://api.etherscan.io/api"


class track_ws():

    def __init__(
        self, 
        web3=None,
        address=None, 
        event=None,
        abi=None,
        contract=None,
        topic=None,
        url=None
        ):

        self.web3 = web3
        self.address = self.web3.toChecksumAddress(address)
        self.event=event,
        self.abi = abi
        self.contract=contract,
        self.topic=topic
        self.event=event
        self.url = url
        

    def _on_open(self, ws):
        ws.send(
            json.dumps(
                    {
                        "id": 1,
                        "method": "eth_subscribe",
                        "params": [
                            "logs",
                            {
                                "address": self.address,
                                "topics": [
                                    self.topic,
                                ],
                            },
                        ],
                    }
                )
            )

    def callback(self, message,contract, w3):

        tx_hash = json.loads(message)["params"]["result"]["transactionHash"]
        print(tx_hash)

    def read(self, ws, msg):
        print(msg)

    
        
    def read_history(
        self,
        event=None,
        argument_filters=None,
        from_block=None,
        to_block=None,
        address=None,
        topics=None,
        interval=None
        ):

        if from_block is None:
            raise TypeError("Missing mandatory keyword argument to getLogs: from_Block")

        if to_block is None:
            raise TypeError("Missing mandatory keyword argument to getLogs: to_Block")
        
        if event is None:
            raise TypeError("Missing mandatory keyword argument to getLogs: event")

        abi = event._get_event_abi()
        abi_codec = event.web3.codec

        argument_filters = dict()
        _filters = dict(**argument_filters)

        intervals = []
        current_block = from_block
        while current_block < to_block:
            intervals.append(
                (
                    current_block, 
                    min(current_block + interval, to_block)
                )
            )
            current_block += interval
        
        print(abi,type(abi))
        print(abi_codec,type(abi_codec))
        print(event.address,type(event.address))
        print(_filters,type(_filters))
        print(from_block,type(from_block))
        print(to_block,type(to_block))
        print(address,type(address))
        print(topics,type(topics))

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
        interval_logs = event.web3.eth.getLogs(event_filter_params)
        print(interval_logs)

        pass


    def test_uniswap(self):


        
        ws = websocket.WebSocketApp(
                self.url,
                on_open=self._on_open,
                on_message=self.read,
            )

            # this is likely to be incorrect
        wst = threading.Thread(target=ws.run_forever, kwargs={"ping_interval": 25})
        current = self.web3.eth.block_number
        history_thread = threading.Thread(target=self.read_history, kwargs={
            'event':self.event,
            'from_block':10000000,
            'to_block':current,
            'address':self.address,
            'interval':100000
        })
        
        #wst.start()
        history_thread.start()
        #reader.track_events(contract, topic, callback)











def http_w3():
    conf = config()
    return Web3(Web3.HTTPProvider(http_url()))


def ws_w3():
    conf = config()
    return Web3(Web3.WebsocketProvider(ws_url()))


def http_url():
    conf = config()
    return f"https://mainnet.infura.io/v3/{conf['infura']}"


def ws_url():
    conf = config()
    return f"wss://mainnet.infura.io/ws/v3/{conf['infura']}"


def config():
    return dotenv_values(".env")


def read_etherscan(name, address):
    # lag to not exceed rate limit by accident
    time.sleep(0.3)
    conf = config()
    json_path = os.path.join(os.getcwd(), "src", "util", "abi", f"{name}.json")
    params = {
        "module": "contract",
        "action": "getabi",
        "address": address,
        "apikey": conf["etherscan"],
    }
    r = requests.get(ETHERSCAN, params=params)
    abi = r.json()["result"]
    with open(json_path, "w") as f:
        f.write(abi)
    return abi


def abi(name, address=None):
    json_path = os.path.join(os.getcwd(), "old2", "util", "abi", f"{name}.json")

    try:
        with open(json_path) as f:
            abi = json.load(f)
        return abi
    # try getting abi file but use etherscan if fails
    except:
        abi = read_etherscan(name, address)
        return abi
        
def create_topic(web3, abi, eventname):
    print(eventname)
    topic_string=f"{eventname}("
    for i in abi:
        
        if 'name' in i and 'type' in i:
            
            if i['name'] == eventname and i['type'] == 'event':
                for j in i['inputs']:
                    topic_string += f"{j['internalType']},"
    topic_string = f"{topic_string[:-1]})"
    print(topic_string)
    return web3.keccak(text=topic_string).hex()




if __name__ == "__main__":

    infura = http_url()
    web3 = Web3(Web3.HTTPProvider(infura))
    address = web3.toChecksumAddress("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852")
    contract_abi = abi(address)
    contract = web3.eth.contract(abi=contract_abi, address=address)
    topic = create_topic(web3, contract_abi, 'Swap')
    event = contract.events.Swap
    url = ws_url()

    
    ws = track_ws(
        web3=web3, 
        address=address,
        abi=abi,
        contract=contract,
        topic=topic,
        event=event,
        url=url
        )
    ws.test_uniswap()

    #test_pool()
    
    









