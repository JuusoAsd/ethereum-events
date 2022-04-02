from web3 import Web3
from event_reader import EventReader
from utils import get_util
import json
import websocket

import threading

def callback(message,contract, w3):

    tx_hash = json.loads(message)["params"]["result"]["transactionHash"]
    print(tx_hash)

def test_uniswap():
    infura = get_util.http_url()
    web3 = Web3(Web3.HTTPProvider(infura))
    reader = EventReader(web3)

    usdt_eth = "0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852"
    abi = get_util.abi(usdt_eth)
    topic = get_util.create_topic(web3, abi, 'Swap')
    
    contract = web3.eth.contract(abi=abi, address= web3.toChecksumAddress(usdt_eth))
    event = contract.events.Swap
    url = get_util.ws_url()

            await 
            subscription_response = await ws.recv()
            print(subscription_response)

            while True:
                message = await asyncio.wait_for(ws.recv(), timeout=600)
                callback(message, contract, self.w3)



    # make this run in a separate thread and save values so everything can be processed from current block
    ws = websocket.WebSocketApp(
            
            on_open=on_open,
            on_message=read,
        )

        # this is likely to be incorrect
    wst = threading.Thread(target=ws.run_forever, kwargs={"ping_interval": 25})

    #reader.track_events(contract, topic, callback)
    
    """current = web3.eth.block_number
    # make this return events in same format as above
    #event_list = reader.fetch_events(event, from_block=14400000, to_block=current, interval=50000)
    
    live_event_thread = threading.Thread(target=reader.get_events_ws, args=(contract, topic, callback))
    live_event_thread.start()

    historical_event_thread = threading.Thread(
        target=reader.fetch_historical_events, 
        args=(event,),
        kwargs={
            'from_block':14400000, 
            'to_block':current, 
            'interval':50000
            }
        )
    historical_event_thread.start()
    """
    
if __name__ == "__main__":

    test_uniswap()

