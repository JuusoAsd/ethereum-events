import requests
import os

from web3 import Web3
from web3._utils.filters import construct_event_filter_params
from dotenv import dotenv_values
import time

ETHERSCAN = "https://api.etherscan.io/api"

_cache = dict()


def get_cached_abi(abi_url):
    """Per process over-the-network ABI file retriever"""
    spec = _cache.get(abi_url)
    if not spec:
        spec = _cache[abi_url] = requests.get(abi_url).json()
    return spec


"""def create_contract(web3, abi_url, address):
    spec = get_cached_abi(abi_url)
    contract = web3.eth.contract(address, abi=spec['abi'])
    return contract"""


def create_contract(web3, abi, address):
    return web3.eth.contract(address=address, abi=abi)


def read_etherscan(address, name=None):
    # lag to not exceed rate limit by accident
    time.sleep(0.3)
    conf = config()

    # save as address if not found
    if not name:
        name = address
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


def config():
    return dotenv_values(".env")

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

def _update_filter_interval(event, event_filter, from_block=None, to_block=None):
    """Helper function to change interval"""
    event_filter["fromBlock"] = int(from_block)
    event_filter["toBlock"] = int(to_block)
    return event_filter

def read_event_interval(event, event_filter, block_interval):

    from_block, to_block = block_interval

    logs = []
    current_block = from_block
    event_interval = to_block - from_block
    adjustment = 1

    while current_block < to_block:
            try:
                event_filter_params = _update_filter_interval(
                    event_filter_params, 
                    from_block=current_block, 
                    to_block=min(current_block + interval / adjustment, to_block)
                )
                interval_logs = event.web3.eth.getLogs(event_filter_params)
                logs += interval_logs
                current_block = event_filter_params['toBlock']
                adjustment = 1
                interval = to_block - current_block
                print(current_block)
            except ValueError:
                adjustment *= 2
                print('Decreasing interval')

    return logs