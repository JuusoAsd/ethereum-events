import os
import json

from web3 import Web3
import requests
from dotenv import dotenv_values
import time

ETHERSCAN = "https://api.etherscan.io/api"


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
    json_path = os.path.join(os.getcwd(), "src", "util", "abi", f"{name}.json")

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
    a = w3()
    print(a)
