import os
import json

from web3 import Web3
from dotenv import dotenv_values


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


def abi(filename):
    json_path = os.path.join(os.getcwd(), "src", "util", "abi", filename)
    with open(json_path) as f:
        abi = json.load(f)
    return abi


if __name__ == "__main__":
    a = w3()
    print(a)
