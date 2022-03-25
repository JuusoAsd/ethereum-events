import requests
import os

from web3 import Web3
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
