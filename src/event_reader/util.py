import os
import json
import time
from hexbytes import HexBytes

import requests
from dotenv import dotenv_values
from web3 import Web3

ETHERSCAN = "https://api.etherscan.io/api"


def read_abi(filename: str, address: str = False) -> dict:
    # try reading file if exists
    json_path = os.path.join(os.getcwd(), "src", "abi", f"{filename}.json")
    try:
        with open(json_path) as f:
            abi = json.load(f)

    # if doesn't and address is known, use that instead
    except:
        abi = get_abi_etherscan(address)
        with open(json_path, "w") as f:
            f.write(abi)

    return abi


def get_abi_etherscan(address):
    # lag to not exceed rate limit by accident
    time.sleep(0.3)
    conf = config()

    # save as address if not found
    params = {
        "module": "contract",
        "action": "getabi",
        "address": address,
        "apikey": conf["etherscan"],
    }
    r = requests.get(ETHERSCAN, params=params)
    abi = r.json()["result"]
    return abi


def config() -> dict:
    return dotenv_values(".env")


def get_web3(conn_type: str = "https") -> Web3:
    conf = config()
    if conn_type == "ws":
        return Web3(
            Web3.WebsocketProvider(f"wss://mainnet.infura.io/ws/v3/{conf['infura']}")
        )
    else:
        return Web3(Web3.HTTPProvider(f"https://mainnet.infura.io/v3/{conf['infura']}"))


def get_ws_endpoint() -> str:
    conf = config()
    return f"wss://mainnet.infura.io/ws/v3/{conf['infura']}"


def create_topic_string(abi: dict, eventname: str) -> str:
    """
    Parses ABI to create a "topic string" used as input in some web3 functions
    Not tested and likely fails with e.g. structs
    """

    topic_string = f"{eventname}("
    for i in abi:
        if "name" in i and "type" in i:
            if i["name"] == eventname and i["type"] == "event":
                for j in i["inputs"]:
                    topic_string += f"{j['internalType']},"
    topic_string = f"{topic_string[:-1]})"
    return topic_string


def convert_ws_response(res):
    """
    Tailor-made to convert the websocket response into correct format accepted by "get_event_data"
    Not tested and might fail in some scenarios
    """
    w3 = get_web3()
    return {
        "address": res["address"],
        "blockHash": HexBytes(res["blockHash"]),
        "blockNumber": w3.toInt(hexstr=res["blockNumber"]),
        "data": res["data"],
        "logIndex": w3.toInt(hexstr=res["logIndex"]),
        "removed": res["removed"],
        "topics": [HexBytes(item) for item in res["topics"]],
        "transactionHash": HexBytes(res["transactionHash"]),
        "transactionIndex": w3.toInt(hexstr=res["transactionIndex"]),
    }
