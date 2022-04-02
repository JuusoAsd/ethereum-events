from event_reader import EventReader
from utils import get_util
import asyncio
import json
from web3.logs import STRICT, IGNORE, DISCARD, WARN


def manage_transfer_event(message, contract, w3):
    tx_hash = json.loads(message)["params"]["result"]["transactionHash"]
    tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
    tx_info = contract.events.Transfer().processReceipt(tx_receipt, errors=DISCARD)[0]
    print(tx_hash)
    print(tx_info["args"]["wad"] / (10**18))
    print()


def main():
    w3 = get_util.ws_w3()
    reader = EventReader(w3)
    fuse = {
        "address": "0x835482FE0532f169024d5E9410199369aAD5C77E",
        "implementation": "0xD662eFB05E8CAFe35d1558b8b5323c73e2919aBD",
    }

    fuse_abi = get_util.abi("rari_fuse_directory", fuse["implementation"])
    fuse_contract = w3.eth.contract(
        abi=fuse_abi, address=w3.toChecksumAddress(fuse["address"])
    )
    pools = fuse_contract.functions.getAllPools().call()

    comptroller_address = pools[0][2]
    comptroller_proxy_abi = get_util.abi("rari_comptroller_proxy", comptroller_address)

    comptroller_proxy_contract = w3.eth.contract(
        abi=comptroller_proxy_abi, address=w3.toChecksumAddress(comptroller_address)
    )

    comptroller_implementation_address = (
        comptroller_proxy_contract.functions.comptrollerImplementation().call()
    )
    comptroller_abi = get_util.abi(
        "rari_comptroller", comptroller_implementation_address
    )

    for pool in pools:
        comptroller = pool[2]
        pool_contract = w3.eth.contract(
            abi=comptroller_abi, address=w3.toChecksumAddress(comptroller)
        )
        markets = pool_contract.functions.getAllMarkets().call()
        print(comptroller, markets)

    print(*pools, sep="\n")


if __name__ == "__main__":
    main()
