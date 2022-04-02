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
    address = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
    abi = get_util.abi("erc20.json")
    contract = w3.eth.contract(abi=abi, address=w3.toChecksumAddress(address))

    topic = w3.keccak(text="Transfer(address,address,uint256)").hex()

    reader.track_events(contract, topic, manage_transfer_event)


if __name__ == "__main__":
    main()
