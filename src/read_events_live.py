import json

from web3.types import LogReceipt
import websocket

from utils import (
    get_web3,
    read_abi,
    create_topic_string,
    get_ws_endpoint,
    convert_ws_response,
)


class LiveEventTracker:
    def __init__(self, address: str, abi: dict, event_name: str):
        self.address = address
        self.abi = abi
        self.w3 = get_web3("ws")
        self.topic = self.w3.keccak(text=create_topic_string(abi, event_name)).hex()

        self.contract = self.w3.eth.contract(address=self.address, abi=self.abi)
        event = self.contract.events.__getitem__(event_name)
        self.event_abi = event._get_event_abi()
        self.abi_codec = event.web3.codec

        self.init_ws()

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

    def _on_close(self, ws):
        print("close")

    def _on_error(self, ws, e):
        print(e)
        pass

    def _on_message(self, ws, msg):
        data = json.loads(msg)
        if "params" in data:
            event_info = data["params"]["result"]
            converted_response = convert_ws_response(event_info)

        else:
            print(data)

    def init_ws(self):

        self.ws = websocket.WebSocketApp(
            get_ws_endpoint(),
            on_open=self._on_open,
            on_close=self._on_close,
            on_error=self._on_error,
            on_message=self._on_message,
        )


def main():
    w3 = get_web3("ws")
    dai_address = w3.toChecksumAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F")
    dai_abi = read_abi("erc20", address=dai_address)
    tracker = LiveEventTracker(dai_address, dai_abi, "Transfer")
    tracker.ws.run_forever()


if __name__ == "__main__":
    main()
