from audioop import add
import json
import asyncio
from websockets import connect
from utils import get_web3, read_abi, create_topic_string, get_ws_endpoint
import websocket


class LiveEventTracker:
    def __init__(self, address: str, abi: dict, event_name: str):
        self.address = address
        self.abi = abi
        self.w3 = get_web3("ws")
        self.topic = self.w3.keccak(text=create_topic_string(abi, event_name)).hex()

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
        pass

    def _on_error(self, ws, e):
        print(e)
        pass

    def _on_message(self, ws, msg):
        print(msg)

    def track_events(self):

        ws = websocket.WebSocketApp(
            get_ws_endpoint(),
            on_open=self._on_open,
            on_close=self._on_close,
            on_error=self._on_error,
            on_message=self._on_message,
        )

        ws.run_forever()


def main():
    w3 = get_web3("ws")
    dai_address = w3.toChecksumAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F")
    dai_abi = read_abi("erc20", address=dai_address)
    tracker = LiveEventTracker(dai_address, dai_abi, "Transfer")
    tracker.track_events()


if __name__ == "__main__":
    main()
