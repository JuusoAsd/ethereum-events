import json


from web3._utils.events import get_event_data
import websocket

from event_reader.util import (
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
        print("MSG received")
        if "params" in data:
            event_info = data["params"]["result"]
            converted_response = convert_ws_response(event_info)
            res = get_event_data(self.abi_codec, self.event_abi, converted_response)
            to_save = f"{res['blockNumber']},{res['transactionIndex']},{res['transactionHash']},{res['args']['reserve0']},{res['args']['reserve0']},{res['args']['reserve1']}\n"
            with open("testfile.txt", "+a") as f:
                f.write(to_save)
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
    # dai_address = w3.toChecksumAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F")
    # dai_abi = read_abi("erc20", address=dai_address)

    address = w3.toChecksumAddress("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc")
    abi = read_abi("uni_v2_pool", address=address)

    tracker = LiveEventTracker(address, abi, "Sync")
    tracker.ws.run_forever()


if __name__ == "__main__":
    main()
