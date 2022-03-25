from web3 import Web3
from utils import create_contract, read_etherscan
from historical_events import fetch_events
from util import get_util


uniswap_factory = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
factory_abi_url = "https://unpkg.com/@uniswap/v2-core@1.0.1/build/UniswapV2Factory.json"
erc20_abi_url = "https://unpkg.com/@uniswap/v2-core@1.0.1/build/IERC20.json"


def parse_event(event):
    print(f'Found pair {event["args"]["token0"]}-{event["args"]["token1"]}')


def run():

    uniswap_factory = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
    factory_abi_url = (
        "https://unpkg.com/@uniswap/v2-core@1.0.1/build/UniswapV2Factory.json"
    )
    erc20_abi_url = "https://unpkg.com/@uniswap/v2-core@1.0.1/build/IERC20.json"

    infura = get_util.http_url()
    web3 = Web3(Web3.HTTPProvider(infura))

    factory = create_contract(web3, factory_abi_url, uniswap_factory)
    event = factory.events.PairCreated
    current = web3.eth.block_number
    event_list = fetch_events(event, from_block=0, to_block=current, interval=100000)
    print(event_list)


def run_rari_directory():
    infura = get_util.http_url()
    web3 = Web3(Web3.HTTPProvider(infura))

    directory = "0x835482FE0532f169024d5E9410199369aAD5C77E"
    abi_address = "0xd662efb05e8cafe35d1558b8b5323c73e2919abd"

    abi = read_etherscan(abi_address)
    rari_directory = create_contract(web3, abi, directory)

    current = web3.eth.block_number
    event = rari_directory.events.PoolRegistered
    event_list = fetch_events(event, from_block=0, to_block=current, interval=1000000)

    print(*event_list, sep="\n")


def run_rari_pool():
    infura = get_util.http_url()
    web3 = Web3(Web3.HTTPProvider(infura))

    pool_address = "0xc54172e34046c1653d1920d40333Dd358c7a1aF4"
    abi_implementation_address = "0xe16db319d9da7ce40b666dd2e365a4b8b3c18217"

    abi = read_etherscan(abi_implementation_address)
    pool_contract = create_contract(web3, abi, pool_address)

    current = web3.eth.block_number
    print(*pool_contract.events, sep="\n")
    event = pool_contract.events.MarketListed
    event_list = fetch_events(event, from_block=0, to_block=current, interval=1000000)

    print(*event_list, sep="\n")


if __name__ == "__main__":

    run_rari_pool()
