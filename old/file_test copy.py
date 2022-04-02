from web3 import Web3
from event_reader import EventReader
from utils import create_contract, read_etherscan
from old.historical_events import fetch_events
from utils import get_util
import json

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

def run_rari_market():
    infura = get_util.http_url()
    web3 = Web3(Web3.HTTPProvider(infura))

    market_address = "0xd8553552f8868C1Ef160eEdf031cF0BCf9686945"
    abi_implementation_address = "0x67db14e73c2dce786b5bbbfa4d010deab4bbfcf9"

    abi = read_etherscan(abi_implementation_address)
    market_contract = create_contract(web3, abi, market_address)

    print(*market_contract.events, sep="\n")

    event = market_contract.events.AccrueInterest
    current = web3.eth.block_number
    event_list = fetch_events(event, from_block=0, to_block=current, interval=1000000)
    print(*event_list, sep="\n\n")
    """interestRateModel.getSupplyRate(
        getCashPrior(), 
        totalBorrows, 
        add_(totalReserves, add_(totalAdminFees, totalFuseFees)), 
        reserveFactorMantissa + fuseFeeMantissa + adminFeeMantissa
        );"""

def setup_event_following(reader, contract, event, callback):
    reader.track_events(contract, event, callback)
    previous_events = fetch_events(event, from_block=0, to_block=current, interval=1000000)

def main():
    
    infura = get_util.http_url()
    web3 = Web3(Web3.HTTPProvider(infura))

    # setup the event listener
    reader = EventReader(web3)
    
    # Setup for fuse directory
    fuse_directory = "0x835482FE0532f169024d5E9410199369aAD5C77E"
    fuse_abi_address = "0xd662efb05e8cafe35d1558b8b5323c73e2919abd"
    fuse_abi = read_etherscan(fuse_abi_address)
    directory_contract = create_contract(web3, fuse_abi, fuse_directory)
    fuse_pool_event = {
        'event':directory_contract.events.PoolRegistered,
        'topic':web3.keccak(text="Transfer(address,address,uint256)").hex()

    }
    

    # Setup for pools
    abi_implementation_address = "0xe16db319d9da7ce40b666dd2e365a4b8b3c18217"
    pool_abi = read_etherscan(abi_implementation_address)
    pool_contract_init = web3.eth.contract(abi=pool_abi)
    pool_market_event = pool_contract.events.MarketListed

    # setup for markets
    market_abi_address = "0x67db14e73c2dce786b5bbbfa4d010deab4bbfcf9"
    market_abi = read_etherscan(market_abi_address)
    market_contract_init = web3.eth.contract(abi=market_abi)
    market_interest_event = market_contract_init.events.AccrueInterest
    

    directory_events = setup_event_following(reader, directory_contract, fuse_pool_event, callback_directory)
    for ev in directory_events:
        pool_contract = pool_contract_init(address=ev['address'])
        pool_events = setup_event_following(reader, pool_contract, pool_market_event, callback_pool)

    for ev in pool_events:
        market_contract = market_contract_init(address = ev['address'])
        market_events = setup_event_following(reader, market_contract, market_interest_event, callback_market)



    # one function that follows all new events
    # new contracts and topics should be added as new arguments to the "follower"

    # one function that gets events till current block
    
    # 3 functions to parse events
    # 1 for directory
    # 1 for pool
    # 1 for market

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

def callback(message,contract, w3):

    tx_hash = json.loads(message)["params"]["result"]["transactionHash"]
    print(tx_hash)

def test_uniswap():
    infura = get_util.http_url()
    web3 = Web3(Web3.HTTPProvider(infura))
    usdt_eth = "0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852"
    abi = get_util.abi(usdt_eth)
    topic = get_util.create_topic(web3, abi, 'Swap')
    reader = EventReader(web3)

    contract = web3.eth.contract(abi=abi, address= web3.toChecksumAddress(usdt_eth))
    event = contract.events.Swap
    
    #reader.track_events(contract, topic, callback)
    current = web3.eth.block_number
    event_list = fetch_events(event, from_block=14400000, to_block=current, interval=50000)
    print(event_list)


    
if __name__ == "__main__":

    #run_rari_market()
    #test_abi(abi, 'Transfer')
    #test_abi(abi_2, 'PoolRegistered')
    test_uniswap()


"""{"anonymous":false,"inputs":
[{"indexed":false,"internalType":"uint256","name":"index","type":"uint256"},
{"components":
[{"internalType":"string","name":"name","type":"string"},
{"internalType":"address","name":"creator","type":"address"},
{"internalType":"address","name":"comptroller","type":"address"},
{"internalType":"uint256","name":"blockPosted","type":"uint256"},
{"internalType":"uint256","name":"timestampPosted","type":"uint256"}]
,"indexed":false,"internalType":"struct FusePoolDirectory.FusePool","name":"pool","type":"tuple"}]
,"name":"PoolRegistered","type":"event"}

{"anonymous":false,"inputs":
[{"indexed":true,"internalType":"address","name":"src","type":"address"},
{"indexed":true,"internalType":"address","name":"dst","type":"address"},
{"indexed":false,"internalType":"uint256","name":"wad","type":"uint256"}]
,"name":"Transfer","type":"event"}"""