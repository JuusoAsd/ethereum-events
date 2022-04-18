from functools import partial
from multiprocessing import Pool

from web3._utils.filters import construct_event_topic_set
from web3._utils.events import get_event_data

from src.event_reader.util import get_web3, read_abi
#from util import get_web3, read_abi

def create_interval_list(
    from_block: int, 
    to_block: int, 
    interval: int
    ) -> list:
    """
    Creates even intervals where last one is cut to match total blocks
    """
    intervals = []
    current_block = from_block
    while current_block < to_block:
        intervals.append(
            (int(current_block), int(min(current_block + interval, to_block)))
        )
        current_block += interval + 1

    return intervals

def _read_single_interval(
    event_filter_params:dict,
    interval:tuple
):
    w3 = get_web3()
    print('start thread')
    from_block, to_block = interval
    current_block=from_block
    chunksize = to_block - from_block
    adjustment = 1
    all_logs = []
    while current_block < to_block:
        try:
            print('go')
            event_filter_params['fromBlock'] = current_block
            event_filter_params['toBlock'] = min(
                to_block,
                current_block + chunksize/adjustment
            )
            logs = w3.eth.get_logs(event_filter_params)
            all_logs+=logs
            adjustment = 1
            current_block = event_filter_params['toBlock']+1

        except Exception as e:
            print(e)
            adjustment *= 2
    print('finish thread')
    return logs


def get_topics(info):
    w3 = get_web3()
    topic_dict = {}
    for i in info:
        contract = w3.eth.contract(abi=i['abi'])
        event = contract.events.__getitem__(i['event'])
        event_abi = event._get_event_abi()
        abi_codec = event.web3.codec
        if 'arguments' not in i:
            args = {}
        topic = construct_event_topic_set(
            event_abi=event_abi,
            abi_codec=abi_codec,
            arguments=args
        )
        topic_dict[topic[0]] = {
            'abi':event_abi,
            'codec':abi_codec
        }
    return topic_dict

def parse_logs(logs, topic_dict):
    parsed = []
    for log in logs:
        log_topic = log['topics'][0].hex()
        parse = get_event_data(
            abi_codec = topic_dict[log_topic]['codec'],
            event_abi = topic_dict[log_topic]['abi'],
            log_entry = log
            )
        parsed.append(parse)
    return parsed

def read_history(
    addresses:list,
    event_abi_argument:list(), # list of dictionaries containing 
    from_block:int,
    to_block:int,
    interval:int,
):

    topic_dict = get_topics(event_abi_argument)
    topics = list(topic_dict.keys())

    setup_dict = {
        'topics':[topics],
        'address':addresses
    }
    
    interval_list = create_interval_list(
        from_block=from_block,
        to_block=to_block,
        interval=interval
    )
    #print(setup_dict)
    with Pool(20) as p:
        log_lists = p.map(
            partial(_read_single_interval, setup_dict),
            interval_list,
        )
    
    logs = [item for sublog in log_lists for item in sublog]
    #print(logs)
    #print('\n'*5)
    return parse_logs(logs, topic_dict)


def test_simple():
    w3 = get_web3()
    current = w3.eth.block_number
    params = {
        'topics': [['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925']],
        'address': ['0x6B175474E89094C44Da98b954EedeAC495271d0F', '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'],
        'fromBlock':current-1000,
        'toBlock':current-500
    }
    l = w3.eth.get_logs(params)
    for i in l:
        print(i['topics'][0].hex())
        print(i['address'])
        print()
    # 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
    #'0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'

def test_simple_2():
    w3 = get_web3()
    current = w3.eth.block_number

    from_b = current-1000
    to_b = current - 500

    usdc = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    dai = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
    abi = read_abi('erc20', dai)

    transfer = [{'abi':abi,'event':'Transfer'}]
    approval = [{'abi':abi,'event':'Approval'}]

    """print('start')
    dai_transfer_logs = read_history(dai, transfer,from_b, to_b, 50)
    print('dai')
    dai_approval_logs = read_history(dai, approval,from_b, to_b, 50)
    print('dai')
    usdc_transfer_logs = read_history(usdc, transfer,from_b, to_b, 50)
    print('usdc')
    usdc_approval_logs = read_history(usdc, approval,from_b, to_b, 50)
    print('usdc')
    everything = read_history(
        [usdc, dai], 
        approval+transfer,
        from_b, 
        to_b, 
        50
        )

    print(f"DAI transfer: {len(dai_transfer_logs)}")
    print(f"DAI approval: {len(dai_approval_logs)}")
    print(f"USDC transfer: {len(usdc_transfer_logs)}")
    print(f"USDC approval: {len(usdc_approval_logs)}")
    total = len(usdc_approval_logs) + len(usdc_transfer_logs) +len(dai_approval_logs)+len(dai_transfer_logs)"""
    #print(f"total: {total} - everything: {len(everything)}")
    everything = read_history(
        [usdc, dai], 
        approval+transfer,
        from_b, 
        to_b, 
        250
        )
    print(len(everything))

if __name__ == '__main__':
    #test_simple()
    test_simple_2()
    exit()
    w3 = get_web3()
    # usdc , "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
    address = ["0x6B175474E89094C44Da98b954EedeAC495271d0F"]
    abi = read_abi('erc20', address[0])
    event_name = 'Transfer'

    stuff = [
        {
            'event':event_name,
            'abi':abi
        }
    ]
    
    current = w3.eth.block_number
    logs = read_history(
        address,
        stuff,
        current-100,
        current,
        50
    )
    print(logs)