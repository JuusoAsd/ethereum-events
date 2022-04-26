import pytest
from src.event_reader.history_v2 import read_history
from src.event_reader.util import get_web3, read_abi
from web3._utils.filters import construct_event_filter_params, construct_event_topic_set


def test_history_single_address_topic():
    print('\n'*5)
    w3 = get_web3()
    address = ["0x6B175474E89094C44Da98b954EedeAC495271d0F"]
    abi = read_abi('erc20', address[0])
    event_name = 'Transfer'

    stuff = [
        {
            'abi':abi,
            'event':event_name
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
    print(len(logs))


def test_history_2_topics():
    print('\n'*5)
    w3 = get_web3()
    address = ["0x6B175474E89094C44Da98b954EedeAC495271d0F"]

    abi = read_abi('erc20', address[0])
    #event_name = ['Transfer', 'Approval']

    stuff = [
        {
            'abi':abi,
            'event':'Approval'
        },
        {
            'abi':abi,
            'event':'Transfer'
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
    #print(*logs, sep='\n\n')
    print(len(logs))

def test_history_single_topic_two_address():
    w3 = get_web3()
    address = [
        "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        ]

    abi = read_abi('erc20', address[0])
    #event_name = ['Transfer', 'Approval']

    stuff = [
        {
            'abi':abi,
            'event':'Transfer'
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
    #print(*logs, sep='\n\n')
    print(len(logs))

def test_history_two_topic_two_address():
    w3 = get_web3()
    address = [
        "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        ]

    abi = read_abi('erc20', address[0])
    #event_name = ['Transfer', 'Approval']

    stuff = [
        {
            'abi':abi,
            'event':'Approval'
        },
        {
            'abi':abi,
            'event':'Transfer'
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
    #print(*logs, sep='\n\n')
    print(len(logs))

def test_everything_matches():
    w3 = get_web3()
    current = w3.eth.block_number

    from_b = current-1000
    to_b = current - 500

    usdc = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    dai = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
    abi = read_abi('erc20', dai)

    transfer = [{'abi':abi,'event':'Transfer'}]
    approval = [{'abi':abi,'event':'Approval'}]

    dai_transfer_logs = read_history(dai, transfer,from_b, to_b, 3000)
    dai_approval_logs = read_history(dai, approval,from_b, to_b, 3000)

    usdc_transfer_logs = read_history(usdc, transfer,from_b, to_b, 3000)
    usdc_approval_logs = read_history(usdc, approval,from_b, to_b, 3000)

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
    total = len(usdc_approval_logs) + len(usdc_transfer_logs) +len(dai_approval_logs)+len(dai_transfer_logs)
    print(f"total: {total} - everything: {len(everything)}")

def test_uniswap():
    w3 =get_web3()
    uniswap_factory = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
    uniswap_pool = "0xDE731bb94FeA8774046cdDAbA47d8866bA0f2202"

    fact_abi = read_abi('uni_fact', uniswap_factory)
    pool_abi = read_abi('pool_abi',uniswap_pool)

    current_block = w3.eth.blockNumber

    event_list_fact = [{"abi": fact_abi, "event": "PairCreated"}]

    pool_events = read_history(
        [uniswap_factory],
        event_list_fact,
        0,
        current_block,
        50000,
    )
    print(f"total pools: {len(pool_events)}")

    pair_list = []
    for i in pool_events:
        pair_list.append(i['args']['pair'])
    
    event_list_pair = [{'abi':pool_abi, 'event':'Sync'}]
    pair_events = read_history(
        pair_list,
        event_list_pair,
        current_block-10,
        current_block,
        1,
    )
    
    print(f"Total {len(pair_events)} sync event")
    assert len(pair_events) != 0