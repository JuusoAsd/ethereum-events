from functools import partial
from multiprocessing import Pool

from web3._utils.filters import construct_event_filter_params
from web3._utils.events import get_event_data

from utils import get_web3, read_abi, create_topic_string


def create_interval_list(from_block: int, to_block: int, interval: int) -> list:
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


def _read_interval(
    address: str, abi: dict, event_name: str, argument_filters: dict, interval: tuple
) -> list:
    """reads specific events in interval from specific contract"""

    """
    Should be updated so that each interval does not need to create objects
    but there are problems when trying to run multithread
    """
    w3 = get_web3()
    from_block, to_block = interval
    contract = w3.eth.contract(address=address, abi=abi)
    event = contract.events.__getitem__(event_name)
    event_abi = event._get_event_abi()
    abi_codec = event.web3.codec
    topic_hash = None

    current_block = from_block
    chunksize = to_block - from_block
    adjustment = 1

    all_logs = []
    while current_block < to_block:
        try:

            data_filter_set, event_filter_params = construct_event_filter_params(
                event_abi,
                abi_codec,
                contract_address=event.address,
                argument_filters=argument_filters,
                fromBlock=current_block,
                toBlock=int(min(to_block, current_block + chunksize / adjustment)),
                address=address,
                topics=topic_hash,
            )
            logs = w3.eth.get_logs(event_filter_params)
            all_logs += logs
            adjustment = 1
            current_block = event_filter_params["toBlock"] + 1

        except Exception as e:
            print("ERROR", e)
            adjustment *= 2

    event_data = []
    for entry in all_logs:
        data = get_event_data(abi_codec, event_abi, entry)
        event_data.append(data)

    return event_data


def read_history(
    address: str,
    abi: dict,
    event_name: str,
    from_block: int,
    to_block: int,
    interval: int,
    argument_filters: dict,
) -> list:
    """
    coordinates reading historical events through threads
    """
    intervals = create_interval_list(from_block, to_block, interval)
    with Pool(20) as p:
        log_lists = p.map(
            partial(_read_interval, address, abi, event_name, argument_filters),
            intervals,
        )

    event_data = [item for sublog in log_lists for item in sublog]
    return event_data


if __name__ == "__main__":
    w3 = get_web3("https")
    pool_address = w3.toChecksumAddress("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852")
    uniswap_pool_abi = read_abi("uni_v2_pool", address=pool_address)

    contract = w3.eth.contract(address=pool_address, abi=uniswap_pool_abi)

    events = read_history(
        pool_address, uniswap_pool_abi, "Sync", 14007042, 14507042, 50000, {}
    )
    print(f"Found total of {len(events)} events")
