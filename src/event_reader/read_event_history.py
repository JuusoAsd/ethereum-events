from functools import partial
from multiprocessing import Pool

from web3._utils.filters import construct_event_filter_params
from web3._utils.events import get_event_data

from event_reader.util import get_web3, read_abi


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


def _read_interval(
    address: list, 
    abi: dict, 
    event_name: str, 
    argument_filters: dict, 
    interval: tuple
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

    return all_logs


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

    logs = [item for sublog in log_lists for item in sublog]
    parsed_events = []

    contract = get_web3("https").eth.contract(address=address, abi=abi)
    event = contract.events.__getitem__(event_name)
    event_abi = event._get_event_abi()
    abi_codec = event.web3.codec

    for event in logs:
        parsed_events.append(get_event_data(abi_codec, event_abi, event))
    return parsed_events

def read_history_simple(

) -> list:

if __name__ == "__main__":
    w3 = get_web3("https")
    # address = w3.toChecksumAddress("0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852")
    # abi = read_abi("uni_v2_pool", address=pool_address)

    # address = w3.toChecksumAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F")
    # abi = read_abi("erc20", address=address)

    address = w3.toChecksumAddress("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc")
    abi = read_abi("uni_v2_pool", address=address)

    contract = w3.eth.contract(address=address, abi=abi)

    event = contract.events.__getitem__("Sync")
    event_abi = event._get_event_abi()
    abi_codec = event.web3.codec

    event_logs = read_history(address, abi, "Sync", 14504000, 14507042, 10000, {})
    print(f"Found total of {len(event_logs)} events")
