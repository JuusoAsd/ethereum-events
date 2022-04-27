from functools import partial
from multiprocessing import Pool

from web3._utils.filters import construct_event_topic_set
from web3._utils.events import get_event_data

from event_reader.util import get_web3, read_abi


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


def _read_single_interval(event_filter_params: dict, interval: tuple):
    w3 = get_web3()
    from_block, to_block = interval
    current_block = from_block
    chunksize = to_block - from_block
    adjustment = 1
    all_logs = []
    while current_block < to_block:
        try:
            event_filter_params["fromBlock"] = current_block
            event_filter_params["toBlock"] = min(
                to_block, current_block + chunksize / adjustment
            )
            logs = w3.eth.get_logs(event_filter_params)
            all_logs += logs
            adjustment = 1
            current_block = event_filter_params["toBlock"] + 1

        except Exception as e:
            print(e)
            adjustment *= 2
    return logs


def get_topics(info):
    w3 = get_web3()
    topic_dict = {}
    for i in info:
        contract = w3.eth.contract(abi=i["abi"])
        event = contract.events.__getitem__(i["event"])
        event_abi = event._get_event_abi()
        abi_codec = event.web3.codec
        if "arguments" not in i:
            args = {}
        topic = construct_event_topic_set(
            event_abi=event_abi, abi_codec=abi_codec, arguments=args
        )
        topic_dict[topic[0]] = {"abi": event_abi, "codec": abi_codec}
    return topic_dict


def parse_logs(logs, topic_dict):
    parsed = []
    for log in logs:
        log_topic = log["topics"][0].hex()
        parse = get_event_data(
            abi_codec=topic_dict[log_topic]["codec"],
            event_abi=topic_dict[log_topic]["abi"],
            log_entry=log,
        )
        parsed.append(parse)
    return parsed


def read_history(
    addresses: list,
    event_abi_argument: list(),  # list of dictionaries containing
    from_block: int,
    to_block: int,
    interval: int,
):

    topic_dict = get_topics(event_abi_argument)
    topics = list(topic_dict.keys())

    setup_dict = {"topics": [topics], "address": addresses}

    interval_list = create_interval_list(
        from_block=from_block, to_block=to_block, interval=interval
    )
    with Pool(20) as p:
        log_lists = p.map(
            partial(_read_single_interval, setup_dict),
            interval_list,
        )

    logs = [item for sublog in log_lists for item in sublog]
    return parse_logs(logs, topic_dict)


if __name__ == "__main__":
    pass
