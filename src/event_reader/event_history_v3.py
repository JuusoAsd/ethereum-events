import random
import os
import logging
from util import get_web3, read_abi
from functools import partial
from multiprocessing import Pool
import time

from dotenv import load_dotenv
import urllib3
from web3._utils.filters import construct_event_filter_params
from web3._utils.events import get_event_data

logging.basicConfig(level=logging.INFO)
load_dotenv()


def create_eventname_filter(w3, event_name, address):
    abi = read_abi("pair", address)
    contract = w3.eth.contract(abi=abi)
    event = contract.events.__getitem__(event_name)
    event_abi = event._get_event_abi()
    abi_codec = event.web3.codec
    _, topics = construct_event_filter_params(event_abi, abi_codec)
    return topics["topics"][0], event_abi, abi_codec


def create_interval_list(from_block, to_block, interval):
    """
    Creates even intervals where last one is cut to match total blocks
    """
    intervals = []
    current_block = from_block
    if to_block - current_block < interval:
        return [(current_block, to_block)]
    while current_block < to_block:
        intervals.append(
            (int(current_block), int(min(current_block + interval, to_block)))
        )
        current_block += interval + 1

    return intervals


def read_single_interval_fixed(address, topics, interval):
    retry = 0
    w3 = get_web3()
    from_block = interval[0]
    to_block = interval[1]
    log_filter = {
        "fromBlock": from_block,
        "toBlock": to_block,
        "address": address,
        "topics": topics,
    }
    while retry < 3:
        try:
            logs = w3.eth.getLogs(log_filter)
            logging.debug(f"Read {len(logs)} logs from interval {interval}")
            return logs
        # react to error from too many events in interval
        except ValueError as e:
            try:
                if e.args[0].get("code") == -32005:
                    logging.debug(f"Interval {interval} too large, splitting")
                    to_middle = int((from_block + to_block) / 2)
                    partial_logs_start = read_single_interval_fixed(
                        address, topics, (from_block, to_middle)
                    )
                    partial_logs_end = read_single_interval_fixed(
                        address, topics, (to_middle + 1, to_block)
                    )
                    if partial_logs_start is not None and partial_logs_end is not None:
                        return partial_logs_start + partial_logs_end
                    else:
                        retry += 1
                # unexpected error, retry
                else:
                    retry += 1
            except:
                logging.error(f"{e}")
                retry += 1
        except urllib3.exceptions.ReadTimeoutError:
            logging.error(f"timeout while reading")
            time.sleep(5)
            retry += 1
    logging.error(f"Exceeded on interval {interval}")
    raise ValueError(f"Retries exceeded")


def parse_events(topic_dict, log):
    topic = log["topics"][0].hex()
    parse = get_event_data(
        abi_codec=topic_dict[topic]["codec"],
        event_abi=topic_dict[topic]["abi"],
        log_entry=log,
    )
    return parse


def get_event_logs_threads(
    w3, address, event_name_list, from_block, to_block, interval
):

    intervals = create_interval_list(from_block, to_block, interval)
    random.shuffle(intervals)

    eventname_topics = []
    topic_unpack_dict = {}
    for event in event_name_list:
        topic, event_abi, abi_codec = create_eventname_filter(w3, event, address)
        eventname_topics.append(topic)
        topic_unpack_dict[topic] = {"abi": event_abi, "codec": abi_codec}

    all_logs = []
    logging.debug(eventname_topics)
    with Pool(int(os.getenv("THREADS"))) as p:
        log_lists = p.map(
            partial(read_single_interval_fixed, address, [eventname_topics]),
            intervals,
        )

    for log_list in log_lists:
        if type(log_list) == list:
            all_logs += log_list

    total_events = len(all_logs)
    logging.info(f"Found total {total_events} events")
    logging.info(f"parsing events...")

    with Pool(int(os.getenv("THREADS"))) as p:
        parsed_logs = p.map(
            partial(parse_events, topic_unpack_dict),
            all_logs,
        )
    return parsed_logs
    """for log in all_logs:
        topic = log["topics"][0].hex()
        parse = get_event_data(
            abi_codec=topic_unpack_dict[topic]['codec'],
            event_abi=topic_unpack_dict[topic]['abi'],
            log_entry=log,
        )
        parsed_logs.append(parse)
        n += 1
        if n % 1000 == 0:
            logging.info(f"parsed {round(n/total_events*100,2)}% events")
    return parsed_logs"""


if __name__ == "__main__":
    w3 = get_web3()
    usdc_eth = w3.toChecksumAddress("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc")
    stop = 14785568
    start = 0

    step = 20000

    logs = get_event_logs_threads(
        w3, usdc_eth, ["Mint", "Burn", "Swap"], start, stop, step
    )
    print(len(logs))

    """
    import cProfile
    import pstats
    with cProfile.Profile() as pr:
        get_event_logs_threads()
    
    stats = pstats.Stats(pr)
    stats.sort_stats(pstats.SortKey.TIME)
    stats.print_stats()
    stats.dump_stats(filename='prof.prof')"""
    """Error reading interval (12100121, 12200121)
Error reading interval (13100131, 13200131)
INFO:root:Current logs: 3078589

Found total events from error interval 133742
INFO:root:Current logs: 3121402

newest: INFO:root:Current logs: 
1: 3119053
2: 3485027
3: 3485027

"""
