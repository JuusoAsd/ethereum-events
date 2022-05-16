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

logging.basicConfig(level=logging.INFO)
load_dotenv()


def create_eventname_filter(w3, event_name, address):
    abi = read_abi("pair", address)
    contract = w3.eth.contract(abi=abi)
    event = contract.events.__getitem__(event_name)
    event_abi = event._get_event_abi()
    abi_codec = event.web3.codec
    _, topics = construct_event_filter_params(event_abi, abi_codec)
    return topics["topics"]


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


def read_single_interval(address, topics, interval):
    try:
        w3 = get_web3()
        logging.info(f"Reading interval {interval}")
        from_block = interval[0]
        to_block = interval[1]
        log_filter = {
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": address,
            "topics": topics,
        }
        logs = w3.eth.getLogs(log_filter)
        logging.info(f"Read {len(logs)} logs from interval {interval}")
    except ValueError as e:
        try:
            if "code" in e.args[0]:
                if e.args[0]["code"] == -32005:
                    logging.info(f"Interval {interval} too large, splitting")
                    to_middle = int((from_block + to_block) / 2)
                    partial_logs_start = read_single_interval(
                        address, topics, (from_block, to_middle)
                    )
                    partial_logs_end = read_single_interval(
                        address, topics, (to_middle + 1, to_block)
                    )
                    return partial_logs_start + partial_logs_end
            raise e
        except:
            logging.error(f"Error reading interval {interval}")
            return interval
    return logs


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
            logging.info(f"Read {len(logs)} logs from interval {interval}")
            return logs
        # react to error from too many events in interval
        except ValueError as e:
            try:
                if e.args[0].get("code") == -32005:
                    logging.info(f"Interval {interval} too large, splitting")
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


def get_event_logs_threads(address, event_name_list, from_block, to_block, interval):

    intervals = create_interval_list(from_block, to_block, interval)
    random.shuffle(intervals)

    eventname_topics = []
    for event in event_name_list:
        eventname_topics.append(create_eventname_filter(w3, event, address))

    all_logs = []
    with Pool(int(os.getenv("THREADS"))) as p:
        log_lists = p.map(
            partial(read_single_interval_fixed, address, eventname_topics),
            intervals,
        )

    for log_list in log_lists:
        if type(log_list) == list:
            all_logs += log_list
    logging.info(f"Current logs: {len(all_logs)}")
    return all_logs


if __name__ == "__main__":
    w3 = get_web3()
    usdc_eth = w3.toChecksumAddress("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc")
    start = 0
    stop = 14785568
    step = 10000

    logs = get_event_logs_threads(usdc_eth, ["Sync"], start, stop, step)

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
