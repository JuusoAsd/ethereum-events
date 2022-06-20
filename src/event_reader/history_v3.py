import random
import os
import logging
from event_reader.util import get_web3, read_abi
from functools import partial
from multiprocessing import Pool
import time

from dotenv import load_dotenv
import urllib3
from web3._utils.filters import construct_event_filter_params
from web3._utils.events import get_event_data


def create_eventname_filter(event, abi):
    w3 = get_web3()
    contract = w3.eth.contract(abi=abi)
    event = contract.events.__getitem__(event)
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
    from_block = interval[0]
    to_block = interval[1]
    log_filter = {
        "fromBlock": from_block,
        "toBlock": to_block,
        "address": address,
        "topics": topics,
    }
    while retry < 3:
        w3 = get_web3()
        try:
            logs = w3.eth.getLogs(log_filter)
            logging.info(
                f"Read {len(logs)} logs from interval {interval}, total blocks: {to_block - from_block}"
            )
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
                    logging.debug(f"Unexpected error: {e}")
                    retry += 1
            except:
                logging.error(f"{e}")
                retry += 1
        except urllib3.exceptions.ReadTimeoutError:
            logging.error(f"timeout while reading")
            time.sleep(5)
            retry += 1
        except Exception as e:
            logging.error(f"{e}")
            time.sleep(10)
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


def create_topics(event_abis):
    eventname_topics = []
    topic_unpack_dict = {}
    for event in event_abis:
        event_name = event["event"]
        abi = event["abi"]
        topic, event_abi, abi_codec = create_eventname_filter(event_name, abi)
        eventname_topics.append(topic)
        topic_unpack_dict[topic] = {"abi": event_abi, "codec": abi_codec}
    return eventname_topics, topic_unpack_dict


def get_event_logs_threads(address, event_abis, from_block, to_block, interval):

    intervals = create_interval_list(from_block, to_block, interval)
    random.shuffle(intervals)

    eventname_topics, topic_unpack_dict = create_topics(event_abis)

    all_logs = []
    logging.debug(eventname_topics)
    logging.info(f"Reading logs...")
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


if __name__ == "__main__":
    pair_abi = read_abi("pair", "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc")
    token_abi = read_abi("token", "0x6B175474E89094C44Da98b954EedeAC495271d0F")
    w3 = get_web3(ws=True)

    pair = w3.toChecksumAddress("0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc")
    stop = w3.eth.blockNumber - 100
    start = stop - 10000
    step = 1253

    event_abi_pairs = [
        {"abi": pair_abi, "event": "Sync"},
    ]

    logs = get_event_logs_threads(
        address=[pair],
        event_abis=event_abi_pairs,
        from_block=start,
        to_block=stop,
        interval=step,
    )
    print(len(logs))
