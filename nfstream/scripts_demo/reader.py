import logging
from sys import stdout
import os
import time
import signal
import numpy as np
import json
import sys
import pickle
import random
from collections import defaultdict
from kafka import KafkaProducer
from typing import Generator, IO, List, Tuple, Dict, Any
from datetime import datetime
from pytz import timezone
import argparse

# Setup LOGGER
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

MAX_SNAPSHOTS = 300
K = 0.7
TIME_INTERVAL = 1.0  # en segundos

class Reader():
    def __init__(self, feature_names, topic:str, delim_csv: str,
                 kafka_url:str, csv_dir:str, monitored_device:str, interface:str):
        LOGGER.info("Creating Data Aggregator")
        self.topic = topic
        self.delim_csv = delim_csv
        self.csv_dir = os.path.join(csv_dir, "")
        self.producer = self.__connect_publisher_client(kafka_url)
        self.monitored_device = monitored_device
        self.interface = interface
        self.feature_names = feature_names.split(",")
        self.prev_timestamp_pcap = 0.0
        self.prev_timestamp_real = 0.0

        
        signal.signal(signal.SIGINT, self.__handler)
            
        
    def __handler(self, num, frame):
        """
            Handles user stopping.
        """
        LOGGER.info("Gracefully stopping...")
        #self.producer.close()
        exit()
    
    def __follow(self, thefile: IO[str], time_sleep:float) -> Generator[str, None, None]:
        """
            Generator function that yields new lines from a file.
            Args:
                thefile (IO[str]): The file to be followed by this method.
                time_sleep (float): Time to be asleep if the file hasn't been updated or is still empty.
            Returns:
                A Generator for the file.
        """

        trozo = ""

        # start infinite loop
        while True:
            # read last line of file
            line = thefile.readline()

            # sleep if file hasn't been updated
            if not line:
                time.sleep(time_sleep)
                continue
            if line[-1] != "\n":
                trozo += line
            else:
                if trozo != "":
                    line = trozo + line
                    trozo = ""
                yield line


    def __load_file(self) -> str:
        """
            Retrieves the tstat generated CSV file.
            Returns:
                Path to the file.
        """
        while True:
            csvs = os.listdir(self.csv_dir)
            if len(csvs) > 0:
                csv = os.path.join(self.csv_dir, csvs[0])
                LOGGER.info("Following %s", csv)
                return csv
            else:
                LOGGER.info("No CSVs directory!")
                time.sleep(5)
    
    def __get_json(self, line_splitted, conn_id, flow_packets, flow_bytes, current_time) -> Dict[str, Any]:
        """
            Returns a Json like dict which stores the specified features and information to be sent.
            Args:
                conn_id (Tuple[str, str, str, str]): snapshot's connection iAnyd.
                flow_packets (int): total number of flow packets.
                flow_bytes (int): total number of flow bytes.
                feature_dict (dict): selected features.
                current_time (float): timestamp for the snapshot.
            Returns:
                Dict of str type keys (Json type dict).
        """
        
        data_dict = {
            "data":{
                "features": {feature: float(line_splitted[i]) for feature, i in self.features},
            },
            "metadata":{
                "timestamp": current_time,
                "timestamp_nfstream": datetime.timestamp(datetime.now()),
                "monitored_device": self.monitored_device,
                "interface": self.interface,
                "connection_id": conn_id,
                "flow_pkts": flow_packets,
                "flow_bytes": flow_bytes,
            }
        }
        
        return data_dict


    def __process_v1(self, line:str) -> None:
        """
            Precesses a snapshot for Version 1.
            Args:
                line (str): snapshot.
                feature_names (list): features to send.
                prev_timestamp_pcap (float): timestamp from field "last" of previous snapshot.
                prev_timestamp_real (float): timestamp assigned to previous snapshot.
        """
        line_splitted = line.split(self.delim_csv)
        conn_id = {feature: line_splitted[i] for feature, i in self.conn_id.items()}
        flow_packets = int(line_splitted[self.all_features["udps.src2dst_pkts_data"]]) + int(line_splitted[self.all_features["udps.dst2src_pkts_data"]])
        flow_bytes = int(line_splitted[self.all_features["udps.src2dst_bytes"]]) + int(line_splitted[self.all_features["udps.dst2src_bytes"]])
        current_timestamp_pcap = float(line_splitted[self.all_features["timestamp"]])
        
        if self.prev_timestamp_real == 0:
            current_time = datetime.timestamp(datetime.now(timezone("Europe/Madrid")))
        else:
            current_time = self.prev_timestamp_real + (current_timestamp_pcap - self.prev_timestamp_pcap)
        self.prev_timestamp_pcap = current_timestamp_pcap
        self.prev_timestamp_real = current_time

        return self.__get_json(line_splitted, conn_id, flow_packets, flow_bytes, current_time)

    def __connect_publisher_client(self, broker_ip_address:str, broker_port:str) -> KafkaProducer:
        """
            Creates a KafkaProducer instance and returns it.
            Args:
                broker_ip_address (str): kafka broker's ip address.
                broker_port (str): kafka broker's port.
            Returns:
                KafkaProducer instance.

        """         
        broker = f'{broker_ip_address}:{broker_port}'
        producer = KafkaProducer(
            bootstrap_servers=broker
        )

        LOGGER.info("Trying to establish connection to brokers...")

        if not producer.bootstrap_connected():
            sys.exit("Failed to connect to brokers.")

        return producer


    def __publish_data(self, window:bytes) -> None:
        '''
            Sends a pickled representetation to the kafka topic.
            Args:
                window (bytes): pickled representation of an object.
                feature_names (list): features to send.
        '''
        version_bytes = self.version.encode('utf-8')

        features = ",".join(self.feature_names)
        features_bytes = features.encode('utf-8')
        
        self.producer.send(
            headers= [("version", version_bytes), ("feature_names", features_bytes)],
            topic=self.topic,
            value=window,
            timestamp_ms=time.time_ns() // 1000000
        )

        self.producer.flush()
        
    def set_features(self, line):
        line_splitted = line.split(self.delim_csv)
        self.all_features = {feature: i for i, feature in enumerate(line_splitted)}
        self.conn_id = {"src_ip": self.all_features["src_ip"], "dst_ip": self.all_features["dst_ip"], "src_port": self.all_features["src_port"], "dst_port": self.all_features["dst_port"], "first": self.all_features["bidirectional_first_seen_ms"], "protocol": self.all_features["protocol"]}
        self.features = {feature: i for feature, i in self.all_features.items() if feature in self.feature_names}

    def start(self) -> None:
        """
            Manages the traffic and sends the appropiate data in the right way, depending on the different specified parameters of the class.
        """
        LOGGER.info("Loading Tstat log file...")
        with open(self.__load_file(), "r") as logfile:

            LOGGER.info("Following Tstat log file...")
            loglines = self.__follow(logfile, 1)
    
            process_time = []
            num_lines = 0
            
            processed = 0
            last_processed = 0
            not_processed = 0
            last_not_processed = 0
            n_intervals = 0
            
            #snapshots_buffer = []
            
            counter = 0
    
            LOGGER.info("Starting to process data...")
            
            while True:
                line = next(loglines, None)
                while line == None:
                    LOGGER.info("Waiting for new data...")
                    time.sleep(0.001)
                    line = next(loglines, None)
                if num_lines == 0:
                    num_lines += 1
                    self.set_features(line)
                    continue
                
                start = time.time()
    
                #if self.version == 'v1':
                snapshot = self.__process_v1(line)
                counter += 1
                #LOGGER.info(snapshot['metadata']['connection_id'])
                
                """if snapshot['metadata']['timestamp'] >= interval_end:
                    n_intervals += 1
                    processed += len(last_snapshot_per_connection)
                    #LOGGER.info("Interval %s ended. %s different connections", (datetime.fromtimestamp(interval_start), datetime.fromtimestamp(interval_end)), len(last_snapshot_per_connection))
                    #LOGGER.info("Number of UNPROCESSED snapshots in this interval: %s", not_processed-last_not_processed)
                    #LOGGER.info("Total number of UNPROCESSED snapshots: %s", not_processed)
                    #LOGGER.info("Mean number of UNPROCESSED snapshots per interval: %s", not_processed/n_intervals)
                    #LOGGER.info("Number of PROCESSED snapshots in this interval: %s", processed-last_processed)
                    #LOGGER.info("Total number of PROCESSED snapshots: %s", processed)
                    #LOGGER.info("Mean number of PROCESSED snapshots per interval: %s", processed/n_intervals)
                    
                    #last_not_processed = not_processed
                    #last_processed = processed
                    
                    print(counter)
                    
                    if counter>MAX_SNAPSHOTS:
                        #LOGGER.info("More than %s unique connections. Splitting...", MAX_SNAPSHOTS)
                        
                        for i in range(0, len(last_snapshot_per_connection), MAX_SNAPSHOTS):
                            self.window = list(last_snapshot_per_connection.values())[i:i+MAX_SNAPSHOTS]
                            window_serialized = pickle.dumps(self.window)
                            self.__publish_data(window_serialized, feature_names)
                            
                    else:
                        self.window = list(last_snapshot_per_connection.values())
                        window_serialized = pickle.dumps(self.window)
                        self.__publish_data(window_serialized, feature_names)
                    
                    interval_start = interval_end
                    interval_end += TIME_INTERVAL
                    counter = 0"""
                    
                #LOGGER.info(last_snapshot_per_connection)
                """if random.random() < K:
                    if tuple(snapshot['metadata']['connection_id'].values()) in last_snapshot_per_connection:
                        not_processed += 1
                    last_snapshot_per_connection[tuple(snapshot['metadata']['connection_id'].values())] = snapshot
                else:
                    not_processed += 1"""
                #last_snapshot_per_connection[tuple(snapshot['metadata']['connection_id'].values())] = snapshot
                #print(counter)
                
                self.__publish_data(pickle.dumps(snapshot), self.feature_names)
                    
                #else:
                #    sys.exit("Selected Version does not exist.")
                    
                process_time.append(time.time() - start)
    	        
                num_lines += 1
                if num_lines % 1000 == 0:
                    LOGGER.info(f"Number of snapshots: {num_lines} - Average processing time of last 1000 snapshots: {sum(process_time) / 1000}")
                    process_time = []
                """if num_lines > 38989000:
                    print(counter+len(last_snapshot_per_connection))
                    print(line)"""

def main(args):
    """
    Main function for setting up and initialize data inference.
    Args:
        args (Any): Command-line arguments and options.
    """

    reader = Reader(feature_names=args.feature_names, topic=args.producer_topic, kafka_url=args.kafka_url,
                    csv_dir=args.output_path, monitored_device=args.monitored_device, interface= args.interface)
    reader.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize and start Reader component.")
    parser.add_argument("--feature_names", type=str, help="Path where to store output files")
    parser.add_argument("--output_path", type=str, default="/app/output", help="Path where to store output files")
    parser.add_argument("--delim_csv", type=str, default="*", help="Output CSV column delimiter")
    parser.add_argument("--kafka_url", type=str, default="localhost:9094", help="IP address and port of the Kafka cluster")
    parser.add_argument("--producer_topic", type=str, default="inference_data", help="Kafka topic which the producer will be publishing to.")
    parser.add_argument("--monitored_device", type=str, default="ceos2", help="Monitored device")
    parser.add_argument("--interface", type=str, default="eht3", help="Interface")

    args = parser.parse_args()
    main(args)

    sys.exit(0)
