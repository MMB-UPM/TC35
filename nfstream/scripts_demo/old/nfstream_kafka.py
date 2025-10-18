from nfmod import NFStreamer, NFPlugin
from os import path
import timeit
from kafka import KafkaProducer
from pickle import dumps
import logging
from sys import stdout
from time import time
from datetime import datetime
import random
         
class KafkaPublisherV1(NFPlugin):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def on_init(self, packet, flow):
        flow.udps.start_interval = packet.time
        flow.udps.end_interval = packet.time + self.t_lim * 1000
        flow.udps.timestamp = packet.time
        flow.udps.src2dst_last = 0
        flow.udps.dst2src_last = 0
        if flow.dst2src_first_seen_ms and flow.src2dst_first_seen_ms:
            flow.udps.diff_dst_src_first = flow.dst2src_first_seen_ms - flow.src2dst_first_seen_ms
        else:
            flow.udps.diff_dst_src_first = 0
        
        if packet.payload_size > 0:
            if packet.direction == 0:
                flow.udps.src2dst_pkts_data = 1
                flow.udps.dst2src_pkts_data = 0
            else:
                flow.udps.dst2src_pkts_data = 1
                flow.udps.src2dst_pkts_data = 0
        else:
            flow.udps.src2dst_pkts_data = 0
            flow.udps.dst2src_pkts_data = 0
        
        """with open('prueba.txt', 'w') as file:
            content = 'timestamp*timestamp_nfstream'
            file.write(content)"""
    
    def on_update(self, packet, flow, producer, flush):
        if packet.payload_size > 0:
            
            if packet.direction == 0:
                flow.udps.src2dst_last = packet.time - flow.src2dst_first_seen_ms
                flow.udps.src2dst_pkts_data += 1
            else:
                flow.udps.dst2src_last = packet.time - flow.dst2src_first_seen_ms
                flow.udps.dst2src_pkts_data += 1
                
        if packet.time > flow.udps.end_interval:          
            if flow.dst2src_first_seen_ms and flow.src2dst_first_seen_ms:
                flow.udps.diff_dst_src_first = flow.dst2src_first_seen_ms - flow.src2dst_first_seen_ms
            
            #if random.random() < self.k_subsampling:
            flow.udps.timestamp = packet.time
                
            data = {"data": {"features": {"udps.protocol": flow.protocol,
                                              "udps.src2dst_last": flow.udps.src2dst_last,
                                              "udps.dst2src_last": flow.udps.dst2src_last,
                                              "udps.src2dst_pkts_data": flow.udps.src2dst_pkts_data,
                                              "udps.dst2src_pkts_data": flow.udps.dst2src_pkts_data,
                                              "src2dst_bytes": flow.src2dst_bytes,
                                              "dst2src_bytes": flow.dst2src_bytes,
                                              "udps.diff_dst_src_first": flow.udps.diff_dst_src_first
                                              }
                                    },
                        "metadata": {"timestamp": flow.udps.timestamp,
                                         "timestamp_nfstream": datetime.timestamp(datetime.now()),
                                         "monitored_device": self.monitored_device,
                                         "interface": self.interface,
                                         "connection_id": {"src_ip": flow.src_ip,
                                                           "dst_ip": flow.dst_ip,
                                                           "src_port": flow.src_port,
                                                           "dst_port": flow.dst_port,
                                                           "protocol": flow.protocol,
                                                           "first": flow.bidirectional_first_seen_ms},
                                         "flow_pkts": flow.udps.src2dst_pkts_data + flow.udps.dst2src_pkts_data,
                                         "flow_bytes": flow.src2dst_bytes + flow.dst2src_bytes
                                        }
                    }
                
            """with open('prueba.txt', 'a') as file:
                    content = f'{data["metadata"]["timestamp"]}*{data["metadata"]["timestamp_nfstream"]}'
                    file.write(content)"""
                    
            conn_id = f"{flow.src_ip}, {flow.dst_ip}, {flow.src_port}, {flow.dst_port}, {flow.protocol}"
            
            producer.send(topic=self.topic, value=data, headers=[("version", "v1".encode("utf-8")), ("features_names", "udps.protocol,udps.src2dst_last,udps.dst2src_last,udps.src2dst_pkts_data,udps.dst2src_pkts_data,src2dst_bytes,dst2src_bytes,udps.diff_dst_src_first".encode("utf-8"))], timestamp_ms=int(time() * 1000), key=conn_id)
            #self.logger.info("Snapshot Sent.")
            if flush:
                producer.flush()
                #self.logger.info("Snapshot Sent.")
                
            flow.udps.start_interval = packet.time
            flow.udps.end_interval = packet.time + self.t_lim * 1000
    
    def on_expire(self, flow, producer, flush):
        return
        data = {"data": {"features": {"udps.protocol": flow.protocol,
                                      "udps.src2dst_last": flow.udps.src2dst_last,
                                      "udps.dst2src_last": flow.udps.dst2src_last,
                                      "udps.src2dst_pkts_data": flow.udps.src2dst_pkts_data,
                                      "udps.dst2src_pkts_data": flow.udps.dst2src_pkts_data,
                                      "src2dst_bytes": flow.src2dst_bytes,
                                      "dst2src_bytes": flow.dst2src_bytes,
                                      "udps.diff_dst_src_first": flow.udps.diff_dst_src_first
                                      }
                        },
                "metadata": {"timestamp": flow.udps.timestamp,
                             "timestamp_nfstream": datetime.timestamp(datetime.now()),
                             "monitored_device": self.monitored_device,
                             "interface": self.interface,
                             "connection_id": {"src_ip": flow.src_ip,
                                               "dst_ip": flow.dst_ip,
                                               "src_port": flow.src_port,
                                               "dst_port": flow.dst_port,
                                               "protocol": flow.protocol,
                                               "first": flow.bidirectional_first_seen_ms},
                             "flow_pkts": flow.udps.src2dst_pkts_data + flow.udps.dst2src_pkts_data,
                             "flow_bytes": flow.src2dst_bytes + flow.dst2src_bytes
                            }
                }
        

        producer.send(topic=self.topic, value=data, headers=[("version", "v1".encode("utf-8")), ("features_names", "udps.protocol,udps.src2dst_last,udps.dst2src_last,udps.src2dst_pkts_data,udps.dst2src_pkts_data,src2dst_bytes,dst2src_bytes,udps.diff_dst_src_first".encode("utf-8"))], timestamp_ms=int(time() * 1000))
        #self.logger.info("Snapshot Sent.")
        if flush:
            producer.flush()
            #self.logger.info("Snapshot Sent.")
                
class KafkaPublisherV2(NFPlugin):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def on_init(self, packet, flow):
        flow.udps.start_interval = packet.time
        flow.udps.end_interval = packet.time + self.t_lim * 1000
        flow.udps.timestamp = packet.time
        flow.udps.src2dst_last = 0
        flow.udps.dst2src_last = 0
        if flow.dst2src_first_seen_ms and flow.src2dst_first_seen_ms:
            flow.udps.diff_dst_src_first = flow.dst2src_first_seen_ms - flow.src2dst_first_seen_ms
        else:
            flow.udps.diff_dst_src_first = 0
        
        if packet.payload_size > 0:
            if packet.direction == 0:
                flow.udps.src2dst_pkts_data = 1
                flow.udps.dst2src_pkts_data = 0
            else:
                flow.udps.dst2src_pkts_data = 1
                flow.udps.src2dst_pkts_data = 0
        else:
            flow.udps.src2dst_pkts_data = 0
            flow.udps.dst2src_pkts_data = 0
        
        flow.udps.last_src2dst_pkts_data = flow.udps.src2dst_pkts_data
        flow.udps.last_dst2src_pkts_data = flow.udps.dst2src_pkts_data
        flow.udps.last_src2dst_bytes = flow.src2dst_bytes
        flow.udps.last_dst2src_bytes = flow.dst2src_bytes
        flow.udps.dst2src_packets_in_interval = 0
        flow.udps.src2dst_packets_in_interval = 0
        flow.udps.dst2src_bytes_in_interval = 0
        flow.udps.src2dst_bytes_in_interval = 0
        flow.udps.dst2src_packets_per_second_interval = 0
        flow.udps.src2dst_packets_per_second_interval = 0
        flow.udps.dst2src_bytes_per_second_interval = 0
        flow.udps.src2dst_bytes_per_second_interval = 0
    
    def on_update(self, packet, flow, producer, flush):
        if packet.payload_size > 0:
            if packet.direction == 0:
                flow.udps.src2dst_last = packet.time - flow.src2dst_first_seen_ms
                flow.udps.src2dst_pkts_data += 1
            else:
                flow.udps.dst2src_last = packet.time - flow.dst2src_first_seen_ms
                flow.udps.dst2src_pkts_data += 1
                
        if packet.time > flow.udps.end_interval:
            flow.udps.dst2src_packets_in_interval = flow.udps.dst2src_pkts_data - flow.udps.last_dst2src_pkts_data
            flow.udps.src2dst_packets_in_interval = flow.udps.src2dst_pkts_data - flow.udps.last_src2dst_pkts_data
            flow.udps.dst2src_bytes_in_interval = flow.dst2src_bytes - flow.udps.last_dst2src_bytes
            flow.udps.src2dst_bytes_in_interval = flow.src2dst_bytes - flow.udps.last_src2dst_bytes
            
            flow.udps.dst2src_packets_per_second_interval = flow.udps.dst2src_packets_in_interval / self.t_lim
            flow.udps.src2dst_packets_per_second_interval = flow.udps.src2dst_packets_in_interval / self.t_lim
            flow.udps.dst2src_bytes_per_second_interval = flow.udps.dst2src_bytes_in_interval / self.t_lim
            flow.udps.src2dst_bytes_per_second_interval = flow.udps.src2dst_bytes_in_interval / self.t_lim
            
            if flow.dst2src_first_seen_ms and flow.src2dst_first_seen_ms:
                flow.udps.diff_dst_src_first = flow.dst2src_first_seen_ms - flow.src2dst_first_seen_ms
            flow.udps.timestamp = packet.time
                
            data = {"data": {"features": {"udps.protocol": flow.protocol,
                                          "udps.src2dst_last": flow.udps.src2dst_last,
                                          "udps.dst2src_last": flow.udps.dst2src_last,
                                          "udps.src2dst_pkts_data": flow.udps.src2dst_pkts_data,
                                          "udps.dst2src_pkts_data": flow.udps.dst2src_pkts_data,
                                          "src2dst_bytes": flow.src2dst_bytes,
                                          "dst2src_bytes": flow.dst2src_bytes,
                                          "udps.diff_dst_src_first": flow.udps.diff_dst_src_first,
                                          "udps.dst2src_packets_per_second_interval": flow.udps.dst2src_packets_per_second_interval,
                                          "udps.src2dst_packets_per_second_interval": flow.udps.src2dst_packets_per_second_interval,
                                          "udps.dst2src_bytes_per_second_interval": flow.udps.dst2src_bytes_per_second_interval,
                                          "udps.src2dst_bytes_per_second_interval": flow.udps.src2dst_bytes_per_second_interval,
                                          "udps.dst2src_packets_in_interval": flow.udps.dst2src_packets_in_interval,
                                          "udps.src2dst_packets_in_interval": flow.udps.src2dst_packets_in_interval,
                                          "udps.dst2src_bytes_in_interval": flow.udps.dst2src_bytes_in_interval,
                                          "udps.src2dst_bytes_in_interval": flow.udps.src2dst_bytes_in_interval
                                          }
                                },
                        "metadata": {"timestamp": flow.udps.timestamp,
                                     "timestamp_nfstream": datetime.timestamp(datetime.now()),
                                     "monitored_device": self.monitored_device,
                                     "interface": self.interface,
                                     "connection_id": {"src_ip": flow.src_ip,
                                                       "dst_ip": flow.dst_ip,
                                                       "src_port": flow.src_port,
                                                       "dst_port": flow.dst_port,
                                                       "protocol": flow.protocol,
                                                       "first": flow.bidirectional_first_seen_ms},
                                     "flow_pkts": flow.udps.src2dst_pkts_data + flow.udps.dst2src_pkts_data,
                                     "flow_bytes": flow.src2dst_bytes + flow.dst2src_bytes
                                    }
                        }
            producer.send(topic=self.topic, value=data, headers=[("version", "v1".encode("utf-8")), ("features_names", "udps.protocol,udps.src2dst_last,udps.dst2src_last,udps.src2dst_pkts_data,udps.dst2src_pkts_data,src2dst_bytes,dst2src_bytes,udps.diff_dst_src_first,udps.dst2src_packets_per_second_interval,udps.src2dst_packets_per_second_interval,udps.dst2src_bytes_per_second_interval,udps.src2dst_bytes_per_second_interval,udps.dst2src_packets_in_interval,udps.src2dst_packets_in_interval,udps.dst2src_bytes_in_interval,udps.src2dst_bytes_in_interval".encode("utf-8"))], timestamp_ms=int(time() * 1000))
            if flush:
                producer.flush()
                #self.logger.info("Flushed data")
                
            flow.udps.start_interval = packet.time
            flow.udps.end_interval = packet.time + self.t_lim * 1000
            flow.udps.last_src2dst_pkts_data = flow.udps.src2dst_pkts_data
            flow.udps.last_dst2src_pkts_data = flow.udps.dst2src_pkts_data
            flow.udps.last_src2dst_bytes = flow.src2dst_bytes
            flow.udps.last_dst2src_bytes = flow.dst2src_bytes
            
    
    def on_expire(self, flow, producer, flush):
        return
        data = {"data": {"features": {"udps.protocol": flow.protocol,
                                      "udps.src2dst_last": flow.udps.src2dst_last,
                                      "udps.dst2src_last": flow.udps.dst2src_last,
                                      "udps.src2dst_pkts_data": flow.udps.src2dst_pkts_data,
                                      "udps.dst2src_pkts_data": flow.udps.dst2src_pkts_data,
                                      "src2dst_bytes": flow.src2dst_bytes,
                                      "dst2src_bytes": flow.dst2src_bytes,
                                      "udps.diff_dst_src_first": flow.udps.diff_dst_src_first,
                                      "udps.dst2src_packets_per_second_interval": flow.udps.dst2src_packets_per_second_interval,
                                      "udps.src2dst_packets_per_second_interval": flow.udps.src2dst_packets_per_second_interval,
                                      "udps.dst2src_bytes_per_second_interval": flow.udps.dst2src_bytes_per_second_interval,
                                      "udps.src2dst_bytes_per_second_interval": flow.udps.src2dst_bytes_per_second_interval,
                                      "udps.dst2src_packets_in_interval": flow.udps.dst2src_packets_in_interval,
                                      "udps.src2dst_packets_in_interval": flow.udps.src2dst_packets_in_interval,
                                      "udps.dst2src_bytes_in_interval": flow.udps.dst2src_bytes_in_interval,
                                      "udps.src2dst_bytes_in_interval": flow.udps.src2dst_bytes_in_interval
                                      }
                        },
                "metadata": {"timestamp": flow.udps.timestamp,
                             "timestamp_nfstream": datetime.timestamp(datetime.now()),
                             "monitored_device": self.monitored_device,
                             "interface": self.interface,
                             "connection_id": {"src_ip": flow.src_ip,
                                               "dst_ip": flow.dst_ip,
                                               "src_port": flow.src_port,
                                               "dst_port": flow.dst_port,
                                               "protocol": flow.protocol,
                                               "first": flow.bidirectional_first_seen_ms},
                             "flow_pkts": flow.udps.src2dst_pkts_data + flow.udps.dst2src_pkts_data,
                             "flow_bytes": flow.src2dst_bytes + flow.dst2src_bytes
                            }
                }
        producer.send(topic=self.topic, value=data, headers=[("version", "v1".encode("utf-8")), ("features_names", "udps.protocol,udps.src2dst_last,udps.dst2src_last,udps.src2dst_pkts_data,udps.dst2src_pkts_data,src2dst_bytes,dst2src_bytes,udps.diff_dst_src_first,udps.dst2src_packets_per_second_interval,udps.src2dst_packets_per_second_interval,udps.dst2src_bytes_per_second_interval,udps.src2dst_bytes_per_second_interval,udps.dst2src_packets_in_interval,udps.src2dst_packets_in_interval,udps.dst2src_bytes_in_interval,udps.src2dst_bytes_in_interval".encode("utf-8"))], timestamp_ms=int(time() * 1000))
        if flush:
            producer.flush()
            #self.logger.info("Flushed data")

def start(pcap_path, pcap_filename, t_lim, kafka_url, producer_topic, producer_client_id, monitored_device, interface, n_round, k_subsampling):
    # Setup LOGGER
    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.INFO)
    logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    consoleHandler = logging.StreamHandler(stdout)
    consoleHandler.setFormatter(logFormatter)
    LOGGER.addHandler(consoleHandler)
    
    archivo_pcap = path.join(pcap_path, pcap_filename)
    
    if n_round == 1:
        udps = [KafkaPublisherV1(t_lim=t_lim, topic=producer_topic, monitored_device=monitored_device, interface=interface, logger=LOGGER, k_subsampling=k_subsampling)]
    elif n_round == 2:
        udps = [KafkaPublisherV2(t_lim=t_lim, topic=producer_topic, monitored_device=monitored_device, interface=interface, logger=LOGGER)]

    streamer = NFStreamer(source=archivo_pcap, accounting_mode=3, udps=udps, statistical_analysis=False, splt_analysis=0, idle_timeout=120, n_meters=10, n_dissections=0, kafka_url=kafka_url)
    
    start_time = timeit.default_timer()
    
    for i in range(100):
        LOGGER.info(f"Processed Snapshots: {i*1000}")

    counter = 0
    for _ in streamer:
        counter += 1
        #LOGGER.info(f"Processed Flows: {counter}")
    
    LOGGER.info(counter)   
    elapsed_time = timeit.default_timer() - start_time
    
    LOGGER.info(f"{int(elapsed_time//60)}m {int(elapsed_time%60)}s")
