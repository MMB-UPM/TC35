from nfstream import NFStreamer, NFPlugin
from os import path
import timeit
import logging
from sys import stdout
from datetime import datetime
import numpy as np

# - pkts_data: Número de paquetes con carga útil
# - n_unique_bytes: Número de bytes únicos en la carga útil

class Prueba(NFPlugin):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        with open(self.full_output, "w") as f:
            f.write(self.delim_csv.join(["udps.timestamp", "protocol", "bidirectional_first_seen_ms", "src_ip", "src_port", "dst_ip", "dst_port"]) + "\n")

    def on_init(self, packet, flow):
        flow.udps.start_interval = packet.time
        flow.udps.end_interval = packet.time + self.t_lim * 1000
        flow.udps.timestamp = packet.time
    
    def on_update(self, packet, flow):
        if flow.protocol != 6 and flow.protocol != 17:
            if packet.time > flow.udps.end_interval:
                flow.udps.timestamp = packet.time
                
                valores = [str(flow.udps.timestamp), str(flow.protocol), str(flow.bidirectional_first_seen_ms), str(flow.src_ip), str(flow.src_port), str(flow.dst_ip), str(flow.dst_port)]
                with open(self.full_output, "a") as f:
                    f.write(self.delim_csv.join(valores)+"\n")
                    
                flow.udps.start_interval = packet.time
                flow.udps.end_interval = packet.time + self.t_lim * 1000
    
    def on_expire(self, flow):
        if flow.protocol != 6 and flow.protocol != 17:
            valores = [str(flow.udps.timestamp), str(flow.protocol), str(flow.bidirectional_first_seen_ms), str(flow.src_ip), str(flow.src_port), str(flow.dst_ip), str(flow.dst_port)]
            with open(self.full_output, "a") as f:
                f.write(self.delim_csv.join(valores)+"\n")

class CSVGeneratorV1(NFPlugin):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        with open(self.full_output, "w") as f:
            f.write(self.delim_csv.join(["udps.timestamp", "udps.protocol", "bidirectional_first_seen_ms", "src_ip", "src_port", "dst_ip", "dst_port", "udps.src2dst_last", "udps.dst2src_last", "udps.src2dst_pkts_data", "udps.dst2src_pkts_data", "src2dst_bytes", "dst2src_bytes", "udps.diff_dst_src_first"]) + "\n")

    def on_init(self, packet, flow):
        if flow.protocol == 6:
            flow.udps.protocol = "tcp"
        elif flow.protocol == 17:
            flow.udps.protocol = "udp"
        elif flow.protocol == 1:
            flow.udps.protocol = "icmp"
        elif flow.protocol == 89:
            flow.udps.protocol = "ospf"
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
    
    def on_update(self, packet, flow):
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
            flow.udps.timestamp = packet.time
            
            valores = [str(flow.udps.timestamp), str(flow.udps.protocol), str(flow.bidirectional_first_seen_ms), str(flow.src_ip), str(flow.src_port), str(flow.dst_ip), str(flow.dst_port), str(flow.udps.src2dst_last), str(flow.udps.dst2src_last), str(flow.udps.src2dst_pkts_data), str(flow.udps.dst2src_pkts_data), str(flow.src2dst_bytes), str(flow.dst2src_bytes), str(flow.udps.diff_dst_src_first)]
            with open(self.full_output, "a") as f:
                f.write(self.delim_csv.join(valores)+"\n")
                
            flow.udps.start_interval = packet.time
            flow.udps.end_interval = packet.time + self.t_lim * 1000
    
    def on_expire(self, flow):
        valores = [str(flow.udps.timestamp), str(flow.udps.protocol), str(flow.bidirectional_first_seen_ms), str(flow.src_ip), str(flow.src_port), str(flow.dst_ip), str(flow.dst_port), str(flow.udps.src2dst_last), str(flow.udps.dst2src_last), str(flow.udps.src2dst_pkts_data), str(flow.udps.dst2src_pkts_data), str(flow.src2dst_bytes), str(flow.dst2src_bytes), str(flow.udps.diff_dst_src_first)]
        with open(self.full_output, "a") as f:
            f.write(self.delim_csv.join(valores)+"\n")
                
class CSVGeneratorV2(NFPlugin):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        with open(self.full_output, "w") as f:
            f.write(self.delim_csv.join(["udps.timestamp", "udps.protocol", "bidirectional_first_seen_ms", "src_ip", "src_port", "dst_ip", "dst_port", "udps.src2dst_last", "udps.dst2src_last", "udps.src2dst_pkts_data", "udps.dst2src_pkts_data", "src2dst_bytes", "dst2src_bytes", "udps.diff_dst_src_first", "udps.dst2src_packets_per_second_interval", "udps.src2dst_packets_per_second_interval", "udps.dst2src_bytes_per_second_interval", "udps.src2dst_bytes_per_second_interval", "udps.dst2src_packets_in_interval", "udps.src2dst_packets_in_interval", "udps.dst2src_bytes_in_interval", "udps.src2dst_bytes_in_interval"]) + "\n")

    def on_init(self, packet, flow):
        if flow.protocol == 6:
            flow.udps.protocol = "tcp"
        elif flow.protocol == 17:
            flow.udps.protocol = "udp"
        elif flow.protocol == 1:
            flow.udps.protocol = "icmp"
        elif flow.protocol == 89:
            flow.udps.protocol = "ospf"
        else:
            flow.udps.protocol = flow.protocol
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
    
    def on_update(self, packet, flow):
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
            
            valores = [str(flow.udps.timestamp), str(flow.udps.protocol), str(flow.bidirectional_first_seen_ms), str(flow.src_ip), str(flow.src_port), str(flow.dst_ip), str(flow.dst_port), str(flow.udps.src2dst_last), str(flow.udps.dst2src_last), str(flow.udps.src2dst_pkts_data), str(flow.udps.dst2src_pkts_data), str(flow.src2dst_bytes), str(flow.dst2src_bytes), str(flow.udps.diff_dst_src_first), str(flow.udps.dst2src_packets_per_second_interval), str(flow.udps.src2dst_packets_per_second_interval), str(flow.udps.dst2src_bytes_per_second_interval), str(flow.udps.src2dst_bytes_per_second_interval), str(flow.udps.dst2src_packets_in_interval), str(flow.udps.src2dst_packets_in_interval), str(flow.udps.dst2src_bytes_in_interval), str(flow.udps.src2dst_bytes_in_interval)]
            with open(self.full_output, "a") as f:
                f.write(self.delim_csv.join(valores)+"\n")
                
            flow.udps.start_interval = packet.time
            flow.udps.end_interval = packet.time + self.t_lim * 1000
            flow.udps.last_src2dst_pkts_data = flow.udps.src2dst_pkts_data
            flow.udps.last_dst2src_pkts_data = flow.udps.dst2src_pkts_data
            flow.udps.last_src2dst_bytes = flow.src2dst_bytes
            flow.udps.last_dst2src_bytes = flow.dst2src_bytes
    
    def on_expire(self, flow):
        valores = [str(flow.udps.timestamp), str(flow.udps.protocol), str(flow.bidirectional_first_seen_ms), str(flow.src_ip), str(flow.src_port), str(flow.dst_ip), str(flow.dst_port), str(flow.udps.src2dst_last), str(flow.udps.dst2src_last), str(flow.udps.src2dst_pkts_data), str(flow.udps.dst2src_pkts_data), str(flow.src2dst_bytes), str(flow.dst2src_bytes), str(flow.udps.diff_dst_src_first), str(flow.udps.dst2src_packets_per_second_interval), str(flow.udps.src2dst_packets_per_second_interval), str(flow.udps.dst2src_bytes_per_second_interval), str(flow.udps.src2dst_bytes_per_second_interval), str(flow.udps.dst2src_packets_in_interval), str(flow.udps.src2dst_packets_in_interval), str(flow.udps.dst2src_bytes_in_interval), str(flow.udps.src2dst_bytes_in_interval)]
        with open(self.full_output, "a") as f:
            f.write(self.delim_csv.join(valores)+"\n")

class CSVGeneratorV3(NFPlugin):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        with open(self.full_output, "w") as f:
            f.write(self.delim_csv.join(["udps.timestamp", "udps.protocol", "bidirectional_first_seen_ms", "src_ip", "src_port", "dst_ip", "dst_port", "udps.src2dst_last", "udps.dst2src_last", "udps.src2dst_pkts_data", "udps.dst2src_pkts_data", "src2dst_bytes", "dst2src_bytes", "udps.diff_dst_src_first", "udps.dst2src_packets_per_second_interval", "udps.src2dst_packets_per_second_interval", "udps.dst2src_bytes_per_second_interval", "udps.src2dst_bytes_per_second_interval", "udps.dst2src_packets_in_interval", "udps.src2dst_packets_in_interval", "udps.dst2src_bytes_in_interval", "udps.src2dst_bytes_in_interval", "udps.fiat_mean", "udps.fiat_std", "udps.fiat_min", "udps.fiat_max", "udps.biat_mean", "udps.biat_std", "udps.biat_min", "udps.biat_max", "udps.flowiat_mean", "udps.flowiat_std", "udps.flowiat_min", "udps.flowiat_max"]) + "\n")

    def on_init(self, packet, flow):
        flow.udps.start_interval = packet.time
        flow.udps.end_interval = packet.time + self.t_lim * 1000
        flow.udps.timestamp = packet.time
        flow.udps.src2dst_last = 0
        flow.udps.dst2src_last = 0
        flow.udps.src2dst_pkts_data = 0
        flow.udps.dst2src_pkts_data = 0
        if flow.dst2src_first_seen_ms and flow.src2dst_first_seen_ms:
            flow.udps.diff_dst_src_first = flow.dst2src_first_seen_ms - flow.src2dst_first_seen_ms
        else:
            flow.udps.diff_dst_src_first = 0
        
        flow.udps.fiat = []
        flow.udps.fiat_mean = 0
        flow.udps.fiat_std = 0
        flow.udps.fiat_min = 0
        flow.udps.fiat_max = 0
        
        flow.udps.biat = []
        flow.udps.biat_mean = 0
        flow.udps.biat_std = 0
        flow.udps.biat_min = 0
        flow.udps.biat_max = 0
        
        flow.udps.flowiat = []
        flow.udps.flowiat_mean = 0
        flow.udps.flowiat_std = 0
        flow.udps.flowiat_min = 0
        flow.udps.flowiat_max = 0
        
        flow.udps.active = []
        flow.udps.active_mean = 0
        flow.udps.active_std = 0
        flow.udps.active_min = 0
        flow.udps.active_max = 0
        
        flow.udps.idle = []
        flow.udps.idle_mean = 0
        flow.udps.idle_std = 0
        flow.udps.idle_min = 0
        flow.udps.idle_max = 0
        
        if packet.payload_size > 0:
            if packet.direction == 0:
                flow.udps.src2dst_pkts_data = 1
                flow.udps.dst2src_pkts_data = 0
                flow.udps.fiat_last = packet.time
                flow.udps.biat_last = 0
            else:
                flow.udps.dst2src_pkts_data = 1
                flow.udps.src2dst_pkts_data = 0
                flow.udps.biat_last = packet.time
                flow.udps.fiat_last = 0
            flow.udps.flowiat_last = packet.time
            flow.udps.active_last = packet.time
            flow.udps.active_first = packet.time
        else:
            flow.udps.fiat_last = 0
            flow.udps.biat_last = 0
            flow.udps.flowiat_last = 0
            flow.udps.active_last = 0
            flow.udps.active_first = 0
        
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
    
    def on_update(self, packet, flow):
        if packet.payload_size > 0:
            if packet.direction == 0:
                flow.udps.src2dst_last = packet.time - flow.src2dst_first_seen_ms
                flow.udps.src2dst_pkts_data += 1
                if flow.udps.fiat_last:
                    flow.udps.fiat.append(packet.time - flow.udps.fiat_last)
                flow.udps.fiat_last = packet.time
            else:
                flow.udps.dst2src_last = packet.time - flow.dst2src_first_seen_ms
                flow.udps.dst2src_pkts_data += 1
                if flow.udps.biat_last:
                    flow.udps.biat.append(packet.time - flow.udps.biat_last)
                flow.udps.biat_last = packet.time
            if flow.udps.flowiat_last:
                flow.udps.flowiat.append(packet.time - flow.udps.flowiat_last)
                if diff >= self.custom_idle_timeout:
                        flow.udps.active = np.append(flow.udps.active, flow.udps.active_last - flow.udps.active_first)
                        flow.udps.idle = np.append(flow.udps.idle, diff)
                        flow.udps.active_first = packet.time
                        flow.udps.active_last = packet.time
            flow.udps.flowiat_last = packet.time
            
        if packet.time > flow.udps.end_interval:
            if flow.udps.fiat:
                flow.udps.fiat_mean = np.mean(flow.udps.fiat)
                flow.udps.fiat_std = np.std(flow.udps.fiat)
                flow.udps.fiat_min = np.min(flow.udps.fiat)
                flow.udps.fiat_max = np.max(flow.udps.fiat)
            
            if flow.udps.biat:
                flow.udps.biat_mean = np.mean(flow.udps.biat)
                flow.udps.biat_std = np.std(flow.udps.biat)
                flow.udps.biat_min = np.min(flow.udps.biat)
                flow.udps.biat_max = np.max(flow.udps.biat)
                
            if flow.udps.flowiat:
                flow.udps.flowiat_mean = np.mean(flow.udps.flowiat)
                flow.udps.flowiat_std = np.std(flow.udps.flowiat)
                flow.udps.flowiat_min = np.min(flow.udps.flowiat)
                flow.udps.flowiat_max = np.max(flow.udps.flowiat)
                
            if flow.udps.active:
                flow.udps.active_mean = np.mean(flow.udps.active)
                flow.udps.active_std = np.std(flow.udps.active)
                flow.udps.active_min = np.min(flow.udps.active)
                flow.udps.active_max = np.max(flow.udps.active)
                
            if flow.udps.idle:
                flow.udps.idle_mean = np.mean(flow.udps.idle)
                flow.udps.idle_std = np.std(flow.udps.idle)
                flow.udps.idle_min = np.min(flow.udps.idle)
                flow.udps.idle_max = np.max(flow.udps.idle)
        
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
            
            valores = [str(flow.udps.timestamp), str(flow.protocol), str(flow.bidirectional_first_seen_ms), str(flow.src_ip), str(flow.src_port), str(flow.dst_ip), str(flow.dst_port), str(flow.udps.src2dst_last), str(flow.udps.dst2src_last), str(flow.udps.src2dst_pkts_data), str(flow.udps.dst2src_pkts_data), str(flow.src2dst_bytes), str(flow.dst2src_bytes), str(flow.udps.diff_dst_src_first), str(flow.udps.dst2src_packets_per_second_interval), str(flow.udps.src2dst_packets_per_second_interval), str(flow.udps.dst2src_bytes_per_second_interval), str(flow.udps.src2dst_bytes_per_second_interval), str(flow.udps.dst2src_packets_in_interval), str(flow.udps.src2dst_packets_in_interval), str(flow.udps.dst2src_bytes_in_interval), str(flow.udps.src2dst_bytes_in_interval), str(flow.udps.fiat_mean), str(flow.udps.fiat_std), str(flow.udps.fiat_min), str(flow.udps.fiat_max), str(flow.udps.biat_mean), str(flow.udps.biat_std), str(flow.udps.biat_min), str(flow.udps.biat_max), str(flow.udps.flowiat_mean), str(flow.udps.flowiat_std), str(flow.udps.flowiat_min), str(flow.udps.flowiat_max), str(flow.udps.active_mean), str(flow.udps.active_std), str(flow.udps.active_min), str(flow.udps.active_max), str(flow.udps.idle_mean), str(flow.udps.idle_std), str(flow.udps.idle_min), str(flow.udps.idle_max)]
            with open(self.full_output, "a") as f:
                f.write(self.delim_csv.join(valores)+"\n")
                
            flow.udps.start_interval = packet.time
            flow.udps.end_interval = packet.time + self.t_lim * 1000
            flow.udps.last_src2dst_pkts_data = flow.udps.src2dst_pkts_data
            flow.udps.last_dst2src_pkts_data = flow.udps.dst2src_pkts_data
            flow.udps.last_src2dst_bytes = flow.src2dst_bytes
            flow.udps.last_dst2src_bytes = flow.dst2src_bytes
    
    def on_expire(self, flow):
        flow.udps.active = np.append(flow.udps.active, flow.udps.active_last - flow.udps.active_first)
        flow.udps.active_mean = np.mean(flow.udps.active)
        flow.udps.active_std = np.std(flow.udps.active)
        flow.udps.active_min = np.min(flow.udps.active)
        flow.udps.active_max = np.max(flow.udps.active)
        
        if flow.udps.idle.size > 0:
            flow.udps.idle_mean = np.mean(flow.udps.idle)
            flow.udps.idle_std = np.std(flow.udps.idle)
            flow.udps.idle_min = np.min(flow.udps.idle)
            flow.udps.idle_max = np.max(flow.udps.idle)
    
        valores = [str(flow.udps.timestamp), str(flow.protocol), str(flow.bidirectional_first_seen_ms), str(flow.src_ip), str(flow.src_port), str(flow.dst_ip), str(flow.dst_port), str(flow.udps.src2dst_last), str(flow.udps.dst2src_last), str(flow.udps.src2dst_pkts_data), str(flow.udps.dst2src_pkts_data), str(flow.src2dst_bytes), str(flow.dst2src_bytes), str(flow.udps.diff_dst_src_first), str(flow.udps.dst2src_packets_per_second_interval), str(flow.udps.src2dst_packets_per_second_interval), str(flow.udps.dst2src_bytes_per_second_interval), str(flow.udps.src2dst_bytes_per_second_interval), str(flow.udps.dst2src_packets_in_interval), str(flow.udps.src2dst_packets_in_interval), str(flow.udps.dst2src_bytes_in_interval), str(flow.udps.src2dst_bytes_in_interval), str(flow.udps.fiat_mean), str(flow.udps.fiat_std), str(flow.udps.fiat_min), str(flow.udps.fiat_max), str(flow.udps.biat_mean), str(flow.udps.biat_std), str(flow.udps.biat_min), str(flow.udps.biat_max), str(flow.udps.flowiat_mean), str(flow.udps.flowiat_std), str(flow.udps.flowiat_min), str(flow.udps.flowiat_max), str(flow.udps.active_mean), str(flow.udps.active_std), str(flow.udps.active_min), str(flow.udps.active_max), str(flow.udps.idle_mean), str(flow.udps.idle_std), str(flow.udps.idle_min), str(flow.udps.idle_max)]
        with open(self.full_output, "a") as f:
            f.write(self.delim_csv.join(valores)+"\n")
            

def generate(pcap_path, pcap_filename, t_lim, delim_csv, output_path, output_filename, monitored_device, interface, n_round, custom_idle_timeout):
    # Setup LOGGER
    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.INFO)
    logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    consoleHandler = logging.StreamHandler(stdout)
    consoleHandler.setFormatter(logFormatter)
    LOGGER.addHandler(consoleHandler)
    
    archivo_pcap = path.join(pcap_path, pcap_filename)
    
    if output_filename == "":
        output_filename = f"{monitored_device}_{interface}.csv"
    
    full_output = path.join(output_path, output_filename)
    extension = full_output.rfind(".")
    full_output = full_output[:extension] + "_" + datetime.now().strftime("%Y-%m-%d_%H.%M.%S") + full_output[extension:]
    if n_round == 1:
        udps = [CSVGeneratorV1(t_lim=t_lim, full_output=full_output, delim_csv=delim_csv)]
    elif n_round == 2:
        udps = [CSVGeneratorV2(t_lim=t_lim, full_output=full_output, delim_csv=delim_csv)]
    elif n_round == 3:
        udps = [CSVGeneratorV3(t_lim=t_lim, full_output=full_output, delim_csv=delim_csv, custom_idle_timeout=custom_idle_timeout)]
    else:
        udps = [Prueba(t_lim=t_lim, full_output=full_output, delim_csv=delim_csv)]
    streamer = NFStreamer(source=archivo_pcap, accounting_mode=3, udps=udps, statistical_analysis=False, splt_analysis=0, idle_timeout=120, n_meters=23, n_dissections=0)
    
    start_time = timeit.default_timer()
    counter = 0
    active = []
    idle = []
    fiat = []
    biat = []
    flowiat = []
    for flow in streamer:
        counter += 1
        LOGGER.info(counter)
        '''active.append(len(flow.udps.active))
        idle.append(len(flow.udps.idle))'''
        '''fiat.append(len(flow.udps.fiat))
        biat.append(len(flow.udps.biat))'''
        #flowiat.append(len(flow.udps.flowiat))
        
    
    '''LOGGER.info(np.mean(active))
    LOGGER.info(np.mean(idle))'''
    '''LOGGER.info(np.mean(fiat))
    LOGGER.info(np.mean(biat))'''
    #LOGGER.info(np.mean(flowiat))
        
    elapsed_time = timeit.default_timer() - start_time
    
    LOGGER.info(f"{int(elapsed_time//60)}m {int(elapsed_time%60)}s")
