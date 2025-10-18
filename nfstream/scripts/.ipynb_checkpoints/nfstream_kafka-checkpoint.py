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

from scapy.all import PcapReader
#import pyshark

# ----------------------
#METERS=1
#METERS=4
METERS= 40 # 6 # 30 # 8 # 35
#METERS=10

#Si meter recoje paquete con estos sec de adelanto se para
#GAP=2.0
GAP= 0.00001 #2.5 # 0.01 #0.25 # 0.5 # 5.0 # 2.0 # 0.5 #15.0 # 0.5
# ----------------------

FEATURE_NAMES= "udps.protocol,udps.src2dst_last,udps.dst2src_last,udps.src2dst_pkts_data,udps.dst2src_pkts_data,src2dst_bytes,dst2src_bytes,udps.diff_dst_src_first"


# --------------
# Alberto. Intentar que el nfsstream no vaya demasiado adelantado para evitar que los paquetes se desordenen
#
from datetime import datetime
#import time
from time import mktime, sleep

#FIRST_TIMESTAMP_PCAP_secs = 1720708093.927968  # original alberto
#FIRST_TIMESTAMP_PCAP_secs = 1728552801.0  # 1_min_traffic.pcap
#FIRST_TIMESTAMP_PCAP_secs = 1728556659.405  # 5_min_traffic.pcap
#FIRST_TIMESTAMP_PCAP_secs = 1728897634.195 # nuevo_pcap_7_min.pcap
#FIRST_TIMESTAMP_PCAP_secs = 1729846587.838

#FIRST_TIMESTAMP_PCAP_secs = 1729941990.069767000 # repetidor_pcap_8_min_alb_1.pcap 

# first_timestamp_nfstream = datetime.now() + timedelta(seconds=SECONDS_DELAY_GRAFANA)
# adjusted_time = timedelta(seconds=metadata['timestamp'] - first_timestamp_real) + first_timestamp_nfstream

#now = datetime.now()
#first_timestamp_nfstream_secs = int(mktime(now.timetuple()))
#first_timestamp_nfstream_secs=0.0

# Setup LOGGER
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

# --------------






class KafkaPublisherV1(NFPlugin):
    def __init__(self, **kwargs):
        super().__init__(**kwargs) #aqui se actualizan todos los self.XXX con los valores de kwargs['XXX']

        LOGGER.info (f"val de self.modo_captura:{self.capture_mode}:")
        
        self.primer_pkt_recibido=False
        
        self.first_timestamp_nfstream_secs=time()
        
        if self.capture_mode in [ "pcap", "pcap-freno"] :
            #self.FIRST_TIMESTAMP_PCAP_secs= FIRST_TIMESTAMP_PCAP_secs
            self.FIRST_TIMESTAMP_PCAP_secs= self.first_packet_timestamp #   kwargs['first_packet_timestamp']
            #LOGGER.info (f"FIRST_TIMESTAMP_PCAP_secs(cte):{FIRST_TIMESTAMP_PCAP_secs} self.first_packet_timestamp(lee de pcap):{self.first_packet_timestamp}")
        else: #network
            self.FIRST_TIMESTAMP_PCAP_secs =self.first_timestamp_nfstream_secs
            print ("capture netwok. first timestamp",)
            
        
        LOGGER.info (f"FIRST_TIMESTAMP_PCAP_secs:{datetime.fromtimestamp(self.FIRST_TIMESTAMP_PCAP_secs)} first_timestamp_nfstream_secs:{datetime.fromtimestamp(self.first_timestamp_nfstream_secs)}")

        self.tot_snapshots_sent=0

        self.l_tiempos_diff=[]

        self.k_subsampling=0.6 # 0.5 #0.6

        '''
        self.nom_file_snapshots="snapshots.csv"
        cabecera="t_envio_kafka(datetime)*t_envio_kafka(timestamp)*last_packet_timestamp*t_preparacion_snap_nfstream*t_ini_intervalo*t_fin_intervalo*ip_src*ip_dst*port_src*port_dst*protocol"
        with open (self.nom_file_snapshots,"w") as file_snapshots:
            file_snapshots.write(cabecera+"\n")
        '''
        
        LOGGER.info (f"tlim:{self.t_lim}:")

        # Volcados de CSV

        # Debugging
        '''
        self.file_dump = open('output_pcap_csv.txt', 'w')
        cabecera="ipo*ipdst*x*y*z"
        self.file_dump.write(cabecera + '\n')
        '''

        '''
        self.output=open(f"/app/pcaps/output_pcap_repetidor_7_min{str(datetime.now()).replace(' ', '_').replace(':', '.')}.csv", "w")
        self.output.write("src_ip,dst_ip,src_port,dst_port,protocol,first,timestamp,flow_pkts\n")
        '''

        #
        '''        
        self.nom_file_snapshots2=f"snapshots_{random.randint(1, 1000)}.csv"
        self.file_snapshots = open(self.nom_file_snapshots2, 'w')
        cabecera2="t_ahora_before_sent*t_ahora_after_sent*t_pcap_adjust*src_ip*dst_ip*src_port*dst_port*protocol"
        self.file_snapshots.write(cabecera2+"\n")
        LOGGER.info (f"nombre del fichero:{self.nom_file_snapshots2}")
        '''
        
        # Volcado de snapshots
        self.volcar_snapshots_csv = self.volcar_snapshots_csv.strip().lower() in ["true", "1"]
        #if self.volcar_snapshots_csv :
        #nom_file_snapshots=f"snapshots_{random.randint(1, 1000)}.csv"
        nom_file_snapshots=f"{self.pcap_filename}_snapshots.csv"
        LOGGER.info (f"Nom fich snapshots: {nom_file_snapshots}")
        self.file_snapshots= open("/app/snapshots/"+nom_file_snapshots, "w")
        self.file_snapshots.write(self.delim_csv.join(["udps.timestamp", "udps.protocol", "bidirectional_first_seen_ms", "src_ip", "src_port", "dst_ip", "dst_port", "udps.src2dst_last", "udps.dst2src_last", "udps.src2dst_pkts_data", "udps.dst2src_pkts_data", "src2dst_bytes", "dst2src_bytes", "udps.diff_dst_src_first"]) + "\n")
        self.file_snapshots.flush()
        

    def on_init(self, packet, flow):
        #LOGGER.info(f"ON_INIT: SRC_IP:{packet.src_ip}, SRC_PORT:{packet.src_port}, DST_IP:{packet.dst_ip}, DST_PORT:{packet.dst_port}, TIMESTAMP:{packet.time}")
        flow.udps.start_interval = packet.time
        flow.udps.end_interval = packet.time + self.t_lim * 1000 #t_lim esta expresado en segundos y hay que pasarlo a milisegundos
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
        
        """
        with open('prueba.txt', 'w') as file:
            content = 'timestamp*timestamp_nfstream'
            file.write(content)
        """
        
    
    
    def on_update(self, packet, flow, producer, flush):

        #LOGGER.info ("inicio on_update")
        #LOGGER.info(f"SRC_IP:{packet.src_ip}, SRC_PORT:{packet.src_port}, DST_IP:{packet.dst_ip}, DST_PORT:{packet.dst_port}")

        if packet.payload_size > 0: 
            #LOGGER.info("TIENE PAYLOAD")
            if packet.direction == 0:
                flow.udps.src2dst_last = packet.time - flow.src2dst_first_seen_ms
                flow.udps.src2dst_pkts_data += 1
            else:
                flow.udps.dst2src_last = packet.time - flow.dst2src_first_seen_ms
                flow.udps.dst2src_pkts_data += 1
                
        #LOGGER.info (f"XXX (pkt) packet.time/1000 {packet.time/1000} timestamp_snapshot: {flow.udps.timestamp}")
        #LOGGER.info (f"FIRST_TIMESTAMP_PCAP_secs {self.FIRST_TIMESTAMP_PCAP_secs}")
                
        if packet.time > flow.udps.end_interval:   
               
            # ---
            # Se ha terminado el intervalo -> Enviar snapshot
            #

            #LOGGER.info("INTERVALO EXCEDIDO")
            if flow.dst2src_first_seen_ms and flow.src2dst_first_seen_ms:
                flow.udps.diff_dst_src_first = flow.dst2src_first_seen_ms - flow.src2dst_first_seen_ms
            
            #if random.random() < self.k_subsampling:
            flow.udps.timestamp = packet.time
            # datetime.timestamp(datetime.now()) es lo mismo que time.time()
            snapshot = {"data": {"features": {"udps.protocol": flow.protocol,
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
                                 "timestamp_nfstream": time(),
                                 "timestamp_ini_interval": flow.udps.start_interval,
                                 "timestamp_fin_interval": flow.udps.end_interval,
                                 "monitored_device": self.monitored_device,
                                 "interface": self.interface,
                                 "connection_id": {"src_ip": flow.src_ip,
                                                   "dst_ip": flow.dst_ip,
                                                   "src_port": flow.src_port,
                                                   "dst_port": flow.dst_port,
                                                   "protocol": flow.protocol,
                                                   "first": flow.bidirectional_first_seen_ms
                                                  },
                                 "flow_pkts": flow.udps.src2dst_pkts_data + flow.udps.dst2src_pkts_data,
                                 "flow_bytes": flow.src2dst_bytes + flow.dst2src_bytes
                                }
                   }
            
            conn_id = f"{flow.src_ip}, {flow.dst_ip}, {flow.src_port}, {flow.dst_port}, {flow.protocol}"
            conn_id_x = f"{flow.src_ip}*{flow.dst_ip}*{flow.src_port}*{flow.dst_port}*{flow.protocol}"
                        
            # packet.time expresado en milisegundos. Todos los tiempos dentro de nfstream van en milisegundos
            pkt_time_adjusted_secs= (packet.time/1000.0 - self.FIRST_TIMESTAMP_PCAP_secs) + self.first_timestamp_nfstream_secs
            now_secs=time()
            diff_secs= pkt_time_adjusted_secs - now_secs
            #LOGGER.info(f"diff_secs:{diff_secs}")
            
            '''
            LOGGER.info (f"snapshot.time {snapshot['metadata']['timestamp']}")
            LOGGER.info (f"FIRST_TIMESTAMP_PCAP_secs {self.FIRST_TIMESTAMP_PCAP_secs}")
            LOGGER.info (f"first_timestamp_nfstream_secs {self.first_timestamp_nfstream_secs}")
            LOGGER.info (f"pkt_time_adjusted_secs {pkt_time_adjusted_secs}")
            LOGGER.info (f"now_secs {now_secs}")
            LOGGER.info (f"diff_secs {diff_secs}")
            '''
            
            # Si el snapshot va muy por delante, frenamos para que no se desordene
            
            if self.capture_mode=="pcap-freno":
                if diff_secs > GAP :
                    # todavia no toca enviarlos
                    #LOGGER.info (f"ZZZZ diff_secs:{diff_secs} > GAP:{GAP}")
                    #LOGGER.info (f"FIRST_TIMESTAMP_PCAP_secs:{datetime.fromtimestamp(self.FIRST_TIMESTAMP_PCAP_secs)} first_timestamp_nfstream_secs:{datetime.fromtimestamp(self.first_timestamp_nfstream_secs)}")
                    #LOGGER.info (f"pkt_time:{datetime.fromtimestamp(packet.time/1000.0)}, pkt_time_adjusted_secs:{datetime.fromtimestamp(pkt_time_adjusted_secs)}, now_secs:{datetime.fromtimestamp(now_secs)}")
                    
                    #t_freno=(diff_secs-GAP)*0.9
                    t_freno=(diff_secs-GAP)
                    #t_freno=min(2*GAP, (diff_secs-GAP) )
                    #*0.2 # Espero algo de tiempo 20% del adelantado, pero no todo para no frenar innecesariamente a este proceso que atiende a otros flujos
                    #LOGGER.info (f"Freno:{t_freno}")
                    #sleep(float(t_freno))
                    sleep(t_freno)
                    #self.l_tiempos_diff.append(f"{diff_secs}*{datetime.fromtimestamp(pkt_time_adjusted_secs)}*{datetime.fromtimestamp(now_secs)}*{flow.src_ip}*{flow.dst_ip}*{flow.src_port}*{flow.dst_port}*{flow.protocol}")
                    #LOGGER.info (f"self.l_tiempos_diff:{self.l_tiempos_diff}")
            
                       
            
            # ----
            # Envia SNAPSHOT
            # ----
            #if random.random() < self.k_subsampling:
            #if
            t_ahora_before_sent=time()
            snapshot["metadata"]["timestamp_nfstream"]= t_ahora_before_sent # dejo el momento en el que entra en kafka con el send
            # Debug
            #LOGGER.info(f"SNAPSHOT ANTES DE ENVIAR:{snapshot}")
            producer.send(topic=self.topic, value=snapshot, headers=[("version", "v1".encode("utf-8")), ("features_names", FEATURE_NAMES.encode("utf-8"))], timestamp_ms=int(t_ahora_before_sent * 1000), key=conn_id)
            
            '''
            connection_id_values = [str(i) for i in snapshot["metadata"]["connection_id"].values()]
            timestamp_value = str(snapshot["metadata"]["timestamp"])
            flow_pkts_value = str(snapshot["metadata"]["flow_pkts"])
            self.output.write(",".join(connection_id_values + [timestamp_value, flow_pkts_value]) + "\n")
            '''
            #self.output.write(",".join([str(i) for i in snapshot["metadata"]["connection_id"].values()]) + "\n")
            
            if self.volcar_snapshots_csv:
                #.              "udps.timestamp",      "udps.protocol",      "bidirectional_first_seen_ms",        "src_ip",          "src_port",        "dst_ip",          "dst_port",        "udps.src2dst_last",         "udps.dst2src_last", "udps.src2dst_pkts_data",               "udps.dst2src_pkts_data",         "src2dst_bytes",        "dst2src_bytes",         "udps.diff_dst_src_first"]  
                valores = [str(flow.udps.timestamp), str(flow.protocol), str(flow.bidirectional_first_seen_ms), str(flow.src_ip), str(flow.src_port), str(flow.dst_ip), str(flow.dst_port), str(flow.udps.src2dst_last), str(flow.udps.dst2src_last), str(flow.udps.src2dst_pkts_data), str(flow.udps.dst2src_pkts_data), str(flow.src2dst_bytes), str(flow.dst2src_bytes), str(flow.udps.diff_dst_src_first)]
                #with open(self.full_output, "a") as f:
                self.file_snapshots.write(self.delim_csv.join(valores)+"\n")
                #self.file_snapshots.flush()
                
            #LOGGER.info("SNAPSHOT ENVIADO")

            '''
            # debug
            t_ahora_after_sent=time()
            linea=f"{t_ahora_before_sent}*{t_ahora_after_sent}*{pkt_time_adjusted_secs}*{conn_id_x}"
            self.file_snapshots.write (linea+"\n")
            #
            '''
            
            
            # cabecera="t_envio_kafka(datetime)*t_envio_kafka(timestamp)*last_packet_timestamp*t_preparacion_snap_nfstream*t_ini_intervalo*t_fin_intervalo*ip_src*ip_dst*port_src*port_dst*protocol"
            #snapshot = f'{datetime.now()}*{time()}*{snapshot["metadata"]["timestamp"]/1000.0}*{snapshot["metadata"]["timestamp_nfstream"]}*{snapshot["metadata"]["timestamp_ini_interval"]/1000.0}*{snapshot["metadata"]["timestamp_fin_interval"]/1000.0}*{flow.src_ip}*{flow.dst_ip}*{flow.src_port}*{flow.dst_port}*{flow.protocol}'
            
            self.tot_snapshots_sent+=1

            # Demo
            if self.tot_snapshots_sent % 30000 == 0:
                LOGGER.info (f"Snapshots sent to AI-Inference: {self.tot_snapshots_sent}")
            
            #with open (self.nom_file_snapshots,"a") as file_snapshots:
            #    file_snapshots.write(snapshot+"\n")
            # end if
            #LOGGER.info (f"snapshot:{snapshot}")  
        
            # Cambia intervalo
            flow.udps.start_interval = packet.time
            flow.udps.end_interval = packet.time + self.t_lim * 1000
            
        # end if packet.time > flow.udps.end_interval:   
        
        # El problema con esta activacion es que se le pasa activado a un on_update de un flujo al recibir un paquete. Si ese paquete no genera salto de intervalo el flush hay que ejecutarlo si o si  porque si no se pierde 
        if flush:
            producer.flush()
            self.file_snapshots.flush()
    
    def on_expire(self, flow, producer, flush):
        self.file_snapshots.flush()
        return
        
        #self.file_dump.flush()
        #conn_id = f"{flow.src_ip}, {flow.dst_ip}, {flow.src_port}, {flow.dst_port}, {flow.protocol}"
        #LOGGER.info(f"KKKK expire. conn_id:{conn_id}")
        #self.file_snapshots.flush()
        
        
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
        
        flow.udps.l_snapshots_pendientes.append (data)
        self.procesa_snapshots_pendientes (flow,producer,flush)
        '''
        producer.send(topic=self.topic, value=data, headers=[("version", "v1".encode("utf-8")), ("features_names", "udps.protocol,udps.src2dst_last,udps.dst2src_last,udps.src2dst_pkts_data,udps.dst2src_pkts_data,src2dst_bytes,dst2src_bytes,udps.diff_dst_src_first".encode("utf-8"))], timestamp_ms=int(time() * 1000))
        #self.logger.info("Snapshot Sent.")
        if flush:
            producer.flush()
            #self.logger.info("Snapshot Sent.")
        '''
                
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

    # Assuming flow and other variables are defined, and you want to create a line from the provided dictionary:

   



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
            
            
            snapshot["metadata"]["timestamp_nfstream"]=time() # dejo el momento en el que entra en kafka con el send
            producer.send(topic=self.topic, value=data, headers=[("version", "v1".encode("utf-8")), ("features_names", "udps.protocol,udps.src2dst_last,udps.dst2src_last,udps.src2dst_pkts_data,udps.dst2src_pkts_data,src2dst_bytes,dst2src_bytes,udps.diff_dst_src_first,udps.dst2src_packets_per_second_interval,udps.src2dst_packets_per_second_interval,udps.dst2src_bytes_per_second_interval,udps.src2dst_bytes_per_second_interval,udps.dst2src_packets_in_interval,udps.src2dst_packets_in_interval,udps.dst2src_bytes_in_interval,udps.src2dst_bytes_in_interval".encode("utf-8"))], timestamp_ms=int(time() * 1000))
            #flush=True
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

        #-- No se ejecuta
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


class KafkaPublisherNetworkInterface(NFPlugin):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    
    def on_update(self, packet, flow, producer, flush):

        LOGGER.info(f"SRC_IP:{packet.src_ip}, SRC_PORT:{packet.src_port}, DST_IP:{packet.dst_ip}, DST_PORT:{packet.dst_port}")

        
    
    def on_expire(self, flow, producer, flush):
        pass

def generate_line_from_dict(data):
    # Extracting all values from the 'features' and 'metadata' sections of the dictionary
    features_values = data["data"]["features"].values()
    metadata_values = data["metadata"].values()
    
    # Extracting values from the 'connection_id' sub-dictionary inside 'metadata'
    connection_id_values = data["metadata"]["connection_id"].values()
    
    # Combine all values into a single list
    all_values = list(features_values) + list(metadata_values) + list(connection_id_values)
    
    # Convert all values to strings and join them with '*'
    line = '*'.join(map(str, all_values))
    
    return line

def start(pcap_path, pcap_filename, t_lim, kafka_url, producer_topic, producer_client_id, monitored_device, interface, n_round, k_subsampling, capture_mode,volcar_snapshots_csv,delim_csv):

    # Setup LOGGER
    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.INFO)
    logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    consoleHandler = logging.StreamHandler(stdout)
    consoleHandler.setFormatter(logFormatter)
    LOGGER.addHandler(consoleHandler)

    archivo_pcap = path.join(pcap_path, pcap_filename)
    if capture_mode in [ "pcap", "pcap-freno"]:  
        with PcapReader(archivo_pcap) as pcap_reader:
            first_packet = next(pcap_reader)  # Lee solo el primer paquete
            first_packet_timestamp = float(first_packet.time)  # Accede al timestamp en epoch
            #first_packet_timestamp = first_packet.time  # Accede al timestamp en epoch
            #LOGGER.info (f"tipo de first_packet_timestamp: {type(first_packet_timestamp)} {type(first_packet.time)}")
            LOGGER.info (f"first_packet_timestamp:{first_packet_timestamp}")
    else:
        first_packet_timestamp = time()

    
    LOGGER.info(archivo_pcap)

    if n_round == 1:
        udps = [KafkaPublisherV1(t_lim=t_lim, topic=producer_topic, monitored_device=monitored_device, interface=interface, logger=LOGGER, k_subsampling=k_subsampling, 
                                 capture_mode=capture_mode, first_packet_timestamp=first_packet_timestamp, volcar_snapshots_csv=volcar_snapshots_csv, delim_csv=delim_csv,
                                 pcap_filename=pcap_filename   )]
    elif n_round == 2:
        udps = [KafkaPublisherV2(t_lim=t_lim, topic=producer_topic, monitored_device=monitored_device, interface=interface, logger=LOGGER)]
    
    #udps = [KafkaPublisherNetworkInterface(logger=LOGGER)]
    if capture_mode in ["pcap", "pcap-freno"] :
        streamer = NFStreamer(source=archivo_pcap, accounting_mode=3, udps=udps, statistical_analysis=False, splt_analysis=0, idle_timeout=120, n_meters=METERS, n_dissections=0, kafka_url=kafka_url)
        #streamer = NFStreamer(source=archivo_pcap, accounting_mode=3, udps=udps, statistical_analysis=False, splt_analysis=0, idle_timeout=120, n_meters=1, n_dissections=0, kafka_url=kafka_url)
        #streamer = NFStreamer(source=archivo_pcap, udps=udps)
    elif capture_mode == "network":
        streamer = NFStreamer(source='enp6s0', accounting_mode=3, udps=udps, statistical_analysis=False, splt_analysis=0, idle_timeout=120, n_meters=METERS, n_dissections=0, kafka_url=kafka_url)
    else:
        LOGGER.info (f"capture_mode desconocido:{capture_mode}")
        return

    
    start_time = timeit.default_timer()
    
    counter = 0
    for flow in streamer:
        counter += 1
        #LOGGER.info(f"Processed Flows: {counter}")
        #LOGGER.info(flow)

    udps[0].file_snapshots.close()
    '''
    udps[0].output.close()
    with open("/app/pcaps/prueba.txt", "w") as f:
        f.write("prueba")
    '''
    LOGGER.info(f"num flows processed:{counter}. tot_snapshots_sent:{udps[0].tot_snapshots_sent}")   
    
    elapsed_time = timeit.default_timer() - start_time
    
    LOGGER.info(f"elapsed time:{int(elapsed_time//60)}m {int(elapsed_time%60)}s")

    LOGGER.info (f"l_tiempos_diff:{udps[0].l_tiempos_diff}")

    # Para poder inspeccionar el contenedor y copiar ficheros, mantenemos vivo el contenedor
    LOGGER.info ("nfstream finalizó. Durmiendo Para poder inspeccionar el contenedor y copiar ficheros")
    while True:
        sleep (5)
        
        
