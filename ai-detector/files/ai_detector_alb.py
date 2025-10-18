from typing import Tuple, Any
import logging
from kafka import KafkaConsumer, KafkaProducer
from sys import stdout, exit
import time
from collections import namedtuple, defaultdict
import signal
import argparse
#import pickle
import dill as pickle
import numpy as np
from datetime import datetime

from collections import namedtuple


# Setup LOGGER
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

T_ESPERA_CONSUMIDOR=0.05
MAX_ESPERAS_SEGUIDAS=10
#MAX_MENSAJES_BUFF=0
MAX_LABELS_BUFF=5

TOT_SNAPSHOTS= 7640000 


ConsumerConfig = namedtuple("ConsumerConfig", ["client_id", "group_id", "topic"])
ProducerConfig = namedtuple("ProducerConfig", ["client_id", "topic"])

class AIDetector:
    def __init__(self, kafka_url: str, producer_client_id: str, consumer_client_id: str, consumer_group_id: str,
                 consumer_topic: str, producer_topic: str):
        LOGGER.info("Initializing AI Detector...")

        self.broker = kafka_url
        LOGGER.info(f"Kafka broker: %s", self.broker)

        self.producer_config = ProducerConfig(producer_client_id, producer_topic)
        self.consumer_config = ConsumerConfig(consumer_client_id, consumer_group_id, consumer_topic)
        
        LOGGER.info("Kafka Producer config: %s", self.producer_config)
        LOGGER.info("Kafka Consumer config: %s", self.consumer_config)

        signal.signal(signal.SIGINT, self.handler)

        self.producer, self.consumer = self.connect_to_kafka()

    def handler(self, num, frame) -> None:
        LOGGER.info("Gracefully stopping...")
        
        self.consumer.close()
        self.producer.close()
        exit()

    def connect_to_kafka(self) -> Tuple[KafkaProducer, KafkaConsumer]:
        """
        Creates and connects a KafkaProducer and a KafkaConsumer instance to the Kafka broker specified in self.broker
        Returns:
            A tuple consisting of the KafkaProducer and KafkaConsumer instances.
        """
        
        LOGGER.info("Attempting to establish connection to Kafka broker %s", self.broker)

        producer = KafkaProducer(bootstrap_servers=self.broker, client_id=self.producer_config.client_id,
                                 value_serializer=lambda x: pickle.dumps(x))
                                 
        consumer = KafkaConsumer(self.consumer_config.topic, bootstrap_servers=self.broker,
                                 client_id=self.consumer_config.client_id,
                                 group_id=self.consumer_config.group_id, value_deserializer=lambda x: pickle.loads(x))

        LOGGER.info("Trying to establish connection to brokers...")
        LOGGER.info("Producer connection status: %s", producer.bootstrap_connected())
        LOGGER.info("Consumer connection status: %s", consumer.bootstrap_connected())

        # Validate if connection to brokers is ready
        if not producer.bootstrap_connected():
            LOGGER.error("%s: Producer failed to connect to Kafka brokers.", __name__)
            exit()
            
        if not consumer.bootstrap_connected():
            LOGGER.error("%s: Consumer failed to connect to brokers.", __name__)
            exit()

        return producer, consumer

    def __send_data(self, labels: str, metadatas: Any, version: str) -> None:
        """
        Publishes the predicted label to self.producer_topic.
        Args:
            label: the predicted class name
            ml_confidence: the model prediction confidence score
            metadata: connection metadata
            version (str): inference version implemented
        """
            
        data = {"data": labels, "metadata": metadatas}
        
        try:
            self.producer.send(topic=self.producer_config.topic, value=data, headers=[("version", version.encode("utf-8"))],
                               timestamp_ms=time.time_ns() // 1000000)
            self.producer.flush()
            
            #LOGGER.info("Sent labels.")
            
        except Exception as e:
            LOGGER.error("%s: Error sending inference probabilities to Kafka cluster: %s", __name__, e)
            
    def add_ml_confidence_to_metadata(self, metadatas: Any, ml_confidence: float) -> dict:
        for i in range(len(metadatas)):
            metadatas[i]["ml_confidence"] = ml_confidence[i]
            
        return metadatas

    def get_labels(self, probs: np.ndarray, label_correspondence: dict):
        labels = []
        ml_confidence = []
        count_per_class = defaultdict(int)
        
        for prob in probs:
            max_prob = np.argmax(prob)
            label = label_correspondence[max_prob]
            labels.append(label)
            ml_confidence.append(prob[max_prob])
            count_per_class[label] += 1
            
        return labels, ml_confidence, dict(sorted(count_per_class.items()))

    def get_labels_x(self, probs: np.ndarray, label_correspondence: dict, metadatas:Any):
        labels = []
        #ml_confidence = []
        count_per_class = defaultdict(int)
        
        for i, prob in enumerate (probs):
            max_prob = np.argmax(prob)
            label = label_correspondence[max_prob]
            labels.append(label)
            confidence=prob[max_prob]
            #ml_confidence.append(confidence)
            metadatas[i]["ml_confidence"]= confidence
            t_now= time.time()
            metadatas[i]["timestamp_detector"] = t_now
            count_per_class[label] += 1
            
        return labels, metadatas, dict(sorted(count_per_class.items()))

    def start_detection(self) -> None:
        """
        Starts detection.
        """
        
        LOGGER.info("Starting detection...")
        #counter_global = 0
        n_labels=0
        classes_counter = defaultdict(int)

        num_esperas_seguidas=0
        num_mensajes_buff=0
        all_labels=[]
        all_metadatas=[]

        #debugging & profiling
        send_counter=0
        
        t_proc_mensaje=0.0
        n_proc_mensaje=0

        t_envio_msg=0.0
        n_envio_msg=0

        t_espera=0.0
        n_espera=0

        MetricsRecord = namedtuple('MetricsRecord', ['send_counter', 'tipo_envio',
                                                     't_proc_mensaje', 'n_proc_mensaje', 
                                                     't_envio_msg', 'n_envio_msg', 
                                                     't_espera', 'n_espera'
                                                    ])
        l_record_profiling=[]

        tot_labels=0 # num_snapshots procesados
        fich_profile_creado=False
        
        while True:
            
                
            messages = self.consumer.poll()
            
            if messages:
                #LOGGER.info (f"Mensajes leidos:{len(messages)}. {messages}")
                #messages = list(messages.values())[0] # Esta mal leida la estructura y se pierden mensajes
                #LOGGER.info (f"Mensajes leidos:{len(messages)}")
                #LOGGER.info (f"Mensajes list(messages.values())[0]: {messages}")

                t1=time.time()
                for partition_id, consumer_records in messages.items():
                    for record in consumer_records:
                        probs = record.value['data']
                        metadatas = record.value['metadata']          
                        label_correspondence = record.value["label_correspondence"]
                        version = record.headers[0][1].decode("utf-8")
                        '''
                        #counter_global += 1
                        data = message.value
                        #version = message.headers[0][1].decode("utf-8")
                        probs = data["data"]
                        metadatas = data["metadata"]
                        label_correspondence = data["label_correspondence"]
                        '''
                        
                        """LOGGER.info("Diferencia de tiempo entre PRIMERO ai-inference y ai-detector: %s", datetime.timestamp(datetime.now())-metadatas[0]['timestamp_inference'])
                        LOGGER.info("Diferencia de tiempo entre ULTIMO ai-inference y ai-detector: %s", datetime.timestamp(datetime.now())-metadatas[-1]['timestamp_inference'])"""

                        #t_now= time.time()
                        #for metadata in metadatas:
                        #    metadata["timestamp_detector"] = t_now # datetime.timestamp(datetime.now())
                        
                        #labels, ml_confidence, count_per_class = self.get_labels(probs, label_correspondence)
                        labels, metadatas, count_per_class = self.get_labels_x(probs, label_correspondence,metadatas)
    
                        
                        #LOGGER.info("Received %s", count_per_class)
                        
                        #for k in count_per_class.keys():
                        #    classes_counter[k] += count_per_class[k]
                        
                        #metadatas = self.add_ml_confidence_to_metadata(metadatas, ml_confidence)
    
                        #n_labels+=len(labels)
                        #self.__send_data(labels, metadatas, version)

                        #LOGGER.info (f"len labels:{len(labels)} len metadatas:{len(metadatas)")
                        all_labels+= labels
                        all_metadatas+= metadatas

                        tot_labels+=len(labels)
                        
                        #LOGGER.info (" ")
                        #LOGGER.info ("-------------")
                        #LOGGER.info (f"labels:{labels}")
                        #LOGGER.info (f"all_labels:{all_labels}")
                        #LOGGER.info ("-------------")
                        #LOGGER.info (f"metadatas:{metadatas}")
                        #LOGGER.info (f"all_metadatas:{all_metadatas}")
                        #LOGGER.info ("-------------")
                        #LOGGER.info (f"version:{version}")
                        
                t2=time.time()
                t_proc_mensaje+=t2-t1
                n_proc_mensaje+=1
                #LOGGER.info(f'Num total de labels:{n_labels}')
                        
                        #if counter_global % 1000 == 0:
                        #LOGGER.info(f'CONTADOR DE CLASES:{classes_counter}')
                
                '''
                LOGGER.info (f"len labels:{len(labels)}")
                self.__send_data(all_labels, all_metadatas, version)
                all_labels=[]
                all_metadatas=[]
                '''
                
                num_mensajes_buff+=1
                if num_mensajes_buff > 0:
                #if len(all_labels) >  MAX_LABELS_BUFF:
                    t1=time.time()
                    #LOGGER.info (f"Enviamos por MAX_LABELS_BUFF. labels:{len(all_labels). num_mensajes:{num_mensajes_buff}}")
                    #LOGGER.info (f"Enviamos por MAX_LABELS_BUFF. len labels:{len(all_labels)}  mensajes:{num_mensajes_buff}")
                    
                    self.__send_data(all_labels, all_metadatas, version)
                    
                    num_esperas_seguidas=0
                    num_mensajes_buff=0
                    all_labels=[]
                    all_metadatas=[]
                    t2=time.time()
                    
                    send_counter+=1
                    t_envio_msg+=t2-t1
                    n_envio_msg+=1
                    tipo_envio="B"

                    record_profiling = MetricsRecord(
                        send_counter=send_counter,
                        tipo_envio=tipo_envio,
                        t_proc_mensaje=t_proc_mensaje, n_proc_mensaje=n_proc_mensaje,
                        t_envio_msg=t_envio_msg, n_envio_msg=n_envio_msg,
                        t_espera=t_espera, n_espera=n_espera
                    )
                    #LOGGER.info (f"XXX record_profiling: {record_profiling}")
                    l_record_profiling.append (record_profiling)

                    t_proc_mensaje=0.0; n_proc_mensaje=0
                    t_envio_msg=0.0; n_envio_msg=0
                    t_espera=0.0; n_espera=0

                    #LOGGER.info (f"l_record_profiling: {l_record_profiling}")
                    if not fich_profile_creado and (tot_labels >= TOT_SNAPSHOTS):
                        fich_profile_creado=True
                        with open('ai-detector-metrics_records.pkl', 'wb') as f:
                            pickle.dump(l_record_profiling, f)            
                        LOGGER.info ("fichero grabado")
                        #LOGGER.info (f"l_record_profiling: {l_record_profiling}")
                    
            else:
                #LOGGER.info ("no hay mensajes")
                if (num_esperas_seguidas > MAX_ESPERAS_SEGUIDAS) and (len(all_labels)>0) :
                    #Forzamos envio para que no se quede pegado
                    LOGGER.info (f"Enviamos por MAX_ESPERAS_SEGUIDAS. labels:{len(all_labels)} mensajes:{num_mensajes_buff}")
                    t1=time.time()
                    #LOGGER.info (f"Enviamos por MAX_LABELS_BUFF. labels:{len(all_labels). num_mensajes:{num_mensajes_buff}}")
                    #LOGGER.info (f"Enviamos por MAX_LABELS_BUFF. len labels:{len(all_labels)}  mensajes:{num_mensajes_buff}")
                    
                    self.__send_data(all_labels, all_metadatas, version)
                    
                    num_esperas_seguidas=0
                    num_mensajes_buff=0
                    all_labels=[]
                    all_metadatas=[]
                    t2=time.time()

                    send_counter+=1
                    t_envio_msg+=t2-t1
                    n_envio_msg+=1
                    tipo_envio="E"

                    record_profiling = MetricsRecord(
                        send_counter=send_counter,
                        tipo_envio=tipo_envio,
                        t_proc_mensaje=t_proc_mensaje, n_proc_mensaje=n_proc_mensaje,
                        t_envio_msg=t_envio_msg, n_envio_msg=n_envio_msg,
                        t_espera=t_espera, n_espera=n_espera
                    )

                    l_record_profiling.append (record_profiling)

                    t_proc_mensaje=0.0; n_proc_mensaje=0
                    t_envio_msg=0.0; n_envio_msg=0
                    t_espera=0.0; n_espera=0

                    #LOGGER.info (f"l_record_profiling: {l_record_profiling}")
                    if not fich_profile_creado and (tot_labels >= TOT_SNAPSHOTS):
                        fich_profile_creado=True
                        with open('ai-detector-metrics_records.pkl', 'wb') as f:
                            pickle.dump(l_record_profiling, f)            
                        LOGGER.info ("fichero grabado")
                        #LOGGER.info (f"l_record_profiling: {l_record_profiling}")
                    
                else:
                    t1=time.time()
                    #LOGGER. info (f"Esperando:{T_ESPERA_CONSUMIDOR}")
                    time.sleep(T_ESPERA_CONSUMIDOR)
                    num_esperas_seguidas+=1
                    #LOGGER.info(f'CONTADOR DE CLASES:{classes_counter}')
                    t2=time.time()
                    t_espera+=t2-t1
                    n_espera+=1
                
            
            
def main(args):
    """
    Main function for setting up and initialize DDOS detection.
    Args:
        args (Any): Command-line arguments and options.
    """

    ai_detector = AIDetector(kafka_url=args.kafka_url, consumer_topic=args.consumer_topic, consumer_client_id=args.consumer_client_id, consumer_group_id=args.consumer_group_id, producer_topic=args.producer_topic, producer_client_id=args.producer_client_id)
    ai_detector.start_detection()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize and start AI Detector component.")
    parser.add_argument("--kafka_url", type=str, default='localhost:9094',
                        help="IP address and port of the Kafka cluster ('localhost:9094' by default).")
    parser.add_argument("--consumer_topic", type=str, default='inference_probs',
                        help="Kafka topic which the consumer will be consuming from.")
    parser.add_argument("--consumer_client_id", type=str, default='ai-detector-consumer',
                        help="ID of the consumer client by which it will be recognized within Kafka.")
    parser.add_argument("--consumer_group_id", type=str, default='ai-detector',
                        help="ID of the consumer group which the consumer belongs to.")
    parser.add_argument("--producer_topic", type=str, default='predicted_labels',
                        help="Kafka topic which the producer will be publishing to.")
    parser.add_argument("--producer_client_id", type=str, default='ai-detector-producer',
                        help="ID of the producer client by which it will be recognized within Kafka.")
    

    args = parser.parse_args()
    main(args)
