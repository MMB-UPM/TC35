from typing import Tuple, Any
import logging
from kafka import KafkaConsumer, KafkaProducer
from sys import stdout, exit
import time
from collections import namedtuple
import signal
import argparse
import pickle
import numpy as np
import csv

LABELS = {0: "Standard", 1: "HH Benign", 2: "HH Malign", 3: "Unclassified"}

# Setup LOGGER
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
logFormatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
LOGGER.addHandler(consoleHandler)

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
        
        # Prueba
        self.correct_predictions_file = 'correct_predictions.txt'
        self.csv_file_path = 'ceos2_eth4.pcap.csv'
        self.csv_file = open(self.csv_file_path, newline='\n')
        self.csvreader = csv.reader(self.csv_file, delimiter=' ')

    def handler(self, num, frame) -> None:
        LOGGER.info("Gracefully stopping...")
        self.consumer.close()
        self.producer.close()
        self.csv.close()
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

    def __send_data(self, label: str, ml_confidence: float, metadata: Any, headers: Any=None) -> None:
        """
        Publishes the predicted label to self.producer_topic.
        Args:
            label: the predicted class name
            ml_confidence: the model prediction confidence score
            metadata: connection metadata
            headers (list): list with the headers of the data.
        """
        metadata["ml_confidence"] = ml_confidence
        data = {"data": label, "metadata": metadata}
        try:
            self.producer.send(topic=self.producer_config.topic, value=data, headers=headers,
                               timestamp_ms=time.time_ns())
            self.producer.flush()
        except Exception as e:
            LOGGER.error("%s: Error sending inference probabilities to Kafka cluster: %s", __name__, e)

    def get_label(self, probs: np.ndarray):
        max_prob = np.argmax(probs)
        return LABELS[max_prob], probs[max_prob]

    #Prueba
    def compare_labels(self, predicted_label: str):
        try:
            row = next(self.csvreader)
            if row['Category'] == 'normal_traffic':
                label_groundtruth = 0
            elif row['Category'] == 'benign_heavy_hitter':
                label_groundtruth = 1  
            elif row['Category'] == 'malign_heavy_hitter':
                label_groundtruth = 2
            elif row['Category'] == 'Unknown':
                label_groundtruth = 3
            return label_groundtruth == predicted_label
        except StopIteration:
            LOGGER.warning("Reached end of CSV file.")
        except Exception as e:
            LOGGER.error(f"Error comparing labels: {e}")

    def start_detection(self) -> None:
        """
        Starts detection.
        """
        LOGGER.info("Starting detection...")
        correct_predictions = 0     #Prueba
        total_predictions = 0       #Prueba
        prediction_is_right = False #Prueba
        while True:
            try:
                messages = self.consumer.poll(10)
                if messages:
                    messages = list(messages.values())[0]
                    for message in messages:
                        LOGGER.info("Message received. t=%s", message.timestamp)
                        data = message.value
                        probs = data["data"]
                        metadata = data["metadata"]
                        label, ml_confidence = self.get_label(probs)
                        total_predictions +=1   #Prueba
                        prediction_is_right = self.compare_labels(label)   #Prueba
                        if prediction_is_right:     #Prueba
                            correct_predictions += 1    #Prueba
                        with open(self.correct_predictions_file, 'a') as predictions_file:  #Prueba
                            predictions_file.write(f'Labels match: {prediction_is_right}, Percentage of matching predictions: {(correct_predictions/total_predictions)*100:.4f}%\n') #Prueba
                        self.__send_data(label, ml_confidence, metadata)
            except Exception as e:
                LOGGER.error("%s: Error in consumer: %s", __name__, e)
            
            
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
