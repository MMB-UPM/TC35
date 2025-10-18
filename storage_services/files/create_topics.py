from kafka import KafkaAdminClient
from kafka.admin import NewTopic


def main(args):
    """
    Main function for setting up and initialize data inference.
    Args:
        args (Any): Command-line arguments and options.
    """
    print(args)
    
    admin = KafkaAdminClient(bootstrap_servers=args.kafka_url)
    
    #admin.delete_topics(admin.list_topics())
    
    admin.create_topics([NewTopic("inference_data", num_partitions=args.num_partitions, replication_factor=1)])
    admin.create_topics([NewTopic("inference_probs", num_partitions=args.num_partitions, replication_factor=1)])
    admin.create_topics([NewTopic("predicted_labels", num_partitions=args.num_partitions, replication_factor=1)])
    #CHEQUEAR QUE SE HAN CREADO 2 PARTICIONES POR TOPIC 
    
    admin.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize and start NFStreamer.")
    parser.add_argument("--num_partitions", type=int, default=2, help="Number of partitions")
    parser.add_argument("--kafka_url", type=str, default="localhost:9094", help="IP address and port of the Kafka cluster")    
    
    args = parser.parse_args()
    main(args)
