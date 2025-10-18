from sys import stdout
from nfstream_csv import generate
from nfstream_kafka import start
import argparse
from kafka import KafkaAdminClient
from kafka.admin import NewTopic


def main(args):
    """
    Main function for setting up and initialize data inference.
    Args:
        args (Any): Command-line arguments and options.
    """
    print(args)
    
    """admin = KafkaAdminClient(bootstrap_servers=args.kafka_url)
    
    #admin.delete_topics(admin.list_topics())
    
    admin.create_topics([NewTopic("inference_data", num_partitions=args.partitions_inf_data, replication_factor=1)])
    admin.create_topics([NewTopic("inference_probs", num_partitions=args.partitions_inf_probs, replication_factor=1)])
    admin.create_topics([NewTopic("predicted_labels", num_partitions=args.partitions_pred_labels, replication_factor=1)])
    admin.close()"""
    
    if args.mode == "csv":
        # OBSOLETO. posiblemente generate no esté actualizdo. Mejor abortamos
        print ("Modo csv: posiblemente funcion 'generate' no esté actualizada. Mejor abortamos")
        return
        #generate(output_path=args.output_path, output_filename=args.output_filename, pcap_path=args.pcap_path, pcap_filename=args.pcap_filename, t_lim=args.t_lim,
        #         delim_csv=args.delim_csv, monitored_device=args.monitored_device, interface=args.interface, n_round=args.n_round, custom_idle_timeout=args.custom_idle_timeout)
                 
    else:
        start(pcap_path=args.pcap_path, pcap_filename=args.pcap_filename, t_lim=args.t_lim, kafka_url=args.kafka_url, producer_topic=args.producer_topic,
              producer_client_id=args.producer_client_id, monitored_device=args.monitored_device, interface=args.interface, n_round=args.n_round, k_subsampling=args.subsampling,
              capture_mode=args.capture_mode,volcar_snapshots_csv=args.volcar_snapshots_csv,delim_csv=args.delim_csv)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize and start NFStreamer.")
    parser.add_argument("--output_path", type=str, default="/app/output", help="Path where to store output files")
    parser.add_argument("--output_filename", type=str, default="", help="Output file name")
    parser.add_argument("--pcap_path", type=str, default="/app/pcaps", help="Path where PCAPs are stored")
    parser.add_argument("--pcap_filename", type=str, default="ceos2_eth3_rev4_training.pcap", help="PCAP filename")
    parser.add_argument("--t_lim", type=float, default=1.0, help="Snapshots time interval")
    parser.add_argument("--delim_csv", type=str, default="*", help="Output CSV column delimiter")
    parser.add_argument("--kafka_url", type=str, default="localhost:9094", help="IP address and port of the Kafka cluster")
    parser.add_argument("--producer_topic", type=str, default="inference_data", help="Kafka topic which the producer will be publishing to.")
    parser.add_argument("--producer_client_id", type=str, default='nfstream-producer', help="ID of the producer client by which it will be recognized within Kafka.")
    parser.add_argument("--mode", type=str, choices=["csv", "kafka"])
    parser.add_argument("--monitored_device", type=str, default="ceos2", help="Monitored device")
    parser.add_argument("--interface", type=str, default="eht3", help="Interface")
    parser.add_argument("--n_round", type=int, choices=[1, 2, 3, 4])
    parser.add_argument("--custom_idle_timeout", type=float, default=0.01)
    parser.add_argument("--subsampling", type=float, default=0.5)
    parser.add_argument("--partitions_inf_data", type=int, default=2)
    parser.add_argument("--partitions_inf_probs", type=int, default=2)
    parser.add_argument("--partitions_pred_labels", type=int, default=2)
    parser.add_argument("--capture_mode", type=str, default="pcap")
    parser.add_argument("--volcar_snapshots_csv", type=str, default="False")
    
    
    args = parser.parse_args()
    main(args)
