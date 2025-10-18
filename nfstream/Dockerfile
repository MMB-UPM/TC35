FROM python:3.11

RUN apt-get update -y && apt-get upgrade -y && apt-get install -y libpcap0.8-dev libuv1-dev

RUN pip install --no-cache-dir --upgrade pip

RUN pip install --upgrade pip setuptools wheel pip-tools

RUN mkdir /app
RUN mkdir /app/pcaps
RUN mkdir /app/output
RUN mkdir /app/snapshots

COPY requirements.in /app
RUN pip-compile --output-file=/app/requirements.txt /app/requirements.in
RUN pip install -r /app/requirements.txt
#RUN pip show pyshark || pip install pyshark
RUN pip show scapy || pip install scapy
COPY scripts/ /app/scripts/

WORKDIR /app/scripts

# Se cogen los valores del docker-compose ante redefinicio de las mismas variables
ENV TZ="Europe/Madrid"
ENV OUTPUT_PATH="/app/output"
ENV OUTPUT_FILENAME=""
#ENV PCAP_PATH="/app/pcaps"
#ENV PCAP_FILENAME="ceos2_eth3_rev4_training.pcap"
#ENV T_LIM=0.1
ENV DELIM_CSV="*"
#ENV KAFKA_URL="localhost:9094"
#ENV PRODUCER_TOPIC="inference_data"
#ENV PRODUCER_CLIENT_ID="nfstream-producer"
#ENV MODE="csv"
#ENV MONITORED_DEVICE="ceos2"
#ENV INTERFACE="eth3"
#ENV N_ROUND=2
ENV CUSTOM_IDLE_TIMEOUT=0.01
ENV K_SUBSAMPLING=1
#ENV PARTITIONS_INF_DATA=2
#ENV PARTITIONS_INF_PROBS=2
#ENV PARTITIONS_PRED_LABELS=2
#ENV CAPTURE_MODE = "pcap"


CMD python3 . --output_path=$OUTPUT_PATH --output_filename=$OUTPUT_FILENAME --pcap_path=$PCAP_PATH --pcap_filename=$PCAP_FILENAME --t_lim=$T_LIM --delim_csv=$DELIM_CSV --kafka_url=$KAFKA_URL --producer_topic=$PRODUCER_TOPIC --producer_client_id=$PRODUCER_CLIENT_ID --mode=$MODE --monitored_device=$MONITORED_DEVICE --interface=$INTERFACE --n_round=$N_ROUND --custom_idle_timeout=$CUSTOM_IDLE_TIMEOUT --subsampling=$K_SUBSAMPLING --partitions_inf_data=$PARTITIONS_INF_DATA --partitions_inf_probs=$PARTITIONS_INF_PROBS --partitions_pred_labels=$PARTITIONS_PRED_LABELS --capture_mode=$CAPTURE_MODE --volcar_snapshots_csv=$Volcar_snapshots_csv

#CMD python3 . --output_path=$OUTPUT_PATH --output_filename=$OUTPUT_FILENAME --pcap_path=$PCAP_PATH --pcap_filename=$PCAP_FILENAME --t_lim=$T_LIM --delim_csv=$DELIM_CSV --kafka_url=$KAFKA_URL --producer_topic=$PRODUCER_TOPIC --producer_client_id=$PRODUCER_CLIENT_ID --mode=$MODE --monitored_device=$MONITORED_DEVICE --interface=$INTERFACE --n_round=$N_ROUND --custom_idle_timeout=$CUSTOM_IDLE_TIMEOUT --subsampling=$K_SUBSAMPLING --partitions_inf_data=$PARTITIONS_INF_DATA --partitions_inf_probs=$PARTITIONS_INF_PROBS --partitions_pred_labels=$PARTITIONS_PRED_LABELS 