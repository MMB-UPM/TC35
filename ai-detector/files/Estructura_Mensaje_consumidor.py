#Num Mensajes:3
Mensajes:[
    
[
ConsumerRecord(topic='predicted_labels', partition=3, offset=15, timestamp=1723998250384, timestamp_type=0, key=None,
               value={'data': ['normal_traffic'], 
                      'metadata': [{'src_ip': '10.0.12.1', 'dst_ip': '142.250.201.68', 'src_port': 60383, 'dst_port': 443, 'protocol': 17, 'first': 1720708131621, 'timestamp': 1720708167.113, 
                                     'timestamp_nfstream': 1723998250.351708, 'timestamp_inference': 1723998250.355708, 'flow_bytes': 87276, 'flow_pkts': 141, 'monitored_device': 'ceos2', 'interface': 'eth3', 
                                     'timestamp_detector': 1723998250.38484, 'ml_confidence': 0.9}]},
               headers=[('version', b'v1')], checksum=None, serialized_key_size=-1, serialized_value_size=475, serialized_header_size=9), 
    
ConsumerRecord(topic='predicted_labels', partition=3, offset=16, timestamp=1723998250466, timestamp_type=0, key=None, 
               value={'data': ['normal_traffic'],
                      'metadata': [{'src_ip': '10.0.12.1', 'dst_ip': '162.247.243.29', 'src_port': 38102, 'dst_port': 443, 'protocol': 6, 'first': 1720708142375, 'timestamp': 1720708167.506, 
                                    'timestamp_nfstream': 1723998250.433506, 'timestamp_inference': 1723998250.43917, 'flow_bytes': 11686, 'flow_pkts': 16, 'monitored_device': 'ceos2', 'interface': 'eth3', 
                                    'timestamp_detector': 1723998250.466161, 'ml_confidence': 0.7}]}, 
                   headers=[('version', b'v1')], checksum=None, serialized_key_size=-1, serialized_value_size=473, serialized_header_size=9)
], 
    
[
ConsumerRecord(topic='predicted_labels', partition=0, offset=12, timestamp=1723998250417, timestamp_type=0, key=None,
               value={'data': ['normal_traffic'], 
                      'metadata': [{'src_ip': '10.0.12.1', 'dst_ip': '100.26.64.197', 'src_port': 46892, 'dst_port': 443, 'protocol': 6, 'first': 1720708141683, 'timestamp': 1720708167.251, 
                                    'timestamp_nfstream': 1723998250.353985, 'timestamp_inference': 1723998250.384415, 'flow_bytes': 31140, 'flow_pkts': 72, 'monitored_device': 'ceos2', 'interface': 'eth3', 
                                    'timestamp_detector': 1723998250.417728, 'ml_confidence': 0.5}]}, 
               headers=[('version', b'v1')], checksum=None, serialized_key_size=-1, serialized_value_size=472, serialized_header_size=9)
], 

[
ConsumerRecord(topic='predicted_labels', partition=1, offset=21, timestamp=1723998250526, timestamp_type=0, key=None, 
               value={'data': ['normal_traffic'], 
                      'metadata': [{'src_ip': '10.0.12.1', 'dst_ip': '151.101.2.217', 'src_port': 50868, 'dst_port': 443, 'protocol': 6, 'first': 1720708141581, 'timestamp': 1720708167.145, 
                                    'timestamp_nfstream': 1723998250.506325, 'timestamp_inference': 1723998250.509095, 'flow_bytes': 10026, 'flow_pkts': 40, 'monitored_device': 'ceos2', 'interface': 'eth3', 
                                    'timestamp_detector': 1723998250.526685, 'ml_confidence': 0.85}]}, 
               headers=[('version', b'v1')], checksum=None, serialized_key_size=-1, serialized_value_size=472, serialized_header_size=9)
]

]

