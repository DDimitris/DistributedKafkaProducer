agent_producer_configs = {
    'bootstrap_servers': '192.168.1.102:9092',
    'compression_type': None,
    'acks': 1,
    'linger_ms': 0,
    'batch_size': 0,
}

master_producer_configs = {
    'bootstrap_servers': '192.168.1.102:9092',
    'compression_type': None,
    'acks': 1,
    'linger_ms': 0,
    'batch_size': 0,
}

consumer_configs = {
    'bootstrap_servers': '192.168.1.102:9092',
    'enable_auto_commit': False
}

internal_sync_topic = "internal-sync"
